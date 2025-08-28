//! Multi-Version Concurrency Control (MVCC) storage layer
//!
//! Provides transaction isolation through versioning while working
//! with PCC for conflict prevention.

use crate::error::{Error, Result};
use crate::hlc::HlcTimestamp;
use crate::sql::types::schema::Table;
use crate::sql::types::value::{StorageRow as Row, Value};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, RwLock};

/// A versioned value in storage
#[derive(Debug, Clone)]
pub struct VersionedValue {
    /// The actual row data
    pub row: Arc<Row>,
    /// Transaction that created this version
    pub created_by: HlcTimestamp,
    /// HLC timestamp when created
    pub created_at: HlcTimestamp,
    /// Transaction that deleted this version (if any)
    pub deleted_by: Option<HlcTimestamp>,
    /// Whether this version is committed
    pub committed: bool,
}

/// Versioned table with MVCC support
#[derive(Debug)]
pub struct VersionedTable {
    pub name: String,
    pub schema: Table,
    /// All versions of all rows (row_id -> versions)
    /// Versions are ordered by creation time (newest last)
    pub versions: BTreeMap<u64, Vec<VersionedValue>>,
    /// Next row ID for inserts
    pub next_id: u64,
    /// Track committed transactions
    pub committed_transactions: Arc<RwLock<HashSet<HlcTimestamp>>>,
    /// Transaction start times for visibility checks
    pub transaction_start_times: Arc<RwLock<HashMap<HlcTimestamp, HlcTimestamp>>>,
}

impl VersionedTable {
    pub fn new(name: String, schema: Table) -> Self {
        Self {
            name,
            schema,
            versions: BTreeMap::new(),
            next_id: 1,
            committed_transactions: Arc::new(RwLock::new(HashSet::new())),
            transaction_start_times: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a transaction's start time
    pub fn register_transaction(&self, txn_id: HlcTimestamp, start_time: HlcTimestamp) {
        self.transaction_start_times
            .write()
            .unwrap()
            .insert(txn_id, start_time);
    }

    /// Mark a transaction as committed
    pub fn mark_committed(&self, txn_id: HlcTimestamp) {
        // Mark all versions created by this transaction as committed
        for versions in self.versions.values() {
            for version in versions {
                if version.created_by == txn_id {
                    // Note: In real implementation, we'd make committed mutable
                    // For now, we track in committed_transactions set
                }
            }
        }

        self.committed_transactions.write().unwrap().insert(txn_id);
    }

    /// Remove all versions created by an aborted transaction
    pub fn remove_transaction_versions(&mut self, txn_id: HlcTimestamp) {
        for versions in self.versions.values_mut() {
            // Remove versions created by this transaction
            versions.retain(|v| v.created_by != txn_id);

            // Clear deletion marks by this transaction
            for version in versions.iter_mut() {
                if version.deleted_by == Some(txn_id) {
                    version.deleted_by = None;
                }
            }
        }

        // Clean up empty version lists
        self.versions.retain(|_, versions| !versions.is_empty());
    }

    /// Insert a new row (creates a new version)
    pub fn insert(
        &mut self,
        txn_id: HlcTimestamp,
        txn_timestamp: HlcTimestamp,
        values: Vec<Value>,
    ) -> Result<u64> {
        // Validate against schema
        self.schema.validate_row(&values)?;

        let row_id = self.next_id;
        self.next_id += 1;

        let version = VersionedValue {
            row: Arc::new(Row::new(row_id, values)),
            created_by: txn_id,
            created_at: txn_timestamp,
            deleted_by: None,
            committed: false, // Will be marked true on commit
        };

        self.versions.entry(row_id).or_default().push(version);
        Ok(row_id)
    }

    /// Update a row (marks old version as deleted, creates new version)
    pub fn update(
        &mut self,
        txn_id: HlcTimestamp,
        txn_timestamp: HlcTimestamp,
        row_id: u64,
        values: Vec<Value>,
    ) -> Result<()> {
        // Validate against schema
        self.schema.validate_row(&values)?;

        // Check if we need to update in place or create new version
        let needs_new_version = {
            let visible_version = self
                .get_visible_version(txn_id, txn_timestamp, row_id)
                .ok_or_else(|| Error::InvalidValue(format!("Row {} not found", row_id)))?;

            // If this transaction created the visible version, we can update in place
            !(visible_version.created_by == txn_id && !visible_version.committed)
        };

        let versions = self.versions.get_mut(&row_id).unwrap();

        if !needs_new_version {
            // Find and update the version
            for version in versions.iter_mut() {
                if version.created_by == txn_id && version.deleted_by.is_none() {
                    version.row = Arc::new(Row::new(row_id, values));
                    return Ok(());
                }
            }
        }

        // Otherwise, mark old version as deleted and create new version
        // We need to find which version to mark as deleted without holding mutable ref
        // Just mark the last undeleted version created before or by this transaction
        for version in versions.iter_mut().rev() {
            // Simple visibility: created by us or committed before us
            let is_visible = if version.created_by == txn_id {
                version.deleted_by.is_none()
            } else {
                let is_committed = self
                    .committed_transactions
                    .read()
                    .unwrap()
                    .contains(&version.created_by);
                is_committed && version.created_at <= txn_timestamp && version.deleted_by.is_none()
            };

            if is_visible {
                version.deleted_by = Some(txn_id);
                break;
            }
        }

        // Create new version
        let new_version = VersionedValue {
            row: Arc::new(Row::new(row_id, values)),
            created_by: txn_id,
            created_at: txn_timestamp,
            deleted_by: None,
            committed: false,
        };

        self.versions.get_mut(&row_id).unwrap().push(new_version);
        Ok(())
    }

    /// Delete a row (marks version as deleted)
    pub fn delete(
        &mut self,
        txn_id: HlcTimestamp,
        _txn_timestamp: HlcTimestamp,
        row_id: u64,
    ) -> Result<()> {
        // First check if the row exists and is visible
        let is_visible = self
            .versions
            .get(&row_id)
            .and_then(|versions| {
                versions
                    .iter()
                    .rev()
                    .find(|v| self.is_version_visible_to(v, txn_id, _txn_timestamp))
            })
            .is_some();

        if !is_visible {
            return Err(Error::InvalidValue(format!(
                "Row {} not found or not visible",
                row_id
            )));
        }

        // Now mark it as deleted
        let versions = self.versions.get_mut(&row_id).unwrap();

        // Find and mark the visible version as deleted
        for version in versions.iter_mut().rev() {
            // Simple visibility check inline
            let is_visible = if version.created_by == txn_id {
                version.deleted_by.is_none()
            } else {
                let is_committed = self
                    .committed_transactions
                    .read()
                    .unwrap()
                    .contains(&version.created_by);
                is_committed && version.created_at <= _txn_timestamp && version.deleted_by.is_none()
            };

            if is_visible {
                version.deleted_by = Some(txn_id);
                return Ok(());
            }
        }

        Err(Error::InvalidValue(format!("Row {} not visible", row_id)))
    }

    /// Read a specific row
    pub fn read(
        &self,
        txn_id: HlcTimestamp,
        txn_timestamp: HlcTimestamp,
        row_id: u64,
    ) -> Option<Arc<Row>> {
        self.get_visible_version(txn_id, txn_timestamp, row_id)
            .map(|v| v.row.clone())
    }

    /// Scan all visible rows for a transaction
    pub fn scan(&self, txn_id: HlcTimestamp, txn_timestamp: HlcTimestamp) -> Vec<(u64, Arc<Row>)> {
        let mut result = Vec::new();

        for (&row_id, versions) in &self.versions {
            if let Some(version) = self.find_visible_version(versions, txn_id, txn_timestamp) {
                result.push((row_id, version.row.clone()));
            }
        }

        result
    }

    /// Get the visible version of a row for a transaction
    fn get_visible_version(
        &self,
        txn_id: HlcTimestamp,
        txn_timestamp: HlcTimestamp,
        row_id: u64,
    ) -> Option<&VersionedValue> {
        let versions = self.versions.get(&row_id)?;
        self.find_visible_version(versions, txn_id, txn_timestamp)
    }

    /// Find the visible version in a list of versions
    fn find_visible_version<'a>(
        &self,
        versions: &'a [VersionedValue],
        txn_id: HlcTimestamp,
        txn_timestamp: HlcTimestamp,
    ) -> Option<&'a VersionedValue> {
        // Search backwards (newest first) for visible version
        versions
            .iter()
            .rev()
            .find(|v| self.is_version_visible_to(v, txn_id, txn_timestamp))
    }

    /// Check if a version is visible to a transaction
    fn is_version_visible_to(
        &self,
        version: &VersionedValue,
        txn_id: HlcTimestamp,
        txn_timestamp: HlcTimestamp,
    ) -> bool {
        // Transaction sees its own writes
        if version.created_by == txn_id {
            return version.deleted_by.is_none() || version.deleted_by != Some(txn_id);
        }

        // Must be committed
        let is_committed = self
            .committed_transactions
            .read()
            .unwrap()
            .contains(&version.created_by);

        if !is_committed {
            return false;
        }

        // Must have been created before this transaction started
        if version.created_at > txn_timestamp {
            return false;
        }

        // Check if deleted
        if let Some(deleter_id) = version.deleted_by {
            // Deleted version is visible if:
            // 1. Deleter is not committed, OR
            // 2. Deleter started after this transaction
            let deleter_committed = self
                .committed_transactions
                .read()
                .unwrap()
                .contains(&deleter_id);

            if !deleter_committed {
                return true; // See undeleted version
            }

            let deleter_start = self
                .transaction_start_times
                .read()
                .unwrap()
                .get(&deleter_id)
                .copied();

            if let Some(deleter_time) = deleter_start {
                return deleter_time > txn_timestamp; // Deletion happened after we started
            }
        }

        true
    }

    /// Commit all changes made by a transaction
    pub fn commit_transaction(&mut self, txn_id: HlcTimestamp) -> Result<()> {
        // Mark transaction as committed
        self.committed_transactions.write().unwrap().insert(txn_id);

        // Update all versions to be committed
        for versions in self.versions.values_mut() {
            for version in versions.iter_mut() {
                if version.created_by == txn_id {
                    version.committed = true;
                }
            }
        }

        Ok(())
    }

    /// Abort a transaction, removing all its changes
    pub fn abort_transaction(&mut self, txn_id: HlcTimestamp) -> Result<()> {
        self.remove_transaction_versions(txn_id);

        // Clean up tracking
        self.transaction_start_times
            .write()
            .unwrap()
            .remove(&txn_id);

        Ok(())
    }

    /// Garbage collection: remove old committed versions that no active transaction can see
    pub fn garbage_collect(&mut self, active_transactions: &[HlcTimestamp]) -> usize {
        if active_transactions.is_empty() {
            return 0; // No GC when no active transactions
        }

        let mut removed_count = 0;

        // Find the oldest active transaction
        let oldest_txn = active_transactions
            .iter()
            .filter_map(|txn_id| {
                self.transaction_start_times
                    .read()
                    .unwrap()
                    .get(txn_id)
                    .copied()
            })
            .min();

        if let Some(oldest_timestamp) = oldest_txn {
            for versions in self.versions.values_mut() {
                // Keep only versions that might be visible to active transactions
                let before_count = versions.len();

                // Keep the newest committed version before oldest_timestamp
                // and all versions after oldest_timestamp
                let mut keep_indices = HashSet::new();
                let mut newest_committed_before = None;

                for (i, version) in versions.iter().enumerate() {
                    if version.created_at >= oldest_timestamp {
                        keep_indices.insert(i);
                    } else if version.committed {
                        newest_committed_before = Some(i);
                    }
                }

                if let Some(idx) = newest_committed_before {
                    keep_indices.insert(idx);
                }

                // Keep only the marked versions
                let mut new_versions = Vec::new();
                for (i, version) in versions.iter().enumerate() {
                    if keep_indices.contains(&i) {
                        new_versions.push(version.clone());
                    }
                }

                removed_count += before_count - new_versions.len();
                *versions = new_versions;
            }
        }

        removed_count
    }
}

/// MVCC-enabled storage engine
pub struct MvccStorage {
    /// Tables with versioned data
    pub tables: RwLock<HashMap<String, VersionedTable>>,
    /// Track all committed transactions globally
    pub global_committed: Arc<RwLock<HashSet<HlcTimestamp>>>,
    /// Track active transactions for GC
    pub active_transactions: Arc<RwLock<HashSet<HlcTimestamp>>>,
}

impl MvccStorage {
    pub fn new() -> Self {
        Self {
            tables: RwLock::new(HashMap::new()),
            global_committed: Arc::new(RwLock::new(HashSet::new())),
            active_transactions: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    /// Create a new table
    pub fn create_table(&self, name: String, schema: Table) -> Result<()> {
        let mut tables = self.tables.write().unwrap();

        if tables.contains_key(&name) {
            return Err(Error::InvalidValue(format!(
                "Table {} already exists",
                name
            )));
        }

        tables.insert(name.clone(), VersionedTable::new(name, schema));
        Ok(())
    }

    /// Drop a table
    pub fn drop_table(&self, name: &str) -> Result<()> {
        let mut tables = self.tables.write().unwrap();

        if tables.remove(name).is_none() {
            return Err(Error::TableNotFound(name.to_string()));
        }

        Ok(())
    }

    /// Register a new transaction
    pub fn register_transaction(&self, txn_id: HlcTimestamp, start_time: HlcTimestamp) {
        self.active_transactions.write().unwrap().insert(txn_id);

        // Register with all tables
        let tables = self.tables.read().unwrap();
        for table in tables.values() {
            table.register_transaction(txn_id, start_time);
        }
    }

    /// Commit a transaction across all tables
    pub fn commit_transaction(&self, txn_id: HlcTimestamp) -> Result<()> {
        // Mark as committed globally
        self.global_committed.write().unwrap().insert(txn_id);

        // Commit in all tables
        let mut tables = self.tables.write().unwrap();
        for table in tables.values_mut() {
            table.commit_transaction(txn_id)?;
        }

        // Remove from active set
        self.active_transactions.write().unwrap().remove(&txn_id);

        Ok(())
    }

    /// Abort a transaction across all tables
    pub fn abort_transaction(&self, txn_id: HlcTimestamp) -> Result<()> {
        // Abort in all tables
        let mut tables = self.tables.write().unwrap();
        for table in tables.values_mut() {
            table.abort_transaction(txn_id)?;
        }

        // Remove from active set
        self.active_transactions.write().unwrap().remove(&txn_id);

        Ok(())
    }

    /// Run garbage collection across all tables
    pub fn garbage_collect(&self) -> usize {
        let active: Vec<HlcTimestamp> = self
            .active_transactions
            .read()
            .unwrap()
            .iter()
            .copied()
            .collect();

        let mut total_removed = 0;
        let mut tables = self.tables.write().unwrap();
        for table in tables.values_mut() {
            total_removed += table.garbage_collect(&active);
        }

        total_removed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hlc::{HlcTimestamp, NodeId};
    use crate::sql::types::schema::Column;
    use crate::sql::types::value::DataType;

    fn create_test_schema() -> Table {
        Table::new(
            "test".to_string(),
            vec![
                Column::new("id".to_string(), DataType::Integer).primary_key(),
                Column::new("name".to_string(), DataType::String),
            ],
        )
        .unwrap()
    }

    fn create_txn_id(timestamp: u64) -> HlcTimestamp {
        HlcTimestamp::new(timestamp, 0, NodeId::new(1))
    }

    #[test]
    fn test_basic_insert_commit() {
        let mut table = VersionedTable::new("test".to_string(), create_test_schema());
        let txn1 = create_txn_id(100);
        let timestamp1 = HlcTimestamp::new(100, 0, NodeId::new(1));

        // Register and insert
        table.register_transaction(txn1, timestamp1);
        let row_id = table
            .insert(
                txn1,
                timestamp1,
                vec![Value::Integer(1), Value::String("Alice".to_string())],
            )
            .unwrap();

        // Before commit, only txn1 can see the row
        assert!(table.read(txn1, timestamp1, row_id).is_some());

        let txn2 = create_txn_id(200);
        let timestamp2 = HlcTimestamp::new(200, 0, NodeId::new(1));
        table.register_transaction(txn2, timestamp2);
        assert!(table.read(txn2, timestamp2, row_id).is_none());

        // After commit, txn2 can see it
        table.commit_transaction(txn1).unwrap();
        assert!(table.read(txn2, timestamp2, row_id).is_some());
    }

    #[test]
    fn test_update_creates_new_version() {
        let mut table = VersionedTable::new("test".to_string(), create_test_schema());

        // Transaction 1: Insert and commit
        let txn1 = create_txn_id(100);
        let timestamp1 = HlcTimestamp::new(100, 0, NodeId::new(1));
        table.register_transaction(txn1, timestamp1);

        let row_id = table
            .insert(
                txn1,
                timestamp1,
                vec![Value::Integer(1), Value::String("Alice".to_string())],
            )
            .unwrap();
        table.commit_transaction(txn1).unwrap();

        // Transaction 2: Update but don't commit yet
        let txn2 = create_txn_id(200);
        let timestamp2 = HlcTimestamp::new(200, 0, NodeId::new(1));
        table.register_transaction(txn2, timestamp2);

        table
            .update(
                txn2,
                timestamp2,
                row_id,
                vec![Value::Integer(1), Value::String("Bob".to_string())],
            )
            .unwrap();

        // Transaction 3: Should still see old version
        let txn3 = create_txn_id(300);
        let timestamp3 = HlcTimestamp::new(300, 0, NodeId::new(1));
        table.register_transaction(txn3, timestamp3);

        let row = table.read(txn3, timestamp3, row_id).unwrap();
        assert_eq!(row.values[1], Value::String("Alice".to_string()));

        // But txn2 sees its own update
        let row = table.read(txn2, timestamp2, row_id).unwrap();
        assert_eq!(row.values[1], Value::String("Bob".to_string()));
    }

    #[test]
    fn test_abort_rollback() {
        let mut table = VersionedTable::new("test".to_string(), create_test_schema());

        // Insert and commit a row
        let txn1 = create_txn_id(100);
        let timestamp1 = HlcTimestamp::new(100, 0, NodeId::new(1));
        table.register_transaction(txn1, timestamp1);

        let row_id = table
            .insert(
                txn1,
                timestamp1,
                vec![Value::Integer(1), Value::String("Alice".to_string())],
            )
            .unwrap();
        table.commit_transaction(txn1).unwrap();

        // Start a transaction, update, then abort
        let txn2 = create_txn_id(200);
        let timestamp2 = HlcTimestamp::new(200, 0, NodeId::new(1));
        table.register_transaction(txn2, timestamp2);

        table
            .update(
                txn2,
                timestamp2,
                row_id,
                vec![Value::Integer(1), Value::String("Bob".to_string())],
            )
            .unwrap();

        // Abort the transaction
        table.abort_transaction(txn2).unwrap();

        // New transaction should still see original value
        let txn3 = create_txn_id(300);
        let timestamp3 = HlcTimestamp::new(300, 0, NodeId::new(1));
        table.register_transaction(txn3, timestamp3);

        let row = table.read(txn3, timestamp3, row_id).unwrap();
        assert_eq!(row.values[1], Value::String("Alice".to_string()));
    }
}
