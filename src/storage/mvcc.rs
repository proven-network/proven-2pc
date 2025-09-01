//! Multi-Version Concurrency Control (MVCC) storage layer
//!
//! Provides transaction isolation through versioning while working
//! with PCC for conflict prevention.

use crate::error::{Error, Result};
use crate::hlc::HlcTimestamp;
use crate::types::schema::Table;
use crate::types::value::{StorageRow as Row, Value};
use rust_decimal::prelude::ToPrimitive;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

/// Key for identifying an index
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct IndexKey {
    /// Table name
    pub table: String,
    /// Column name(s) - for composite indexes this contains multiple columns
    pub columns: Vec<String>,
}

/// Composite index key that can hold multiple values
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum CompositeKey {
    /// Single value (for single-column indexes)
    Single(Value),
    /// Multiple values (for composite indexes)
    Composite(Vec<Value>),
}

impl CompositeKey {
    /// Create a composite key from values
    pub fn from_values(values: Vec<Value>) -> Self {
        if values.len() == 1 {
            CompositeKey::Single(values.into_iter().next().unwrap())
        } else {
            CompositeKey::Composite(values)
        }
    }

    /// Check if this key matches a prefix of values (for partial lookups)
    pub fn matches_prefix(&self, prefix: &[Value]) -> bool {
        match self {
            CompositeKey::Single(v) => prefix.len() == 1 && v == &prefix[0],
            CompositeKey::Composite(values) => {
                prefix.len() <= values.len()
                    && prefix.iter().zip(values.iter()).all(|(p, v)| p == v)
            }
        }
    }
}

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
}

/// Versioned index entry - tracks which transaction added/removed a row_id
#[derive(Debug, Clone)]
pub struct IndexEntry {
    pub row_id: u64,
    pub added_by: HlcTimestamp,
    pub removed_by: Option<HlcTimestamp>,
    /// Values for included columns (for covering indexes)
    pub included_values: Option<Vec<Value>>,
}

/// Versioned table with MVCC support
#[derive(Debug)]
pub struct VersionedTable {
    pub name: String,
    pub schema: Table,
    /// All versions of all rows (row_id -> versions)
    /// Versions are ordered by creation time (newest last)
    pub versions: BTreeMap<u64, Vec<VersionedValue>>,
    /// Indexes with MVCC: index_name -> (composite_key -> versioned entries)
    /// Single-column indexes use CompositeKey::Single, multi-column use CompositeKey::Composite
    pub indexes: HashMap<String, BTreeMap<CompositeKey, Vec<IndexEntry>>>,
    /// Map from index name to column names for that index
    pub index_columns: HashMap<String, Vec<String>>,
    /// Map from index name to included columns (for covering indexes)
    pub index_included_columns: HashMap<String, Vec<String>>,
    /// Next row ID for inserts
    pub next_id: u64,
    /// Track committed transactions
    pub committed_transactions: HashSet<HlcTimestamp>,
    /// Transaction start times for visibility checks
    pub transaction_start_times: HashMap<HlcTimestamp, HlcTimestamp>,
}

impl VersionedTable {
    pub fn new(name: String, schema: Table) -> Self {
        // Initialize indexes for columns marked as indexed
        let mut indexes = HashMap::new();
        let mut index_columns = HashMap::new();
        let index_included_columns = HashMap::new();

        for column in &schema.columns {
            if column.index || column.primary_key || column.unique {
                // Create single-column index
                let index_name = column.name.clone();
                indexes.insert(index_name.clone(), BTreeMap::new());
                index_columns.insert(index_name, vec![column.name.clone()]);
            }
        }

        Self {
            name,
            schema,
            versions: BTreeMap::new(),
            indexes,
            index_columns,
            index_included_columns,
            next_id: 1,
            committed_transactions: HashSet::new(),
            transaction_start_times: HashMap::new(),
        }
    }

    /// Register a transaction's start time
    pub fn register_transaction(&mut self, txn_id: HlcTimestamp, start_time: HlcTimestamp) {
        self.transaction_start_times.insert(txn_id, start_time);
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

        // Check unique constraints before inserting
        for (index_name, column_names) in &self.index_columns.clone() {
            // Check if this is a unique index (all columns must be unique/pk for composite to be unique)
            let is_unique = column_names.iter().all(|col_name| {
                self.schema
                    .columns
                    .iter()
                    .any(|c| c.name == *col_name && (c.unique || c.primary_key))
            });

            if is_unique {
                if let Some(index) = self.indexes.get(index_name) {
                    // Build composite key from values
                    let mut key_values = Vec::new();
                    let mut has_null = false;
                    for col_name in column_names {
                        if let Some(col_idx) =
                            self.schema.columns.iter().position(|c| c.name == *col_name)
                        {
                            let value = &values[col_idx];
                            if value.is_null() {
                                has_null = true;
                                break;
                            }
                            key_values.push(value.clone());
                        }
                    }

                    if !has_null {
                        let key = CompositeKey::from_values(key_values);
                        if let Some(entries) = index.get(&key) {
                            // Check if any entry is visible to this transaction
                            for entry in entries {
                                // Entry is visible if added by committed txn and not removed
                                let is_visible = if entry.added_by == txn_id {
                                    entry.removed_by.is_none() || entry.removed_by != Some(txn_id)
                                } else if self.committed_transactions.contains(&entry.added_by) {
                                    entry.removed_by.is_none()
                                        || (entry.removed_by != Some(txn_id)
                                            && !self
                                                .committed_transactions
                                                .contains(&entry.removed_by.unwrap_or(txn_id)))
                                } else {
                                    false
                                };

                                if is_visible {
                                    // Verify the row still exists and is visible
                                    if let Some(versions) = self.versions.get(&entry.row_id) {
                                        if self
                                            .find_visible_version(versions, txn_id, txn_timestamp)
                                            .is_some()
                                        {
                                            return Err(Error::InvalidValue(format!(
                                                "Unique constraint violation on index '{}'",
                                                index_name
                                            )));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        let row_id = self.next_id;
        self.next_id += 1;

        // Update indexes with versioned entries
        for (index_name, column_names) in &self.index_columns.clone() {
            if let Some(index) = self.indexes.get_mut(index_name) {
                // Build composite key from values
                let mut key_values = Vec::new();
                let mut has_null = false;
                for col_name in column_names {
                    if let Some(col_idx) =
                        self.schema.columns.iter().position(|c| c.name == *col_name)
                    {
                        let value = &values[col_idx];
                        if value.is_null() {
                            has_null = true;
                            break;
                        }
                        key_values.push(value.clone());
                    }
                }

                if !has_null {
                    let key = CompositeKey::from_values(key_values);
                    // Collect included column values if any
                    let included_values =
                        if let Some(included_cols) = self.index_included_columns.get(index_name) {
                            let mut vals = Vec::new();
                            for col_name in included_cols {
                                if let Some(col_idx) =
                                    self.schema.columns.iter().position(|c| c.name == *col_name)
                                {
                                    vals.push(values[col_idx].clone());
                                }
                            }
                            Some(vals)
                        } else {
                            None
                        };

                    let entry = IndexEntry {
                        row_id,
                        added_by: txn_id,
                        removed_by: None,
                        included_values,
                    };
                    index.entry(key).or_insert_with(Vec::new).push(entry);
                }
            }
        }

        let version = VersionedValue {
            row: Arc::new(Row::new(row_id, values)),
            created_by: txn_id,
            created_at: txn_timestamp,
            deleted_by: None,
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

        // Get old values for index updates
        let old_values = self
            .get_visible_version(txn_id, txn_timestamp, row_id)
            .ok_or_else(|| Error::InvalidValue(format!("Row {} not found", row_id)))?
            .row
            .values
            .clone();

        // Check unique constraints for new values
        for (index_name, column_names) in &self.index_columns.clone() {
            // Check if this is a unique index
            let is_unique = column_names.iter().all(|col_name| {
                self.schema
                    .columns
                    .iter()
                    .any(|c| c.name == *col_name && (c.unique || c.primary_key))
            });

            if is_unique {
                if let Some(index) = self.indexes.get(index_name) {
                    // Build composite keys from old and new values
                    let mut old_key_values = Vec::new();
                    let mut new_key_values = Vec::new();
                    let mut new_has_null = false;
                    let mut values_changed = false;

                    for col_name in column_names {
                        if let Some(col_idx) =
                            self.schema.columns.iter().position(|c| c.name == *col_name)
                        {
                            let old_value = &old_values[col_idx];
                            let new_value = &values[col_idx];

                            if !old_value.is_null() {
                                old_key_values.push(old_value.clone());
                            }

                            if new_value.is_null() {
                                new_has_null = true;
                            } else {
                                new_key_values.push(new_value.clone());
                            }

                            if old_value != new_value {
                                values_changed = true;
                            }
                        }
                    }

                    // Only check if value is changing and new value is not null
                    if values_changed && !new_has_null {
                        let new_key = CompositeKey::from_values(new_key_values);
                        if let Some(entries) = index.get(&new_key) {
                            for entry in entries {
                                // Skip self
                                if entry.row_id == row_id {
                                    continue;
                                }

                                // Check if entry is visible
                                let is_visible = if entry.added_by == txn_id {
                                    entry.removed_by.is_none() || entry.removed_by != Some(txn_id)
                                } else if self.committed_transactions.contains(&entry.added_by) {
                                    entry.removed_by.is_none()
                                        || (entry.removed_by != Some(txn_id)
                                            && !self
                                                .committed_transactions
                                                .contains(&entry.removed_by.unwrap_or(txn_id)))
                                } else {
                                    false
                                };

                                if is_visible {
                                    if let Some(versions) = self.versions.get(&entry.row_id) {
                                        if self
                                            .find_visible_version(versions, txn_id, txn_timestamp)
                                            .is_some()
                                        {
                                            return Err(Error::InvalidValue(format!(
                                                "Unique constraint violation on index '{}'",
                                                index_name
                                            )));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // Update indexes with MVCC
        for (index_name, column_names) in &self.index_columns.clone() {
            if let Some(index) = self.indexes.get_mut(index_name) {
                // Build composite keys from old and new values
                let mut old_key_values = Vec::new();
                let mut new_key_values = Vec::new();
                let mut old_has_null = false;
                let mut new_has_null = false;
                let mut values_changed = false;

                for col_name in column_names {
                    if let Some(col_idx) =
                        self.schema.columns.iter().position(|c| c.name == *col_name)
                    {
                        let old_value = &old_values[col_idx];
                        let new_value = &values[col_idx];

                        if old_value.is_null() {
                            old_has_null = true;
                        } else {
                            old_key_values.push(old_value.clone());
                        }

                        if new_value.is_null() {
                            new_has_null = true;
                        } else {
                            new_key_values.push(new_value.clone());
                        }

                        if old_value != new_value {
                            values_changed = true;
                        }
                    }
                }

                // Mark old entry as removed (if value is changing)
                if values_changed && !old_has_null {
                    let old_key = CompositeKey::from_values(old_key_values);
                    if let Some(entries) = index.get_mut(&old_key) {
                        // Find the entry for this row and mark it as removed
                        for entry in entries.iter_mut() {
                            if entry.row_id == row_id && entry.removed_by.is_none() {
                                // Only mark our own uncommitted entries or committed entries
                                if entry.added_by == txn_id
                                    || self.committed_transactions.contains(&entry.added_by)
                                {
                                    entry.removed_by = Some(txn_id);
                                    break;
                                }
                            }
                        }
                    }
                }

                // Add new entry (if value is changing)
                if values_changed && !new_has_null {
                    let new_key = CompositeKey::from_values(new_key_values);
                    // Collect included column values if any
                    let included_values =
                        if let Some(included_cols) = self.index_included_columns.get(index_name) {
                            let mut vals = Vec::new();
                            for col_name in included_cols {
                                if let Some(col_idx) =
                                    self.schema.columns.iter().position(|c| c.name == *col_name)
                                {
                                    vals.push(values[col_idx].clone());
                                }
                            }
                            Some(vals)
                        } else {
                            None
                        };

                    let entry = IndexEntry {
                        row_id,
                        added_by: txn_id,
                        removed_by: None,
                        included_values,
                    };
                    index.entry(new_key).or_insert_with(Vec::new).push(entry);
                }
            }
        }

        // Check if we need to update in place or create new version
        let needs_new_version = {
            let visible_version = self
                .get_visible_version(txn_id, txn_timestamp, row_id)
                .unwrap();

            // If this transaction created the visible version, we can update in place
            !(visible_version.created_by == txn_id
                && !self
                    .committed_transactions
                    .contains(&visible_version.created_by))
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
                let is_committed = self.committed_transactions.contains(&version.created_by);
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
        };

        self.versions.get_mut(&row_id).unwrap().push(new_version);
        Ok(())
    }

    /// Delete a row (marks version as deleted and updates indexes)
    pub fn delete(
        &mut self,
        txn_id: HlcTimestamp,
        _txn_timestamp: HlcTimestamp,
        row_id: u64,
    ) -> Result<()> {
        // First get the visible version to extract values for index removal
        let visible_version = self
            .versions
            .get(&row_id)
            .and_then(|versions| {
                versions
                    .iter()
                    .rev()
                    .find(|v| self.is_version_visible_to(v, txn_id, _txn_timestamp))
            })
            .ok_or_else(|| {
                Error::InvalidValue(format!("Row {} not found or not visible", row_id))
            })?;

        // Get the values for index removal
        let values = visible_version.row.values.clone();

        // Mark index entries as removed (MVCC style)
        for (index_name, column_names) in &self.index_columns.clone() {
            if let Some(index) = self.indexes.get_mut(index_name) {
                // Build composite key from values
                let mut key_values = Vec::new();
                let mut has_null = false;
                for col_name in column_names {
                    if let Some(col_idx) =
                        self.schema.columns.iter().position(|c| c.name == *col_name)
                    {
                        let value = &values[col_idx];
                        if value.is_null() {
                            has_null = true;
                            break;
                        }
                        key_values.push(value.clone());
                    }
                }

                if !has_null {
                    let key = CompositeKey::from_values(key_values);
                    if let Some(entries) = index.get_mut(&key) {
                        // Find and mark the entry as removed
                        for entry in entries.iter_mut() {
                            if entry.row_id == row_id && entry.removed_by.is_none() {
                                // Only mark our own uncommitted entries or committed entries
                                if entry.added_by == txn_id
                                    || self.committed_transactions.contains(&entry.added_by)
                                {
                                    entry.removed_by = Some(txn_id);
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }

        // Now mark it as deleted
        let versions = self.versions.get_mut(&row_id).unwrap();

        // Find and mark the visible version as deleted
        for version in versions.iter_mut().rev() {
            // Simple visibility check inline
            let is_visible = if version.created_by == txn_id {
                version.deleted_by.is_none()
            } else {
                let is_committed = self.committed_transactions.contains(&version.created_by);
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
    pub fn find_visible_version<'a>(
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
        if !self.committed_transactions.contains(&version.created_by) {
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
            if !self.committed_transactions.contains(&deleter_id) {
                return true; // See undeleted version
            }

            let deleter_start = self.transaction_start_times.get(&deleter_id).copied();

            if let Some(deleter_time) = deleter_start {
                return deleter_time > txn_timestamp; // Deletion happened after we started
            }
        }

        true
    }

    /// Commit all changes made by a transaction
    pub fn commit_transaction(&mut self, txn_id: HlcTimestamp) -> Result<()> {
        // Just mark transaction as committed - O(1) operation
        // The committed status is checked via committed_transactions set
        self.committed_transactions.insert(txn_id);
        Ok(())
    }

    /// Abort a transaction, removing all its changes
    pub fn abort_transaction(&mut self, txn_id: HlcTimestamp) -> Result<()> {
        // Remove row versions
        self.remove_transaction_versions(txn_id);

        // Remove index entries created by this transaction
        // and unmark removals by this transaction
        for index in self.indexes.values_mut() {
            for entries in index.values_mut() {
                // Remove entries added by this transaction
                entries.retain(|entry| entry.added_by != txn_id);

                // Clear removal marks by this transaction
                for entry in entries.iter_mut() {
                    if entry.removed_by == Some(txn_id) {
                        entry.removed_by = None;
                    }
                }
            }
        }

        // Clean up tracking
        self.transaction_start_times.remove(&txn_id);

        Ok(())
    }

    /// Create an index on one or more columns (builds the index from existing data)
    pub fn create_index(
        &mut self,
        index_name: String,
        column_names: Vec<String>,
        included_columns: Option<Vec<String>>,
        unique: bool,
    ) -> Result<()> {
        // Check if all columns exist and get their indices
        let mut col_indices = Vec::new();
        for column_name in &column_names {
            let col_idx = self
                .schema
                .columns
                .iter()
                .position(|c| &c.name == column_name)
                .ok_or_else(|| {
                    Error::InvalidValue(format!("Column '{}' not found", column_name))
                })?;
            col_indices.push(col_idx);
        }

        // Create new MVCC index
        let mut index: BTreeMap<CompositeKey, Vec<IndexEntry>> = BTreeMap::new();

        // Build index from existing committed data
        for (&row_id, versions) in &self.versions {
            // Find the latest committed version that's not deleted
            if let Some(version) = versions.iter().rev().find(|v| {
                self.committed_transactions.contains(&v.created_by) && v.deleted_by.is_none()
            }) {
                // Build composite key
                let mut key_values = Vec::new();
                let mut has_null = false;
                for &col_idx in &col_indices {
                    let value = &version.row.values[col_idx];
                    if value.is_null() {
                        has_null = true;
                        break;
                    }
                    key_values.push(value.clone());
                }

                if !has_null {
                    let key = CompositeKey::from_values(key_values);

                    // Check uniqueness if required
                    if unique && index.contains_key(&key) {
                        return Err(Error::InvalidValue(format!(
                            "Cannot create unique index '{}': duplicate values exist",
                            index_name
                        )));
                    }

                    // Collect included column values if any
                    let included_values = if let Some(ref included) = included_columns {
                        let mut values = Vec::new();
                        for col_name in included {
                            if let Some(col_idx) =
                                self.schema.columns.iter().position(|c| c.name == *col_name)
                            {
                                values.push(version.row.values[col_idx].clone());
                            }
                        }
                        Some(values)
                    } else {
                        None
                    };

                    let entry = IndexEntry {
                        row_id,
                        added_by: version.created_by, // Use the original creator
                        removed_by: None,
                        included_values,
                    };
                    index.entry(key).or_insert_with(Vec::new).push(entry);
                }
            }
        }

        self.indexes.insert(index_name.clone(), index);
        self.index_columns.insert(index_name.clone(), column_names);
        if let Some(included) = included_columns {
            self.index_included_columns
                .insert(index_name.clone(), included);
        }

        Ok(())
    }

    /// Get statistics for query planning
    pub fn get_table_stats(&self) -> crate::types::statistics::TableStatistics {
        use crate::types::statistics::ColumnStatistics;

        let mut stats = crate::types::statistics::TableStatistics {
            row_count: self.versions.len(),
            indexes: HashMap::new(),
            columns: HashMap::new(),
            correlations: HashMap::new(),
        };

        // Calculate column statistics
        for (col_idx, column) in self.schema.columns.iter().enumerate() {
            let mut col_stats = ColumnStatistics {
                name: column.name.clone(),
                distinct_count: 0,
                null_count: 0,
                min_value: None,
                max_value: None,
                most_common_values: Vec::new(),
                histogram: None,
            };

            // Collect all values for this column from committed versions
            let mut value_counts: HashMap<Value, usize> = HashMap::new();
            let mut all_values = Vec::new();

            for versions in self.versions.values() {
                // Only count the latest committed version
                if let Some(version) = versions.iter().rev().find(|v| {
                    self.committed_transactions.contains(&v.created_by) && v.deleted_by.is_none()
                }) {
                    if col_idx < version.row.values.len() {
                        let value = &version.row.values[col_idx];
                        if value.is_null() {
                            col_stats.null_count += 1;
                        } else {
                            all_values.push(value.clone());
                            *value_counts.entry(value.clone()).or_insert(0) += 1;
                        }
                    }
                }
            }

            col_stats.distinct_count = value_counts.len();

            // Find min/max for ordered types
            if !all_values.is_empty() {
                all_values.sort();
                col_stats.min_value = Some(all_values.first().unwrap().clone());
                col_stats.max_value = Some(all_values.last().unwrap().clone());

                // Create histogram for numeric columns
                if matches!(
                    column.datatype,
                    crate::types::value::DataType::Integer
                        | crate::types::value::DataType::Decimal(_, _)
                ) {
                    col_stats.histogram = self.create_histogram(&all_values, 10);
                }
            }

            // Find most common values (top 5)
            let mut value_freq: Vec<(Value, usize)> = value_counts.into_iter().collect();
            value_freq.sort_by(|a, b| b.1.cmp(&a.1));
            col_stats.most_common_values = value_freq.into_iter().take(5).collect();

            stats.columns.insert(column.name.clone(), col_stats);
        }

        // Calculate correlations between columns (for join selectivity)
        // Sample-based correlation for efficiency
        let sample_size = 100.min(stats.row_count);
        if sample_size >= 10 {
            let mut sampled_rows = Vec::new();
            let mut count = 0;

            // Sample rows evenly across the table
            let step = stats.row_count.max(1) / sample_size;
            for (i, versions) in self.versions.values().enumerate() {
                if i % step == 0 && count < sample_size {
                    if let Some(version) = versions.iter().rev().find(|v| {
                        self.committed_transactions.contains(&v.created_by)
                            && v.deleted_by.is_none()
                    }) {
                        sampled_rows.push(&version.row.values);
                        count += 1;
                    }
                }
            }

            // Calculate pairwise correlations for numeric columns
            for (i, col1) in self.schema.columns.iter().enumerate() {
                if !matches!(
                    col1.datatype,
                    crate::types::value::DataType::Integer
                        | crate::types::value::DataType::Decimal(_, _)
                ) {
                    continue;
                }

                for (j, col2) in self.schema.columns.iter().enumerate().skip(i + 1) {
                    if !matches!(
                        col2.datatype,
                        crate::types::value::DataType::Integer
                            | crate::types::value::DataType::Decimal(_, _)
                    ) {
                        continue;
                    }

                    // Calculate Pearson correlation coefficient
                    let correlation = self.calculate_correlation(&sampled_rows, i, j);
                    if let Some(corr) = correlation {
                        stats
                            .correlations
                            .insert((col1.name.clone(), col2.name.clone()), corr);
                        stats
                            .correlations
                            .insert((col2.name.clone(), col1.name.clone()), corr);
                    }
                }
            }
        }

        // Calculate statistics for each index on-demand
        for (index_name, index) in &self.indexes {
            if let Some(columns) = self.index_columns.get(index_name) {
                let total_entries: usize = index.values().map(|entries| entries.len()).sum();
                let distinct_keys = index.len();

                // Calculate per-column cardinality (simplified)
                let cardinality = vec![distinct_keys; columns.len()];
                let selectivity: Vec<f64> = cardinality
                    .iter()
                    .map(|&c| if c > 0 { 1.0 / c as f64 } else { 1.0 })
                    .collect();

                stats.indexes.insert(
                    index_name.clone(),
                    crate::types::statistics::IndexStatistics {
                        name: index_name.clone(),
                        columns: columns.clone(),
                        cardinality,
                        total_entries,
                        selectivity,
                    },
                );
            }
        }

        stats
    }

    /// Create a histogram from sorted values
    fn create_histogram(
        &self,
        sorted_values: &[Value],
        num_buckets: usize,
    ) -> Option<crate::types::statistics::Histogram> {
        if sorted_values.is_empty() || num_buckets == 0 {
            return None;
        }

        let mut boundaries = Vec::new();
        let mut frequencies = vec![0; num_buckets];

        // Create equal-width buckets (simplified - could be improved with equal-depth)
        let step = sorted_values.len() / num_buckets;
        for i in 0..=num_buckets {
            let idx = (i * step).min(sorted_values.len() - 1);
            boundaries.push(sorted_values[idx].clone());
        }

        // Count values in each bucket
        let mut bucket_idx = 0;
        for value in sorted_values {
            while bucket_idx < num_buckets - 1 && value > &boundaries[bucket_idx + 1] {
                bucket_idx += 1;
            }
            if bucket_idx < num_buckets {
                frequencies[bucket_idx] += 1;
            }
        }

        Some(crate::types::statistics::Histogram {
            num_buckets,
            boundaries,
            frequencies,
        })
    }

    /// Calculate Pearson correlation coefficient between two columns
    fn calculate_correlation(
        &self,
        sampled_rows: &[&Vec<Value>],
        col1: usize,
        col2: usize,
    ) -> Option<f64> {
        if sampled_rows.is_empty() {
            return None;
        }

        let mut x_values = Vec::new();
        let mut y_values = Vec::new();

        // Extract numeric values
        for row in sampled_rows {
            if col1 < row.len() && col2 < row.len() {
                let x_val = match &row[col1] {
                    Value::Integer(i) => *i as f64,
                    Value::Decimal(d) => d.to_f64().unwrap_or(0.0),
                    _ => continue,
                };
                let y_val = match &row[col2] {
                    Value::Integer(i) => *i as f64,
                    Value::Decimal(d) => d.to_f64().unwrap_or(0.0),
                    _ => continue,
                };
                x_values.push(x_val);
                y_values.push(y_val);
            }
        }

        if x_values.len() < 2 {
            return None;
        }

        // Calculate means
        let x_mean = x_values.iter().sum::<f64>() / x_values.len() as f64;
        let y_mean = y_values.iter().sum::<f64>() / y_values.len() as f64;

        // Calculate correlation
        let mut covariance: f64 = 0.0;
        let mut x_variance: f64 = 0.0;
        let mut y_variance: f64 = 0.0;

        for i in 0..x_values.len() {
            let x_diff = x_values[i] - x_mean;
            let y_diff = y_values[i] - y_mean;
            covariance += x_diff * y_diff;
            x_variance += x_diff * x_diff;
            y_variance += y_diff * y_diff;
        }

        if x_variance == 0.0 || y_variance == 0.0 {
            return Some(0.0);
        }

        Some(covariance / (x_variance.sqrt() * y_variance.sqrt()))
    }

    /// Drop an index
    pub fn drop_index(&mut self, index_name: &str) -> Result<()> {
        self.indexes.remove(index_name);
        self.index_columns.remove(index_name);
        self.index_included_columns.remove(index_name);
        Ok(())
    }

    /// Lookup rows by indexed value (equality) - supports composite indexes
    pub fn index_lookup(
        &self,
        index_name: &str,
        values: Vec<Value>,
        txn_id: HlcTimestamp,
        txn_timestamp: HlcTimestamp,
    ) -> Vec<Arc<Row>> {
        let mut result = Vec::new();
        let mut seen_rows = HashSet::new();

        if let Some(index) = self.indexes.get(index_name) {
            let key = CompositeKey::from_values(values);
            if let Some(entries) = index.get(&key) {
                for entry in entries {
                    // Check if entry is visible to this transaction
                    let is_visible = if entry.added_by == txn_id {
                        // See our own additions if not removed by us
                        entry.removed_by.is_none() || entry.removed_by != Some(txn_id)
                    } else if self.committed_transactions.contains(&entry.added_by) {
                        // See committed additions if not removed
                        entry.removed_by.is_none()
                            || (entry.removed_by != Some(txn_id)
                                && !self
                                    .committed_transactions
                                    .contains(&entry.removed_by.unwrap_or(txn_id)))
                    } else {
                        false
                    };

                    if is_visible && !seen_rows.contains(&entry.row_id) {
                        seen_rows.insert(entry.row_id);
                        if let Some(row) = self.read(txn_id, txn_timestamp, entry.row_id) {
                            result.push(row);
                        }
                    }
                }
            }
        }

        result
    }

    /// Lookup rows by index range - supports composite indexes
    pub fn index_range_lookup(
        &self,
        index_name: &str,
        start: Option<Vec<Value>>,
        start_inclusive: bool,
        end: Option<Vec<Value>>,
        end_inclusive: bool,
        txn_id: HlcTimestamp,
        txn_timestamp: HlcTimestamp,
    ) -> Vec<Arc<Row>> {
        let mut result = Vec::new();
        let mut seen_rows = HashSet::new();

        if let Some(index) = self.indexes.get(index_name) {
            // Convert values to composite keys
            let start_key = start.map(CompositeKey::from_values);
            let end_key = end.map(CompositeKey::from_values);

            // Create range based on bounds
            let range: Box<dyn Iterator<Item = (&CompositeKey, &Vec<IndexEntry>)>> =
                match (start_key.as_ref(), end_key.as_ref()) {
                    (None, None) => Box::new(index.iter()),
                    (Some(s), None) => {
                        if start_inclusive {
                            Box::new(index.range(s..))
                        } else {
                            Box::new(
                                index.range((
                                    std::ops::Bound::Excluded(s),
                                    std::ops::Bound::Unbounded,
                                )),
                            )
                        }
                    }
                    (None, Some(e)) => {
                        if end_inclusive {
                            Box::new(index.range(..=e))
                        } else {
                            Box::new(index.range(..e))
                        }
                    }
                    (Some(s), Some(e)) => {
                        match (start_inclusive, end_inclusive) {
                            (true, true) => Box::new(index.range(s..=e)),
                            (true, false) => Box::new(index.range(s..e)),
                            (false, true) => Box::new(index.range((
                                std::ops::Bound::Excluded(s),
                                std::ops::Bound::Included(e),
                            ))),
                            (false, false) => Box::new(index.range((
                                std::ops::Bound::Excluded(s),
                                std::ops::Bound::Excluded(e),
                            ))),
                        }
                    }
                };

            // Collect visible rows from the range
            for (_, entries) in range {
                for entry in entries {
                    // Check if entry is visible
                    let is_visible = if entry.added_by == txn_id {
                        entry.removed_by.is_none() || entry.removed_by != Some(txn_id)
                    } else if self.committed_transactions.contains(&entry.added_by) {
                        entry.removed_by.is_none()
                            || (entry.removed_by != Some(txn_id)
                                && !self
                                    .committed_transactions
                                    .contains(&entry.removed_by.unwrap_or(txn_id)))
                    } else {
                        false
                    };

                    if is_visible && !seen_rows.contains(&entry.row_id) {
                        seen_rows.insert(entry.row_id);
                        if let Some(row) = self.read(txn_id, txn_timestamp, entry.row_id) {
                            result.push(row);
                        }
                    }
                }
            }
        }

        result
    }

    /// Lookup rows by composite index prefix (for partial key lookups)
    pub fn index_prefix_lookup(
        &self,
        index_name: &str,
        prefix_values: Vec<Value>,
        txn_id: HlcTimestamp,
        txn_timestamp: HlcTimestamp,
    ) -> Vec<Arc<Row>> {
        let mut result = Vec::new();
        let mut seen_rows = HashSet::new();

        if let Some(index) = self.indexes.get(index_name) {
            // For prefix matching, we iterate through all entries and check prefix
            for (key, entries) in index.iter() {
                if key.matches_prefix(&prefix_values) {
                    for entry in entries {
                        // Check if entry is visible
                        let is_visible = if entry.added_by == txn_id {
                            entry.removed_by.is_none() || entry.removed_by != Some(txn_id)
                        } else if self.committed_transactions.contains(&entry.added_by) {
                            entry.removed_by.is_none()
                                || (entry.removed_by != Some(txn_id)
                                    && !self
                                        .committed_transactions
                                        .contains(&entry.removed_by.unwrap_or(txn_id)))
                        } else {
                            false
                        };

                        if is_visible && !seen_rows.contains(&entry.row_id) {
                            seen_rows.insert(entry.row_id);
                            if let Some(row) = self.read(txn_id, txn_timestamp, entry.row_id) {
                                result.push(row);
                            }
                        }
                    }
                }
            }
        }

        result
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
            .filter_map(|txn_id| self.transaction_start_times.get(txn_id).copied())
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
                    } else if self.committed_transactions.contains(&version.created_by) {
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

/// Iterator over MVCC rows that handles visibility checks
pub struct MvccRowIterator<'a> {
    table: &'a VersionedTable,
    tx_id: HlcTimestamp,
    tx_timestamp: HlcTimestamp,
    position: std::collections::btree_map::Iter<'a, u64, Vec<VersionedValue>>,
}

impl<'a> MvccRowIterator<'a> {
    fn new(table: &'a VersionedTable, tx_id: HlcTimestamp, tx_timestamp: HlcTimestamp) -> Self {
        Self {
            table,
            tx_id,
            tx_timestamp,
            position: table.versions.iter(),
        }
    }
}

impl<'a> Iterator for MvccRowIterator<'a> {
    type Item = Arc<Vec<Value>>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some((_, versions)) = self.position.next() {
            if let Some(visible_version) =
                self.table
                    .find_visible_version(versions, self.tx_id, self.tx_timestamp)
            {
                return Some(Arc::new(visible_version.row.values.clone()));
            }
        }
        None
    }
}

/// Iterator that yields row IDs along with row data
pub struct MvccRowWithIdIterator<'a> {
    table: &'a VersionedTable,
    tx_id: HlcTimestamp,
    tx_timestamp: HlcTimestamp,
    position: std::collections::btree_map::Iter<'a, u64, Vec<VersionedValue>>,
}

impl<'a> MvccRowWithIdIterator<'a> {
    fn new(table: &'a VersionedTable, tx_id: HlcTimestamp, tx_timestamp: HlcTimestamp) -> Self {
        Self {
            table,
            tx_id,
            tx_timestamp,
            position: table.versions.iter(),
        }
    }
}

impl<'a> Iterator for MvccRowWithIdIterator<'a> {
    type Item = (u64, Arc<Vec<Value>>);

    fn next(&mut self) -> Option<Self::Item> {
        while let Some((&row_id, versions)) = self.position.next() {
            if let Some(visible_version) =
                self.table
                    .find_visible_version(versions, self.tx_id, self.tx_timestamp)
            {
                return Some((row_id, Arc::new(visible_version.row.values.clone())));
            }
        }
        None
    }
}

impl VersionedTable {
    /// Create an iterator over visible rows for a transaction
    pub fn iter<'a>(
        &'a self,
        tx_id: HlcTimestamp,
        tx_timestamp: HlcTimestamp,
    ) -> MvccRowIterator<'a> {
        MvccRowIterator::new(self, tx_id, tx_timestamp)
    }

    /// Create an iterator that includes row IDs
    pub fn iter_with_ids<'a>(
        &'a self,
        tx_id: HlcTimestamp,
        tx_timestamp: HlcTimestamp,
    ) -> MvccRowWithIdIterator<'a> {
        MvccRowWithIdIterator::new(self, tx_id, tx_timestamp)
    }
}

/// Index metadata for tracking index names
#[derive(Debug, Clone)]
pub struct IndexMetadata {
    pub name: String,
    pub table: String,
    pub columns: Vec<String>, // Changed to support composite indexes
    pub unique: bool,
}

/// MVCC-enabled storage engine
///
/// Since operations are processed sequentially by the state machine,
/// we don't need RwLock for the tables - only one operation executes at a time.
/// However, we'll keep it for now until Phase 4 of the refactor.
pub struct MvccStorage {
    /// Tables with versioned data
    pub tables: HashMap<String, VersionedTable>,
    /// Index metadata: index_name -> metadata
    pub index_metadata: HashMap<String, IndexMetadata>,
    /// Track all committed transactions globally
    pub global_committed: HashSet<HlcTimestamp>,
    /// Track active transactions for GC
    pub active_transactions: HashSet<HlcTimestamp>,
}

impl MvccStorage {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
            index_metadata: HashMap::new(),
            global_committed: HashSet::new(),
            active_transactions: HashSet::new(),
        }
    }

    /// Create a new table
    pub fn create_table(&mut self, name: String, schema: Table) -> Result<()> {
        if self.tables.contains_key(&name) {
            return Err(Error::InvalidValue(format!(
                "Table {} already exists",
                name
            )));
        }

        self.tables
            .insert(name.clone(), VersionedTable::new(name, schema));
        Ok(())
    }

    /// Drop a table
    pub fn drop_table(&mut self, name: &str) -> Result<()> {
        if self.tables.remove(name).is_none() {
            return Err(Error::TableNotFound(name.to_string()));
        }

        Ok(())
    }

    /// Execute a DDL operation directly in storage
    /// This centralizes all DDL operations in storage where they belong
    pub fn execute_ddl(&mut self, plan: &crate::planner::plan::Plan) -> Result<String> {
        use crate::planner::plan::Plan;

        match plan {
            Plan::CreateTable { name, schema } => {
                self.create_table(name.clone(), schema.clone())?;
                Ok(format!("Table '{}' created", name))
            }

            Plan::DropTable { name, if_exists } => match self.drop_table(name) {
                Ok(_) => Ok(format!("Table '{}' dropped", name)),
                Err(Error::TableNotFound(_)) if *if_exists => Ok(format!(
                    "Table '{}' does not exist (IF EXISTS specified)",
                    name
                )),
                Err(e) => Err(e),
            },

            Plan::CreateIndex {
                name,
                table,
                columns,
                unique,
                included_columns,
            } => {
                self.create_index(
                    name.clone(),
                    table,
                    columns.clone(),
                    included_columns.clone(),
                    *unique,
                )?;
                let column_list = columns.join(", ");
                let result_msg = if let Some(included) = included_columns {
                    format!(
                        "Index '{}' created on {}({}) INCLUDE ({})",
                        name,
                        table,
                        column_list,
                        included.join(", ")
                    )
                } else {
                    format!("Index '{}' created on {}({})", name, table, column_list)
                };
                Ok(result_msg)
            }

            Plan::DropIndex { name, if_exists } => match self.drop_index(name) {
                Ok(_) => Ok(format!("Index '{}' dropped", name)),
                Err(Error::InvalidValue(_)) if *if_exists => Ok(format!(
                    "Index '{}' does not exist (IF EXISTS specified)",
                    name
                )),
                Err(e) => Err(e),
            },

            _ => Err(Error::InvalidValue("Not a DDL operation".into())),
        }
    }

    /// Create an index on one or more table columns
    pub fn create_index(
        &mut self,
        index_name: String,
        table_name: &str,
        column_names: Vec<String>,
        included_columns: Option<Vec<String>>,
        unique: bool,
    ) -> Result<()> {
        // Check if index name already exists
        if self.index_metadata.contains_key(&index_name) {
            return Err(Error::InvalidValue(format!(
                "Index '{}' already exists",
                index_name
            )));
        }

        let table = self
            .tables
            .get_mut(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;

        // Create the actual index
        table.create_index(
            index_name.clone(),
            column_names.clone(),
            included_columns,
            unique,
        )?;

        // Store metadata
        self.index_metadata.insert(
            index_name.clone(),
            IndexMetadata {
                name: index_name,
                table: table_name.to_string(),
                columns: column_names,
                unique,
            },
        );

        Ok(())
    }

    /// Drop an index by name
    pub fn drop_index(&mut self, index_name: &str) -> Result<()> {
        // Look up index metadata
        let metadata = self
            .index_metadata
            .remove(index_name)
            .ok_or_else(|| Error::InvalidValue(format!("Index '{}' not found", index_name)))?;

        // Drop the actual index
        if let Some(table) = self.tables.get_mut(&metadata.table) {
            table.drop_index(index_name)?;
        }

        Ok(())
    }

    /// Get schemas (without statistics)
    pub fn get_schemas(&self) -> HashMap<String, crate::types::schema::Table> {
        let mut schemas = HashMap::new();
        for (table_name, versioned_table) in &self.tables {
            schemas.insert(table_name.clone(), versioned_table.schema.clone());
        }
        schemas
    }

    /// Calculate current database statistics
    pub fn calculate_statistics(&self) -> crate::types::statistics::DatabaseStatistics {
        let mut db_stats = crate::types::statistics::DatabaseStatistics::new();

        for (table_name, versioned_table) in &self.tables {
            let table_stats = versioned_table.get_table_stats();
            db_stats.update_table(table_name.clone(), table_stats);
        }

        db_stats
    }

    /// Register a new transaction
    pub fn register_transaction(&mut self, txn_id: HlcTimestamp, start_time: HlcTimestamp) {
        self.active_transactions.insert(txn_id);

        // Register with all tables
        for table in self.tables.values_mut() {
            table.register_transaction(txn_id, start_time);
        }
    }

    /// Commit a transaction across all tables
    pub fn commit_transaction(&mut self, txn_id: HlcTimestamp) -> Result<()> {
        // Mark as committed globally
        self.global_committed.insert(txn_id);

        // Commit in all tables
        for table in self.tables.values_mut() {
            table.commit_transaction(txn_id)?;
        }

        // Remove from active set
        self.active_transactions.remove(&txn_id);

        Ok(())
    }

    /// Abort a transaction across all tables
    pub fn abort_transaction(&mut self, txn_id: HlcTimestamp) -> Result<()> {
        // Abort in all tables
        for table in self.tables.values_mut() {
            table.abort_transaction(txn_id)?;
        }

        // Remove from active set
        self.active_transactions.remove(&txn_id);

        Ok(())
    }

    /// Run garbage collection across all tables
    pub fn garbage_collect(&mut self) -> usize {
        let active: Vec<HlcTimestamp> = self.active_transactions.iter().copied().collect();

        let mut total_removed = 0;
        for table in self.tables.values_mut() {
            total_removed += table.garbage_collect(&active);
        }

        total_removed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hlc::{HlcTimestamp, NodeId};
    use crate::types::schema::Column;
    use crate::types::value::DataType;

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

    #[test]
    fn test_mvcc_index_creation() {
        let schema = Table::new(
            "users".to_string(),
            vec![
                Column::new("id".to_string(), DataType::Integer).primary_key(),
                Column::new("email".to_string(), DataType::String),
                Column::new("age".to_string(), DataType::Integer),
            ],
        )
        .unwrap();

        let mut table = VersionedTable::new("users".to_string(), schema);

        // Insert some committed data
        let txn1 = create_txn_id(100);
        let timestamp1 = HlcTimestamp::new(100, 0, NodeId::new(1));
        table.register_transaction(txn1, timestamp1);

        table
            .insert(
                txn1,
                timestamp1,
                vec![
                    Value::Integer(1),
                    Value::String("alice@example.com".to_string()),
                    Value::Integer(25),
                ],
            )
            .unwrap();

        table
            .insert(
                txn1,
                timestamp1,
                vec![
                    Value::Integer(2),
                    Value::String("bob@example.com".to_string()),
                    Value::Integer(30),
                ],
            )
            .unwrap();

        table.commit_transaction(txn1).unwrap();

        // Create an index on email
        table
            .create_index("email".to_string(), vec!["email".to_string()], None, false)
            .unwrap();

        // Verify index contains the data
        assert!(table.indexes.contains_key("email"));
        let email_index = table.indexes.get("email").unwrap();
        assert_eq!(email_index.len(), 2);

        // Verify we can look up by index
        let txn2 = create_txn_id(200);
        let timestamp2 = HlcTimestamp::new(200, 0, NodeId::new(1));
        table.register_transaction(txn2, timestamp2);

        let results = table.index_lookup(
            "email",
            vec![Value::String("alice@example.com".to_string())],
            txn2,
            timestamp2,
        );
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].values[0], Value::Integer(1));
    }

    #[test]
    fn test_mvcc_index_isolation() {
        let schema = Table::new(
            "products".to_string(),
            vec![
                Column::new("id".to_string(), DataType::Integer).primary_key(),
                Column::new("name".to_string(), DataType::String).with_index(true),
                Column::new("price".to_string(), DataType::Decimal(10, 2)),
            ],
        )
        .unwrap();

        let mut table = VersionedTable::new("products".to_string(), schema);

        // Transaction 1: Insert a product
        let txn1 = create_txn_id(100);
        let timestamp1 = HlcTimestamp::new(100, 0, NodeId::new(1));
        table.register_transaction(txn1, timestamp1);

        table
            .insert(
                txn1,
                timestamp1,
                vec![
                    Value::Integer(1),
                    Value::String("Laptop".to_string()),
                    Value::Decimal(rust_decimal::Decimal::from(999)),
                ],
            )
            .unwrap();

        // Transaction 2: Should not see uncommitted data
        let txn2 = create_txn_id(200);
        let timestamp2 = HlcTimestamp::new(200, 0, NodeId::new(1));
        table.register_transaction(txn2, timestamp2);

        let results = table.index_lookup(
            "name",
            vec![Value::String("Laptop".to_string())],
            txn2,
            timestamp2,
        );
        assert_eq!(results.len(), 0, "Should not see uncommitted data");

        // Transaction 1 can see its own data
        let results = table.index_lookup(
            "name",
            vec![Value::String("Laptop".to_string())],
            txn1,
            timestamp1,
        );
        assert_eq!(results.len(), 1, "Should see own uncommitted data");

        // Commit transaction 1
        table.commit_transaction(txn1).unwrap();

        // Now transaction 2 can see it
        let results = table.index_lookup(
            "name",
            vec![Value::String("Laptop".to_string())],
            txn2,
            timestamp2,
        );
        assert_eq!(results.len(), 1, "Should see committed data");
    }

    #[test]
    fn test_mvcc_index_update() {
        let schema = Table::new(
            "items".to_string(),
            vec![
                Column::new("id".to_string(), DataType::Integer).primary_key(),
                Column::new("category".to_string(), DataType::String).with_index(true),
            ],
        )
        .unwrap();

        let mut table = VersionedTable::new("items".to_string(), schema);

        // Insert and commit initial data
        let txn1 = create_txn_id(100);
        let timestamp1 = HlcTimestamp::new(100, 0, NodeId::new(1));
        table.register_transaction(txn1, timestamp1);

        let row_id = table
            .insert(
                txn1,
                timestamp1,
                vec![Value::Integer(1), Value::String("Electronics".to_string())],
            )
            .unwrap();

        table.commit_transaction(txn1).unwrap();

        // Update the category
        let txn2 = create_txn_id(200);
        let timestamp2 = HlcTimestamp::new(200, 0, NodeId::new(1));
        table.register_transaction(txn2, timestamp2);

        table
            .update(
                txn2,
                timestamp2,
                row_id,
                vec![Value::Integer(1), Value::String("Computers".to_string())],
            )
            .unwrap();

        // Transaction 2 sees new value
        let results = table.index_lookup(
            "category",
            vec![Value::String("Computers".to_string())],
            txn2,
            timestamp2,
        );
        assert_eq!(results.len(), 1);

        // Transaction 3 still sees old value
        let txn3 = create_txn_id(300);
        let timestamp3 = HlcTimestamp::new(300, 0, NodeId::new(1));
        table.register_transaction(txn3, timestamp3);

        let results = table.index_lookup(
            "category",
            vec![Value::String("Electronics".to_string())],
            txn3,
            timestamp3,
        );
        assert_eq!(results.len(), 1);

        let results = table.index_lookup(
            "category",
            vec![Value::String("Computers".to_string())],
            txn3,
            timestamp3,
        );
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_mvcc_index_delete() {
        let schema = Table::new(
            "records".to_string(),
            vec![
                Column::new("id".to_string(), DataType::Integer).primary_key(),
                Column::new("status".to_string(), DataType::String).with_index(true),
            ],
        )
        .unwrap();

        let mut table = VersionedTable::new("records".to_string(), schema);

        // Insert and commit data
        let txn1 = create_txn_id(100);
        let timestamp1 = HlcTimestamp::new(100, 0, NodeId::new(1));
        table.register_transaction(txn1, timestamp1);

        let row_id = table
            .insert(
                txn1,
                timestamp1,
                vec![Value::Integer(1), Value::String("active".to_string())],
            )
            .unwrap();

        table.commit_transaction(txn1).unwrap();

        // Delete the row
        let txn2 = create_txn_id(200);
        let timestamp2 = HlcTimestamp::new(200, 0, NodeId::new(1));
        table.register_transaction(txn2, timestamp2);

        table.delete(txn2, timestamp2, row_id).unwrap();

        // Transaction 2 should not see the deleted row in index
        let results = table.index_lookup(
            "status",
            vec![Value::String("active".to_string())],
            txn2,
            timestamp2,
        );
        assert_eq!(results.len(), 0, "Deleted row should not be visible");

        // Transaction 3 should still see it (delete not committed)
        let txn3 = create_txn_id(300);
        let timestamp3 = HlcTimestamp::new(300, 0, NodeId::new(1));
        table.register_transaction(txn3, timestamp3);

        let results = table.index_lookup(
            "status",
            vec![Value::String("active".to_string())],
            txn3,
            timestamp3,
        );
        assert_eq!(
            results.len(),
            1,
            "Uncommitted delete should not affect other transactions"
        );

        // Commit the delete
        table.commit_transaction(txn2).unwrap();

        // New transaction should not see deleted row
        let txn4 = create_txn_id(400);
        let timestamp4 = HlcTimestamp::new(400, 0, NodeId::new(1));
        table.register_transaction(txn4, timestamp4);

        let results = table.index_lookup(
            "status",
            vec![Value::String("active".to_string())],
            txn4,
            timestamp4,
        );
        assert_eq!(results.len(), 0, "Committed delete should be visible");
    }

    #[test]
    fn test_mvcc_index_abort() {
        let schema = Table::new(
            "test".to_string(),
            vec![
                Column::new("id".to_string(), DataType::Integer).primary_key(),
                Column::new("value".to_string(), DataType::String).with_index(true),
            ],
        )
        .unwrap();

        let mut table = VersionedTable::new("test".to_string(), schema);

        // Transaction 1: Insert and abort
        let txn1 = create_txn_id(100);
        let timestamp1 = HlcTimestamp::new(100, 0, NodeId::new(1));
        table.register_transaction(txn1, timestamp1);

        table
            .insert(
                txn1,
                timestamp1,
                vec![Value::Integer(1), Value::String("test_value".to_string())],
            )
            .unwrap();

        // Verify txn1 can see via index
        let results = table.index_lookup(
            "value",
            vec![Value::String("test_value".to_string())],
            txn1,
            timestamp1,
        );
        assert_eq!(results.len(), 1);

        // Abort
        table.abort_transaction(txn1).unwrap();

        // New transaction should not see aborted data
        let txn2 = create_txn_id(200);
        let timestamp2 = HlcTimestamp::new(200, 0, NodeId::new(1));
        table.register_transaction(txn2, timestamp2);

        let results = table.index_lookup(
            "value",
            vec![Value::String("test_value".to_string())],
            txn2,
            timestamp2,
        );
        assert_eq!(results.len(), 0, "Aborted insert should not be visible");
    }

    #[test]
    fn test_mvcc_unique_index_constraint() {
        let schema = Table::new(
            "users".to_string(),
            vec![
                Column::new("id".to_string(), DataType::Integer).primary_key(),
                Column::new("email".to_string(), DataType::String).unique(),
            ],
        )
        .unwrap();

        let mut table = VersionedTable::new("users".to_string(), schema);

        // Insert first user
        let txn1 = create_txn_id(100);
        let timestamp1 = HlcTimestamp::new(100, 0, NodeId::new(1));
        table.register_transaction(txn1, timestamp1);

        table
            .insert(
                txn1,
                timestamp1,
                vec![
                    Value::Integer(1),
                    Value::String("user@example.com".to_string()),
                ],
            )
            .unwrap();

        table.commit_transaction(txn1).unwrap();

        // Try to insert duplicate email
        let txn2 = create_txn_id(200);
        let timestamp2 = HlcTimestamp::new(200, 0, NodeId::new(1));
        table.register_transaction(txn2, timestamp2);

        let result = table.insert(
            txn2,
            timestamp2,
            vec![
                Value::Integer(2),
                Value::String("user@example.com".to_string()),
            ],
        );

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Unique constraint violation")
        );
    }

    #[test]
    fn test_mvcc_index_range_lookup() {
        let schema = Table::new(
            "scores".to_string(),
            vec![
                Column::new("id".to_string(), DataType::Integer).primary_key(),
                Column::new("score".to_string(), DataType::Integer).with_index(true),
            ],
        )
        .unwrap();

        let mut table = VersionedTable::new("scores".to_string(), schema);

        // Insert test data
        let txn1 = create_txn_id(100);
        let timestamp1 = HlcTimestamp::new(100, 0, NodeId::new(1));
        table.register_transaction(txn1, timestamp1);

        for i in 1..=10 {
            table
                .insert(
                    txn1,
                    timestamp1,
                    vec![
                        Value::Integer(i),
                        Value::Integer(i * 10), // scores: 10, 20, 30, ..., 100
                    ],
                )
                .unwrap();
        }

        table.commit_transaction(txn1).unwrap();

        // Test range lookup
        let txn2 = create_txn_id(200);
        let timestamp2 = HlcTimestamp::new(200, 0, NodeId::new(1));
        table.register_transaction(txn2, timestamp2);

        // Range: score >= 30 AND score <= 70
        let results = table.index_range_lookup(
            "score",
            Some(vec![Value::Integer(30)]),
            true, // inclusive
            Some(vec![Value::Integer(70)]),
            true, // inclusive
            txn2,
            timestamp2,
        );

        assert_eq!(results.len(), 5); // Should get scores 30, 40, 50, 60, 70

        // Range: score > 50
        let results = table.index_range_lookup(
            "score",
            Some(vec![Value::Integer(50)]),
            false, // exclusive
            None,
            false,
            txn2,
            timestamp2,
        );

        assert_eq!(results.len(), 5); // Should get scores 60, 70, 80, 90, 100
    }

    #[test]
    fn test_composite_index() {
        let schema = Table::new(
            "orders".to_string(),
            vec![
                Column::new("id".to_string(), DataType::Integer).primary_key(),
                Column::new("customer_id".to_string(), DataType::Integer),
                Column::new("status".to_string(), DataType::String),
                Column::new("amount".to_string(), DataType::Integer),
            ],
        )
        .unwrap();

        let mut table = VersionedTable::new("orders".to_string(), schema);

        // Insert some test data
        let txn1 = create_txn_id(100);
        let timestamp1 = HlcTimestamp::new(100, 0, NodeId::new(1));
        table.register_transaction(txn1, timestamp1);

        // Customer 1 orders
        table
            .insert(
                txn1,
                timestamp1,
                vec![
                    Value::Integer(1),
                    Value::Integer(1),
                    Value::String("pending".to_string()),
                    Value::Integer(100),
                ],
            )
            .unwrap();

        table
            .insert(
                txn1,
                timestamp1,
                vec![
                    Value::Integer(2),
                    Value::Integer(1),
                    Value::String("shipped".to_string()),
                    Value::Integer(200),
                ],
            )
            .unwrap();

        // Customer 2 orders
        table
            .insert(
                txn1,
                timestamp1,
                vec![
                    Value::Integer(3),
                    Value::Integer(2),
                    Value::String("pending".to_string()),
                    Value::Integer(150),
                ],
            )
            .unwrap();

        table
            .insert(
                txn1,
                timestamp1,
                vec![
                    Value::Integer(4),
                    Value::Integer(2),
                    Value::String("shipped".to_string()),
                    Value::Integer(300),
                ],
            )
            .unwrap();

        table.commit_transaction(txn1).unwrap();

        // Create composite index on (customer_id, status)
        table
            .create_index(
                "idx_customer_status".to_string(),
                vec!["customer_id".to_string(), "status".to_string()],
                None,
                false,
            )
            .unwrap();

        // Test exact composite key lookup
        let txn2 = create_txn_id(200);
        let timestamp2 = HlcTimestamp::new(200, 0, NodeId::new(1));
        table.register_transaction(txn2, timestamp2);

        let results = table.index_lookup(
            "idx_customer_status",
            vec![Value::Integer(1), Value::String("pending".to_string())],
            txn2,
            timestamp2,
        );
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].values[0], Value::Integer(1)); // Order ID 1

        // Test prefix lookup (customer_id = 2)
        let results = table.index_prefix_lookup(
            "idx_customer_status",
            vec![Value::Integer(2)],
            txn2,
            timestamp2,
        );
        assert_eq!(results.len(), 2); // Both orders for customer 2

        // Test composite range lookup
        let results = table.index_range_lookup(
            "idx_customer_status",
            Some(vec![
                Value::Integer(1),
                Value::String("pending".to_string()),
            ]),
            true,
            Some(vec![
                Value::Integer(2),
                Value::String("pending".to_string()),
            ]),
            true,
            txn2,
            timestamp2,
        );
        assert_eq!(results.len(), 3); // Orders 1, 2, 3

        // Test uniqueness on composite index
        let mut unique_table = VersionedTable::new(
            "unique_test".to_string(),
            Table::new(
                "unique_test".to_string(),
                vec![
                    Column::new("id".to_string(), DataType::Integer).primary_key(),
                    Column::new("col1".to_string(), DataType::String),
                    Column::new("col2".to_string(), DataType::Integer),
                ],
            )
            .unwrap(),
        );

        let txn3 = create_txn_id(300);
        let timestamp3 = HlcTimestamp::new(300, 0, NodeId::new(1));
        unique_table.register_transaction(txn3, timestamp3);

        unique_table
            .insert(
                txn3,
                timestamp3,
                vec![
                    Value::Integer(1),
                    Value::String("a".to_string()),
                    Value::Integer(1),
                ],
            )
            .unwrap();

        unique_table.commit_transaction(txn3).unwrap();

        // Create unique composite index
        unique_table
            .create_index(
                "idx_unique_composite".to_string(),
                vec!["col1".to_string(), "col2".to_string()],
                None,
                true,
            )
            .unwrap();

        // Try to insert duplicate composite key
        let txn4 = create_txn_id(400);
        let timestamp4 = HlcTimestamp::new(400, 0, NodeId::new(1));
        unique_table.register_transaction(txn4, timestamp4);

        // This should fail due to unique constraint
        unique_table.index_columns.insert(
            "idx_unique_composite".to_string(),
            vec!["col1".to_string(), "col2".to_string()],
        );

        // Mark col1 and col2 as unique for the constraint check to work
        unique_table.schema.columns[1].unique = true;
        unique_table.schema.columns[2].unique = true;

        let result = unique_table.insert(
            txn4,
            timestamp4,
            vec![
                Value::Integer(2),
                Value::String("a".to_string()),
                Value::Integer(1),
            ],
        );
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Unique constraint violation")
        );
    }
}
