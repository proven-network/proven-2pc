//! Multi-Version Concurrency Control (MVCC) storage layer
//!
//! Provides transaction isolation through versioning while working
//! with PCC for conflict prevention.

use crate::error::{Error, Result};
use crate::types::data_type::DataType;
use crate::types::schema::Table;
use crate::types::value::Value;
use proven_hlc::HlcTimestamp;
use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

/// A versioned row in storage with metadata
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageRow {
    /// Primary key (deterministic ID)
    pub id: u64,
    /// Column values
    pub values: Vec<Value>,
    /// Soft delete flag
    pub deleted: bool,
}

impl StorageRow {
    pub fn new(id: u64, values: Vec<Value>) -> Self {
        Self {
            id,
            values,
            deleted: false,
        }
    }
}

// Alias for compatibility
type Row = StorageRow;

/// Composite index key that can hold multiple values
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
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
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
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
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
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
    /// Map from index name to index column definitions (expressions and directions)
    pub index_definitions: HashMap<String, Vec<crate::planning::plan::IndexColumn>>,
    /// Map from index name to sort directions for each column (None means ASC default)
    pub index_directions: HashMap<String, Vec<Option<crate::types::query::Direction>>>,
    /// Map from index name to included columns (for covering indexes)
    pub index_included_columns: HashMap<String, Vec<String>>,
    /// Map from index name to whether it's a unique index
    pub unique_indexes: HashMap<String, bool>,
    /// Next row ID for inserts
    pub next_id: u64,
    /// Track committed transactions
    pub committed_transactions: HashSet<HlcTimestamp>,
}

impl VersionedTable {
    pub fn new(name: String, schema: Table) -> Self {
        // Initialize indexes for columns marked as indexed
        let mut indexes = HashMap::new();
        let mut index_columns = HashMap::new();
        let mut index_definitions = HashMap::new();
        let mut index_directions = HashMap::new();
        let index_included_columns = HashMap::new();
        let mut unique_indexes = HashMap::new();

        for column in &schema.columns {
            if column.index || column.primary_key || column.unique {
                // Create single-column index
                let index_name = column.name.clone();
                indexes.insert(index_name.clone(), BTreeMap::new());
                index_columns.insert(index_name.clone(), vec![column.name.clone()]);
                // Create a simple column expression for the definition
                let col_expr = crate::parsing::ast::Expression::Column(None, column.name.clone());
                let index_col = crate::planning::plan::IndexColumn {
                    expression: col_expr,
                    direction: None, // Default ASC
                };
                index_definitions.insert(index_name.clone(), vec![index_col]);
                index_directions.insert(index_name.clone(), vec![None]); // Default ASC
                // Mark as unique if column is unique or primary key
                if column.unique || column.primary_key {
                    unique_indexes.insert(index_name, true);
                }
            }
        }

        Self {
            name,
            schema,
            versions: BTreeMap::new(),
            indexes,
            index_columns,
            index_definitions,
            index_directions,
            index_included_columns,
            unique_indexes,
            next_id: 1,
            committed_transactions: HashSet::new(),
        }
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
            // Check if this is a unique index
            let is_unique = self
                .unique_indexes
                .get(index_name)
                .copied()
                .unwrap_or(false);

            if is_unique && let Some(index) = self.indexes.get(index_name) {
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
                                if let Some(versions) = self.versions.get(&entry.row_id)
                                    && self
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

        let row_id = self.next_id;
        self.next_id += 1;

        // Create a temporary row for expression evaluation
        let temp_row = Row::new(row_id, values.clone());

        // Collect index updates first to avoid borrow conflicts
        let mut index_updates: Vec<(String, CompositeKey, IndexEntry)> = Vec::new();

        for (index_name, index_cols) in &self.index_definitions {
            // Build composite key by evaluating expressions
            let mut key_values = Vec::new();
            let mut skip_index = false;
            for col in index_cols {
                // Evaluate the expression for this row
                match self.evaluate_expression(&col.expression, &temp_row) {
                    Ok(value) => {
                        // Include NULL values in the index for ORDER BY support
                        key_values.push(value);
                    }
                    Err(_) => {
                        // Skip index update if expression evaluation fails
                        skip_index = true;
                        break;
                    }
                }
            }

            if !skip_index {
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
                index_updates.push((index_name.clone(), key, entry));
            }
        }

        // Now apply the index updates
        for (index_name, key, entry) in index_updates {
            if let Some(index) = self.indexes.get_mut(&index_name) {
                index.entry(key).or_insert_with(Vec::new).push(entry);
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
            let is_unique = self
                .unique_indexes
                .get(index_name)
                .copied()
                .unwrap_or(false);

            if is_unique && let Some(index) = self.indexes.get(index_name) {
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

                            if is_visible
                                && let Some(versions) = self.versions.get(&entry.row_id)
                                && self
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
            visible_version.created_by != txn_id
                || self
                    .committed_transactions
                    .contains(&visible_version.created_by)
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
                for col_name in column_names {
                    if let Some(col_idx) =
                        self.schema.columns.iter().position(|c| c.name == *col_name)
                    {
                        let value = &values[col_idx];
                        // Include NULL values in the key
                        key_values.push(value.clone());
                    }
                }

                // Process all keys, including those with NULLs
                {
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
        // Note: For snapshot reads, txn_id == txn_timestamp
        if version.created_at > txn_timestamp {
            return false;
        }

        // Check if deleted
        if let Some(deleter_id) = version.deleted_by {
            // Deleted version is visible if:
            // 1. Deleter is not committed (uncommitted delete), OR
            // 2. Deleter started after this transaction (delete happened later)
            if !self.committed_transactions.contains(&deleter_id) {
                return true; // See undeleted version
            }

            // The deleter_id IS the timestamp when the delete transaction started
            // If it's after our start time, we don't see the deletion
            return deleter_id > txn_timestamp;
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

        // No transaction-specific tracking to clean up

        Ok(())
    }

    /// Evaluate an AST expression against a row
    fn evaluate_expression(
        &self,
        expr: &crate::parsing::ast::Expression,
        row: &Row,
    ) -> Result<Value> {
        use crate::parsing::ast::{Expression, Literal, Operator};

        match expr {
            Expression::Column(_, name) => {
                // Find column index by name
                let col_idx = self
                    .schema
                    .columns
                    .iter()
                    .position(|c| &c.name == name)
                    .ok_or_else(|| Error::InvalidValue(format!("Column '{}' not found", name)))?;
                row.values.get(col_idx).cloned().ok_or_else(|| {
                    Error::InvalidValue(format!("Column index {} not found", col_idx))
                })
            }
            Expression::Literal(lit) => {
                // Convert AST literal to Value
                match lit {
                    Literal::Null => Ok(Value::Null),
                    Literal::Boolean(b) => Ok(Value::Bool(*b)),
                    Literal::Integer(i) => Ok(Value::I128(*i)),
                    Literal::Float(f) => Ok(Value::F64(*f)),
                    Literal::String(s) => Ok(Value::Str(s.clone())),
                    _ => Err(Error::InvalidValue("Unsupported literal type".into())),
                }
            }
            Expression::Operator(op) => match op {
                Operator::Add(left, right) => {
                    let left_val = self.evaluate_expression(left, row)?;
                    let right_val = self.evaluate_expression(right, row)?;
                    crate::operators::execute_add(&left_val, &right_val)
                }
                Operator::Subtract(left, right) => {
                    let left_val = self.evaluate_expression(left, row)?;
                    let right_val = self.evaluate_expression(right, row)?;
                    crate::operators::execute_subtract(&left_val, &right_val)
                }
                Operator::Multiply(left, right) => {
                    let left_val = self.evaluate_expression(left, row)?;
                    let right_val = self.evaluate_expression(right, row)?;
                    crate::operators::execute_multiply(&left_val, &right_val)
                }
                Operator::Divide(left, right) => {
                    let left_val = self.evaluate_expression(left, row)?;
                    let right_val = self.evaluate_expression(right, row)?;
                    crate::operators::execute_divide(&left_val, &right_val)
                }
                _ => Err(Error::InvalidValue(
                    "Unsupported operator type in index expression".into(),
                )),
            },
            _ => Err(Error::InvalidValue(
                "Unsupported expression type in index".into(),
            )),
        }
    }

    /// Create an index on one or more columns (builds the index from existing data)
    pub fn create_index(
        &mut self,
        index_name: String,
        columns: Vec<crate::planning::plan::IndexColumn>,
        included_columns: Option<Vec<String>>,
        unique: bool,
        current_txn: HlcTimestamp, // Current transaction ID to include uncommitted data
    ) -> Result<()> {
        // Store column names for simple column references (used for metadata)
        // For complex expressions, we'll store the expression string
        let mut column_names = Vec::new();
        for col in &columns {
            match &col.expression {
                crate::parsing::ast::Expression::Column(_, name) => {
                    // Verify column exists
                    self.schema
                        .columns
                        .iter()
                        .position(|c| &c.name == name)
                        .ok_or_else(|| {
                            Error::InvalidValue(format!("Column '{}' not found", name))
                        })?;
                    column_names.push(name.clone());
                }
                _ => {
                    // For complex expressions, store a string representation
                    column_names.push(format!("{:?}", col.expression));
                }
            }
        }

        // Create new MVCC index
        let mut index: BTreeMap<CompositeKey, Vec<IndexEntry>> = BTreeMap::new();

        // Build index from existing data visible to current transaction
        for (&row_id, versions) in &self.versions {
            // Find the latest version visible to the current transaction
            // Include uncommitted data from current transaction OR committed data
            let visible_version = versions.iter().rev().find(|v| {
                (v.created_by == current_txn || self.committed_transactions.contains(&v.created_by))
                    && (v.deleted_by.is_none() || v.deleted_by == Some(current_txn))
            });

            if let Some(version) = visible_version {
                // Build composite key by evaluating expressions
                let mut key_values = Vec::new();
                for col in &columns {
                    // Evaluate the expression for this row
                    let value = self.evaluate_expression(&col.expression, &version.row)?;
                    key_values.push(value);
                }

                // Include all rows in the index, even those with NULLs
                // This is necessary for ORDER BY to work correctly
                {
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
                    index.entry(key).or_default().push(entry);
                }
            }
        }

        self.indexes.insert(index_name.clone(), index);
        self.index_columns.insert(index_name.clone(), column_names);
        self.index_definitions
            .insert(index_name.clone(), columns.clone());

        // Store sort directions from the columns
        let directions: Vec<Option<crate::types::query::Direction>> =
            columns.iter().map(|col| col.direction).collect();
        self.index_directions.insert(index_name.clone(), directions);

        if let Some(included) = included_columns {
            self.index_included_columns
                .insert(index_name.clone(), included);
        }
        // Store unique flag
        self.unique_indexes.insert(index_name, unique);

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
                }) && col_idx < version.row.values.len()
                {
                    let value = &version.row.values[col_idx];
                    if value.is_null() {
                        col_stats.null_count += 1;
                    } else {
                        all_values.push(value.clone());
                        *value_counts.entry(value.clone()).or_insert(0) += 1;
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
                if matches!(column.datatype, DataType::I64 | DataType::Decimal(_, _)) {
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
                if i % step == 0
                    && count < sample_size
                    && let Some(version) = versions.iter().rev().find(|v| {
                        self.committed_transactions.contains(&v.created_by)
                            && v.deleted_by.is_none()
                    })
                {
                    sampled_rows.push(&version.row.values);
                    count += 1;
                }
            }

            // Calculate pairwise correlations for numeric columns
            for (i, col1) in self.schema.columns.iter().enumerate() {
                if !matches!(col1.datatype, DataType::I64 | DataType::Decimal(_, _)) {
                    continue;
                }

                for (j, col2) in self.schema.columns.iter().enumerate().skip(i + 1) {
                    if !matches!(col2.datatype, DataType::I64 | DataType::Decimal(_, _)) {
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
                    Value::I64(i) => *i as f64,
                    Value::Decimal(d) => d.to_f64().unwrap_or(0.0),
                    _ => continue,
                };
                let y_val = match &row[col2] {
                    Value::I64(i) => *i as f64,
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
                        // For committed entries, check if removal is visible
                        if let Some(removed_by) = entry.removed_by {
                            // If removed by us, not visible
                            if removed_by == txn_id {
                                false
                            } else if !self.committed_transactions.contains(&removed_by) {
                                // Uncommitted removal - ignore it, entry is visible
                                true
                            } else {
                                // Committed removal - check if it happened after we started
                                // removed_by IS the timestamp when removal happened
                                removed_by > txn_timestamp
                            }
                        } else {
                            // Not removed
                            true
                        }
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
    #[allow(clippy::too_many_arguments)]
    pub fn index_range_lookup(
        &self,
        index_name: &str,
        start: Option<Vec<Value>>,
        start_inclusive: bool,
        end: Option<Vec<Value>>,
        end_inclusive: bool,
        reverse: bool,
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
                        // For committed entries, check if removal is visible
                        if let Some(removed_by) = entry.removed_by {
                            // If removed by us, not visible
                            if removed_by == txn_id {
                                false
                            } else if !self.committed_transactions.contains(&removed_by) {
                                // Uncommitted removal - ignore it, entry is visible
                                true
                            } else {
                                // Committed removal - check if it happened after we started
                                // removed_by IS the timestamp when removal happened
                                removed_by > txn_timestamp
                            }
                        } else {
                            // Not removed
                            true
                        }
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

        // Reverse the result if requested (for DESC ordering)
        if reverse {
            result.reverse();
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
        for (_, versions) in self.position.by_ref() {
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
        for (&row_id, versions) in self.position.by_ref() {
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

/// Index column metadata
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct IndexColumnMeta {
    /// The expression string (serialized form)
    pub expression: String,
    /// Sort direction for this column
    pub direction: Option<crate::types::query::Direction>,
}

/// Index metadata for tracking index names
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct IndexMetadata {
    pub name: String,
    pub table: String,
    pub columns: Vec<IndexColumnMeta>, // Updated to support expressions and directions
    pub unique: bool,
}

/// MVCC-enabled storage engine
///
/// Since operations are processed sequentially by the state machine,
/// we don't need RwLock for the tables - only one operation executes at a time.
/// However, we'll keep it for now until Phase 4 of the refactor.
#[derive(Default)]
pub struct MvccStorage {
    /// Tables with versioned data
    pub tables: HashMap<String, VersionedTable>,
    /// Index metadata: index_name -> metadata
    pub index_metadata: HashMap<String, IndexMetadata>,
    /// Track all committed transactions globally
    pub global_committed: HashSet<HlcTimestamp>,
}

impl MvccStorage {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
            index_metadata: HashMap::new(),
            global_committed: HashSet::new(),
        }
    }

    /// Create a new table
    pub fn create_table(&mut self, name: String, schema: Table) -> Result<()> {
        if self.tables.contains_key(&name) {
            return Err(Error::DuplicateTable(name.clone()));
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
    pub fn execute_ddl(
        &mut self,
        plan: &crate::planning::plan::Plan,
        tx_id: HlcTimestamp,
    ) -> Result<String> {
        use crate::planning::plan::Plan;

        match plan {
            Plan::CreateTable {
                name,
                schema,
                foreign_keys: _,
                if_not_exists,
            } => {
                // TODO: Store and validate foreign key constraints
                // For now, just create the table without FK validation
                match self.create_table(name.clone(), schema.clone()) {
                    Ok(_) => Ok(format!("Table '{}' created", name)),
                    Err(Error::DuplicateTable(_)) if *if_not_exists => Ok(format!(
                        "Table '{}' already exists (IF NOT EXISTS specified)",
                        name
                    )),
                    Err(e) => Err(e),
                }
            }

            Plan::DropTable {
                names,
                if_exists,
                cascade: _,
            } => {
                // TODO: Handle CASCADE option for foreign keys
                let mut dropped_count = 0;
                let mut errors = Vec::new();

                for name in names {
                    match self.drop_table(name) {
                        Ok(_) => dropped_count += 1,
                        Err(Error::TableNotFound(_)) if *if_exists => {
                            // Silently skip non-existent tables when IF EXISTS is specified
                        }
                        Err(e) => {
                            if !if_exists {
                                return Err(e);
                            }
                            errors.push(format!("{}: {}", name, e));
                        }
                    }
                }

                if dropped_count == 0 && names.len() == 1 && *if_exists {
                    Ok(format!(
                        "Table '{}' does not exist (IF EXISTS specified)",
                        names[0]
                    ))
                } else if dropped_count == 1 {
                    Ok("1 table dropped".to_string())
                } else {
                    Ok(format!("{} tables dropped", dropped_count))
                }
            }

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
                    tx_id,
                )?;
                // Format column list for display
                let column_list = columns
                    .iter()
                    .map(|col| {
                        let expr_str = format!("{:?}", col.expression); // TODO: better formatting
                        if let Some(dir) = &col.direction {
                            format!("{} {:?}", expr_str, dir)
                        } else {
                            expr_str
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
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
        columns: Vec<crate::planning::plan::IndexColumn>,
        included_columns: Option<Vec<String>>,
        unique: bool,
        tx_id: HlcTimestamp,
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
            columns.clone(),
            included_columns,
            unique,
            tx_id, // Include uncommitted data from current transaction
        )?;

        // Store metadata - convert IndexColumn to IndexColumnMeta
        let column_meta: Vec<IndexColumnMeta> = columns
            .iter()
            .map(|col| {
                // Serialize expression in a more structured way
                let expression_str = match &col.expression {
                    crate::parsing::ast::Expression::Column(None, name) => {
                        // Simple column - just store the name
                        name.clone()
                    }
                    expr => {
                        // Complex expression - store as debug string for now
                        // This will be improved to use proper serialization
                        format!("{:?}", expr)
                    }
                };
                IndexColumnMeta {
                    expression: expression_str,
                    direction: col.direction,
                }
            })
            .collect();

        self.index_metadata.insert(
            index_name.clone(),
            IndexMetadata {
                name: index_name,
                table: table_name.to_string(),
                columns: column_meta,
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

    /// Get index metadata for planning
    pub fn get_index_metadata(&self) -> HashMap<String, Vec<IndexMetadata>> {
        let mut table_indexes = HashMap::new();

        // Group indexes by table
        for metadata in self.index_metadata.values() {
            table_indexes
                .entry(metadata.table.clone())
                .or_insert_with(Vec::new)
                .push(metadata.clone());
        }

        table_indexes
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

    /// Commit a transaction across all tables
    pub fn commit_transaction(&mut self, txn_id: HlcTimestamp) -> Result<()> {
        // Mark as committed globally
        self.global_committed.insert(txn_id);

        // Commit in all tables
        for table in self.tables.values_mut() {
            table.commit_transaction(txn_id)?;
        }

        Ok(())
    }

    /// Abort a transaction across all tables
    pub fn abort_transaction(&mut self, txn_id: HlcTimestamp) -> Result<()> {
        // Abort in all tables
        for table in self.tables.values_mut() {
            table.abort_transaction(txn_id)?;
        }

        Ok(())
    }

    /// Get a compacted view of storage for snapshots
    /// Returns only committed data (latest version per row)
    pub fn get_compacted_data(&self) -> CompactedSqlData {
        let mut compacted_tables = HashMap::new();

        for (table_name, table) in &self.tables {
            let mut compacted_rows = BTreeMap::new();

            // Get only the latest committed version of each row
            for (row_id, versions) in &table.versions {
                // Find the latest committed version that isn't deleted
                for version in versions.iter().rev() {
                    if table.committed_transactions.contains(&version.created_by) {
                        if version.deleted_by.is_none() {
                            // Include this row
                            compacted_rows.insert(*row_id, (*version.row).clone());
                        }
                        break; // Found the latest committed version
                    }
                }
            }

            // Create compacted table data
            let compacted_table = CompactedTableData {
                schema: table.schema.clone(),
                rows: compacted_rows,
                next_id: table.next_id,
                indexes: table.index_columns.clone(),
                index_included_columns: table.index_included_columns.clone(),
                unique_indexes: table.unique_indexes.clone(),
            };

            compacted_tables.insert(table_name.clone(), compacted_table);
        }

        CompactedSqlData {
            tables: compacted_tables,
            index_metadata: self.index_metadata.clone(),
        }
    }

    /// Restore from compacted data
    /// Should only be called on a fresh storage instance
    pub fn restore_from_compacted(&mut self, data: CompactedSqlData) {
        use proven_hlc::NodeId;

        // Clear any existing data
        self.tables.clear();
        self.index_metadata.clear();
        self.global_committed.clear();

        // Create a special "restore" transaction that's already committed
        let restore_txn = HlcTimestamp::new(0, 0, NodeId::new(0));
        self.global_committed.insert(restore_txn);

        // Restore index metadata
        self.index_metadata = data.index_metadata;

        // Restore each table
        for (table_name, compacted_table) in data.tables {
            let mut table = VersionedTable::new(table_name.clone(), compacted_table.schema);

            // Set the next_id
            table.next_id = compacted_table.next_id;

            // Restore indexes structure
            table.index_columns = compacted_table.indexes;
            table.index_included_columns = compacted_table.index_included_columns;
            table.unique_indexes = compacted_table.unique_indexes;

            // Initialize index storage
            for index_name in table.index_columns.keys() {
                table.indexes.insert(index_name.clone(), BTreeMap::new());
            }

            // Mark restore transaction as committed in this table
            table.committed_transactions.insert(restore_txn);

            // Restore all rows
            for (row_id, row) in compacted_table.rows {
                let version = VersionedValue {
                    row: Arc::new(row.clone()),
                    created_by: restore_txn,
                    created_at: restore_txn,
                    deleted_by: None,
                };
                table.versions.insert(row_id, vec![version]);

                // Rebuild indexes for this row
                for (index_name, columns) in &table.index_columns {
                    let mut key_values = Vec::new();
                    for col_name in columns {
                        if let Some(col_idx) = table
                            .schema
                            .columns
                            .iter()
                            .position(|c| &c.name == col_name)
                            && col_idx < row.values.len()
                        {
                            key_values.push(row.values[col_idx].clone());
                        }
                    }

                    if !key_values.is_empty() {
                        let composite_key = CompositeKey::from_values(key_values);
                        let entry = IndexEntry {
                            row_id,
                            added_by: restore_txn,
                            removed_by: None,
                            included_values: None, // Simplified for snapshot
                        };

                        table
                            .indexes
                            .get_mut(index_name)
                            .unwrap()
                            .entry(composite_key)
                            .or_default()
                            .push(entry);
                    }
                }
            }

            self.tables.insert(table_name, table);
        }
    }
}

/// Compacted SQL data for snapshots
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CompactedSqlData {
    pub tables: HashMap<String, CompactedTableData>,
    pub index_metadata: HashMap<String, IndexMetadata>,
}

/// Compacted table data
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CompactedTableData {
    pub schema: Table,
    pub rows: BTreeMap<u64, StorageRow>,
    pub next_id: u64,
    pub indexes: HashMap<String, Vec<String>>,
    pub index_included_columns: HashMap<String, Vec<String>>,
    pub unique_indexes: HashMap<String, bool>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::data_type::DataType;
    use crate::types::schema::Column;
    use proven_hlc::{HlcTimestamp, NodeId};

    fn create_test_schema() -> Table {
        Table::new(
            "test".to_string(),
            vec![
                Column::new("id".to_string(), DataType::I64).primary_key(),
                Column::new("name".to_string(), DataType::Str),
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

        // Insert
        let row_id = table
            .insert(
                txn1,
                timestamp1,
                vec![Value::integer(1), Value::string("Alice".to_string())],
            )
            .unwrap();

        // Before commit, only txn1 can see the row
        assert!(table.read(txn1, timestamp1, row_id).is_some());

        let txn2 = create_txn_id(200);
        let timestamp2 = HlcTimestamp::new(200, 0, NodeId::new(1));
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

        let row_id = table
            .insert(
                txn1,
                timestamp1,
                vec![Value::integer(1), Value::string("Alice".to_string())],
            )
            .unwrap();
        table.commit_transaction(txn1).unwrap();

        // Transaction 2: Update but don't commit yet
        let txn2 = create_txn_id(200);
        let timestamp2 = HlcTimestamp::new(200, 0, NodeId::new(1));

        table
            .update(
                txn2,
                timestamp2,
                row_id,
                vec![Value::integer(1), Value::string("Bob".to_string())],
            )
            .unwrap();

        // Transaction 3: Should still see old version
        let txn3 = create_txn_id(300);
        let timestamp3 = HlcTimestamp::new(300, 0, NodeId::new(1));

        let row = table.read(txn3, timestamp3, row_id).unwrap();
        assert_eq!(row.values[1], Value::string("Alice".to_string()));

        // But txn2 sees its own update
        let row = table.read(txn2, timestamp2, row_id).unwrap();
        assert_eq!(row.values[1], Value::string("Bob".to_string()));
    }

    #[test]
    fn test_abort_rollback() {
        let mut table = VersionedTable::new("test".to_string(), create_test_schema());

        // Insert and commit a row
        let txn1 = create_txn_id(100);
        let timestamp1 = HlcTimestamp::new(100, 0, NodeId::new(1));

        let row_id = table
            .insert(
                txn1,
                timestamp1,
                vec![Value::integer(1), Value::string("Alice".to_string())],
            )
            .unwrap();
        table.commit_transaction(txn1).unwrap();

        // Start a transaction, update, then abort
        let txn2 = create_txn_id(200);
        let timestamp2 = HlcTimestamp::new(200, 0, NodeId::new(1));

        table
            .update(
                txn2,
                timestamp2,
                row_id,
                vec![Value::integer(1), Value::string("Bob".to_string())],
            )
            .unwrap();

        // Abort the transaction
        table.abort_transaction(txn2).unwrap();

        // New transaction should still see original value
        let txn3 = create_txn_id(300);
        let timestamp3 = HlcTimestamp::new(300, 0, NodeId::new(1));

        let row = table.read(txn3, timestamp3, row_id).unwrap();
        assert_eq!(row.values[1], Value::string("Alice".to_string()));
    }

    #[test]
    fn test_mvcc_index_creation() {
        let schema = Table::new(
            "users".to_string(),
            vec![
                Column::new("id".to_string(), DataType::I64).primary_key(),
                Column::new("email".to_string(), DataType::Str),
                Column::new("age".to_string(), DataType::I64),
            ],
        )
        .unwrap();

        let mut table = VersionedTable::new("users".to_string(), schema);

        // Insert some committed data
        let txn1 = create_txn_id(100);
        let timestamp1 = HlcTimestamp::new(100, 0, NodeId::new(1));

        table
            .insert(
                txn1,
                timestamp1,
                vec![
                    Value::integer(1),
                    Value::string("alice@example.com".to_string()),
                    Value::integer(25),
                ],
            )
            .unwrap();

        table
            .insert(
                txn1,
                timestamp1,
                vec![
                    Value::integer(2),
                    Value::string("bob@example.com".to_string()),
                    Value::integer(30),
                ],
            )
            .unwrap();

        table.commit_transaction(txn1).unwrap();

        // Create an index on email
        let email_col = crate::planning::plan::IndexColumn {
            expression: crate::parsing::ast::Expression::Column(None, "email".to_string()),
            direction: None,
        };
        table
            .create_index("email".to_string(), vec![email_col], None, false, txn1)
            .unwrap();

        // Verify index contains the data
        assert!(table.indexes.contains_key("email"));
        let email_index = table.indexes.get("email").unwrap();
        assert_eq!(email_index.len(), 2);

        // Verify we can look up by index
        let txn2 = create_txn_id(200);
        let timestamp2 = HlcTimestamp::new(200, 0, NodeId::new(1));

        let results = table.index_lookup(
            "email",
            vec![Value::string("alice@example.com".to_string())],
            txn2,
            timestamp2,
        );
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].values[0], Value::integer(1));
    }

    #[test]
    fn test_mvcc_index_isolation() {
        let schema = Table::new(
            "products".to_string(),
            vec![
                Column::new("id".to_string(), DataType::I64).primary_key(),
                Column::new("name".to_string(), DataType::Str).with_index(true),
                Column::new("price".to_string(), DataType::Decimal(Some(10), Some(2))),
            ],
        )
        .unwrap();

        let mut table = VersionedTable::new("products".to_string(), schema);

        // Transaction 1: Insert a product
        let txn1 = create_txn_id(100);
        let timestamp1 = HlcTimestamp::new(100, 0, NodeId::new(1));

        table
            .insert(
                txn1,
                timestamp1,
                vec![
                    Value::integer(1),
                    Value::string("Laptop".to_string()),
                    Value::Decimal(rust_decimal::Decimal::from(999)),
                ],
            )
            .unwrap();

        // Transaction 2: Should not see uncommitted data
        let txn2 = create_txn_id(200);
        let timestamp2 = HlcTimestamp::new(200, 0, NodeId::new(1));

        let results = table.index_lookup(
            "name",
            vec![Value::string("Laptop".to_string())],
            txn2,
            timestamp2,
        );
        assert_eq!(results.len(), 0, "Should not see uncommitted data");

        // Transaction 1 can see its own data
        let results = table.index_lookup(
            "name",
            vec![Value::string("Laptop".to_string())],
            txn1,
            timestamp1,
        );
        assert_eq!(results.len(), 1, "Should see own uncommitted data");

        // Commit transaction 1
        table.commit_transaction(txn1).unwrap();

        // Now transaction 2 can see it
        let results = table.index_lookup(
            "name",
            vec![Value::string("Laptop".to_string())],
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
                Column::new("id".to_string(), DataType::I64).primary_key(),
                Column::new("category".to_string(), DataType::Str).with_index(true),
            ],
        )
        .unwrap();

        let mut table = VersionedTable::new("items".to_string(), schema);

        // Insert and commit initial data
        let txn1 = create_txn_id(100);
        let timestamp1 = HlcTimestamp::new(100, 0, NodeId::new(1));

        let row_id = table
            .insert(
                txn1,
                timestamp1,
                vec![Value::integer(1), Value::string("Electronics".to_string())],
            )
            .unwrap();

        table.commit_transaction(txn1).unwrap();

        // Update the category
        let txn2 = create_txn_id(200);
        let timestamp2 = HlcTimestamp::new(200, 0, NodeId::new(1));

        table
            .update(
                txn2,
                timestamp2,
                row_id,
                vec![Value::integer(1), Value::string("Computers".to_string())],
            )
            .unwrap();

        // Transaction 2 sees new value
        let results = table.index_lookup(
            "category",
            vec![Value::string("Computers".to_string())],
            txn2,
            timestamp2,
        );
        assert_eq!(results.len(), 1);

        // Transaction 3 still sees old value
        let txn3 = create_txn_id(300);
        let timestamp3 = HlcTimestamp::new(300, 0, NodeId::new(1));

        let results = table.index_lookup(
            "category",
            vec![Value::string("Electronics".to_string())],
            txn3,
            timestamp3,
        );
        assert_eq!(results.len(), 1);

        let results = table.index_lookup(
            "category",
            vec![Value::string("Computers".to_string())],
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
                Column::new("id".to_string(), DataType::I64).primary_key(),
                Column::new("status".to_string(), DataType::Str).with_index(true),
            ],
        )
        .unwrap();

        let mut table = VersionedTable::new("records".to_string(), schema);

        // Insert and commit data
        let txn1 = create_txn_id(100);
        let timestamp1 = HlcTimestamp::new(100, 0, NodeId::new(1));

        let row_id = table
            .insert(
                txn1,
                timestamp1,
                vec![Value::integer(1), Value::string("active".to_string())],
            )
            .unwrap();

        table.commit_transaction(txn1).unwrap();

        // Delete the row
        let txn2 = create_txn_id(200);
        let timestamp2 = HlcTimestamp::new(200, 0, NodeId::new(1));

        table.delete(txn2, timestamp2, row_id).unwrap();

        // Transaction 2 should not see the deleted row in index
        let results = table.index_lookup(
            "status",
            vec![Value::string("active".to_string())],
            txn2,
            timestamp2,
        );
        assert_eq!(results.len(), 0, "Deleted row should not be visible");

        // Transaction 3 should still see it (delete not committed)
        let txn3 = create_txn_id(300);
        let timestamp3 = HlcTimestamp::new(300, 0, NodeId::new(1));

        let results = table.index_lookup(
            "status",
            vec![Value::string("active".to_string())],
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

        let results = table.index_lookup(
            "status",
            vec![Value::string("active".to_string())],
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
                Column::new("id".to_string(), DataType::I64).primary_key(),
                Column::new("value".to_string(), DataType::Str).with_index(true),
            ],
        )
        .unwrap();

        let mut table = VersionedTable::new("test".to_string(), schema);

        // Transaction 1: Insert and abort
        let txn1 = create_txn_id(100);
        let timestamp1 = HlcTimestamp::new(100, 0, NodeId::new(1));

        table
            .insert(
                txn1,
                timestamp1,
                vec![Value::integer(1), Value::string("test_value".to_string())],
            )
            .unwrap();

        // Verify txn1 can see via index
        let results = table.index_lookup(
            "value",
            vec![Value::string("test_value".to_string())],
            txn1,
            timestamp1,
        );
        assert_eq!(results.len(), 1);

        // Abort
        table.abort_transaction(txn1).unwrap();

        // New transaction should not see aborted data
        let txn2 = create_txn_id(200);
        let timestamp2 = HlcTimestamp::new(200, 0, NodeId::new(1));

        let results = table.index_lookup(
            "value",
            vec![Value::string("test_value".to_string())],
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
                Column::new("id".to_string(), DataType::I64).primary_key(),
                Column::new("email".to_string(), DataType::Str).unique(),
            ],
        )
        .unwrap();

        let mut table = VersionedTable::new("users".to_string(), schema);

        // Insert first user
        let txn1 = create_txn_id(100);
        let timestamp1 = HlcTimestamp::new(100, 0, NodeId::new(1));

        table
            .insert(
                txn1,
                timestamp1,
                vec![
                    Value::integer(1),
                    Value::string("user@example.com".to_string()),
                ],
            )
            .unwrap();

        table.commit_transaction(txn1).unwrap();

        // Try to insert duplicate email
        let txn2 = create_txn_id(200);
        let timestamp2 = HlcTimestamp::new(200, 0, NodeId::new(1));

        let result = table.insert(
            txn2,
            timestamp2,
            vec![
                Value::integer(2),
                Value::string("user@example.com".to_string()),
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
                Column::new("id".to_string(), DataType::I64).primary_key(),
                Column::new("score".to_string(), DataType::I64).with_index(true),
            ],
        )
        .unwrap();

        let mut table = VersionedTable::new("scores".to_string(), schema);

        // Insert test data
        let txn1 = create_txn_id(100);
        let timestamp1 = HlcTimestamp::new(100, 0, NodeId::new(1));

        for i in 1..=10 {
            table
                .insert(
                    txn1,
                    timestamp1,
                    vec![
                        Value::integer(i),
                        Value::integer(i * 10), // scores: 10, 20, 30, ..., 100
                    ],
                )
                .unwrap();
        }

        table.commit_transaction(txn1).unwrap();

        // Test range lookup
        let txn2 = create_txn_id(200);
        let timestamp2 = HlcTimestamp::new(200, 0, NodeId::new(1));
        // table.register_transaction(txn2, timestamp2);

        // Range: score >= 30 AND score <= 70
        let results = table.index_range_lookup(
            "score",
            Some(vec![Value::integer(30)]),
            true, // inclusive
            Some(vec![Value::integer(70)]),
            true,  // inclusive
            false, // not reversed
            txn2,
            timestamp2,
        );

        assert_eq!(results.len(), 5); // Should get scores 30, 40, 50, 60, 70

        // Range: score > 50
        let results = table.index_range_lookup(
            "score",
            Some(vec![Value::integer(50)]),
            false, // exclusive
            None,
            false,
            false, // not reversed
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
                Column::new("id".to_string(), DataType::I64).primary_key(),
                Column::new("customer_id".to_string(), DataType::I64),
                Column::new("status".to_string(), DataType::Str),
                Column::new("amount".to_string(), DataType::I64),
            ],
        )
        .unwrap();

        let mut table = VersionedTable::new("orders".to_string(), schema);

        // Insert some test data
        let txn1 = create_txn_id(100);
        let timestamp1 = HlcTimestamp::new(100, 0, NodeId::new(1));

        // Customer 1 orders
        table
            .insert(
                txn1,
                timestamp1,
                vec![
                    Value::integer(1),
                    Value::integer(1),
                    Value::string("pending".to_string()),
                    Value::integer(100),
                ],
            )
            .unwrap();

        table
            .insert(
                txn1,
                timestamp1,
                vec![
                    Value::integer(2),
                    Value::integer(1),
                    Value::string("shipped".to_string()),
                    Value::integer(200),
                ],
            )
            .unwrap();

        // Customer 2 orders
        table
            .insert(
                txn1,
                timestamp1,
                vec![
                    Value::integer(3),
                    Value::integer(2),
                    Value::string("pending".to_string()),
                    Value::integer(150),
                ],
            )
            .unwrap();

        table
            .insert(
                txn1,
                timestamp1,
                vec![
                    Value::integer(4),
                    Value::integer(2),
                    Value::string("shipped".to_string()),
                    Value::integer(300),
                ],
            )
            .unwrap();

        table.commit_transaction(txn1).unwrap();

        // Create composite index on (customer_id, status)
        let customer_col = crate::planning::plan::IndexColumn {
            expression: crate::parsing::ast::Expression::Column(None, "customer_id".to_string()),
            direction: None,
        };
        let status_col = crate::planning::plan::IndexColumn {
            expression: crate::parsing::ast::Expression::Column(None, "status".to_string()),
            direction: None,
        };
        table
            .create_index(
                "idx_customer_status".to_string(),
                vec![customer_col, status_col],
                None,
                false,
                txn1,
            )
            .unwrap();

        // Test exact composite key lookup
        let txn2 = create_txn_id(200);
        let timestamp2 = HlcTimestamp::new(200, 0, NodeId::new(1));

        let results = table.index_lookup(
            "idx_customer_status",
            vec![Value::integer(1), Value::string("pending".to_string())],
            txn2,
            timestamp2,
        );
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].values[0], Value::integer(1)); // Order ID 1

        // Test prefix lookup (customer_id = 2)
        let results = table.index_prefix_lookup(
            "idx_customer_status",
            vec![Value::integer(2)],
            txn2,
            timestamp2,
        );
        assert_eq!(results.len(), 2); // Both orders for customer 2

        // Test composite range lookup
        let results = table.index_range_lookup(
            "idx_customer_status",
            Some(vec![
                Value::integer(1),
                Value::string("pending".to_string()),
            ]),
            true,
            Some(vec![
                Value::integer(2),
                Value::string("pending".to_string()),
            ]),
            true,
            false, // not reversed
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
                    Column::new("id".to_string(), DataType::I64).primary_key(),
                    Column::new("col1".to_string(), DataType::Str),
                    Column::new("col2".to_string(), DataType::I64),
                ],
            )
            .unwrap(),
        );

        let txn3 = create_txn_id(300);
        let timestamp3 = HlcTimestamp::new(300, 0, NodeId::new(1));

        unique_table
            .insert(
                txn3,
                timestamp3,
                vec![
                    Value::integer(1),
                    Value::string("a".to_string()),
                    Value::integer(1),
                ],
            )
            .unwrap();

        unique_table.commit_transaction(txn3).unwrap();

        // Create unique composite index
        let col1 = crate::planning::plan::IndexColumn {
            expression: crate::parsing::ast::Expression::Column(None, "col1".to_string()),
            direction: None,
        };
        let col2 = crate::planning::plan::IndexColumn {
            expression: crate::parsing::ast::Expression::Column(None, "col2".to_string()),
            direction: None,
        };
        unique_table
            .create_index(
                "idx_unique_composite".to_string(),
                vec![col1, col2],
                None,
                true,
                txn1,
            )
            .unwrap();

        // Try to insert duplicate composite key
        let txn4 = create_txn_id(400);
        let timestamp4 = HlcTimestamp::new(400, 0, NodeId::new(1));

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
                Value::integer(2),
                Value::string("a".to_string()),
                Value::integer(1),
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
