//! Single-version in-memory storage engine

use crate::error::{Error, Result};
use crate::types::{DataType, Value};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};

/// A row in a table
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Row {
    /// Primary key (deterministic ID)
    pub id: u64,
    /// Column values
    pub values: Vec<Value>,
    /// Soft delete flag
    pub deleted: bool,
}

impl Row {
    pub fn new(id: u64, values: Vec<Value>) -> Self {
        Self {
            id,
            values,
            deleted: false,
        }
    }
}

/// Column definition
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub default: Option<Value>,
    pub primary_key: bool,
    pub unique: bool,
}

impl Column {
    pub fn new(name: String, data_type: DataType) -> Self {
        Self {
            name,
            data_type,
            nullable: false,
            default: None,
            primary_key: false,
            unique: false,
        }
    }

    pub fn nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    pub fn default(mut self, value: Value) -> Self {
        self.default = Some(value);
        self
    }

    pub fn primary_key(mut self) -> Self {
        self.primary_key = true;
        self.nullable = false; // Primary keys can't be null
        self
    }

    pub fn unique(mut self) -> Self {
        self.unique = true;
        self
    }
}

/// Schema definition for a table
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Schema {
    pub columns: Vec<Column>,
    pub column_index: HashMap<String, usize>,
}

impl Schema {
    pub fn new(columns: Vec<Column>) -> Result<Self> {
        let mut column_index = HashMap::new();

        for (i, col) in columns.iter().enumerate() {
            if column_index.insert(col.name.clone(), i).is_some() {
                return Err(Error::DuplicateColumn(col.name.clone()));
            }
        }

        Ok(Self {
            columns,
            column_index,
        })
    }

    pub fn get_column(&self, name: &str) -> Option<&Column> {
        self.column_index.get(name).map(|&i| &self.columns[i])
    }

    pub fn get_column_index(&self, name: &str) -> Option<usize> {
        self.column_index.get(name).copied()
    }

    pub fn validate_row(&self, values: &[Value]) -> Result<()> {
        if values.len() != self.columns.len() {
            return Err(Error::InvalidValue(format!(
                "Expected {} values, got {}",
                self.columns.len(),
                values.len()
            )));
        }

        for (_i, (col, val)) in self.columns.iter().zip(values.iter()).enumerate() {
            // Null check first
            if val.is_null() {
                if !col.nullable {
                    return Err(Error::NullConstraintViolation(col.name.clone()));
                }
                // Skip type check for nulls in nullable columns
                continue;
            }

            // Type check for non-null values
            val.check_type(&col.data_type)?;
        }

        Ok(())
    }
}

/// An in-memory table with Arc-wrapped rows for efficient sharing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub schema: Schema,
    /// Rows are Arc-wrapped for cheap cloning when returning results
    pub rows: BTreeMap<u64, Arc<Row>>,
    pub next_id: u64,
    /// Secondary indexes (column_name -> value -> row_ids)
    pub indexes: HashMap<String, BTreeMap<Value, Vec<u64>>>,
}

impl Table {
    pub fn new(name: String, schema: Schema) -> Self {
        Self {
            name,
            schema,
            rows: BTreeMap::new(),
            next_id: 1,
            indexes: HashMap::new(),
        }
    }

    /// Insert a new row
    pub fn insert(&mut self, values: Vec<Value>) -> Result<u64> {
        self.schema.validate_row(&values)?;

        let id = self.next_id;
        self.next_id += 1;

        let row = Arc::new(Row::new(id, values));

        // Update indexes
        self.update_indexes_for_insert(id, &row)?;

        self.rows.insert(id, row);
        Ok(id)
    }

    /// Get a row by ID (returns Arc for cheap cloning)
    pub fn get(&self, id: u64) -> Option<Arc<Row>> {
        self.rows.get(&id).filter(|r| !r.deleted).cloned() // Cheap clone of Arc
    }

    /// Update a row (creates new Arc-wrapped row)
    pub fn update(&mut self, id: u64, values: Vec<Value>) -> Result<()> {
        self.schema.validate_row(&values)?;

        // Get the old row for index update (clone to release borrow)
        let old_row = self
            .rows
            .get(&id)
            .ok_or_else(|| Error::InvalidValue(format!("Row {} not found", id)))?
            .clone(); // Clone the Arc to release the borrow

        if old_row.deleted {
            return Err(Error::InvalidValue(format!("Row {} is deleted", id)));
        }

        // Update indexes
        self.update_indexes_for_update(id, &old_row.values, &values)?;

        // Create new row (immutable pattern with Arc)
        let new_row = Arc::new(Row::new(id, values));
        self.rows.insert(id, new_row);
        Ok(())
    }

    /// Delete a row (soft delete by creating new Arc-wrapped row)
    pub fn delete(&mut self, id: u64) -> Result<()> {
        // Get the row for index update (clone to release borrow)
        let old_row = self
            .rows
            .get(&id)
            .ok_or_else(|| Error::InvalidValue(format!("Row {} not found", id)))?
            .clone(); // Clone the Arc to release the borrow

        if old_row.deleted {
            return Err(Error::InvalidValue(format!("Row {} already deleted", id)));
        }

        // Update indexes
        self.update_indexes_for_delete(id, &old_row.values)?;

        // Create new row marked as deleted (immutable pattern)
        let mut deleted_row = Row::new(id, old_row.values.clone());
        deleted_row.deleted = true;
        self.rows.insert(id, Arc::new(deleted_row));
        Ok(())
    }

    /// Scan all rows (returns Arc for efficient sharing)
    pub fn scan(&self) -> impl Iterator<Item = (u64, Arc<Row>)> + '_ {
        self.rows
            .iter()
            .filter(|(_, row)| !row.deleted)
            .map(|(&id, row)| (id, row.clone())) // Cheap Arc clone
    }

    /// Scan rows in a range (returns Arc for efficient sharing)
    pub fn scan_range(&self, start: u64, end: u64) -> impl Iterator<Item = (u64, Arc<Row>)> + '_ {
        self.rows
            .range(start..=end)
            .filter(|(_, row)| !row.deleted)
            .map(|(&id, row)| (id, row.clone())) // Cheap Arc clone
    }

    /// Create an index on a column
    pub fn create_index(&mut self, column_name: String) -> Result<()> {
        let col_idx = self
            .schema
            .get_column_index(&column_name)
            .ok_or_else(|| Error::ColumnNotFound(column_name.clone()))?;

        let mut index = BTreeMap::new();

        for (&id, row) in self.rows.iter().filter(|(_, r)| !r.deleted) {
            let value = &row.values[col_idx];
            index.entry(value.clone()).or_insert_with(Vec::new).push(id);
        }

        self.indexes.insert(column_name, index);
        Ok(())
    }

    /// Look up rows by indexed value
    pub fn index_lookup(&self, column_name: &str, value: &Value) -> Result<Vec<u64>> {
        let index = self
            .indexes
            .get(column_name)
            .ok_or_else(|| Error::InvalidValue(format!("No index on column {}", column_name)))?;

        Ok(index.get(value).cloned().unwrap_or_default())
    }

    // Index maintenance helpers

    fn update_indexes_for_insert(&mut self, id: u64, row: &Row) -> Result<()> {
        for (col_name, index) in &mut self.indexes {
            let col_idx = self.schema.get_column_index(col_name).unwrap();
            let value = &row.values[col_idx];
            index.entry(value.clone()).or_insert_with(Vec::new).push(id);
        }
        Ok(())
    }

    fn update_indexes_for_update(
        &mut self,
        id: u64,
        old_values: &[Value],
        new_values: &[Value],
    ) -> Result<()> {
        for (col_name, index) in &mut self.indexes {
            let col_idx = self.schema.get_column_index(col_name).unwrap();
            let old_value = &old_values[col_idx];
            let new_value = &new_values[col_idx];

            if old_value != new_value {
                // Remove from old index entry
                if let Some(ids) = index.get_mut(old_value) {
                    ids.retain(|&x| x != id);
                    if ids.is_empty() {
                        index.remove(old_value);
                    }
                }

                // Add to new index entry
                index
                    .entry(new_value.clone())
                    .or_insert_with(Vec::new)
                    .push(id);
            }
        }
        Ok(())
    }

    fn update_indexes_for_delete(&mut self, id: u64, values: &[Value]) -> Result<()> {
        for (col_name, index) in &mut self.indexes {
            let col_idx = self.schema.get_column_index(col_name).unwrap();
            let value = &values[col_idx];

            if let Some(ids) = index.get_mut(value) {
                ids.retain(|&x| x != id);
                if ids.is_empty() {
                    index.remove(value);
                }
            }
        }
        Ok(())
    }
}

/// The main storage engine
pub struct Storage {
    tables: Arc<RwLock<HashMap<String, Table>>>,
}

impl Storage {
    pub fn new() -> Self {
        Self {
            tables: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create a new table
    pub fn create_table(&self, name: String, schema: Schema) -> Result<()> {
        let mut tables = self.tables.write().unwrap();

        if tables.contains_key(&name) {
            return Err(Error::DuplicateTable(name));
        }

        tables.insert(name.clone(), Table::new(name, schema));
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

    /// Get a table for reading
    pub fn get_table(&self, name: &str) -> Result<Table> {
        let tables = self.tables.read().unwrap();
        tables
            .get(name)
            .cloned()
            .ok_or_else(|| Error::TableNotFound(name.to_string()))
    }

    /// Execute a function on a table with write access
    pub fn with_table_mut<F, R>(&self, name: &str, f: F) -> Result<R>
    where
        F: FnOnce(&mut Table) -> Result<R>,
    {
        let mut tables = self.tables.write().unwrap();
        let table = tables
            .get_mut(name)
            .ok_or_else(|| Error::TableNotFound(name.to_string()))?;
        f(table)
    }

    /// List all tables
    pub fn list_tables(&self) -> Vec<String> {
        let tables = self.tables.read().unwrap();
        tables.keys().cloned().collect()
    }

    /// Get storage statistics
    pub fn stats(&self) -> StorageStats {
        let tables = self.tables.read().unwrap();

        let mut stats = StorageStats {
            table_count: tables.len(),
            total_rows: 0,
            total_indexes: 0,
        };

        for table in tables.values() {
            stats.total_rows += table.rows.len();
            stats.total_indexes += table.indexes.len();
        }

        stats
    }
}

impl Default for Storage {
    fn default() -> Self {
        Self::new()
    }
}

/// Storage statistics
#[derive(Debug, Clone)]
pub struct StorageStats {
    pub table_count: usize,
    pub total_rows: usize,
    pub total_indexes: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_operations() {
        let storage = Storage::new();

        // Create table schema
        let schema = Schema::new(vec![
            Column::new("id".into(), DataType::Integer).primary_key(),
            Column::new("name".into(), DataType::String),
            Column::new("age".into(), DataType::Integer).nullable(true),
        ])
        .unwrap();

        // Create table
        storage.create_table("users".into(), schema).unwrap();

        // Insert rows
        storage
            .with_table_mut("users", |table| {
                let id1 = table.insert(vec![
                    Value::Integer(1),
                    Value::String("Alice".into()),
                    Value::Integer(30),
                ])?;
                assert_eq!(id1, 1);

                let id2 = table.insert(vec![
                    Value::Integer(2),
                    Value::String("Bob".into()),
                    Value::Null,
                ])?;
                assert_eq!(id2, 2);

                Ok(())
            })
            .unwrap();

        // Query rows
        let table = storage.get_table("users").unwrap();
        assert_eq!(table.rows.len(), 2);

        let row1 = table.get(1).unwrap();
        assert_eq!(row1.values[1], Value::String("Alice".into()));
    }

    #[test]
    fn test_indexes() {
        let storage = Storage::new();

        // Create table with index
        let schema = Schema::new(vec![
            Column::new("id".into(), DataType::Integer).primary_key(),
            Column::new("email".into(), DataType::String).unique(),
        ])
        .unwrap();

        storage.create_table("users".into(), schema).unwrap();

        storage
            .with_table_mut("users", |table| {
                // Insert data
                table.insert(vec![
                    Value::Integer(1),
                    Value::String("alice@example.com".into()),
                ])?;
                table.insert(vec![
                    Value::Integer(2),
                    Value::String("bob@example.com".into()),
                ])?;

                // Create index
                table.create_index("email".into())?;

                // Lookup by index
                let ids =
                    table.index_lookup("email", &Value::String("alice@example.com".into()))?;
                assert_eq!(ids, vec![1]);

                Ok(())
            })
            .unwrap();
    }
}
