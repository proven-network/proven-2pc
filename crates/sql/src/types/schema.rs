//! SQL schema types (tables and columns)
//!
//! Adapted from toydb for proven-sql's PCC architecture.
//! Tables are immutable after creation - no ALTER TABLE support.

use super::data_type::DataType;
use super::value::Row;
use crate::error::{Error, Result};
use crate::parsing::ast::ddl::ForeignKeyConstraint;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// Default schema version (starts at 1)
fn default_schema_version() -> u32 {
    1
}

/// A table schema, which specifies its data structure and constraints.
///
/// Tables can't change after they are created. There is no ALTER TABLE nor
/// CREATE/DROP INDEX, only CREATE TABLE and DROP TABLE.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct Table {
    /// The table name. Unique identifier for the table. Can't be empty.
    pub name: String,
    /// Schema version for this table. Incremented on schema changes.
    /// Stored in each row to identify which version of the schema to use for decoding.
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    /// The primary key column index. None if no primary key specified.
    /// Note: Tables still have internal row IDs for storage and locking.
    pub primary_key: Option<usize>,
    /// The table's columns. Must have at least one.
    pub columns: Vec<Column>,
    /// Foreign key constraints defined on this table
    #[serde(default)]
    pub foreign_keys: Vec<ForeignKeyConstraint>,
}

impl Table {
    /// Creates a new table schema.
    pub fn new(name: String, columns: Vec<Column>) -> Result<Self> {
        Self::new_with_foreign_keys(name, columns, Vec::new())
    }

    /// Creates a new table schema with foreign key constraints.
    pub fn new_with_foreign_keys(
        name: String,
        columns: Vec<Column>,
        foreign_keys: Vec<ForeignKeyConstraint>,
    ) -> Result<Self> {
        if name.is_empty() {
            return Err(Error::InvalidValue("Table name cannot be empty".into()));
        }
        // Allow tables without columns (as per SQL standard)

        // Find the primary key column (optional)
        let primary_keys: Vec<_> = columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.primary_key)
            .map(|(i, _)| i)
            .collect();

        let primary_key = if primary_keys.is_empty() {
            // No primary key is allowed - tables use internal row IDs
            None
        } else if primary_keys.len() > 1 {
            return Err(Error::InvalidValue(
                "Table can only have one primary key (composite keys not yet supported)".into(),
            ));
        } else {
            // Validate primary key column
            let pk_idx = primary_keys[0];
            let pk_column = &columns[pk_idx];
            if pk_column.nullable {
                return Err(Error::InvalidValue("Primary key cannot be nullable".into()));
            }
            if pk_column.default.is_some() {
                return Err(Error::InvalidValue(
                    "Primary key cannot have a default value".into(),
                ));
            }
            Some(pk_idx)
        };

        Ok(Table {
            name,
            schema_version: 1, // Start at version 1
            primary_key,
            columns,
            foreign_keys,
        })
    }

    /// Check if a column exists in this table
    pub fn has_column(&self, column_name: &str) -> bool {
        self.columns.iter().any(|c| c.name == column_name)
    }

    /// Get the index of a column by name
    pub fn column_index(&self, column_name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == column_name)
    }

    /// Validates a row against this table's schema.
    pub fn validate_row(&self, row: &Row) -> Result<()> {
        if row.len() != self.columns.len() {
            return Err(Error::InvalidValue(format!(
                "Row has {} columns, table {} has {}",
                row.len(),
                self.name,
                self.columns.len()
            )));
        }

        for (column, value) in self.columns.iter().zip(row.iter()) {
            // Check nullability
            if value.is_null() && !column.nullable {
                return Err(Error::NullConstraintViolation(column.name.clone()));
            }

            // Check data type
            if !value.is_null() {
                value.check_type(&column.data_type)?;
            }
        }

        Ok(())
    }

    /// Returns the column with the given name, if it exists.
    pub fn get_column(&self, name: &str) -> Option<(usize, &Column)> {
        self.columns
            .iter()
            .enumerate()
            .find(|(_, c)| c.name == name)
    }
}

/// A table column.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct Column {
    /// Column name. Can't be empty.
    pub name: String,
    /// Column data_type.
    pub data_type: DataType,
    /// Whether this is the primary key column.
    pub primary_key: bool,
    /// Whether the column allows null values. Not legal for primary keys.
    pub nullable: bool,
    /// The column's default expression. If None, the user must specify an explicit value.
    pub default: Option<crate::types::expression::DefaultExpression>,
    /// Whether the column should only allow unique values (ignoring NULLs).
    pub unique: bool,
    /// Whether the column should have a secondary index.
    pub index: bool,
    /// If set, this column is a foreign key reference to the given table's primary key.
    pub references: Option<String>,
}

impl Column {
    /// Creates a new column.
    pub fn new(name: String, data_type: DataType) -> Self {
        Column {
            name,
            data_type,
            primary_key: false,
            nullable: true,
            default: None,
            unique: false,
            index: false,
            references: None,
        }
    }

    /// Set the index flag for this column.
    pub fn with_index(mut self, index: bool) -> Self {
        self.index = index;
        self
    }

    /// Sets this column as the primary key.
    pub fn primary_key(mut self) -> Self {
        self.primary_key = true;
        self.nullable = false;
        self.unique = true;
        self.index = false; // Primary key is inherently indexed
        self
    }

    /// Sets whether this column is nullable.
    pub fn nullable(mut self, nullable: bool) -> Self {
        if self.primary_key && nullable {
            // Primary keys can't be nullable, ignore
            return self;
        }
        self.nullable = nullable;
        self
    }

    /// Sets the default expression for this column.
    pub fn default(mut self, expr: crate::types::expression::DefaultExpression) -> Self {
        self.default = Some(expr);
        self
    }

    /// Sets this column as unique.
    pub fn unique(mut self) -> Self {
        self.unique = true;
        if !self.primary_key {
            self.index = true; // Unique columns need an index
        }
        self
    }
}

// Formats the table as a SQL CREATE TABLE statement.
impl Display for Table {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "CREATE TABLE {} (", self.name)?;
        for (i, column) in self.columns.iter().enumerate() {
            write!(f, "  {} {}", column.name, column.data_type)?;

            if column.primary_key {
                write!(f, " PRIMARY KEY")?;
            } else if !column.nullable {
                write!(f, " NOT NULL")?;
            }

            if let Some(default) = &column.default {
                write!(f, " DEFAULT {}", default)?;
            }

            if !column.primary_key {
                if column.unique {
                    write!(f, " UNIQUE")?;
                }
                if column.index && !column.unique && column.references.is_none() {
                    write!(f, " INDEX")?;
                }
                if let Some(ref table) = column.references {
                    write!(f, " REFERENCES {}", table)?;
                }
            }

            if i < self.columns.len() - 1 {
                writeln!(f, ",")?;
            }
        }
        writeln!(f, "\n)")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::value::Value;

    #[test]
    fn test_table_creation() {
        let columns = vec![
            Column::new("id".into(), DataType::I64).primary_key(),
            Column::new("name".into(), DataType::Str).nullable(false),
            Column::new("email".into(), DataType::Str).unique(),
        ];

        let table = Table::new("users".into(), columns).unwrap();
        assert_eq!(table.name, "users");
        assert_eq!(table.primary_key, Some(0));
        assert_eq!(table.columns.len(), 3);
        assert!(table.columns[0].primary_key);
        assert!(!table.columns[0].nullable);
        assert!(table.columns[2].unique);
        assert!(table.columns[2].index);
    }

    #[test]
    fn test_table_validation_errors() {
        // No primary key is now allowed
        let columns = vec![
            Column::new("id".into(), DataType::I64),
            Column::new("name".into(), DataType::Str),
        ];
        assert!(Table::new("nopk".into(), columns).is_ok());

        // Multiple primary keys
        let columns = vec![
            Column::new("id1".into(), DataType::I64).primary_key(),
            Column::new("id2".into(), DataType::I64).primary_key(),
        ];
        assert!(Table::new("multipk".into(), columns).is_err());
    }

    #[test]
    fn test_row_validation() {
        let columns = vec![
            Column::new("id".into(), DataType::I64).primary_key(),
            Column::new("name".into(), DataType::Str).nullable(false),
            Column::new("age".into(), DataType::I64).nullable(true),
        ];
        let table = Table::new("users".into(), columns).unwrap();

        // Valid row
        let row = vec![
            Value::integer(1),
            Value::string("Alice"),
            Value::integer(30),
        ];
        assert!(table.validate_row(&row).is_ok());

        // Wrong number of columns
        let row = vec![Value::integer(1), Value::string("Bob")];
        assert!(table.validate_row(&row).is_err());

        // Null in non-nullable column
        let row = vec![Value::integer(2), Value::Null, Value::integer(25)];
        assert!(table.validate_row(&row).is_err());

        // Wrong type
        let row = vec![
            Value::string("not_an_int"),
            Value::string("Charlie"),
            Value::integer(35),
        ];
        assert!(table.validate_row(&row).is_err());
    }
}
