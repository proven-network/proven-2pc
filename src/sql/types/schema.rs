//! SQL schema types (tables and columns)
//!
//! Adapted from toydb for proven-sql's PCC architecture.
//! Tables are immutable after creation - no ALTER TABLE support.

use super::value::{DataType, Row, Value};
use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// A table schema, which specifies its data structure and constraints.
///
/// Tables can't change after they are created. There is no ALTER TABLE nor
/// CREATE/DROP INDEX, only CREATE TABLE and DROP TABLE.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct Table {
    /// The table name. Unique identifier for the table. Can't be empty.
    pub name: String,
    /// The primary key column index. A table must have a primary key.
    pub primary_key: usize,
    /// The table's columns. Must have at least one.
    pub columns: Vec<Column>,
}

impl Table {
    /// Creates a new table schema.
    pub fn new(name: String, columns: Vec<Column>) -> Result<Self> {
        if name.is_empty() {
            return Err(Error::InvalidValue("Table name cannot be empty".into()));
        }
        if columns.is_empty() {
            return Err(Error::InvalidValue(
                "Table must have at least one column".into(),
            ));
        }

        // Find the primary key column
        let primary_keys: Vec<_> = columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.primary_key)
            .map(|(i, _)| i)
            .collect();

        if primary_keys.is_empty() {
            return Err(Error::InvalidValue("Table must have a primary key".into()));
        }
        if primary_keys.len() > 1 {
            return Err(Error::InvalidValue(
                "Table can only have one primary key".into(),
            ));
        }

        let primary_key = primary_keys[0];

        // Validate primary key column
        let pk_column = &columns[primary_key];
        if pk_column.nullable {
            return Err(Error::InvalidValue("Primary key cannot be nullable".into()));
        }
        if pk_column.default.is_some() {
            return Err(Error::InvalidValue(
                "Primary key cannot have a default value".into(),
            ));
        }

        Ok(Table {
            name,
            primary_key,
            columns,
        })
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

        for (_i, (column, value)) in self.columns.iter().zip(row.iter()).enumerate() {
            // Check nullability
            if value.is_null() && !column.nullable {
                return Err(Error::NullConstraintViolation(column.name.clone()));
            }

            // Check data type
            if !value.is_null() {
                value.check_type(&column.datatype)?;
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

    /// Returns true if the table has any indexed columns (besides primary key).
    pub fn has_indexes(&self) -> bool {
        self.columns.iter().any(|c| c.index && !c.primary_key)
    }
}

/// A table column.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct Column {
    /// Column name. Can't be empty.
    pub name: String,
    /// Column datatype.
    pub datatype: DataType,
    /// Whether this is the primary key column.
    pub primary_key: bool,
    /// Whether the column allows null values. Not legal for primary keys.
    pub nullable: bool,
    /// The column's default value. If None, the user must specify an explicit value.
    pub default: Option<Value>,
    /// Whether the column should only allow unique values (ignoring NULLs).
    pub unique: bool,
    /// Whether the column should have a secondary index.
    pub index: bool,
    /// If set, this column is a foreign key reference to the given table's primary key.
    pub references: Option<String>,
}

impl Column {
    /// Creates a new column.
    pub fn new(name: String, datatype: DataType) -> Self {
        Column {
            name,
            datatype,
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

    /// Sets the default value for this column.
    pub fn default(mut self, value: Value) -> Self {
        self.default = Some(value);
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

    /// Sets this column as indexed.
    pub fn indexed(mut self) -> Self {
        if !self.primary_key {
            self.index = true;
        }
        self
    }

    /// Sets this column as a foreign key reference.
    pub fn references(mut self, table: String) -> Self {
        self.references = Some(table);
        if !self.primary_key {
            self.index = true; // Foreign keys need an index
        }
        self
    }
}

// Formats the table as a SQL CREATE TABLE statement.
impl Display for Table {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "CREATE TABLE {} (", self.name)?;
        for (i, column) in self.columns.iter().enumerate() {
            write!(f, "  {} {}", column.name, column.datatype)?;

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

/// Label for a column in query results.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Label {
    /// No label.
    None,
    /// An unqualified column name.
    Unqualified(String),
    /// A fully qualified table.column name.
    Qualified(String, String),
}

impl Display for Label {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Label::None => write!(f, "?"),
            Label::Unqualified(name) => write!(f, "{}", name),
            Label::Qualified(table, column) => write!(f, "{}.{}", table, column),
        }
    }
}

impl Label {
    /// Returns the column name part of the label.
    pub fn column(&self) -> Option<&str> {
        match self {
            Label::None => None,
            Label::Unqualified(name) => Some(name),
            Label::Qualified(_, column) => Some(column),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_creation() {
        let columns = vec![
            Column::new("id".into(), DataType::Integer).primary_key(),
            Column::new("name".into(), DataType::String).nullable(false),
            Column::new("email".into(), DataType::String).unique(),
        ];

        let table = Table::new("users".into(), columns).unwrap();
        assert_eq!(table.name, "users");
        assert_eq!(table.primary_key, 0);
        assert_eq!(table.columns.len(), 3);
        assert!(table.columns[0].primary_key);
        assert!(!table.columns[0].nullable);
        assert!(table.columns[2].unique);
        assert!(table.columns[2].index);
    }

    #[test]
    fn test_table_validation_errors() {
        // No columns
        assert!(Table::new("empty".into(), vec![]).is_err());

        // No primary key
        let columns = vec![
            Column::new("id".into(), DataType::Integer),
            Column::new("name".into(), DataType::String),
        ];
        assert!(Table::new("nopk".into(), columns).is_err());

        // Multiple primary keys
        let columns = vec![
            Column::new("id1".into(), DataType::Integer).primary_key(),
            Column::new("id2".into(), DataType::Integer).primary_key(),
        ];
        assert!(Table::new("multipk".into(), columns).is_err());
    }

    #[test]
    fn test_row_validation() {
        let columns = vec![
            Column::new("id".into(), DataType::Integer).primary_key(),
            Column::new("name".into(), DataType::String).nullable(false),
            Column::new("age".into(), DataType::Integer).nullable(true),
        ];
        let table = Table::new("users".into(), columns).unwrap();

        // Valid row
        let row = vec![
            Value::Integer(1),
            Value::String("Alice".into()),
            Value::Integer(30),
        ];
        assert!(table.validate_row(&row).is_ok());

        // Wrong number of columns
        let row = vec![Value::Integer(1), Value::String("Bob".into())];
        assert!(table.validate_row(&row).is_err());

        // Null in non-nullable column
        let row = vec![Value::Integer(2), Value::Null, Value::Integer(25)];
        assert!(table.validate_row(&row).is_err());

        // Wrong type
        let row = vec![
            Value::String("not_an_int".into()),
            Value::String("Charlie".into()),
            Value::Integer(35),
        ];
        assert!(table.validate_row(&row).is_err());
    }
}
