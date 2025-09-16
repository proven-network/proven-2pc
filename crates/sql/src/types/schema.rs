//! SQL schema types (tables and columns)
//!
//! Adapted from toydb for proven-sql's PCC architecture.
//! Tables are immutable after creation - no ALTER TABLE support.

use super::data_type::DataType;
use super::value::{Row, Value};
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
    /// The primary key column index. None if no primary key specified.
    /// Note: Tables still have internal row IDs for storage and locking.
    pub primary_key: Option<usize>,
    /// The table's columns. Must have at least one.
    pub columns: Vec<Column>,
}

impl Table {
    /// Creates a new table schema.
    pub fn new(name: String, columns: Vec<Column>) -> Result<Self> {
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

        for (column, value) in self.columns.iter().zip(row.iter()) {
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

    /// Validates that a value matches this column's data type and constraints.
    pub fn validate_value(&self, value: &Value) -> Result<()> {
        // Check nullable constraint
        if value == &Value::Null {
            if !self.nullable {
                return Err(Error::NullConstraintViolation(self.name.clone()));
            }
            return Ok(()); // NULL is valid for nullable columns
        }

        // Check data type match for collections
        match (&self.datatype, value) {
            // Array validation
            (DataType::Array(elem_type, Some(size)), Value::Array(vals)) => {
                // Check array has exactly 'size' elements
                if vals.len() != *size {
                    return Err(Error::InvalidValue(format!(
                        "Array column '{}' expects {} elements, got {}",
                        self.name,
                        size,
                        vals.len()
                    )));
                }
                // Check each element matches elem_type
                for (i, val) in vals.iter().enumerate() {
                    if !val.is_null() {
                        val.check_type(elem_type).map_err(|_| {
                            Error::InvalidValue(format!(
                                "Array element {} in column '{}' has wrong type",
                                i, self.name
                            ))
                        })?;
                    }
                }
            }
            (DataType::Array(elem_type, None), Value::Array(vals)) => {
                // Variable-size array - just check element types
                for (i, val) in vals.iter().enumerate() {
                    if !val.is_null() {
                        val.check_type(elem_type).map_err(|_| {
                            Error::InvalidValue(format!(
                                "Array element {} in column '{}' has wrong type",
                                i, self.name
                            ))
                        })?;
                    }
                }
            }

            // List validation
            (DataType::List(elem_type), Value::List(vals)) => {
                // Check each element matches elem_type
                for (i, val) in vals.iter().enumerate() {
                    if !val.is_null() {
                        val.check_type(elem_type).map_err(|_| {
                            Error::InvalidValue(format!(
                                "List element {} in column '{}' has wrong type",
                                i, self.name
                            ))
                        })?;
                    }
                }
            }

            // Map validation
            (DataType::Map(key_type, val_type), Value::Map(map)) => {
                // All keys are strings in our HashMap implementation
                // Check that key_type is String
                if !matches!(key_type.as_ref(), DataType::Str) {
                    return Err(Error::InvalidValue(format!(
                        "Map column '{}' requires String keys",
                        self.name
                    )));
                }
                // Check all values match val_type
                for (key, val) in map.iter() {
                    if !val.is_null() {
                        val.check_type(val_type).map_err(|_| {
                            Error::InvalidValue(format!(
                                "Map value for key '{}' in column '{}' has wrong type",
                                key, self.name
                            ))
                        })?;
                    }
                }
            }

            // Struct validation
            (DataType::Struct(expected_fields), Value::Struct(actual_fields)) => {
                // Check all required fields are present and have correct types
                if expected_fields.len() != actual_fields.len() {
                    return Err(Error::InvalidValue(format!(
                        "Struct column '{}' expects {} fields, got {}",
                        self.name,
                        expected_fields.len(),
                        actual_fields.len()
                    )));
                }
                for (expected, actual) in expected_fields.iter().zip(actual_fields.iter()) {
                    if expected.0 != actual.0 {
                        return Err(Error::InvalidValue(format!(
                            "Struct field mismatch in column '{}': expected '{}', got '{}'",
                            self.name, expected.0, actual.0
                        )));
                    }
                    if !actual.1.is_null() {
                        actual.1.check_type(&expected.1).map_err(|_| {
                            Error::InvalidValue(format!(
                                "Struct field '{}' in column '{}' has wrong type",
                                actual.0, self.name
                            ))
                        })?;
                    }
                }
            }

            // For non-collection types, use the existing check_type method
            _ => {
                value.check_type(&self.datatype)?;
            }
        }

        Ok(())
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
