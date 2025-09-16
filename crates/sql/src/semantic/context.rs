//! Analysis context for tracking state during semantic analysis

use super::types::{ParameterExpectation, StatementMetadata};
use crate::error::{Error, Result};
use crate::types::data_type::DataType;
use crate::types::schema::Table;
use std::collections::HashMap;

/// Information about a table in the current context
#[derive(Debug, Clone)]
pub struct TableInfo {
    /// The actual table name in the schema
    pub name: String,
    /// Optional alias for this table
    pub alias: Option<String>,
    /// Schema information
    pub schema: Table,
}

/// Information about a column reference
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    /// Table this column belongs to
    pub table_name: String,
    /// Column name
    pub column_name: String,
    /// Column data type
    pub data_type: DataType,
    /// Whether the column is nullable
    pub nullable: bool,
}

/// Context for semantic analysis
pub struct AnalysisContext {
    /// Available schemas
    schemas: HashMap<String, Table>,
    /// Tables in current query scope
    tables: Vec<TableInfo>,
    /// Metadata being collected
    metadata: StatementMetadata,
}

impl AnalysisContext {
    /// Create a new analysis context
    pub fn new(schemas: HashMap<String, Table>) -> Self {
        Self {
            schemas,
            tables: Vec::new(),
            metadata: StatementMetadata::new(),
        }
    }

    /// Add a table to the current context
    pub fn add_table(&mut self, table_name: String, alias: Option<String>) -> Result<()> {
        // Check if table exists in schemas
        let schema = self
            .schemas
            .get(&table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.clone()))?
            .clone();

        // Check for duplicate aliases
        if let Some(ref alias) = alias
            && self.tables.iter().any(|t| t.alias.as_ref() == Some(alias))
        {
            return Err(Error::DuplicateTable(alias.clone()));
        }

        self.tables.push(TableInfo {
            name: table_name.clone(),
            alias,
            schema,
        });

        self.metadata.referenced_tables.insert(table_name);
        Ok(())
    }

    /// Resolve a column reference
    pub fn resolve_column(
        &mut self,
        table_ref: Option<&str>,
        column_name: &str,
    ) -> Result<ColumnInfo> {
        // If table reference is provided, look for that specific table
        if let Some(table_ref) = table_ref {
            // First try to find a matching table or alias
            if let Some(table) = self
                .tables
                .iter()
                .find(|t| t.name == table_ref || t.alias.as_deref() == Some(table_ref))
            {
                // Found a table - look for the column in it
                let column = table
                    .schema
                    .columns
                    .iter()
                    .find(|c| c.name == column_name)
                    .ok_or_else(|| Error::ColumnNotFound(column_name.to_string()))?;

                self.metadata
                    .referenced_columns
                    .insert((table.name.clone(), column_name.to_string()));

                return Ok(ColumnInfo {
                    table_name: table.name.clone(),
                    column_name: column_name.to_string(),
                    data_type: column.datatype.clone(),
                    nullable: column.nullable,
                });
            }

            // Didn't find a table - check if table_ref is actually a column with struct type
            // This handles cases like "details.name" where details is a STRUCT column
            for table in &self.tables {
                if let Some(column) = table.schema.columns.iter().find(|c| c.name == table_ref) {
                    // Found a column with this name - check if it's a struct
                    if matches!(&column.datatype, DataType::Struct(_)) {
                        // This is a struct field access, not a table.column reference
                        // Return a synthetic column info for the field access
                        // The actual field validation will happen during type checking
                        self.metadata
                            .referenced_columns
                            .insert((table.name.clone(), table_ref.to_string()));

                        return Ok(ColumnInfo {
                            table_name: table.name.clone(),
                            column_name: format!("{}.{}", table_ref, column_name),
                            data_type: DataType::Nullable(Box::new(DataType::Text)), // Will be refined during type checking
                            nullable: true,
                        });
                    }
                }
            }

            // Neither a table nor a struct column was found
            return Err(Error::TableNotFound(table_ref.to_string()));
        }

        // No table reference - search all tables
        let mut found = None;
        for table in &self.tables {
            if let Some(column) = table.schema.columns.iter().find(|c| c.name == column_name) {
                if found.is_some() {
                    // Ambiguous column reference
                    return Err(Error::ExecutionError(format!(
                        "Column '{}' is ambiguous",
                        column_name
                    )));
                }
                found = Some((table, column));
            }
        }

        let (table, column) =
            found.ok_or_else(|| Error::ColumnNotFound(column_name.to_string()))?;

        self.metadata
            .referenced_columns
            .insert((table.name.clone(), column_name.to_string()));

        Ok(ColumnInfo {
            table_name: table.name.clone(),
            column_name: column_name.to_string(),
            data_type: column.datatype.clone(),
            nullable: column.nullable,
        })
    }

    /// Add a parameter expectation
    pub fn add_parameter_expectation(&mut self, expectation: ParameterExpectation) {
        // Check if we already have an expectation for this parameter
        if let Some(existing) = self
            .metadata
            .parameter_expectations
            .iter_mut()
            .find(|e| e.index == expectation.index)
        {
            // Merge the type expectations
            for typ in expectation.expected_types {
                if !existing.expected_types.contains(&typ) {
                    existing.expected_types.push(typ);
                }
            }
            // Update context if more specific
            if expectation.context.len() > existing.context.len() {
                existing.context = expectation.context;
            }
        } else {
            self.metadata.parameter_expectations.push(expectation);
        }
    }

    /// Get the current metadata
    pub fn metadata(&self) -> &StatementMetadata {
        &self.metadata
    }

    /// Consume the context and return the metadata
    pub fn into_metadata(self) -> StatementMetadata {
        self.metadata
    }

    /// Get available schemas
    pub fn schemas(&self) -> &HashMap<String, Table> {
        &self.schemas
    }

    /// Set whether the statement is deterministic
    pub fn set_deterministic(&mut self, deterministic: bool) {
        self.metadata.is_deterministic = deterministic;
    }

    /// Set whether the statement is a mutation
    pub fn set_mutation(&mut self, is_mutation: bool) {
        self.metadata.is_mutation = is_mutation;
    }
}
