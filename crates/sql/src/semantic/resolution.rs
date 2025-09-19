//! Name resolution phase of semantic analysis
//!
//! This module handles all name resolution, including:
//! - Table and alias resolution
//! - Column name resolution and ambiguity detection
//! - Function name resolution
//! - Building the column resolution map for O(1) lookups

use super::statement::{ColumnResolution, ColumnResolutionMap};
use crate::error::{Error, Result};
use crate::parsing::ast::{DmlStatement, FromClause, Statement};
use crate::types::schema::Table;
use std::collections::HashMap;

/// Handles all name resolution in the semantic analysis phase
pub struct NameResolver {
    /// Available table schemas
    schemas: HashMap<String, Table>,
}

impl NameResolver {
    /// Create a new name resolver
    pub fn new(schemas: HashMap<String, Table>) -> Self {
        Self { schemas }
    }

    /// Extract tables from a statement
    pub fn extract_tables(&self, statement: &Statement) -> Result<Vec<(Option<String>, String)>> {
        let mut tables = Vec::new();

        if let Statement::Dml(dml) = statement {
            match dml {
                DmlStatement::Select(select) => {
                    for from_clause in &select.from {
                        self.extract_from_tables(from_clause, &mut tables)?;
                    }
                }
                DmlStatement::Insert { table, .. } => {
                    self.validate_table_exists(table)?;
                    tables.push((None, table.clone()));
                }
                DmlStatement::Update { table, .. } => {
                    self.validate_table_exists(table)?;
                    tables.push((None, table.clone()));
                }
                DmlStatement::Delete { table, .. } => {
                    self.validate_table_exists(table)?;
                    tables.push((None, table.clone()));
                }
                DmlStatement::Values(_) => {
                    // VALUES statements don't reference tables
                }
            }
        }

        Ok(tables)
    }

    /// Extract tables from FROM clause
    fn extract_from_tables(
        &self,
        from: &FromClause,
        tables: &mut Vec<(Option<String>, String)>,
    ) -> Result<()> {
        match from {
            FromClause::Table { name, alias } => {
                self.validate_table_exists(name)?;
                tables.push((alias.clone(), name.clone()));
            }
            FromClause::Join { left, right, .. } => {
                self.extract_from_tables(left, tables)?;
                self.extract_from_tables(right, tables)?;
            }
        }
        Ok(())
    }

    /// Build column map from resolved tables
    pub fn build_column_map(
        &self,
        tables: &[(Option<String>, String)],
        schemas: &HashMap<String, Table>,
    ) -> Result<ColumnResolutionMap> {
        let mut resolution_map = ColumnResolutionMap::default();
        let mut column_counts: HashMap<String, usize> = HashMap::new();
        let mut global_offset = 0;

        for (alias, table_name) in tables.iter() {
            if let Some(schema) = schemas.get(table_name) {
                for column in schema.columns.iter() {
                    // Track column name frequency
                    *column_counts.entry(column.name.clone()).or_insert(0) += 1;

                    let resolution = ColumnResolution {
                        global_offset,
                        table_name: table_name.clone(),
                        data_type: column.datatype.clone(),
                        nullable: column.nullable,
                        is_indexed: column.index,
                    };

                    let table_qualifier = alias.clone().unwrap_or_else(|| table_name.clone());
                    resolution_map.add_resolution(
                        Some(table_qualifier),
                        column.name.clone(),
                        resolution,
                    );

                    global_offset += 1;
                }
            }
        }

        // Mark ambiguous columns
        for (column_name, count) in column_counts {
            if count > 1 {
                resolution_map.mark_ambiguous(column_name);
            }
        }

        Ok(resolution_map)
    }

    /// Validate that a table exists in the schema
    fn validate_table_exists(&self, table_name: &str) -> Result<()> {
        if !self.schemas.contains_key(table_name) {
            return Err(Error::TableNotFound(table_name.to_string()));
        }
        Ok(())
    }
}
