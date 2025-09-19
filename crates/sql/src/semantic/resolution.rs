//! Name resolution phase of semantic analysis
//!
//! This module handles all name resolution, including:
//! - Table and alias resolution
//! - Column name resolution and ambiguity detection
//! - Function name resolution
//! - Building the column resolution map for O(1) lookups

use super::statement::{ColumnResolution, ColumnResolutionMap};
use crate::error::{Error, Result};
use crate::parsing::ast::common::SubquerySource;
use crate::parsing::ast::{DmlStatement, FromClause, Statement};
use crate::types::{DataType, schema::Table};
use std::collections::HashMap;

/// Information about a table source (regular table or subquery)
#[derive(Debug, Clone)]
pub enum TableSource {
    /// Regular table from schema
    Table { alias: Option<String>, name: String },
    /// Subquery with generated columns
    Subquery {
        alias: String,
        source: SubquerySource,
    },
}

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

    /// Extract table sources from a statement
    pub fn extract_table_sources(&self, statement: &Statement) -> Result<Vec<TableSource>> {
        let mut sources = Vec::new();

        if let Statement::Dml(dml) = statement {
            match dml {
                DmlStatement::Select(select) => {
                    for from_clause in &select.from {
                        self.extract_from_sources(from_clause, &mut sources)?;
                    }
                }
                DmlStatement::Insert { table, .. } => {
                    self.validate_table_exists(table)?;
                    sources.push(TableSource::Table {
                        alias: None,
                        name: table.clone(),
                    });
                }
                DmlStatement::Update { table, .. } => {
                    self.validate_table_exists(table)?;
                    sources.push(TableSource::Table {
                        alias: None,
                        name: table.clone(),
                    });
                }
                DmlStatement::Delete { table, .. } => {
                    self.validate_table_exists(table)?;
                    sources.push(TableSource::Table {
                        alias: None,
                        name: table.clone(),
                    });
                }
                DmlStatement::Values(_) => {
                    // VALUES statements don't reference tables
                }
            }
        }

        Ok(sources)
    }

    /// Extract table sources from FROM clause
    fn extract_from_sources(
        &self,
        from: &FromClause,
        sources: &mut Vec<TableSource>,
    ) -> Result<()> {
        match from {
            FromClause::Table { name, alias } => {
                self.validate_table_exists(name)?;
                sources.push(TableSource::Table {
                    alias: alias.clone(),
                    name: name.clone(),
                });
            }
            FromClause::Subquery { source, alias } => {
                // For subqueries, we keep the source information
                sources.push(TableSource::Subquery {
                    alias: alias.clone(),
                    source: source.clone(),
                });
            }
            FromClause::Join { left, right, .. } => {
                self.extract_from_sources(left, sources)?;
                self.extract_from_sources(right, sources)?;
            }
        }
        Ok(())
    }

    /// Build column map from resolved table sources
    pub fn build_column_map(
        &self,
        sources: &[TableSource],
        schemas: &HashMap<String, Table>,
    ) -> Result<ColumnResolutionMap> {
        let mut resolution_map = ColumnResolutionMap::default();
        let mut column_counts: HashMap<String, usize> = HashMap::new();
        let mut global_offset = 0;

        for source in sources {
            match source {
                TableSource::Table { alias, name } => {
                    if let Some(schema) = schemas.get(name) {
                        for column in schema.columns.iter() {
                            // Track column name frequency
                            *column_counts.entry(column.name.clone()).or_insert(0) += 1;

                            let resolution = ColumnResolution {
                                global_offset,
                                table_name: name.clone(),
                                data_type: column.datatype.clone(),
                                nullable: column.nullable,
                                is_indexed: column.index,
                            };

                            let table_qualifier = alias.clone().unwrap_or_else(|| name.clone());
                            resolution_map.add_resolution(
                                Some(table_qualifier),
                                column.name.clone(),
                                resolution,
                            );

                            global_offset += 1;
                        }
                    }
                }
                TableSource::Subquery { alias, source } => {
                    // For subqueries, create synthetic columns
                    match source {
                        SubquerySource::Values(values_stmt) => {
                            // VALUES creates columns named column1, column2, etc.
                            // Use the first row to determine column count and types
                            if let Some(first_row) = values_stmt.rows.first() {
                                for (idx, _expr) in first_row.iter().enumerate() {
                                    let column_name = format!("column{}", idx + 1);

                                    // Track column name frequency
                                    *column_counts.entry(column_name.clone()).or_insert(0) += 1;

                                    // For VALUES, we don't know the exact type yet, use Null as placeholder
                                    // The typing phase will determine the actual type
                                    let resolution = ColumnResolution {
                                        global_offset,
                                        table_name: alias.clone(),
                                        data_type: DataType::Null,
                                        nullable: true,
                                        is_indexed: false,
                                    };

                                    resolution_map.add_resolution(
                                        Some(alias.clone()),
                                        column_name.clone(),
                                        resolution.clone(),
                                    );

                                    // Also add without table qualifier for column references
                                    resolution_map.add_resolution(None, column_name, resolution);

                                    global_offset += 1;
                                }
                            }
                        }
                        SubquerySource::Select(_select_stmt) => {
                            // For SELECT subqueries, we'd need to analyze the SELECT to get columns
                            // This is more complex and would require recursive analysis
                            // For now, we'll skip this as it's not needed for VALUES
                        }
                    }
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
