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
use crate::parsing::ast::{DmlStatement, Expression, FromClause, SelectStatement, Statement};
use crate::types::{DataType, schema::Table};
use std::collections::HashMap;

/// Information about a table source (regular table or subquery)
#[derive(Debug, Clone)]
pub enum TableSource {
    /// Regular table from schema
    Table {
        alias: Option<String>,
        name: String,
        column_aliases: Vec<String>,
    },
    /// Subquery with generated columns
    Subquery {
        alias: String,
        source: SubquerySource,
        column_aliases: Vec<String>,
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
                        column_aliases: Vec::new(),
                    });
                }
                DmlStatement::Update { table, .. } => {
                    self.validate_table_exists(table)?;
                    sources.push(TableSource::Table {
                        alias: None,
                        name: table.clone(),
                        column_aliases: Vec::new(),
                    });
                }
                DmlStatement::Delete { table, .. } => {
                    self.validate_table_exists(table)?;
                    sources.push(TableSource::Table {
                        alias: None,
                        name: table.clone(),
                        column_aliases: Vec::new(),
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
                    alias: alias.as_ref().map(|a| a.name.clone()),
                    name: name.clone(),
                    column_aliases: alias
                        .as_ref()
                        .map(|a| a.columns.clone())
                        .unwrap_or_default(),
                });
            }
            FromClause::Subquery { source, alias } => {
                // For subqueries, we keep the source information
                sources.push(TableSource::Subquery {
                    alias: alias.name.clone(),
                    source: source.clone(),
                    column_aliases: alias.columns.clone(),
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
                TableSource::Table {
                    alias,
                    name,
                    column_aliases,
                } => {
                    if let Some(schema) = schemas.get(name) {
                        // Validate column aliases count
                        if !column_aliases.is_empty() && column_aliases.len() > schema.columns.len()
                        {
                            return Err(Error::TooManyColumnAliases(
                                alias.clone().unwrap_or_else(|| name.clone()),
                                schema.columns.len(),
                                column_aliases.len(),
                            ));
                        }

                        for (idx, column) in schema.columns.iter().enumerate() {
                            // Use column alias if provided, otherwise use original name
                            let column_name = if idx < column_aliases.len() {
                                column_aliases[idx].clone()
                            } else {
                                column.name.clone()
                            };

                            // Track column name frequency
                            *column_counts.entry(column_name.clone()).or_insert(0) += 1;

                            let resolution = ColumnResolution {
                                global_offset,
                                table_name: name.clone(),
                                data_type: column.data_type.clone(),
                                nullable: column.nullable,
                                is_indexed: column.index,
                            };

                            let table_qualifier = alias.clone().unwrap_or_else(|| name.clone());
                            resolution_map.add_resolution(
                                Some(table_qualifier),
                                column_name,
                                resolution,
                            );

                            global_offset += 1;
                        }
                    }
                }
                TableSource::Subquery {
                    alias,
                    source,
                    column_aliases,
                } => {
                    // For subqueries, create synthetic columns
                    match source {
                        SubquerySource::Values(values_stmt) => {
                            // VALUES creates columns named column1, column2, etc.
                            // Use the first row to determine column count and types
                            if let Some(first_row) = values_stmt.rows.first() {
                                let total_columns = first_row.len();

                                // Validate column aliases count
                                if !column_aliases.is_empty()
                                    && column_aliases.len() > total_columns
                                {
                                    return Err(Error::TooManyColumnAliases(
                                        alias.clone(),
                                        total_columns,
                                        column_aliases.len(),
                                    ));
                                }

                                for (idx, _expr) in first_row.iter().enumerate() {
                                    // Use column alias if provided, otherwise use default name
                                    let column_name = if idx < column_aliases.len() {
                                        column_aliases[idx].clone()
                                    } else {
                                        format!("column{}", idx + 1)
                                    };

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
                        SubquerySource::Select(select_stmt) => {
                            // Recursively analyze the SELECT subquery to get its output schema
                            let subquery_columns =
                                self.extract_select_output_columns(select_stmt, schemas)?;

                            // Validate column aliases count
                            if !column_aliases.is_empty()
                                && column_aliases.len() > subquery_columns.len()
                            {
                                return Err(Error::TooManyColumnAliases(
                                    alias.clone(),
                                    subquery_columns.len(),
                                    column_aliases.len(),
                                ));
                            }

                            // Build column resolutions for each output column
                            for (idx, (col_name, col_type, nullable)) in
                                subquery_columns.iter().enumerate()
                            {
                                // Use column alias if provided, otherwise use the column name from SELECT
                                let final_col_name = if idx < column_aliases.len() {
                                    column_aliases[idx].clone()
                                } else {
                                    col_name.clone()
                                };

                                // Track column name frequency
                                *column_counts.entry(final_col_name.clone()).or_insert(0) += 1;

                                let resolution = ColumnResolution {
                                    global_offset,
                                    table_name: alias.clone(),
                                    data_type: col_type.clone(),
                                    nullable: *nullable,
                                    is_indexed: false,
                                };

                                // Add with subquery alias qualifier
                                resolution_map.add_resolution(
                                    Some(alias.clone()),
                                    final_col_name.clone(),
                                    resolution.clone(),
                                );

                                // Also add without qualifier for unambiguous lookups
                                resolution_map.add_resolution(None, final_col_name, resolution);

                                global_offset += 1;
                            }
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

    /// Extract output column schema from a SELECT statement
    /// Returns Vec<(column_name, data_type, nullable)>
    fn extract_select_output_columns(
        &self,
        select_stmt: &SelectStatement,
        _schemas: &HashMap<String, Table>,
    ) -> Result<Vec<(String, DataType, bool)>> {
        // Recursively analyze the subquery to determine its output types
        let subquery_analyzer = super::analyzer::SemanticAnalyzer::new(self.schemas.clone());
        let subquery_statement =
            Statement::Dml(DmlStatement::Select(Box::new(select_stmt.clone())));
        let analyzed = subquery_analyzer.analyze(subquery_statement, Vec::new())?;

        let mut columns = Vec::new();

        // Process each projection in the SELECT list
        for (expr, alias) in &select_stmt.select {
            match expr {
                Expression::All | Expression::QualifiedWildcard(_) => {
                    // Wildcards expand to all available columns from the inner query's column map
                    // We need to get all columns from the analyzed subquery's resolution map
                    let col_map = &analyzed.column_resolution_map;

                    // Collect all resolutions and sort by global_offset to maintain order
                    let mut all_cols: Vec<_> = col_map
                        .columns
                        .iter()
                        .filter(|((table_qualifier, _), _)| {
                            // For All, include all columns
                            // For QualifiedWildcard, filter by table
                            match expr {
                                Expression::All => table_qualifier.is_some(), // Skip duplicate unqualified entries
                                Expression::QualifiedWildcard(table) => {
                                    table_qualifier.as_deref() == Some(table.as_str())
                                }
                                _ => false,
                            }
                        })
                        .collect();

                    // Sort by global offset to maintain column order
                    all_cols.sort_by_key(|(_, resolution)| resolution.global_offset);

                    // Add each expanded column
                    for ((_, col_name), resolution) in all_cols {
                        columns.push((
                            col_name.clone(),
                            resolution.data_type.clone(),
                            resolution.nullable,
                        ));
                    }
                }
                _ => {
                    // For non-wildcard expressions, determine the column name
                    let col_name = if let Some(alias_name) = alias {
                        // Use explicit alias
                        alias_name.clone()
                    } else {
                        // Infer name from expression
                        match expr {
                            Expression::Column(_table, col) => col.clone(),
                            _ => {
                                // For expressions without alias, generate a name
                                format!("?column?")
                            }
                        }
                    };

                    // Get the type information from the analyzed statement
                    // For now, we'll use a conservative approach and default to nullable text
                    // A more complete implementation would track expression IDs and look them up
                    let data_type = DataType::Text; // Conservative default
                    let nullable = true; // Conservative default

                    columns.push((col_name, data_type, nullable));
                }
            }
        }

        Ok(columns)
    }

    /// Validate that a table exists in the schema
    fn validate_table_exists(&self, table_name: &str) -> Result<()> {
        if !self.schemas.contains_key(table_name) {
            return Err(Error::TableNotFound(table_name.to_string()));
        }
        Ok(())
    }
}
