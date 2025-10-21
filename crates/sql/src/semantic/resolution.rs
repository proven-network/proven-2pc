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
use crate::parsing::ast::{DmlStatement, FromClause, SelectStatement, Statement};
use crate::planning::planner::Planner;
use crate::types::{DataType, schema::Table};
use std::collections::HashMap;
use std::sync::Arc;

/// Output schema from a SELECT subquery: (column_name, data_type, nullable)
type SubqueryOutputSchema = Vec<(String, DataType, bool)>;

/// Result from analyzing a SELECT subquery: schema and analyzed statement
type SubqueryAnalysisResult = (
    SubqueryOutputSchema,
    Arc<super::statement::AnalyzedStatement>,
);

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
    /// SERIES(N) table-valued function
    Series {
        alias: Option<String>,
        column_aliases: Vec<String>,
    },
    /// UNNEST(array) table-valued function
    Unnest {
        alias: Option<String>,
        column_aliases: Vec<String>,
    },
}

/// Handles all name resolution in the semantic analysis phase
pub struct NameResolver {
    /// Available table schemas
    schemas: HashMap<String, Table>,
    /// Index metadata for checking if columns are indexed
    index_metadata: HashMap<String, crate::types::index::IndexMetadata>,
    /// Optional outer query column map for correlated subqueries
    outer_column_map: Option<ColumnResolutionMap>,
}

impl NameResolver {
    /// Create a new name resolver
    pub fn new(
        schemas: HashMap<String, Table>,
        index_metadata: HashMap<String, crate::types::index::IndexMetadata>,
    ) -> Self {
        Self {
            schemas,
            index_metadata,
            outer_column_map: None,
        }
    }

    /// Create a name resolver with outer query context for correlated subqueries
    pub fn with_outer_context(
        schemas: HashMap<String, Table>,
        index_metadata: HashMap<String, crate::types::index::IndexMetadata>,
        outer_column_map: ColumnResolutionMap,
    ) -> Self {
        Self {
            schemas,
            index_metadata,
            outer_column_map: Some(outer_column_map),
        }
    }

    /// Extract table sources from a statement
    pub fn extract_table_sources(&self, statement: &Statement) -> Result<Vec<TableSource>> {
        let mut sources = Vec::new();

        if let Statement::Dml(dml) = statement {
            match dml {
                DmlStatement::Select(select) => {
                    if select.from.is_empty() {
                        // SELECT without FROM defaults to SERIES(1)
                        sources.push(TableSource::Series {
                            alias: None,
                            column_aliases: Vec::new(),
                        });
                    } else {
                        for from_clause in &select.from {
                            self.extract_from_sources(from_clause, &mut sources)?;
                        }
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
            FromClause::Series { alias, .. } => {
                // SERIES(N) generates a single column named "n" (lowercase)
                sources.push(TableSource::Series {
                    alias: alias.as_ref().map(|a| a.name.clone()),
                    column_aliases: alias
                        .as_ref()
                        .map(|a| a.columns.clone())
                        .unwrap_or_default(),
                });
            }
            FromClause::Unnest { alias, .. } => {
                // UNNEST(array) generates a single column for the unnested values
                sources.push(TableSource::Unnest {
                    alias: alias.as_ref().map(|a| a.name.clone()),
                    column_aliases: alias
                        .as_ref()
                        .map(|a| a.columns.clone())
                        .unwrap_or_default(),
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
    /// Returns (column_resolution_map, subquery_analyses)
    pub fn build_column_map(
        &self,
        sources: &[TableSource],
        schemas: &HashMap<String, Table>,
    ) -> Result<(
        ColumnResolutionMap,
        HashMap<String, Arc<super::statement::AnalyzedStatement>>,
    )> {
        let mut resolution_map = ColumnResolutionMap::default();
        let mut column_counts: HashMap<String, usize> = HashMap::new();
        let mut offset = 0;
        let mut subquery_analyses: HashMap<String, Arc<super::statement::AnalyzedStatement>> =
            HashMap::new();

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

                            // Check if column is indexed either via inline declaration or CREATE INDEX
                            let is_indexed =
                                column.index || self.is_column_indexed(name, &column.name);

                            let resolution = ColumnResolution {
                                offset,
                                table_name: name.clone(),
                                data_type: column.data_type.clone(),
                                nullable: column.nullable,
                                is_indexed,
                            };

                            let table_qualifier = alias.clone().unwrap_or_else(|| name.clone());
                            resolution_map.add_resolution(
                                Some(table_qualifier),
                                column_name,
                                resolution,
                            );

                            offset += 1;
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
                                        offset,
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

                                    offset += 1;
                                }
                            }
                        }
                        SubquerySource::Select(select_stmt) => {
                            // Recursively analyze the SELECT subquery to get its output schema
                            let (subquery_columns, subquery_analyzed) =
                                self.extract_select_output_columns(select_stmt, schemas)?;

                            // Store the analyzed subquery for later use by the planner
                            subquery_analyses.insert(alias.clone(), subquery_analyzed);

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
                                    offset,
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

                                offset += 1;
                            }
                        }
                    }
                }
                TableSource::Series {
                    alias,
                    column_aliases,
                } => {
                    // SERIES(N) generates a single column named "N" of type I64
                    // Validate column aliases count (should be 0 or 1)
                    if !column_aliases.is_empty() && column_aliases.len() > 1 {
                        return Err(Error::TooManyColumnAliases(
                            alias.clone().unwrap_or_else(|| "SERIES".to_string()),
                            1,
                            column_aliases.len(),
                        ));
                    }

                    // Use column alias if provided, otherwise use "n" (lowercase for case-insensitive matching)
                    let column_name = if !column_aliases.is_empty() {
                        column_aliases[0].clone()
                    } else {
                        "n".to_string()
                    };

                    // Track column name frequency
                    *column_counts.entry(column_name.clone()).or_insert(0) += 1;

                    let resolution = ColumnResolution {
                        offset,
                        table_name: alias.clone().unwrap_or_else(|| "SERIES".to_string()),
                        data_type: DataType::I64,
                        nullable: false,
                        is_indexed: false,
                    };

                    // Add with alias if present
                    if let Some(table_alias) = alias {
                        resolution_map.add_resolution(
                            Some(table_alias.clone()),
                            column_name.clone(),
                            resolution.clone(),
                        );
                    }

                    // Also add without table qualifier for unambiguous lookups
                    resolution_map.add_resolution(None, column_name, resolution);

                    offset += 1;
                }
                TableSource::Unnest {
                    alias,
                    column_aliases,
                } => {
                    // UNNEST(array) generates a single column for unnested values
                    // The data type is unknown at this stage (depends on array element type)
                    // Validate column aliases count (should be 0 or 1)
                    if !column_aliases.is_empty() && column_aliases.len() > 1 {
                        return Err(Error::TooManyColumnAliases(
                            alias.clone().unwrap_or_else(|| "UNNEST".to_string()),
                            1,
                            column_aliases.len(),
                        ));
                    }

                    // Use column alias if provided, otherwise use "unnest" (lowercase for case-insensitive matching)
                    let column_name = if !column_aliases.is_empty() {
                        column_aliases[0].clone()
                    } else {
                        "unnest".to_string()
                    };

                    // Track column name frequency
                    *column_counts.entry(column_name.clone()).or_insert(0) += 1;

                    let resolution = ColumnResolution {
                        offset,
                        table_name: alias.clone().unwrap_or_else(|| "UNNEST".to_string()),
                        data_type: DataType::Text, // Placeholder - actual type depends on array element type
                        nullable: true,            // Array elements might be nullable
                        is_indexed: false,
                    };

                    // Add with alias if present
                    if let Some(table_alias) = alias {
                        resolution_map.add_resolution(
                            Some(table_alias.clone()),
                            column_name.clone(),
                            resolution.clone(),
                        );
                    }

                    // Also add without table qualifier for unambiguous lookups
                    resolution_map.add_resolution(None, column_name, resolution);

                    offset += 1;
                }
            }
        }

        // Add outer query columns if this is a correlated subquery
        // Outer columns are appended after inner columns, so adjust their offsets
        if let Some(outer_map) = &self.outer_column_map {
            let inner_column_count = offset; // Number of columns in inner query
            for ((table_qualifier, col_name), outer_resolution) in &outer_map.columns {
                // Only add if not already present in inner query
                // Outer columns are accessible but don't override inner columns
                let key = (table_qualifier.clone(), col_name.clone());
                resolution_map.columns.entry(key).or_insert_with(|| {
                    // Adjust the offset to account for inner columns
                    let mut adjusted_resolution = outer_resolution.clone();
                    adjusted_resolution.offset += inner_column_count;
                    adjusted_resolution
                });
            }
        }

        // Mark ambiguous columns
        for (column_name, count) in column_counts {
            if count > 1 {
                resolution_map.mark_ambiguous(column_name);
            }
        }

        Ok((resolution_map, subquery_analyses))
    }

    /// Extract output column schema from a SELECT statement
    /// Returns (output_schema, analyzed_statement) for the subquery
    fn extract_select_output_columns(
        &self,
        select_stmt: &SelectStatement,
        _schemas: &HashMap<String, Table>,
    ) -> Result<SubqueryAnalysisResult> {
        // Plan the subquery to get its actual output schema
        let subquery_analyzer = super::analyzer::SemanticAnalyzer::new(
            self.schemas.clone(),
            self.index_metadata.clone(),
        );
        let subquery_statement =
            Statement::Dml(DmlStatement::Select(Box::new(select_stmt.clone())));
        let analyzed = subquery_analyzer.analyze(subquery_statement, Vec::new())?;

        // Plan it to get the execution plan with proper output schema
        let subquery_planner = Planner::new(self.schemas.clone(), self.index_metadata.clone());
        let subquery_plan = subquery_planner.plan_select(select_stmt, &analyzed)?;

        // Extract column names from the plan
        let root_node = match subquery_plan {
            crate::types::plan::Plan::Query { root, .. } => root,
            _ => {
                return Err(Error::ExecutionError(
                    "Expected Query plan for SELECT subquery".into(),
                ));
            }
        };

        let column_names = root_node.get_column_names(&self.schemas);

        // For SELECT *, we need to get types from the column resolution map
        // For other expressions, we can try type inference
        let mut columns = Vec::new();

        // Check if this is a SELECT * query by looking at the first SELECT item
        let is_select_star =
            if let Statement::Dml(DmlStatement::Select(select)) = analyzed.ast.as_ref() {
                select.select.len() == 1
                    && matches!(select.select[0].0, crate::parsing::ast::Expression::All)
            } else {
                false
            };

        if is_select_star {
            // For SELECT *, get types from column resolution map
            for col_name in column_names {
                // Look up the column in the resolution map
                if let Some(resolution) = analyzed.column_resolution_map.resolve(None, &col_name) {
                    columns.push((col_name, resolution.data_type.clone(), resolution.nullable));
                } else {
                    // Fallback if not found
                    columns.push((col_name, DataType::I64, true));
                }
            }
        } else {
            // For explicit column expressions or aggregates, infer types
            let checker = super::typing::TypeChecker::new();
            let resolution_view = super::analyzer::ResolutionView {
                column_map: &analyzed.column_resolution_map,
            };
            let expression_types = checker.infer_all_types(&analyzed.ast, &resolution_view, &[])?;

            for (idx, col_name) in column_names.iter().enumerate() {
                let expr_id = super::statement::ExpressionId::from_path(vec![idx]);
                let (data_type, nullable) = if let Some(type_info) = expression_types.get(&expr_id)
                {
                    (type_info.data_type.clone(), type_info.nullable)
                } else {
                    // Fallback: try to look up in column resolution map
                    if let Some(resolution) = analyzed.column_resolution_map.resolve(None, col_name)
                    {
                        (resolution.data_type.clone(), resolution.nullable)
                    } else {
                        (DataType::I64, true)
                    }
                };
                columns.push((col_name.clone(), data_type, nullable));
            }
        }

        Ok((columns, Arc::new(analyzed)))
    }

    /// Validate that a table exists in the schema
    fn validate_table_exists(&self, table_name: &str) -> Result<()> {
        if !self.schemas.contains_key(table_name) {
            return Err(Error::TableNotFound(table_name.to_string()));
        }
        Ok(())
    }

    /// Check if a column has an index created via CREATE INDEX
    fn is_column_indexed(&self, table: &str, column: &str) -> bool {
        self.index_metadata.values().any(|idx| {
            idx.table.eq_ignore_ascii_case(table)
                && !idx.columns.is_empty()
                && idx.columns[0]
                    .as_column()
                    .map(|col| col.eq_ignore_ascii_case(column))
                    .unwrap_or(false)
        })
    }
}
