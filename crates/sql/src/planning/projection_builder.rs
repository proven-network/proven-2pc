//! Projection building utilities
//!
//! This module provides functions for building projections from SELECT expressions.
//! It handles:
//! - Wildcard expansion (* and table.*)
//! - Column aliasing
//! - Regular projections
//! - Aggregate projections (with GROUP BY)

use super::expression_resolver::TableRef;
use crate::error::Result;
use crate::parsing::ast::Expression as AstExpression;
use crate::semantic::AnalyzedStatement;
use crate::types::expression::Expression;
use crate::types::schema::Table;
use std::collections::HashMap;

/// Helper for building projections from SELECT expressions
pub struct ProjectionBuilder;

impl ProjectionBuilder {
    /// Plan a regular (non-aggregate) projection
    ///
    /// This handles:
    /// - Wildcard expansion (* and table.*)
    /// - Column references with automatic aliasing
    /// - Complex expressions with explicit or generated aliases
    ///
    /// Returns a tuple of (expressions, aliases)
    pub fn plan_projection(
        select: &[(AstExpression, Option<String>)],
        tables: &[TableRef],
        schemas: &HashMap<String, Table>,
        analyzed: &AnalyzedStatement,
        resolve_expression: impl Fn(&AstExpression) -> Result<Expression>,
    ) -> Result<(Vec<Expression>, Vec<Option<String>>)> {
        let mut expressions = Vec::new();
        let mut aliases = Vec::new();

        for (expr, alias) in select {
            match expr {
                AstExpression::All => {
                    // Expand * to all columns
                    // For regular tables, use the old logic with table.start_column + i
                    // For subqueries (VALUES/SELECT), fall back to resolution map

                    if tables.is_empty() {
                        // No tables in context - this is a subquery (VALUES/SELECT)
                        // Use resolution map for both offsets and names
                        let mut all_resolutions: Vec<_> =
                            analyzed.column_resolution_map.columns.values().collect();
                        all_resolutions.sort_by_key(|r| r.offset);
                        all_resolutions.dedup_by_key(|r| r.offset);

                        let column_names =
                            analyzed.column_resolution_map.get_ordered_column_names();

                        for (resolution, col_name) in
                            all_resolutions.iter().zip(column_names.iter())
                        {
                            expressions.push(Expression::Column(resolution.offset));
                            aliases.push(Some(col_name.clone()));
                        }
                    } else {
                        // Regular tables - use original logic for column offsets
                        // but get column names from resolution map (may be aliased)
                        let column_names =
                            analyzed.column_resolution_map.get_ordered_column_names();

                        let mut name_idx = 0;
                        for table in tables {
                            if let Some(schema) = schemas.get(&table.name) {
                                for (i, _col) in schema.columns.iter().enumerate() {
                                    expressions.push(Expression::Column(table.start_column + i));
                                    // Use aliased name if available, otherwise use schema name
                                    if let Some(col_name) = column_names.get(name_idx) {
                                        aliases.push(Some(col_name.clone()));
                                    } else {
                                        aliases.push(Some(_col.name.clone()));
                                    }
                                    name_idx += 1;
                                }
                            }
                        }
                    }
                }
                AstExpression::QualifiedWildcard(table_alias) => {
                    // Expand table.* to all columns from that specific table
                    // First check if this is a SERIES or other non-table source
                    let mut found = false;

                    for table in tables {
                        // Check if this is the table we're looking for (by alias or name)
                        if (table.alias.as_deref() == Some(table_alias.as_str())
                            || table.name == *table_alias)
                            && let Some(schema) = schemas.get(&table.name)
                        {
                            for (i, col) in schema.columns.iter().enumerate() {
                                expressions.push(Expression::Column(table.start_column + i));
                                aliases.push(Some(col.name.clone()));
                            }
                            found = true;
                            break;
                        }
                    }

                    // If not found in tables, try to expand from column resolution map
                    // This handles SERIES and subqueries
                    if !found {
                        let map = &analyzed.column_resolution_map;
                        let matching_columns: Vec<_> = map
                            .columns
                            .iter()
                            .filter(|((tbl, _), _)| tbl.as_deref() == Some(table_alias.as_str()))
                            .collect();

                        // Sort by offset to maintain column order
                        let mut sorted: Vec<_> = matching_columns.into_iter().collect();
                        sorted.sort_by_key(|(_, res)| res.offset);

                        for ((_, col_name), resolution) in sorted {
                            expressions.push(Expression::Column(resolution.offset));
                            aliases.push(Some(col_name.clone()));
                        }
                    }
                }
                AstExpression::Column(_table_ref, col_name) if alias.is_none() => {
                    expressions.push(resolve_expression(expr)?);
                    aliases.push(Some(col_name.clone()));
                }
                _ => {
                    expressions.push(resolve_expression(expr)?);
                    // If no alias provided, generate one from the expression
                    if alias.is_none() {
                        aliases.push(Some(expr.to_column_name()));
                    } else {
                        aliases.push(alias.clone());
                    }
                }
            }
        }

        Ok((expressions, aliases))
    }

    /// Plan an aggregate projection (with GROUP BY)
    ///
    /// This is more complex than regular projections because:
    /// - Aggregate functions need to be mapped to their position in the aggregates list
    /// - Complex expressions with aggregates need special resolution
    /// - Non-aggregate expressions must be GROUP BY columns
    ///
    /// Returns a tuple of (expressions, aliases)
    #[allow(clippy::too_many_arguments)]
    pub fn plan_aggregate_projection(
        select: &[(AstExpression, Option<String>)],
        is_aggregate: impl Fn(&AstExpression) -> bool,
        contains_aggregate: impl Fn(&AstExpression) -> bool,
        find_aggregate_index: impl Fn(&AstExpression) -> Result<usize>,
        find_group_by_index: impl Fn(&AstExpression) -> Result<usize>,
        resolve_aggregate_expression: impl Fn(&AstExpression) -> Result<Expression>,
    ) -> Result<(Vec<Expression>, Vec<Option<String>>)> {
        let mut expressions = Vec::new();
        let mut aliases = Vec::new();

        for (expr, alias) in select {
            if is_aggregate(expr) {
                // For simple aggregate functions, find the corresponding column
                let aggregate_idx = find_aggregate_index(expr)?;
                expressions.push(Expression::Column(aggregate_idx));

                // Generate alias for aggregate function
                let func_alias = alias.clone().or_else(|| {
                    if let AstExpression::Function(name, args) = expr {
                        let func_name = name.to_uppercase();

                        // Handle DISTINCT functions - they come as "AVG_DISTINCT" etc
                        let (base_func, is_distinct) = if func_name.ends_with("_DISTINCT") {
                            (func_name.trim_end_matches("_DISTINCT"), true)
                        } else {
                            (func_name.as_str(), false)
                        };

                        let arg_str = if args.is_empty() || matches!(args[0], AstExpression::All) {
                            "*".to_string()
                        } else {
                            args[0].to_column_name()
                        };

                        if is_distinct {
                            Some(format!("{}(DISTINCT {})", base_func, arg_str))
                        } else {
                            Some(format!("{}({})", base_func, arg_str))
                        }
                    } else {
                        None
                    }
                });
                aliases.push(func_alias);
            } else if contains_aggregate(expr) {
                // For complex expressions containing aggregates (e.g., CASE with aggregates),
                // resolve them by replacing aggregate calls with column references
                let resolved_expr = resolve_aggregate_expression(expr)?;
                expressions.push(resolved_expr);
                aliases.push(alias.clone().or_else(|| Some(expr.to_column_name())));
            } else {
                // For non-aggregate expressions in GROUP BY context,
                // they must be GROUP BY columns - find which position
                let group_by_idx = find_group_by_index(expr)?;
                expressions.push(Expression::Column(group_by_idx));

                if let AstExpression::Column(_, col_name) = expr {
                    aliases.push(alias.clone().or(Some(col_name.clone())));
                } else {
                    aliases.push(alias.clone());
                }
            }
        }

        Ok((expressions, aliases))
    }
}
