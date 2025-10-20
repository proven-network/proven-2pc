//! SELECT statement planner
//!
//! This module contains the main SELECT query planning logic, orchestrating
//! the various specialized planners (FROM, WHERE, aggregates, projection, etc.)
//! to build a complete query execution plan.

use super::expression_resolver::{AnalyzedPlanContext, ProjectionContext};
use crate::error::Result;
use crate::parsing::ast::common::Direction as AstDirection;
use crate::parsing::ast::{Expression as AstExpression, SelectStatement};
use crate::semantic::AnalyzedStatement;
use crate::types::plan::{Direction, Node, Plan};

/// SELECT query planner
pub(super) struct SelectPlanner;

impl SelectPlanner {
    /// Plan a SELECT query
    ///
    /// This is the main entry point for SELECT planning. It orchestrates the entire
    /// SELECT planning process by delegating to specialized planners:
    ///
    /// 1. FROM clause - handled by FromPlanner
    /// 2. WHERE clause - optimized with IndexSelector when possible
    /// 3. GROUP BY and aggregates - handled by AggregatePlanner
    /// 4. HAVING clause
    /// 5. SELECT expressions (projection) - handled by ProjectionBuilder
    /// 6. DISTINCT/DISTINCT ON
    /// 7. ORDER BY
    /// 8. LIMIT/OFFSET
    ///
    /// # Arguments
    /// * `planner` - The parent planner instance (for accessing schemas, indexes, and helper methods)
    /// * `select` - The SELECT statement AST
    /// * `analyzed` - The semantic analysis results
    ///
    /// # Returns
    /// A Plan::Query containing the execution plan tree
    pub(super) fn plan_select(
        planner: &super::planner::Planner,
        select: &SelectStatement,
        analyzed: &AnalyzedStatement,
    ) -> Result<Plan> {
        // Check if SELECT contains wildcards - we'll use this later for column names
        let has_wildcard = select.select.iter().any(|(expr, _)| {
            matches!(
                expr,
                AstExpression::All | AstExpression::QualifiedWildcard(_)
            )
        });

        // Create context that uses the analyzed statement
        let mut context = AnalyzedPlanContext::new(
            planner.get_schemas(),
            planner.get_index_metadata(),
            analyzed,
        );

        // Start with FROM clause
        let mut node = planner.plan_from(&select.from, &mut context)?;

        // Apply WHERE filter
        if let Some(ref where_expr) = select.r#where {
            node = planner.plan_where_with_index(where_expr, node, &mut context)?;
        }

        // Apply GROUP BY and aggregates
        // Check if there are any aggregate functions in the SELECT clause or HAVING clause
        let has_aggregates = select
            .select
            .iter()
            .any(|(expr, _)| planner.contains_aggregate_expr(expr))
            || select
                .having
                .as_ref()
                .map(|h| planner.contains_aggregate_expr(h))
                .unwrap_or(false);
        let group_by_count = select.group_by.len();

        if !select.group_by.is_empty() || has_aggregates {
            let group_exprs = select
                .group_by
                .iter()
                .map(|e| context.resolve_expression(e))
                .collect::<Result<Vec<_>>>()?;

            let aggregates =
                planner.extract_aggregates(&select.select, select.having.as_ref(), &mut context)?;
            let group_by_count = group_exprs.len();

            node = Node::Aggregate {
                source: Box::new(node),
                group_by: group_exprs,
                aggregates: aggregates.clone(),
            };

            // Apply HAVING filter
            if let Some(ref having_expr) = select.having {
                // Resolve HAVING with aggregate-to-column mapping
                let predicate = planner.resolve_having_expression(
                    having_expr,
                    &select.group_by,
                    &aggregates,
                    group_by_count,
                    &mut context,
                )?;
                node = Node::Filter {
                    source: Box::new(node),
                    predicate,
                };
            }
        }

        // Check if ORDER BY references any columns not in SELECT
        // If so, we need to add them to projection temporarily
        let needs_extended_projection = !select.order_by.is_empty() && {
            select.order_by.iter().any(|(expr, _)| {
                // Check if this expression is not in the SELECT list
                !select.select.iter().any(|(sel_expr, sel_alias)| {
                    // Check if ORDER BY references a SELECT alias
                    if let AstExpression::Column(None, col_name) = expr
                        && let Some(alias) = sel_alias
                        && alias == col_name
                    {
                        return true; // ORDER BY references SELECT alias
                    }
                    // Simple structural check
                    match (expr, sel_expr) {
                        (AstExpression::Column(t1, c1), AstExpression::Column(t2, c2)) => {
                            t1 == t2 && c1 == c2
                        }
                        _ => false,
                    }
                })
            })
        };

        // If ORDER BY needs columns not in SELECT, apply it before projection
        if needs_extended_projection {
            // Apply ORDER BY before projection (PostgreSQL-style)
            let order = select
                .order_by
                .iter()
                .map(|(e, d)| {
                    let expr = context.resolve_expression(e)?;
                    let dir = match d {
                        AstDirection::Asc => Direction::Ascending,
                        AstDirection::Desc => Direction::Descending,
                    };
                    Ok((expr, dir))
                })
                .collect::<Result<Vec<_>>>()?;

            node = Node::Order {
                source: Box::new(node),
                order_by: order,
            };
        }

        // Apply projection
        let (expressions, aliases) = if has_aggregates {
            planner.plan_aggregate_projection(
                &select.select,
                &select.group_by,
                group_by_count,
                &mut context,
            )?
        } else {
            planner.plan_projection(&select.select, &mut context)?
        };

        node = Node::Projection {
            source: Box::new(node),
            expressions: expressions.clone(),
            aliases: aliases.clone(),
        };

        // Create projection context for ORDER BY resolution
        // This maps column names/aliases to their position in projection output
        let projection_ctx = if has_wildcard {
            // For wildcards, use the column names from semantic analysis
            let column_names = analyzed.column_resolution_map.get_ordered_column_names();
            ProjectionContext::with_column_names(expressions, select.select.clone(), column_names)
        } else {
            ProjectionContext::new(expressions, select.select.clone())
        };

        // Apply DISTINCT (SQL standard: after projection, before ORDER BY)
        match &select.distinct {
            crate::parsing::ast::DistinctClause::All => {
                node = Node::Distinct {
                    source: Box::new(node),
                };
            }
            crate::parsing::ast::DistinctClause::On(exprs) => {
                // Resolve DISTINCT ON expressions
                let distinct_exprs = exprs
                    .iter()
                    .map(|e| context.resolve_expression(e))
                    .collect::<Result<Vec<_>>>()?;

                node = Node::DistinctOn {
                    source: Box::new(node),
                    on: distinct_exprs,
                };
            }
            crate::parsing::ast::DistinctClause::None => {
                // No DISTINCT
            }
        }

        // Apply ORDER BY after projection (only if it wasn't applied before)
        if !select.order_by.is_empty() && !needs_extended_projection {
            let order = select
                .order_by
                .iter()
                .map(|(e, d)| {
                    let expr = context.resolve_order_by_expression(e, &projection_ctx)?;
                    let dir = match d {
                        AstDirection::Asc => Direction::Ascending,
                        AstDirection::Desc => Direction::Descending,
                    };
                    Ok((expr, dir))
                })
                .collect::<Result<Vec<_>>>()?;

            node = Node::Order {
                source: Box::new(node),
                order_by: order,
            };
        }

        // Apply OFFSET
        if let Some(ref offset_expr) = select.offset {
            let offset = planner.eval_constant(offset_expr)?;
            node = Node::Offset {
                source: Box::new(node),
                offset,
            };
        }

        // Apply LIMIT
        if let Some(ref limit_expr) = select.limit {
            let limit = planner.eval_constant(limit_expr)?;
            node = Node::Limit {
                source: Box::new(node),
                limit,
            };
        }

        // Extract column names from the column resolution map only if SELECT contains wildcards
        // For other cases (aggregates, expressions), let get_column_names() handle it
        let column_names = if has_wildcard {
            Some(analyzed.column_resolution_map.get_ordered_column_names())
        } else {
            None
        };

        Ok(Plan::Query {
            root: Box::new(node),
            params: Vec::new(),
            column_names,
        })
    }
}
