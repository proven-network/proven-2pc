//! FROM clause and JOIN planning utilities
//!
//! This module handles the planning of FROM clauses and JOIN operations,
//! including:
//! - Single table scans
//! - JOIN operations (INNER, LEFT, RIGHT, FULL, CROSS)
//! - Subqueries in FROM clause (SELECT and VALUES)
//! - Table series functions (SERIES(N))
//! - Join condition analysis (equi-join vs nested loop)
//! - Join type conversion from AST to execution plan types

use super::expression_resolver::AnalyzedPlanContext;
use super::planner::Planner;
use crate::error::{Error, Result};
use crate::parsing::ast::common::{FromClause, SubquerySource};
use crate::parsing::ast::dml::ValuesStatement;
use crate::parsing::ast::{Expression as AstExpression, Literal, Operator};
use crate::types::Value;
use crate::types::expression::Expression;
use crate::types::plan::{JoinType, Node, Plan};

/// Helper struct for planning FROM clauses and JOINs
///
/// This struct provides static methods for extracting and planning FROM clause
/// components from SQL statements. It handles various FROM clause types including
/// simple table scans, complex joins, subqueries, and table-generating functions.
pub(super) struct FromPlanner;

impl FromPlanner {
    /// Plan a FROM clause, producing the base scan/join node
    ///
    /// This is the main entry point for FROM clause planning. It processes a list
    /// of FROM clause items and produces a single execution plan node representing
    /// the complete FROM clause.
    ///
    /// # Arguments
    ///
    /// * `planner` - The main planner instance for accessing schemas and planning subqueries
    /// * `from` - Slice of FROM clause items to plan
    /// * `context` - Mutable planning context for tracking tables and resolving columns
    ///
    /// # Returns
    ///
    /// A `Node` representing the complete FROM clause execution plan
    ///
    /// # Behavior
    ///
    /// - Empty FROM clause: Returns a `SeriesScan` with size 1 (default for SELECT without FROM)
    /// - Single table: Returns a simple `Scan` node
    /// - Multiple tables (comma-separated): Creates CROSS JOINs via `NestedLoopJoin` nodes
    /// - Explicit JOINs: Analyzes join conditions and creates `HashJoin` or `NestedLoopJoin`
    /// - Subqueries: Plans the subquery and wraps it in the FROM clause
    /// - SERIES(N): Creates a `SeriesScan` node for generating N rows
    ///
    /// # Join Strategy Selection
    ///
    /// For explicit JOINs with ON predicates:
    /// - Attempts to identify equi-join conditions (column = column)
    /// - Uses `HashJoin` for equi-joins with high selectivity (> 0.1)
    /// - Falls back to `NestedLoopJoin` for:
    ///   - Non-equi-join conditions
    ///   - Very selective joins (< 0.1 selectivity)
    ///   - Joins without join hints
    pub(super) fn plan_from<'a>(
        planner: &Planner,
        from: &[FromClause],
        context: &mut AnalyzedPlanContext<'a>,
    ) -> Result<Node> {
        if from.is_empty() {
            // Default to SERIES(1) for SELECT without FROM
            return Ok(Node::SeriesScan {
                size: Expression::Constant(Value::I64(1)),
                alias: None,
            });
        }

        let mut node = None;

        for from_item in from {
            match from_item {
                FromClause::Table { name, alias } => {
                    context.add_table(name.clone(), alias.as_ref().map(|a| a.name.clone()))?;
                    let scan = Node::Scan {
                        table: name.clone(),
                        alias: alias.as_ref().map(|a| a.name.clone()),
                    };

                    node = Some(if let Some(prev) = node {
                        Node::NestedLoopJoin {
                            left: Box::new(prev),
                            right: Box::new(scan),
                            predicate: Expression::Constant(Value::boolean(true)),
                            join_type: JoinType::Inner,
                        }
                    } else {
                        scan
                    });
                }

                FromClause::Subquery { source, alias } => {
                    // Plan the subquery based on its type
                    let subquery_node = match source {
                        SubquerySource::Select(select_stmt) => {
                            // IMPORTANT: Use the subquery's OWN analyzed statement, not the parent's!
                            // The parent analyzed statement contains subquery_analyses for each subquery alias
                            let subquery_analyzed = context
                                .analyzed
                                .subquery_analyses
                                .get(&alias.name)
                                .ok_or_else(|| {
                                    Error::ExecutionError(format!(
                                        "Subquery analysis not found for alias {}",
                                        alias.name
                                    ))
                                })?;

                            // Plan the SELECT subquery with its own analyzed statement
                            let subplan = planner.plan_select(select_stmt, subquery_analyzed)?;
                            match subplan {
                                Plan::Query { root: node, .. } => *node,
                                _ => {
                                    return Err(Error::ExecutionError(
                                        "Invalid subquery plan".into(),
                                    ));
                                }
                            }
                        }
                        SubquerySource::Values(values_stmt) => {
                            // Plan the VALUES subquery
                            Self::plan_values_as_subquery(planner, values_stmt, context)?
                        }
                    };

                    // Subqueries don't need to be added to the context as tables
                    // They produce their own column structure

                    node = Some(if let Some(prev) = node {
                        Node::NestedLoopJoin {
                            left: Box::new(prev),
                            right: Box::new(subquery_node),
                            predicate: Expression::Constant(Value::boolean(true)),
                            join_type: JoinType::Inner,
                        }
                    } else {
                        subquery_node
                    });
                }

                FromClause::Series { size, alias } => {
                    // Convert AST expression to typed expression
                    // For now, we only support literal integer expressions
                    let size_expr = match size {
                        AstExpression::Literal(Literal::Integer(n)) => {
                            // Convert the i128 to an i64
                            let val = *n as i64;
                            Expression::Constant(Value::I64(val))
                        }
                        AstExpression::Operator(Operator::Identity(expr)) => {
                            // Handle unary plus: +N
                            if let AstExpression::Literal(Literal::Integer(n)) = &**expr {
                                let val = *n as i64;
                                Expression::Constant(Value::I64(val))
                            } else {
                                return Err(Error::ExecutionError(
                                    "SERIES size must be a constant integer".into(),
                                ));
                            }
                        }
                        _ => {
                            return Err(Error::ExecutionError(
                                "SERIES size must be a constant integer".into(),
                            ));
                        }
                    };

                    let series_scan = Node::SeriesScan {
                        size: size_expr,
                        alias: alias.as_ref().map(|a| a.name.clone()),
                    };

                    node = Some(if let Some(prev) = node {
                        Node::NestedLoopJoin {
                            left: Box::new(prev),
                            right: Box::new(series_scan),
                            predicate: Expression::Constant(Value::boolean(true)),
                            join_type: JoinType::Inner,
                        }
                    } else {
                        series_scan
                    });
                }

                FromClause::Unnest { array, alias } => {
                    // Resolve the array expression using the current context
                    let array_expr = context.resolve_expression(array)?;

                    let unnest_scan = Node::UnnestScan {
                        array: array_expr,
                        alias: alias.as_ref().map(|a| a.name.clone()),
                    };

                    node = Some(if let Some(prev) = node {
                        Node::NestedLoopJoin {
                            left: Box::new(prev),
                            right: Box::new(unnest_scan),
                            predicate: Expression::Constant(Value::boolean(true)),
                            join_type: JoinType::Inner,
                        }
                    } else {
                        unnest_scan
                    });
                }

                FromClause::Join {
                    left,
                    right,
                    r#type,
                    predicate,
                } => {
                    let left_node = Self::plan_from(planner, &[*left.clone()], context)?;
                    let right_node = Self::plan_from(planner, &[*right.clone()], context)?;

                    // Extract table names from the nodes
                    let (left_table, right_table) =
                        Self::extract_table_names(&left_node, &right_node);

                    let join_node = if let Some(pred) = predicate {
                        // Check if this is an equi-join
                        if let Some((left_col, right_col)) =
                            Self::extract_equi_join_columns(pred, &left_node, &right_node, context)
                        {
                            // Check join hints for selectivity guidance
                            let use_hash_join =
                                if let (Some(lt), Some(rt)) = (&left_table, &right_table) {
                                    context
                                        .analyzed
                                        .join_hints
                                        .iter()
                                        .find(|h| {
                                            (h.left_table == *lt && h.right_table == *rt)
                                                || (h.left_table == *rt && h.right_table == *lt)
                                        })
                                        .map(|h| h.selectivity_estimate > 0.1) // Use hash join for higher selectivity
                                        .unwrap_or(true) // Default to hash join
                                } else {
                                    true // Default to hash join
                                };

                            if use_hash_join {
                                Node::HashJoin {
                                    left: Box::new(left_node),
                                    right: Box::new(right_node),
                                    left_col,
                                    right_col,
                                    join_type: Self::convert_join_type(r#type),
                                }
                            } else {
                                // Use nested loop for very selective joins
                                let join_predicate = context.resolve_expression(pred)?;
                                Node::NestedLoopJoin {
                                    left: Box::new(left_node),
                                    right: Box::new(right_node),
                                    predicate: join_predicate,
                                    join_type: Self::convert_join_type(r#type),
                                }
                            }
                        } else {
                            let join_predicate = context.resolve_expression(pred)?;
                            Node::NestedLoopJoin {
                                left: Box::new(left_node),
                                right: Box::new(right_node),
                                predicate: join_predicate,
                                join_type: Self::convert_join_type(r#type),
                            }
                        }
                    } else {
                        Node::NestedLoopJoin {
                            left: Box::new(left_node),
                            right: Box::new(right_node),
                            predicate: Expression::Constant(Value::boolean(true)),
                            join_type: JoinType::Cross,
                        }
                    };

                    node = Some(if let Some(prev) = node {
                        Node::NestedLoopJoin {
                            left: Box::new(prev),
                            right: Box::new(join_node),
                            predicate: Expression::Constant(Value::boolean(true)),
                            join_type: JoinType::Inner,
                        }
                    } else {
                        join_node
                    });
                }
            }
        }

        Ok(node.unwrap_or(Node::Nothing))
    }

    /// Convert AST join type to execution plan join type
    ///
    /// Maps the parsed JOIN type from the AST to the internal JoinType enum
    /// used in the execution plan.
    ///
    /// # Arguments
    ///
    /// * `join_type` - The AST join type to convert
    ///
    /// # Returns
    ///
    /// The corresponding execution plan `JoinType`
    pub(super) fn convert_join_type(join_type: &crate::parsing::ast::common::JoinType) -> JoinType {
        match join_type {
            crate::parsing::ast::common::JoinType::Cross => JoinType::Cross,
            crate::parsing::ast::common::JoinType::Inner => JoinType::Inner,
            crate::parsing::ast::common::JoinType::Left => JoinType::Left,
            crate::parsing::ast::common::JoinType::Right => JoinType::Right,
            crate::parsing::ast::common::JoinType::Full => JoinType::Full,
        }
    }

    /// Extract table names from scan nodes
    ///
    /// This helper examines the left and right nodes of a join and attempts to
    /// extract the base table names if they are simple table scans. This information
    /// is used for looking up join hints and selectivity estimates.
    ///
    /// # Arguments
    ///
    /// * `left_node` - The left side of the join
    /// * `right_node` - The right side of the join
    ///
    /// # Returns
    ///
    /// A tuple of `(Option<String>, Option<String>)` containing the left and right
    /// table names if they can be extracted, or None if the nodes are not simple scans
    pub(super) fn extract_table_names(
        left_node: &Node,
        right_node: &Node,
    ) -> (Option<String>, Option<String>) {
        let left_table = match left_node {
            Node::Scan { table, .. } => Some(table.clone()),
            _ => None,
        };
        let right_table = match right_node {
            Node::Scan { table, .. } => Some(table.clone()),
            _ => None,
        };
        (left_table, right_table)
    }

    /// Attempt to extract equi-join columns from a join predicate
    ///
    /// Analyzes a join predicate to determine if it's an equi-join (equality between
    /// columns from different tables). If it is, returns the column offsets for use
    /// in a HashJoin node.
    ///
    /// # Arguments
    ///
    /// * `_predicate` - The join predicate AST expression
    /// * `_left_node` - The left side of the join
    /// * `_right_node` - The right side of the join
    /// * `_context` - The planning context
    ///
    /// # Returns
    ///
    /// `Some((left_col, right_col))` if this is an equi-join, containing the column
    /// offsets for the join columns. `None` if this is not an equi-join or cannot
    /// be determined.
    ///
    /// # Current Implementation
    ///
    /// This is currently a stub that always returns `None`. A complete implementation
    /// would parse the predicate to identify patterns like `table1.col = table2.col`
    /// and extract the column offsets.
    pub(super) fn extract_equi_join_columns(
        _predicate: &AstExpression,
        _left_node: &Node,
        _right_node: &Node,
        _context: &AnalyzedPlanContext,
    ) -> Option<(usize, usize)> {
        // Simplified for now - a complete implementation would:
        // 1. Check if predicate is an equality operator (col1 = col2)
        // 2. Verify one column is from left node, one from right node
        // 3. Extract the column offsets from the planning context
        // 4. Return Some((left_offset, right_offset))
        None
    }

    /// Plan a VALUES statement as a subquery in a FROM clause
    ///
    /// This helper plans a VALUES clause that appears as a subquery in the FROM clause.
    /// It converts the VALUES rows into a `Node::Values` execution node.
    ///
    /// # Arguments
    ///
    /// * `planner` - The main planner instance (unused, but kept for consistency)
    /// * `values_stmt` - The VALUES statement to plan
    /// * `context` - The planning context for resolving expressions
    ///
    /// # Returns
    ///
    /// A `Node::Values` containing the resolved expression rows
    ///
    /// # Note
    ///
    /// This is simpler than the standalone VALUES planning as subqueries don't
    /// support ORDER BY, LIMIT, or OFFSET clauses.
    fn plan_values_as_subquery<'a>(
        _planner: &Planner,
        values_stmt: &ValuesStatement,
        context: &mut AnalyzedPlanContext<'a>,
    ) -> Result<Node> {
        // Similar to plan_values but simpler - no ORDER BY, LIMIT, OFFSET for subqueries
        let rows = values_stmt
            .rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|e| context.resolve_expression_simple(e))
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Node::Values { rows })
    }
}
