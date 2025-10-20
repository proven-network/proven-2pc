//! Index selection utilities
//!
//! This module provides functions for selecting appropriate indexes for query optimization.
//! It handles:
//! - Single-column index selection
//! - Composite index matching
//! - Expression-based index selection
//! - Range scan optimization

use crate::error::{Error, Result};
use crate::parsing::ast::Expression as AstExpression;
use crate::semantic::AnalyzedStatement;
use crate::types::expression::Expression;
use crate::types::index::IndexMetadata;
use crate::types::plan::Node;
use std::collections::HashMap;

/// Helper for selecting appropriate indexes for queries
pub struct IndexSelector;

impl IndexSelector {
    /// Plan a WHERE clause, attempting to use an index if possible
    ///
    /// This method analyzes the WHERE expression and the source node to determine
    /// if an index can be used. It checks:
    /// 1. Composite indexes (multiple equality predicates)
    /// 2. Single-column indexes
    /// 3. Expression-based indexes
    /// 4. Range indexes
    ///
    /// If no suitable index is found, falls back to a Filter node.
    pub fn plan_where_with_index(
        where_expr: &AstExpression,
        source: Node,
        index_metadata: &HashMap<String, IndexMetadata>,
        analyzed: &AnalyzedStatement,
        resolve_expression: impl Fn(&AstExpression) -> Result<Expression>,
    ) -> Result<Node> {
        // Try to use indexes if source is a Scan node
        if let Node::Scan { table, alias } = &source {
            // First, check for composite index opportunities (multiple equality predicates)
            let predicate_refs: Vec<_> = analyzed.predicate_templates.iter().collect();
            if let Some((index_name, values)) =
                Self::find_composite_index_match(table, &predicate_refs, index_metadata, |val| {
                    Self::predicate_value_to_expression(val)
                })?
            {
                return Ok(Node::IndexScan {
                    table: table.clone(),
                    alias: alias.clone(),
                    index_name,
                    values,
                });
            }

            // Check predicate templates for single-column index opportunities
            for template in &analyzed.predicate_templates {
                use crate::semantic::statement::PredicateTemplate;

                match template {
                    // IndexedColumn template - explicitly marked as indexed
                    PredicateTemplate::IndexedColumn {
                        table: tbl,
                        column,
                        value,
                    } if tbl == table => {
                        if let Some(index_name) =
                            Self::find_index_for_column(table, column, index_metadata)
                        {
                            let value_expr = Self::predicate_value_to_expression(value)?;
                            return Ok(Node::IndexScan {
                                table: table.clone(),
                                alias: alias.clone(),
                                index_name,
                                values: vec![value_expr],
                            });
                        }
                    }

                    // Equality template - check if column is indexed
                    PredicateTemplate::Equality {
                        table: tbl,
                        column_name,
                        value_expr: value,
                    } if tbl == table => {
                        if let Some(index_name) =
                            Self::find_index_for_column(table, column_name, index_metadata)
                        {
                            let value_expr = Self::predicate_value_to_expression(value)?;
                            return Ok(Node::IndexScan {
                                table: table.clone(),
                                alias: alias.clone(),
                                index_name,
                                values: vec![value_expr],
                            });
                        }
                    }

                    // Range template - use IndexRangeScan
                    PredicateTemplate::Range {
                        table: tbl,
                        column_name,
                        lower,
                        upper,
                    } if tbl == table => {
                        if let Some(index_name) =
                            Self::find_index_for_column(table, column_name, index_metadata)
                        {
                            let start = lower
                                .as_ref()
                                .map(|(v, _)| Self::predicate_value_to_expression(v))
                                .transpose()?;
                            let end = upper
                                .as_ref()
                                .map(|(v, _)| Self::predicate_value_to_expression(v))
                                .transpose()?;

                            return Ok(Node::IndexRangeScan {
                                table: table.clone(),
                                alias: alias.clone(),
                                index_name,
                                start: start.map(|v| vec![v]),
                                start_inclusive: lower
                                    .as_ref()
                                    .map(|(_, inc)| *inc)
                                    .unwrap_or(true),
                                end: end.map(|v| vec![v]),
                                end_inclusive: upper.as_ref().map(|(_, inc)| *inc).unwrap_or(true),
                                reverse: false,
                            });
                        }
                    }

                    // Expression equality template - check if we have an index on this expression
                    PredicateTemplate::ExpressionEquality {
                        table: tbl,
                        expression,
                        value,
                    } if tbl == table => {
                        if let Some(index_name) =
                            Self::find_index_for_expression(table, expression, index_metadata)
                        {
                            let value_expr = Self::predicate_value_to_expression(value)?;
                            return Ok(Node::IndexScan {
                                table: table.clone(),
                                alias: alias.clone(),
                                index_name,
                                values: vec![value_expr],
                            });
                        }
                    }

                    _ => continue,
                }
            }
        }

        // Fallback to Filter node
        let predicate = resolve_expression(where_expr)?;
        Ok(Node::Filter {
            source: Box::new(source),
            predicate,
        })
    }

    /// Convert a PredicateValue to an Expression
    pub fn predicate_value_to_expression(
        value: &crate::semantic::statement::PredicateValue,
    ) -> Result<Expression> {
        use crate::semantic::statement::PredicateValue;

        match value {
            PredicateValue::Constant(val) => Ok(Expression::Constant(val.clone())),
            PredicateValue::Parameter(idx) => Ok(Expression::Parameter(*idx)),
            PredicateValue::Expression(_expr_id) => {
                // For complex expressions, we'd need to walk the AST
                // For now, return an error - this should be rare for index predicates
                Err(Error::ExecutionError(
                    "Complex expressions in index predicates not yet supported".into(),
                ))
            }
        }
    }

    /// Find an index that covers the given column
    ///
    /// Searches through index metadata to find an index where the first column
    /// matches the given column name (case-insensitive).
    pub fn find_index_for_column(
        table: &str,
        column: &str,
        index_metadata: &HashMap<String, IndexMetadata>,
    ) -> Option<String> {
        // Search through index metadata for an index on this table and column
        for (index_name, metadata) in index_metadata {
            // Check if this index is for the right table
            if metadata.table.eq_ignore_ascii_case(table) {
                // Check if this index includes the column we're looking for
                if !metadata.columns.is_empty()
                    && let Some(col_name) = metadata.columns[0].as_column()
                    && col_name.eq_ignore_ascii_case(column)
                {
                    return Some(index_name.clone());
                }
            }
        }

        None
    }

    /// Find an index that matches the given expression
    ///
    /// Searches for an expression-based index where the indexed expression
    /// matches the given expression (after normalization).
    pub fn find_index_for_expression(
        table: &str,
        expr: &crate::types::expression::Expression,
        index_metadata: &HashMap<String, IndexMetadata>,
    ) -> Option<String> {
        use crate::semantic::normalize::{expressions_equal, normalize_expression};

        let normalized_expr = normalize_expression(expr);

        // Search through index metadata for an index on this table with matching expression
        for (index_name, metadata) in index_metadata {
            // Check if this index is for the right table
            if metadata.table.eq_ignore_ascii_case(table) {
                // Check if the first column is an expression that matches
                if !metadata.columns.is_empty()
                    && let Some(index_expr) = metadata.columns[0].as_expression()
                    && expressions_equal(index_expr, &normalized_expr)
                {
                    return Some(index_name.clone());
                }
            }
        }

        None
    }

    /// Find a composite index that matches multiple equality predicates
    ///
    /// Returns (index_name, column_values) where column_values are in index column order.
    /// A composite index can be used when we have equality predicates for a prefix of
    /// the index columns.
    pub fn find_composite_index_match(
        table: &str,
        predicates: &[&crate::semantic::statement::PredicateTemplate],
        index_metadata: &HashMap<String, IndexMetadata>,
        value_converter: impl Fn(&crate::semantic::statement::PredicateValue) -> Result<Expression>,
    ) -> Result<Option<(String, Vec<crate::types::expression::Expression>)>> {
        use crate::semantic::statement::PredicateTemplate;

        // Extract equality predicates for this table
        let mut equality_map = std::collections::HashMap::new();
        for template in predicates {
            match template {
                PredicateTemplate::Equality {
                    table: tbl,
                    column_name,
                    value_expr,
                } if tbl.eq_ignore_ascii_case(table) => {
                    equality_map.insert(column_name.to_lowercase(), value_expr.clone());
                }
                PredicateTemplate::IndexedColumn {
                    table: tbl,
                    column,
                    value: value_expr,
                } if tbl.eq_ignore_ascii_case(table) => {
                    equality_map.insert(column.to_lowercase(), value_expr.clone());
                }
                _ => {}
            }
        }

        // Check each index to see if it's a composite index we can use
        for (index_name, metadata) in index_metadata {
            if !metadata.table.eq_ignore_ascii_case(table) {
                continue;
            }

            // Check if we have equality predicates for a prefix of the index columns
            let mut matched_values = Vec::new();
            for index_col in &metadata.columns {
                // Only match simple column indexes (skip expression indexes for composite matching)
                if let Some(col_name) = index_col.as_column() {
                    if let Some(value_expr) = equality_map.get(&col_name.to_lowercase()) {
                        matched_values.push(value_expr.clone());
                    } else {
                        // Stop at first non-matching column (index prefix rule)
                        break;
                    }
                } else {
                    // Expression index - can't use for composite matching
                    break;
                }
            }

            // If we matched more than one column, we found a composite index match
            if matched_values.len() > 1 {
                // Convert PredicateValues to Expressions
                let mut expr_values = Vec::new();
                for val in matched_values {
                    expr_values.push(value_converter(&val)?);
                }
                return Ok(Some((index_name.clone(), expr_values)));
            }
        }

        Ok(None)
    }
}
