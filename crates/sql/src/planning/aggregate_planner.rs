//! Aggregate planning utilities
//!
//! This module provides functions for extracting and resolving aggregate functions
//! in queries with GROUP BY and HAVING clauses. It handles:
//! - Extracting aggregate functions from SELECT and HAVING clauses
//! - Resolving HAVING expressions with aggregate-to-column mapping
//! - Finding aggregate and GROUP BY column positions
//! - Detecting aggregate expressions

use super::aggregate_utils::AggregateBuilder;
use crate::error::{Error, Result};
use crate::parsing::ast::Expression as AstExpression;
use crate::types::expression::Expression;
use crate::types::plan::AggregateFunc;

/// Helper for planning aggregate queries
pub struct AggregatePlanner;

impl AggregatePlanner {
    /// Check if an expression is an aggregate function
    pub fn is_aggregate_expr(expr: &AstExpression) -> bool {
        match expr {
            AstExpression::Function(name, _) => AggregateBuilder::is_aggregate_name(name),
            _ => false,
        }
    }

    /// Check if an expression contains any aggregate functions (recursively)
    pub fn contains_aggregate_expr(expr: &AstExpression) -> bool {
        use crate::parsing::ast::Operator;

        match expr {
            // Check if this is an aggregate function
            AstExpression::Function(_name, _) if Self::is_aggregate_expr(expr) => true,
            // Check function arguments
            AstExpression::Function(_name, args) => args.iter().any(Self::contains_aggregate_expr),
            // Recursively check operators
            AstExpression::Operator(op) => match op {
                Operator::And(l, r)
                | Operator::Or(l, r)
                | Operator::Xor(l, r)
                | Operator::Equal(l, r)
                | Operator::NotEqual(l, r)
                | Operator::GreaterThan(l, r)
                | Operator::GreaterThanOrEqual(l, r)
                | Operator::LessThan(l, r)
                | Operator::LessThanOrEqual(l, r)
                | Operator::Add(l, r)
                | Operator::Concat(l, r)
                | Operator::Subtract(l, r)
                | Operator::Multiply(l, r)
                | Operator::Divide(l, r)
                | Operator::Remainder(l, r)
                | Operator::Exponentiate(l, r)
                | Operator::BitwiseAnd(l, r)
                | Operator::BitwiseOr(l, r)
                | Operator::BitwiseXor(l, r)
                | Operator::BitwiseShiftLeft(l, r)
                | Operator::BitwiseShiftRight(l, r) => {
                    Self::contains_aggregate_expr(l) || Self::contains_aggregate_expr(r)
                }
                Operator::Not(e)
                | Operator::Negate(e)
                | Operator::Identity(e)
                | Operator::Factorial(e)
                | Operator::BitwiseNot(e) => Self::contains_aggregate_expr(e),
                Operator::Between {
                    expr, low, high, ..
                } => {
                    Self::contains_aggregate_expr(expr)
                        || Self::contains_aggregate_expr(low)
                        || Self::contains_aggregate_expr(high)
                }
                Operator::InList { expr, list, .. } => {
                    Self::contains_aggregate_expr(expr)
                        || list.iter().any(Self::contains_aggregate_expr)
                }
                Operator::Like { expr, pattern, .. } | Operator::ILike { expr, pattern, .. } => {
                    Self::contains_aggregate_expr(expr) || Self::contains_aggregate_expr(pattern)
                }
                Operator::Is(e, _) => Self::contains_aggregate_expr(e),
                Operator::InSubquery { expr, subquery, .. } => {
                    Self::contains_aggregate_expr(expr) || Self::contains_aggregate_expr(subquery)
                }
                Operator::Exists { subquery, .. } => Self::contains_aggregate_expr(subquery),
            },
            // CASE expressions
            AstExpression::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                // Check operand
                if let Some(op) = operand
                    && Self::contains_aggregate_expr(op)
                {
                    return true;
                }
                // Check WHEN/THEN pairs
                for (when_expr, then_expr) in when_clauses {
                    if Self::contains_aggregate_expr(when_expr)
                        || Self::contains_aggregate_expr(then_expr)
                    {
                        return true;
                    }
                }
                // Check ELSE clause
                if let Some(else_expr) = else_clause
                    && Self::contains_aggregate_expr(else_expr)
                {
                    return true;
                }
                false
            }
            // Array access: base[index]
            AstExpression::ArrayAccess { base, index } => {
                Self::contains_aggregate_expr(base) || Self::contains_aggregate_expr(index)
            }
            // Field access: base.field
            AstExpression::FieldAccess { base, .. } => Self::contains_aggregate_expr(base),
            // Array literal: [expr1, expr2, ...]
            AstExpression::ArrayLiteral(elements) => {
                elements.iter().any(Self::contains_aggregate_expr)
            }
            // Map literal: {key1: value1, key2: value2, ...}
            AstExpression::MapLiteral(pairs) => pairs
                .iter()
                .any(|(k, v)| Self::contains_aggregate_expr(k) || Self::contains_aggregate_expr(v)),
            // Subquery - already handled separately, don't recurse
            AstExpression::Subquery(_) => false,
            // CAST expression - check the inner expression
            AstExpression::Cast { expr, .. } => Self::contains_aggregate_expr(expr),
            // These don't contain nested expressions
            AstExpression::All
            | AstExpression::QualifiedWildcard(_)
            | AstExpression::Column(_, _)
            | AstExpression::Literal(_)
            | AstExpression::Parameter(_) => false,
        }
    }

    /// Extract all aggregate functions from SELECT and HAVING clauses
    ///
    /// This traverses the expressions to find all aggregate function calls,
    /// then deduplicates them while preserving order (since projection depends on order).
    pub fn extract_aggregates(
        select: &[(AstExpression, Option<String>)],
        having: Option<&AstExpression>,
        resolve_expression: impl Fn(&AstExpression) -> Result<Expression>,
    ) -> Result<Vec<AggregateFunc>> {
        let mut aggregates = Vec::new();

        // Extract aggregate functions from SELECT expressions
        for (expr, _) in select.iter() {
            Self::extract_aggregates_from_expr(expr, &mut aggregates, &resolve_expression)?;
        }

        // Extract aggregate functions from HAVING clause
        if let Some(having_expr) = having {
            Self::extract_aggregates_from_expr(having_expr, &mut aggregates, &resolve_expression)?;
        }

        // Deduplicate aggregates while preserving order
        // We need to maintain the original order because the projection depends on it
        let mut seen = std::collections::HashSet::new();
        aggregates.retain(|agg| {
            let key = format!("{:?}", agg);
            seen.insert(key)
        });

        Ok(aggregates)
    }

    /// Extract aggregate functions from a single expression (recursive)
    fn extract_aggregates_from_expr(
        expr: &AstExpression,
        aggregates: &mut Vec<AggregateFunc>,
        resolve_expression: &impl Fn(&AstExpression) -> Result<Expression>,
    ) -> Result<()> {
        use crate::parsing::ast::Operator;

        match expr {
            AstExpression::Function(name, args) if Self::is_aggregate_expr(expr) => {
                // Parse the aggregate argument using AggregateBuilder
                let arg = AggregateBuilder::parse_aggregate_arg(args, name, |arg_expr| {
                    resolve_expression(arg_expr)
                })?;

                // Build the aggregate function using AggregateBuilder
                let agg = AggregateBuilder::build_aggregate_func(name, arg)?;
                aggregates.push(agg);
            }
            // Recursively search for aggregates in operators
            AstExpression::Operator(op) => match op {
                Operator::And(l, r)
                | Operator::Or(l, r)
                | Operator::Xor(l, r)
                | Operator::Equal(l, r)
                | Operator::NotEqual(l, r)
                | Operator::GreaterThan(l, r)
                | Operator::GreaterThanOrEqual(l, r)
                | Operator::LessThan(l, r)
                | Operator::LessThanOrEqual(l, r)
                | Operator::Add(l, r)
                | Operator::Concat(l, r)
                | Operator::Subtract(l, r)
                | Operator::Multiply(l, r)
                | Operator::Divide(l, r)
                | Operator::Remainder(l, r)
                | Operator::Exponentiate(l, r)
                | Operator::BitwiseAnd(l, r)
                | Operator::BitwiseOr(l, r)
                | Operator::BitwiseXor(l, r)
                | Operator::BitwiseShiftLeft(l, r)
                | Operator::BitwiseShiftRight(l, r) => {
                    Self::extract_aggregates_from_expr(l, aggregates, resolve_expression)?;
                    Self::extract_aggregates_from_expr(r, aggregates, resolve_expression)?;
                }
                Operator::Not(e)
                | Operator::Negate(e)
                | Operator::Identity(e)
                | Operator::Factorial(e)
                | Operator::BitwiseNot(e) => {
                    Self::extract_aggregates_from_expr(e, aggregates, resolve_expression)?;
                }
                Operator::Between {
                    expr, low, high, ..
                } => {
                    Self::extract_aggregates_from_expr(expr, aggregates, resolve_expression)?;
                    Self::extract_aggregates_from_expr(low, aggregates, resolve_expression)?;
                    Self::extract_aggregates_from_expr(high, aggregates, resolve_expression)?;
                }
                Operator::InList { expr, list, .. } => {
                    Self::extract_aggregates_from_expr(expr, aggregates, resolve_expression)?;
                    for item in list {
                        Self::extract_aggregates_from_expr(item, aggregates, resolve_expression)?;
                    }
                }
                Operator::Like { expr, pattern, .. } | Operator::ILike { expr, pattern, .. } => {
                    Self::extract_aggregates_from_expr(expr, aggregates, resolve_expression)?;
                    Self::extract_aggregates_from_expr(pattern, aggregates, resolve_expression)?;
                }
                Operator::Is(e, _) => {
                    Self::extract_aggregates_from_expr(e, aggregates, resolve_expression)?;
                }
                Operator::InSubquery { expr, subquery, .. } => {
                    Self::extract_aggregates_from_expr(expr, aggregates, resolve_expression)?;
                    Self::extract_aggregates_from_expr(subquery, aggregates, resolve_expression)?;
                }
                Operator::Exists { subquery, .. } => {
                    Self::extract_aggregates_from_expr(subquery, aggregates, resolve_expression)?;
                }
            },
            // Non-aggregate functions - check their arguments
            AstExpression::Function(_name, args) => {
                for arg in args {
                    Self::extract_aggregates_from_expr(arg, aggregates, resolve_expression)?;
                }
            }
            // CASE expressions
            AstExpression::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                // Extract from operand if present
                if let Some(op) = operand {
                    Self::extract_aggregates_from_expr(op, aggregates, resolve_expression)?;
                }
                // Extract from all WHEN/THEN pairs
                for (when_expr, then_expr) in when_clauses {
                    Self::extract_aggregates_from_expr(when_expr, aggregates, resolve_expression)?;
                    Self::extract_aggregates_from_expr(then_expr, aggregates, resolve_expression)?;
                }
                // Extract from ELSE clause if present
                if let Some(else_expr) = else_clause {
                    Self::extract_aggregates_from_expr(else_expr, aggregates, resolve_expression)?;
                }
            }
            // Array access: base[index]
            AstExpression::ArrayAccess { base, index } => {
                Self::extract_aggregates_from_expr(base, aggregates, resolve_expression)?;
                Self::extract_aggregates_from_expr(index, aggregates, resolve_expression)?;
            }
            // Field access: base.field
            AstExpression::FieldAccess { base, .. } => {
                Self::extract_aggregates_from_expr(base, aggregates, resolve_expression)?;
            }
            // Array literal: [expr1, expr2, ...]
            AstExpression::ArrayLiteral(elements) => {
                for elem in elements {
                    Self::extract_aggregates_from_expr(elem, aggregates, resolve_expression)?;
                }
            }
            // Map literal: {key1: value1, key2: value2, ...}
            AstExpression::MapLiteral(pairs) => {
                for (key, value) in pairs {
                    Self::extract_aggregates_from_expr(key, aggregates, resolve_expression)?;
                    Self::extract_aggregates_from_expr(value, aggregates, resolve_expression)?;
                }
            }
            // Subquery - already handled separately, don't recurse
            AstExpression::Subquery(_) => {}
            // CAST expression - extract from the inner expression
            AstExpression::Cast { expr, .. } => {
                Self::extract_aggregates_from_expr(expr, aggregates, resolve_expression)?;
            }
            // These don't contain nested expressions
            AstExpression::All
            | AstExpression::QualifiedWildcard(_)
            | AstExpression::Column(_, _)
            | AstExpression::Literal(_)
            | AstExpression::Parameter(_) => {}
        }

        Ok(())
    }

    /// Resolve HAVING expression, replacing aggregate functions with column references
    ///
    /// In the HAVING clause, aggregate functions need to be replaced with references
    /// to the columns produced by the Aggregate node. This method walks the expression
    /// tree and replaces aggregate calls with Column(index) references.
    pub fn resolve_having_expression<F>(
        having_expr: &AstExpression,
        group_by: &[AstExpression],
        aggregates: &[AggregateFunc],
        group_by_count: usize,
        resolve_expression: &F,
    ) -> Result<Expression>
    where
        F: Fn(&AstExpression) -> Result<Expression>,
    {
        use crate::parsing::ast::Operator;

        match having_expr {
            // Check if this is an aggregate function that needs to be replaced with a column reference
            AstExpression::Function(name, args) if Self::is_aggregate_expr(having_expr) => {
                // Parse the aggregate argument
                let arg = AggregateBuilder::parse_aggregate_arg(args, name, |arg_expr| {
                    resolve_expression(arg_expr)
                })?;

                // Build the target aggregate function
                let target_agg = AggregateBuilder::build_aggregate_func(name, arg)?;

                // Find the index of this aggregate
                if let Some(idx) = aggregates.iter().position(|a| a == &target_agg) {
                    // Return column reference: GROUP BY columns + aggregate index
                    Ok(Expression::Column(group_by_count + idx))
                } else {
                    Err(Error::ExecutionError(format!(
                        "Aggregate function {} not found in aggregate list",
                        name
                    )))
                }
            }
            // Check if this is a column reference from GROUP BY
            AstExpression::Column(table, col) => {
                // First check if it's in GROUP BY
                for (idx, group_expr) in group_by.iter().enumerate() {
                    if let AstExpression::Column(gt, gc) = group_expr
                        && gc == col
                        && (table.is_none() || table == gt)
                    {
                        return Ok(Expression::Column(idx));
                    }
                }
                // If not in GROUP BY, resolve normally
                resolve_expression(having_expr)
            }
            // Recursively handle operators
            AstExpression::Operator(op) => {
                let resolved_op = match op {
                    Operator::And(l, r) => {
                        let left = Self::resolve_having_expression(
                            l,
                            group_by,
                            aggregates,
                            group_by_count,
                            resolve_expression,
                        )?;
                        let right = Self::resolve_having_expression(
                            r,
                            group_by,
                            aggregates,
                            group_by_count,
                            resolve_expression,
                        )?;
                        Expression::And(Box::new(left), Box::new(right))
                    }
                    Operator::Or(l, r) => {
                        let left = Self::resolve_having_expression(
                            l,
                            group_by,
                            aggregates,
                            group_by_count,
                            resolve_expression,
                        )?;
                        let right = Self::resolve_having_expression(
                            r,
                            group_by,
                            aggregates,
                            group_by_count,
                            resolve_expression,
                        )?;
                        Expression::Or(Box::new(left), Box::new(right))
                    }
                    Operator::GreaterThan(l, r) => {
                        let left = Self::resolve_having_expression(
                            l,
                            group_by,
                            aggregates,
                            group_by_count,
                            resolve_expression,
                        )?;
                        let right = Self::resolve_having_expression(
                            r,
                            group_by,
                            aggregates,
                            group_by_count,
                            resolve_expression,
                        )?;
                        Expression::GreaterThan(Box::new(left), Box::new(right))
                    }
                    Operator::GreaterThanOrEqual(l, r) => {
                        let left = Self::resolve_having_expression(
                            l,
                            group_by,
                            aggregates,
                            group_by_count,
                            resolve_expression,
                        )?;
                        let right = Self::resolve_having_expression(
                            r,
                            group_by,
                            aggregates,
                            group_by_count,
                            resolve_expression,
                        )?;
                        Expression::GreaterThanOrEqual(Box::new(left), Box::new(right))
                    }
                    Operator::LessThan(l, r) => {
                        let left = Self::resolve_having_expression(
                            l,
                            group_by,
                            aggregates,
                            group_by_count,
                            resolve_expression,
                        )?;
                        let right = Self::resolve_having_expression(
                            r,
                            group_by,
                            aggregates,
                            group_by_count,
                            resolve_expression,
                        )?;
                        Expression::LessThan(Box::new(left), Box::new(right))
                    }
                    Operator::LessThanOrEqual(l, r) => {
                        let left = Self::resolve_having_expression(
                            l,
                            group_by,
                            aggregates,
                            group_by_count,
                            resolve_expression,
                        )?;
                        let right = Self::resolve_having_expression(
                            r,
                            group_by,
                            aggregates,
                            group_by_count,
                            resolve_expression,
                        )?;
                        Expression::LessThanOrEqual(Box::new(left), Box::new(right))
                    }
                    Operator::Equal(l, r) => {
                        let left = Self::resolve_having_expression(
                            l,
                            group_by,
                            aggregates,
                            group_by_count,
                            resolve_expression,
                        )?;
                        let right = Self::resolve_having_expression(
                            r,
                            group_by,
                            aggregates,
                            group_by_count,
                            resolve_expression,
                        )?;
                        Expression::Equal(Box::new(left), Box::new(right))
                    }
                    Operator::NotEqual(l, r) => {
                        let left = Self::resolve_having_expression(
                            l,
                            group_by,
                            aggregates,
                            group_by_count,
                            resolve_expression,
                        )?;
                        let right = Self::resolve_having_expression(
                            r,
                            group_by,
                            aggregates,
                            group_by_count,
                            resolve_expression,
                        )?;
                        Expression::NotEqual(Box::new(left), Box::new(right))
                    }
                    Operator::Add(l, r) => {
                        let left = Self::resolve_having_expression(
                            l,
                            group_by,
                            aggregates,
                            group_by_count,
                            resolve_expression,
                        )?;
                        let right = Self::resolve_having_expression(
                            r,
                            group_by,
                            aggregates,
                            group_by_count,
                            resolve_expression,
                        )?;
                        Expression::Add(Box::new(left), Box::new(right))
                    }
                    Operator::Subtract(l, r) => {
                        let left = Self::resolve_having_expression(
                            l,
                            group_by,
                            aggregates,
                            group_by_count,
                            resolve_expression,
                        )?;
                        let right = Self::resolve_having_expression(
                            r,
                            group_by,
                            aggregates,
                            group_by_count,
                            resolve_expression,
                        )?;
                        Expression::Subtract(Box::new(left), Box::new(right))
                    }
                    Operator::Multiply(l, r) => {
                        let left = Self::resolve_having_expression(
                            l,
                            group_by,
                            aggregates,
                            group_by_count,
                            resolve_expression,
                        )?;
                        let right = Self::resolve_having_expression(
                            r,
                            group_by,
                            aggregates,
                            group_by_count,
                            resolve_expression,
                        )?;
                        Expression::Multiply(Box::new(left), Box::new(right))
                    }
                    Operator::Divide(l, r) => {
                        let left = Self::resolve_having_expression(
                            l,
                            group_by,
                            aggregates,
                            group_by_count,
                            resolve_expression,
                        )?;
                        let right = Self::resolve_having_expression(
                            r,
                            group_by,
                            aggregates,
                            group_by_count,
                            resolve_expression,
                        )?;
                        Expression::Divide(Box::new(left), Box::new(right))
                    }
                    _ => {
                        // For other operators, resolve normally
                        return resolve_expression(having_expr);
                    }
                };
                Ok(resolved_op)
            }
            // For other expressions, resolve normally
            _ => resolve_expression(having_expr),
        }
    }

    /// Resolve an expression containing aggregates by replacing aggregate calls with column references
    ///
    /// Similar to resolve_having_expression but for SELECT clause expressions.
    /// This is used for complex expressions like CASE statements that contain aggregates.
    pub fn resolve_aggregate_expression<F, G>(
        expr: &AstExpression,
        group_by: &[AstExpression],
        aggregates: &[AggregateFunc],
        group_by_count: usize,
        resolve_expression: &F,
        find_aggregate_index: &G,
    ) -> Result<Expression>
    where
        F: Fn(&AstExpression) -> Result<Expression>,
        G: Fn(&AstExpression) -> Result<usize>,
    {
        use crate::parsing::ast::Operator;

        match expr {
            // Check if this is an aggregate function that needs to be replaced with a column reference
            AstExpression::Function(_, _) if Self::is_aggregate_expr(expr) => {
                let idx = find_aggregate_index(expr)?;
                Ok(Expression::Column(idx))
            }
            // Recursively handle operators
            AstExpression::Operator(op) => {
                let resolved_op = match op {
                    Operator::Between {
                        expr,
                        low,
                        high,
                        negated,
                    } => {
                        let resolved_expr = Self::resolve_aggregate_expression(
                            expr,
                            group_by,
                            aggregates,
                            group_by_count,
                            resolve_expression,
                            find_aggregate_index,
                        )?;
                        let resolved_low = Self::resolve_aggregate_expression(
                            low,
                            group_by,
                            aggregates,
                            group_by_count,
                            resolve_expression,
                            find_aggregate_index,
                        )?;
                        let resolved_high = Self::resolve_aggregate_expression(
                            high,
                            group_by,
                            aggregates,
                            group_by_count,
                            resolve_expression,
                            find_aggregate_index,
                        )?;
                        Expression::Between(
                            Box::new(resolved_expr),
                            Box::new(resolved_low),
                            Box::new(resolved_high),
                            *negated,
                        )
                    }
                    _ => {
                        // For other operators, use the existing resolve_having_expression logic
                        return Self::resolve_having_expression(
                            expr,
                            group_by,
                            aggregates,
                            group_by_count,
                            resolve_expression,
                        );
                    }
                };
                Ok(resolved_op)
            }
            // CASE expressions
            AstExpression::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                let resolved_operand = if let Some(op) = operand {
                    Some(Box::new(Self::resolve_aggregate_expression(
                        op,
                        group_by,
                        aggregates,
                        group_by_count,
                        resolve_expression,
                        find_aggregate_index,
                    )?))
                } else {
                    None
                };

                let mut resolved_when_clauses = Vec::new();
                for (when_expr, then_expr) in when_clauses {
                    let resolved_when = Self::resolve_aggregate_expression(
                        when_expr,
                        group_by,
                        aggregates,
                        group_by_count,
                        resolve_expression,
                        find_aggregate_index,
                    )?;
                    let resolved_then = Self::resolve_aggregate_expression(
                        then_expr,
                        group_by,
                        aggregates,
                        group_by_count,
                        resolve_expression,
                        find_aggregate_index,
                    )?;
                    resolved_when_clauses.push((resolved_when, resolved_then));
                }

                let resolved_else = if let Some(else_expr) = else_clause {
                    Some(Box::new(Self::resolve_aggregate_expression(
                        else_expr,
                        group_by,
                        aggregates,
                        group_by_count,
                        resolve_expression,
                        find_aggregate_index,
                    )?))
                } else {
                    None
                };

                Ok(Expression::Case {
                    operand: resolved_operand,
                    when_clauses: resolved_when_clauses,
                    else_clause: resolved_else,
                })
            }
            // For other expressions, delegate to resolve_having_expression
            _ => Self::resolve_having_expression(
                expr,
                group_by,
                aggregates,
                group_by_count,
                resolve_expression,
            ),
        }
    }

    /// Find the index of an aggregate function in the aggregates list
    ///
    /// Used to map aggregate function calls to their position in the output
    /// of the Aggregate node (which is GROUP BY columns + aggregate results).
    pub fn find_aggregate_index(
        expr: &AstExpression,
        aggregates: &[AggregateFunc],
        group_by_count: usize,
        resolve_expression: impl Fn(&AstExpression) -> Result<Expression>,
    ) -> Result<usize> {
        if let AstExpression::Function(name, args) = expr {
            // Parse the aggregate argument
            let arg = AggregateBuilder::parse_aggregate_arg(args, name, |arg_expr| {
                resolve_expression(arg_expr)
            })?;

            // Build the target aggregate function
            let target_agg = AggregateBuilder::build_aggregate_func(name, arg)?;

            if let Some(idx) = aggregates.iter().position(|a| a == &target_agg) {
                Ok(group_by_count + idx)
            } else {
                Err(Error::ExecutionError(format!(
                    "Aggregate function {} not found in aggregate list",
                    name
                )))
            }
        } else {
            Err(Error::ExecutionError("Not an aggregate function".into()))
        }
    }

    /// Find the index of an expression in the GROUP BY clause
    ///
    /// Used to ensure non-aggregate expressions in SELECT are valid GROUP BY columns.
    pub fn find_group_by_index(expr: &AstExpression, group_by: &[AstExpression]) -> Result<usize> {
        // Try to match the expression with a GROUP BY expression
        for (idx, gb_expr) in group_by.iter().enumerate() {
            if Self::expressions_match(expr, gb_expr) {
                return Ok(idx);
            }
        }

        Err(Error::ExecutionError(format!(
            "Expression '{}' in SELECT is not in GROUP BY clause",
            expr.to_column_name()
        )))
    }

    /// Check if two AST expressions match structurally
    ///
    /// This is a simple structural comparison used to match SELECT expressions
    /// with GROUP BY expressions. It doesn't handle all cases but covers the
    /// common ones (columns, literals, simple functions).
    fn expressions_match(a: &AstExpression, b: &AstExpression) -> bool {
        match (a, b) {
            (AstExpression::Column(t1, c1), AstExpression::Column(t2, c2)) => t1 == t2 && c1 == c2,
            (AstExpression::Literal(v1), AstExpression::Literal(v2)) => v1 == v2,
            (AstExpression::Function(n1, args1), AstExpression::Function(n2, args2)) => {
                n1 == n2
                    && args1.len() == args2.len()
                    && args1
                        .iter()
                        .zip(args2.iter())
                        .all(|(a1, a2)| Self::expressions_match(a1, a2))
            }
            _ => false,
        }
    }
}
