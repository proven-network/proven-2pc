//! Caching parser for SQL statements
//!
//! This module provides a caching wrapper around the SQL parser that
//! maintains an LRU cache of parsed statements to avoid redundant parsing.

use super::{Parser, Statement};
use crate::error::Result;
use lru::LruCache;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;

/// Default capacity for the parse cache
const DEFAULT_CACHE_CAPACITY: usize = 1000;

/// A caching wrapper around the SQL parser
pub struct CachingParser {
    /// LRU cache for parsed statements
    cache: LruCache<String, Arc<Statement>>,
    /// Track parameter counts for validation
    param_counts: HashMap<String, usize>,
}

impl CachingParser {
    /// Create a new caching parser with default capacity
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_CACHE_CAPACITY)
    }

    /// Create a new caching parser with specified capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            cache: LruCache::new(
                NonZeroUsize::new(capacity).unwrap_or(NonZeroUsize::new(100).unwrap()),
            ),
            param_counts: HashMap::new(),
        }
    }

    /// Parse SQL with caching
    pub fn parse(&mut self, sql: &str) -> Result<Arc<Statement>> {
        // Normalize SQL for better cache hits (trim whitespace)
        let normalized = normalize_sql(sql);

        // Check cache
        if let Some(statement) = self.cache.get(&normalized) {
            return Ok(statement.clone());
        }

        // Parse the statement
        let statement = Parser::parse(sql)?;
        let param_count = count_parameters(&statement);
        let arc_statement = Arc::new(statement);

        // Cache the result
        self.cache.put(normalized.clone(), arc_statement.clone());
        self.param_counts.insert(normalized, param_count);

        Ok(arc_statement)
    }

    /// Clear the cache
    pub fn clear(&mut self) {
        self.cache.clear();
        self.param_counts.clear();
    }
}

impl Default for CachingParser {
    fn default() -> Self {
        Self::new()
    }
}

/// Normalize SQL for consistent caching
#[inline]
fn normalize_sql(sql: &str) -> String {
    // For parameterized queries, they're usually already normalized
    // Just trim to avoid unnecessary allocations
    sql.trim().to_string()
}

/// Count the number of parameter placeholders in a statement
fn count_parameters(stmt: &Statement) -> usize {
    use crate::parsing::ast::{DmlStatement, Expression, InsertSource, Operator};

    fn count_expr_params(expr: &Expression) -> usize {
        match expr {
            Expression::Parameter(_) => 1,
            Expression::Operator(op) => count_operator_params(op),
            Expression::Function(_, args) => args.iter().map(count_expr_params).sum(),
            Expression::ArrayLiteral(elements) => elements.iter().map(count_expr_params).sum(),
            Expression::MapLiteral(entries) => entries
                .iter()
                .map(|(k, v)| count_expr_params(k) + count_expr_params(v))
                .sum(),
            Expression::ArrayAccess { base, index } => {
                count_expr_params(base) + count_expr_params(index)
            }
            Expression::FieldAccess { base, .. } => count_expr_params(base),
            Expression::Subquery(_) => {
                // For now, don't count params in subqueries
                0
            }
            _ => 0,
        }
    }

    fn count_operator_params(op: &Operator) -> usize {
        match op {
            Operator::And(l, r)
            | Operator::Or(l, r)
            | Operator::Equal(l, r)
            | Operator::NotEqual(l, r)
            | Operator::LessThan(l, r)
            | Operator::LessThanOrEqual(l, r)
            | Operator::GreaterThan(l, r)
            | Operator::GreaterThanOrEqual(l, r)
            | Operator::Add(l, r)
            | Operator::Concat(l, r)
            | Operator::Subtract(l, r)
            | Operator::Multiply(l, r)
            | Operator::Divide(l, r)
            | Operator::Remainder(l, r)
            | Operator::Exponentiate(l, r)
            | Operator::Like(l, r) => count_expr_params(l) + count_expr_params(r),

            Operator::Not(e)
            | Operator::Negate(e)
            | Operator::Identity(e)
            | Operator::Factorial(e) => count_expr_params(e),

            Operator::Is(e, _) => count_expr_params(e),

            Operator::InList { expr, list, .. } => {
                count_expr_params(expr) + list.iter().map(count_expr_params).sum::<usize>()
            }

            Operator::Between {
                expr, low, high, ..
            } => count_expr_params(expr) + count_expr_params(low) + count_expr_params(high),

            Operator::InSubquery { expr, subquery, .. } => {
                // For now, don't count params in subqueries
                count_expr_params(expr) + count_expr_params(subquery)
            }

            Operator::Exists { subquery, .. } => {
                // For now, don't count params in subqueries
                count_expr_params(subquery)
            }
        }
    }

    match stmt {
        Statement::Dml(dml) => match dml {
            DmlStatement::Select(select) => {
                let mut count = 0;

                // SELECT expressions
                for (expr, _) in &select.select {
                    count += count_expr_params(expr);
                }

                // WHERE clause
                if let Some(where_expr) = &select.r#where {
                    count += count_expr_params(where_expr);
                }

                // GROUP BY
                for expr in &select.group_by {
                    count += count_expr_params(expr);
                }

                // HAVING
                if let Some(having) = &select.having {
                    count += count_expr_params(having);
                }

                // ORDER BY
                for (expr, _) in &select.order_by {
                    count += count_expr_params(expr);
                }

                count
            }

            DmlStatement::Insert { source, .. } => {
                match source {
                    InsertSource::Values(rows) => rows
                        .iter()
                        .flat_map(|row| row.iter())
                        .map(count_expr_params)
                        .sum(),
                    InsertSource::Select(select) => {
                        // Recursively count in subselect
                        count_parameters(&Statement::Dml(DmlStatement::Select(select.clone())))
                    }
                    InsertSource::DefaultValues => 0,
                }
            }

            DmlStatement::Update { set, r#where, .. } => {
                let mut count = 0;

                // SET expressions
                for e in set.values().flatten() {
                    count += count_expr_params(e);
                }

                // WHERE clause
                if let Some(where_expr) = r#where {
                    count += count_expr_params(where_expr);
                }

                count
            }

            DmlStatement::Delete { r#where, .. } => r#where.as_ref().map_or(0, count_expr_params),
        },

        Statement::Ddl(_) => 0,     // DDL statements don't have parameters
        Statement::Explain(_) => 0, // TODO: handle explain
    }
}
