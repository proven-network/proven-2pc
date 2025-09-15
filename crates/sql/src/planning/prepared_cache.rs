//! Prepared statement cache for SQL queries
//!
//! This module implements an LRU cache for prepared SQL statements,
//! storing parsed and planned queries to avoid re-processing.

use super::plan::Plan;
use crate::error::Result;
use crate::types::value::Value;
use std::collections::{HashMap, VecDeque};

/// Maximum number of prepared statements to cache
const DEFAULT_CACHE_SIZE: usize = 100;

/// Location where a parameter appears in the plan
#[derive(Debug, Clone)]
pub enum ParameterLocation {
    /// Parameter in an expression at this path
    Expression(Vec<usize>),
    /// Parameter in a VALUES clause
    Values { row: usize, col: usize },
}

/// A prepared plan template with parameter information
#[derive(Debug, Clone)]
pub struct PreparedPlan {
    /// The plan template with Parameter expressions
    pub plan_template: Plan,
    /// Number of parameters expected
    pub param_count: usize,
    /// Locations of parameters in the plan
    pub param_locations: Vec<ParameterLocation>,
}

/// LRU cache for prepared statements
pub struct PreparedCache {
    /// Map from SQL string to cache entry
    cache: HashMap<String, PreparedPlan>,
    /// LRU queue tracking access order (most recent at back)
    lru: VecDeque<String>,
    /// Maximum cache size
    max_size: usize,
}

impl PreparedCache {
    /// Create a new prepared statement cache
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_CACHE_SIZE)
    }

    /// Create a cache with specified capacity
    pub fn with_capacity(max_size: usize) -> Self {
        Self {
            cache: HashMap::with_capacity(max_size.min(DEFAULT_CACHE_SIZE)),
            lru: VecDeque::with_capacity(max_size.min(DEFAULT_CACHE_SIZE)),
            max_size,
        }
    }

    /// Get a prepared plan from the cache
    pub fn get(&mut self, sql: &str) -> Option<&PreparedPlan> {
        if self.cache.contains_key(sql) {
            // Move to end of LRU queue
            self.update_lru(sql);
            self.cache.get(sql)
        } else {
            None
        }
    }

    /// Insert a prepared plan into the cache
    pub fn insert(&mut self, sql: String, plan: PreparedPlan) {
        // Check if we need to evict
        if self.cache.len() >= self.max_size && !self.cache.contains_key(&sql) {
            // Evict least recently used
            if let Some(evicted) = self.lru.pop_front() {
                self.cache.remove(&evicted);
            }
        }

        // Insert or update
        if self.cache.contains_key(&sql) {
            self.update_lru(&sql);
        } else {
            self.lru.push_back(sql.clone());
        }
        self.cache.insert(sql, plan);
    }

    /// Clear the cache
    pub fn clear(&mut self) {
        self.cache.clear();
        self.lru.clear();
    }

    /// Get the current cache size
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    /// Check if cache is empty
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    /// Update LRU order for an accessed key
    fn update_lru(&mut self, sql: &str) {
        // Remove from current position
        if let Some(pos) = self.lru.iter().position(|s| s == sql) {
            self.lru.remove(pos);
        }
        // Add to end (most recently used)
        self.lru.push_back(sql.to_string());
    }
}

impl Default for PreparedCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Bind parameter values to a prepared plan
pub fn bind_parameters(plan: &Plan, params: &[Value]) -> Result<Plan> {
    // We allow more parameters than needed (in case optimizer removed some)
    // but we still check that referenced parameters are available
    // The actual validation happens during binding when we try to access params[idx]

    // Clone the plan and replace all Parameter expressions with values
    let bound_plan = match plan {
        Plan::Select(node) => Plan::Select(Box::new(bind_node_parameters(node, params)?)),
        Plan::Insert {
            table,
            columns,
            source,
        } => Plan::Insert {
            table: table.clone(),
            columns: columns.clone(),
            source: Box::new(bind_node_parameters(source, params)?),
        },
        Plan::Update {
            table,
            assignments,
            source,
        } => Plan::Update {
            table: table.clone(),
            assignments: assignments
                .iter()
                .map(|(idx, expr)| Ok((*idx, bind_expression_parameters(expr, params)?)))
                .collect::<Result<Vec<_>>>()?,
            source: Box::new(bind_node_parameters(source, params)?),
        },
        Plan::Delete { table, source } => Plan::Delete {
            table: table.clone(),
            source: Box::new(bind_node_parameters(source, params)?),
        },
        // DDL operations shouldn't have parameters
        Plan::CreateTable { .. }
        | Plan::DropTable { .. }
        | Plan::CreateIndex { .. }
        | Plan::DropIndex { .. } => plan.clone(),
    };

    Ok(bound_plan)
}

/// Bind parameters in a plan node
fn bind_node_parameters(node: &super::plan::Node, params: &[Value]) -> Result<super::plan::Node> {
    use super::plan::Node;

    Ok(match node {
        Node::Scan { table, alias } => Node::Scan {
            table: table.clone(),
            alias: alias.clone(),
        },
        Node::IndexScan {
            table,
            alias,
            index_name,
            values,
        } => Node::IndexScan {
            table: table.clone(),
            alias: alias.clone(),
            index_name: index_name.clone(),
            values: values
                .iter()
                .map(|expr| bind_expression_parameters(expr, params))
                .collect::<Result<Vec<_>>>()?,
        },
        Node::IndexRangeScan {
            table,
            alias,
            index_name,
            start,
            start_inclusive,
            end,
            end_inclusive,
            reverse,
        } => Node::IndexRangeScan {
            table: table.clone(),
            alias: alias.clone(),
            index_name: index_name.clone(),
            start: start
                .as_ref()
                .map(|exprs| {
                    exprs
                        .iter()
                        .map(|expr| bind_expression_parameters(expr, params))
                        .collect::<Result<Vec<_>>>()
                })
                .transpose()?,
            start_inclusive: *start_inclusive,
            end: end
                .as_ref()
                .map(|exprs| {
                    exprs
                        .iter()
                        .map(|expr| bind_expression_parameters(expr, params))
                        .collect::<Result<Vec<_>>>()
                })
                .transpose()?,
            end_inclusive: *end_inclusive,
            reverse: *reverse,
        },
        Node::Filter { source, predicate } => Node::Filter {
            source: Box::new(bind_node_parameters(source, params)?),
            predicate: bind_expression_parameters(predicate, params)?,
        },
        Node::Projection {
            source,
            expressions,
            aliases,
        } => Node::Projection {
            source: Box::new(bind_node_parameters(source, params)?),
            expressions: expressions
                .iter()
                .map(|expr| bind_expression_parameters(expr, params))
                .collect::<Result<Vec<_>>>()?,
            aliases: aliases.clone(),
        },
        Node::Aggregate {
            source,
            group_by,
            aggregates,
        } => Node::Aggregate {
            source: Box::new(bind_node_parameters(source, params)?),
            group_by: group_by
                .iter()
                .map(|expr| bind_expression_parameters(expr, params))
                .collect::<Result<Vec<_>>>()?,
            aggregates: aggregates
                .iter()
                .map(|agg| bind_aggregate_parameters(agg, params))
                .collect::<Result<Vec<_>>>()?,
        },
        Node::Order { source, order_by } => Node::Order {
            source: Box::new(bind_node_parameters(source, params)?),
            order_by: order_by
                .iter()
                .map(|(expr, dir)| Ok((bind_expression_parameters(expr, params)?, *dir)))
                .collect::<Result<Vec<_>>>()?,
        },
        Node::Limit { source, limit } => Node::Limit {
            source: Box::new(bind_node_parameters(source, params)?),
            limit: *limit,
        },
        Node::Offset { source, offset } => Node::Offset {
            source: Box::new(bind_node_parameters(source, params)?),
            offset: *offset,
        },
        Node::HashJoin {
            left,
            right,
            left_col,
            right_col,
            join_type,
        } => Node::HashJoin {
            left: Box::new(bind_node_parameters(left, params)?),
            right: Box::new(bind_node_parameters(right, params)?),
            left_col: *left_col,
            right_col: *right_col,
            join_type: *join_type,
        },
        Node::NestedLoopJoin {
            left,
            right,
            predicate,
            join_type,
        } => Node::NestedLoopJoin {
            left: Box::new(bind_node_parameters(left, params)?),
            right: Box::new(bind_node_parameters(right, params)?),
            predicate: bind_expression_parameters(predicate, params)?,
            join_type: *join_type,
        },
        Node::Values { rows } => Node::Values {
            rows: rows
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|expr| bind_expression_parameters(expr, params))
                        .collect::<Result<Vec<_>>>()
                })
                .collect::<Result<Vec<_>>>()?,
        },
        Node::Nothing => Node::Nothing,
    })
}

/// Bind parameters in an expression
fn bind_expression_parameters(
    expr: &crate::types::expression::Expression,
    params: &[Value],
) -> Result<crate::types::expression::Expression> {
    use crate::types::expression::Expression;

    Ok(match expr {
        Expression::Parameter(idx) => {
            if *idx >= params.len() {
                return Err(crate::error::Error::ExecutionError(format!(
                    "parameter index {} out of bounds (have {} parameters)",
                    idx,
                    params.len()
                )));
            }
            Expression::Constant(params[*idx].clone())
        }
        Expression::Constant(v) => Expression::Constant(v.clone()),
        Expression::Column(i) => Expression::Column(*i),
        Expression::All => Expression::All,

        // Recursively bind parameters in nested expressions
        Expression::And(l, r) => Expression::And(
            Box::new(bind_expression_parameters(l, params)?),
            Box::new(bind_expression_parameters(r, params)?),
        ),
        Expression::Or(l, r) => Expression::Or(
            Box::new(bind_expression_parameters(l, params)?),
            Box::new(bind_expression_parameters(r, params)?),
        ),
        Expression::Not(e) => Expression::Not(Box::new(bind_expression_parameters(e, params)?)),

        Expression::Equal(l, r) => Expression::Equal(
            Box::new(bind_expression_parameters(l, params)?),
            Box::new(bind_expression_parameters(r, params)?),
        ),
        Expression::NotEqual(l, r) => Expression::NotEqual(
            Box::new(bind_expression_parameters(l, params)?),
            Box::new(bind_expression_parameters(r, params)?),
        ),
        Expression::GreaterThan(l, r) => Expression::GreaterThan(
            Box::new(bind_expression_parameters(l, params)?),
            Box::new(bind_expression_parameters(r, params)?),
        ),
        Expression::GreaterThanOrEqual(l, r) => Expression::GreaterThanOrEqual(
            Box::new(bind_expression_parameters(l, params)?),
            Box::new(bind_expression_parameters(r, params)?),
        ),
        Expression::LessThan(l, r) => Expression::LessThan(
            Box::new(bind_expression_parameters(l, params)?),
            Box::new(bind_expression_parameters(r, params)?),
        ),
        Expression::LessThanOrEqual(l, r) => Expression::LessThanOrEqual(
            Box::new(bind_expression_parameters(l, params)?),
            Box::new(bind_expression_parameters(r, params)?),
        ),
        Expression::Is(e, v) => {
            Expression::Is(Box::new(bind_expression_parameters(e, params)?), v.clone())
        }

        Expression::Add(l, r) => Expression::Add(
            Box::new(bind_expression_parameters(l, params)?),
            Box::new(bind_expression_parameters(r, params)?),
        ),
        Expression::Subtract(l, r) => Expression::Subtract(
            Box::new(bind_expression_parameters(l, params)?),
            Box::new(bind_expression_parameters(r, params)?),
        ),
        Expression::Multiply(l, r) => Expression::Multiply(
            Box::new(bind_expression_parameters(l, params)?),
            Box::new(bind_expression_parameters(r, params)?),
        ),
        Expression::Divide(l, r) => Expression::Divide(
            Box::new(bind_expression_parameters(l, params)?),
            Box::new(bind_expression_parameters(r, params)?),
        ),
        Expression::Remainder(l, r) => Expression::Remainder(
            Box::new(bind_expression_parameters(l, params)?),
            Box::new(bind_expression_parameters(r, params)?),
        ),
        Expression::Exponentiate(l, r) => Expression::Exponentiate(
            Box::new(bind_expression_parameters(l, params)?),
            Box::new(bind_expression_parameters(r, params)?),
        ),
        Expression::Factorial(e) => {
            Expression::Factorial(Box::new(bind_expression_parameters(e, params)?))
        }
        Expression::Identity(e) => {
            Expression::Identity(Box::new(bind_expression_parameters(e, params)?))
        }
        Expression::Negate(e) => {
            Expression::Negate(Box::new(bind_expression_parameters(e, params)?))
        }

        Expression::Like(l, r) => Expression::Like(
            Box::new(bind_expression_parameters(l, params)?),
            Box::new(bind_expression_parameters(r, params)?),
        ),

        Expression::Function(name, args) => Expression::Function(
            name.clone(),
            args.iter()
                .map(|arg| bind_expression_parameters(arg, params))
                .collect::<Result<Vec<_>>>()?,
        ),

        Expression::InList(expr, list, negated) => Expression::InList(
            Box::new(bind_expression_parameters(expr, params)?),
            list.iter()
                .map(|e| bind_expression_parameters(e, params))
                .collect::<Result<Vec<_>>>()?,
            *negated,
        ),

        Expression::Between(expr, low, high, negated) => Expression::Between(
            Box::new(bind_expression_parameters(expr, params)?),
            Box::new(bind_expression_parameters(low, params)?),
            Box::new(bind_expression_parameters(high, params)?),
            *negated,
        ),

        Expression::ArrayAccess(base, index) => Expression::ArrayAccess(
            Box::new(bind_expression_parameters(base, params)?),
            Box::new(bind_expression_parameters(index, params)?),
        ),

        Expression::FieldAccess(base, field) => Expression::FieldAccess(
            Box::new(bind_expression_parameters(base, params)?),
            field.clone(),
        ),

        Expression::ArrayLiteral(elements) => Expression::ArrayLiteral(
            elements
                .iter()
                .map(|e| bind_expression_parameters(e, params))
                .collect::<Result<Vec<_>>>()?,
        ),

        Expression::MapLiteral(pairs) => Expression::MapLiteral(
            pairs
                .iter()
                .map(|(k, v)| {
                    Ok((
                        bind_expression_parameters(k, params)?,
                        bind_expression_parameters(v, params)?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?,
        ),
    })
}

/// Bind parameters in an aggregate function
fn bind_aggregate_parameters(
    agg: &super::plan::AggregateFunc,
    params: &[Value],
) -> Result<super::plan::AggregateFunc> {
    use super::plan::AggregateFunc;

    Ok(match agg {
        AggregateFunc::Count(e) => AggregateFunc::Count(bind_expression_parameters(e, params)?),
        AggregateFunc::CountDistinct(e) => {
            AggregateFunc::CountDistinct(bind_expression_parameters(e, params)?)
        }
        AggregateFunc::Sum(e) => AggregateFunc::Sum(bind_expression_parameters(e, params)?),
        AggregateFunc::SumDistinct(e) => {
            AggregateFunc::SumDistinct(bind_expression_parameters(e, params)?)
        }
        AggregateFunc::Avg(e) => AggregateFunc::Avg(bind_expression_parameters(e, params)?),
        AggregateFunc::AvgDistinct(e) => {
            AggregateFunc::AvgDistinct(bind_expression_parameters(e, params)?)
        }
        AggregateFunc::Min(e) => AggregateFunc::Min(bind_expression_parameters(e, params)?),
        AggregateFunc::MinDistinct(e) => {
            AggregateFunc::MinDistinct(bind_expression_parameters(e, params)?)
        }
        AggregateFunc::Max(e) => AggregateFunc::Max(bind_expression_parameters(e, params)?),
        AggregateFunc::MaxDistinct(e) => {
            AggregateFunc::MaxDistinct(bind_expression_parameters(e, params)?)
        }
        AggregateFunc::StDev(e) => AggregateFunc::StDev(bind_expression_parameters(e, params)?),
        AggregateFunc::StDevDistinct(e) => {
            AggregateFunc::StDevDistinct(bind_expression_parameters(e, params)?)
        }
        AggregateFunc::Variance(e) => {
            AggregateFunc::Variance(bind_expression_parameters(e, params)?)
        }
        AggregateFunc::VarianceDistinct(e) => {
            AggregateFunc::VarianceDistinct(bind_expression_parameters(e, params)?)
        }
    })
}
