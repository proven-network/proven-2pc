//! MVCC-aware aggregation execution for GROUP BY queries
//!
//! This module handles aggregation operations with proper MVCC transaction context,
//! ensuring all expression evaluation respects transaction visibility rules.

use crate::error::{Error, Result};
use crate::planning::plan::AggregateFunc;
use crate::storage::MvccStorage;
use crate::stream::transaction::TransactionContext;
use crate::types::expression::Expression;
use crate::types::value::Value;
use std::collections::HashMap;
use std::sync::Arc;

/// MVCC-aware aggregator that groups rows by key and computes aggregate functions
pub struct Aggregator {
    /// The group key expressions (empty for no grouping)
    group_by: Vec<Expression>,
    /// The aggregate functions to compute
    aggregates: Vec<AggregateFunc>,
    /// Buckets for each group key
    buckets: HashMap<Vec<Value>, GroupAccumulator>,
}

impl Aggregator {
    /// Create a new aggregator
    pub fn new(group_by: Vec<Expression>, aggregates: Vec<AggregateFunc>) -> Self {
        Self {
            group_by,
            aggregates,
            buckets: HashMap::new(),
        }
    }

    /// Add a row to the aggregator with MVCC context
    pub fn add(
        &mut self,
        row: &Arc<Vec<Value>>,
        context: &TransactionContext,
        storage: &MvccStorage,
    ) -> Result<()> {
        // Evaluate the group key
        let key = self
            .group_by
            .iter()
            .map(|expr| evaluate_expression(expr, Some(row), context, storage))
            .collect::<Result<Vec<_>>>()?;

        // Get or create the bucket for this key
        let bucket = self
            .buckets
            .entry(key)
            .or_insert_with(|| GroupAccumulator::new(self.aggregates.clone()));

        // Add row to the bucket
        bucket.add_row(row, context, storage)?;

        Ok(())
    }

    /// Finalize and return all aggregated results
    pub fn finalize(self) -> Result<Vec<Arc<Vec<Value>>>> {
        if self.buckets.is_empty() && !self.aggregates.is_empty() {
            // No groups but we have aggregates - return single row with aggregate results
            let accumulator = GroupAccumulator::new(self.aggregates.clone());
            let result = accumulator.finalize()?;
            Ok(vec![Arc::new(result)])
        } else {
            // Return one row per group
            self.buckets
                .into_iter()
                .map(|(mut key, accumulator)| {
                    let mut values = accumulator.finalize()?;
                    key.append(&mut values);
                    Ok(Arc::new(key))
                })
                .collect()
        }
    }
}

/// Accumulator for a single group
struct GroupAccumulator {
    aggregates: Vec<AggregateFunc>,
    accumulators: Vec<Box<dyn Accumulator>>,
}

impl GroupAccumulator {
    fn new(aggregates: Vec<AggregateFunc>) -> Self {
        let accumulators: Vec<Box<dyn Accumulator>> = aggregates
            .iter()
            .map(|agg| create_accumulator(agg))
            .collect();

        Self {
            aggregates,
            accumulators,
        }
    }

    fn add_row(
        &mut self,
        row: &Arc<Vec<Value>>,
        context: &TransactionContext,
        storage: &MvccStorage,
    ) -> Result<()> {
        for (agg, acc) in self.aggregates.iter().zip(self.accumulators.iter_mut()) {
            acc.add(row, agg, context, storage)?;
        }
        Ok(())
    }

    fn finalize(self) -> Result<Vec<Value>> {
        self.accumulators
            .into_iter()
            .map(|acc| acc.finalize())
            .collect()
    }
}

/// Trait for aggregate accumulators
trait Accumulator: Send {
    /// Add a value to the accumulator
    fn add(
        &mut self,
        row: &Arc<Vec<Value>>,
        agg: &AggregateFunc,
        context: &TransactionContext,
        storage: &MvccStorage,
    ) -> Result<()>;

    /// Finalize and return the aggregate result
    fn finalize(self: Box<Self>) -> Result<Value>;
}

/// COUNT aggregate accumulator
struct CountAccumulator {
    count: i64,
}

impl Accumulator for CountAccumulator {
    fn add(
        &mut self,
        row: &Arc<Vec<Value>>,
        agg: &AggregateFunc,
        context: &TransactionContext,
        storage: &MvccStorage,
    ) -> Result<()> {
        if let AggregateFunc::Count(expr) = agg {
            let val = evaluate_expression(expr, Some(row), context, storage)?;
            if val != Value::Null {
                self.count += 1;
            }
        }
        Ok(())
    }

    fn finalize(self: Box<Self>) -> Result<Value> {
        Ok(Value::Integer(self.count))
    }
}

/// SUM aggregate accumulator
struct SumAccumulator {
    sum: Value,
}

impl Accumulator for SumAccumulator {
    fn add(
        &mut self,
        row: &Arc<Vec<Value>>,
        agg: &AggregateFunc,
        context: &TransactionContext,
        storage: &MvccStorage,
    ) -> Result<()> {
        if let AggregateFunc::Sum(expr) = agg {
            let val = evaluate_expression(expr, Some(row), context, storage)?;
            if val != Value::Null {
                self.sum = if self.sum == Value::Null {
                    val
                } else {
                    self.sum.add(&val)?
                };
            }
        }
        Ok(())
    }

    fn finalize(self: Box<Self>) -> Result<Value> {
        Ok(self.sum)
    }
}

/// AVG aggregate accumulator
struct AvgAccumulator {
    sum: Value,
    count: i64,
}

impl Accumulator for AvgAccumulator {
    fn add(
        &mut self,
        row: &Arc<Vec<Value>>,
        agg: &AggregateFunc,
        context: &TransactionContext,
        storage: &MvccStorage,
    ) -> Result<()> {
        if let AggregateFunc::Avg(expr) = agg {
            let val = evaluate_expression(expr, Some(row), context, storage)?;
            if val != Value::Null {
                self.sum = if self.sum == Value::Null {
                    val
                } else {
                    self.sum.add(&val)?
                };
                self.count += 1;
            }
        }
        Ok(())
    }

    fn finalize(self: Box<Self>) -> Result<Value> {
        if self.count > 0 {
            self.sum.divide(&Value::Integer(self.count))
        } else {
            Ok(Value::Null)
        }
    }
}

/// MIN aggregate accumulator
struct MinAccumulator {
    min: Value,
}

impl Accumulator for MinAccumulator {
    fn add(
        &mut self,
        row: &Arc<Vec<Value>>,
        agg: &AggregateFunc,
        context: &TransactionContext,
        storage: &MvccStorage,
    ) -> Result<()> {
        if let AggregateFunc::Min(expr) = agg {
            let val = evaluate_expression(expr, Some(row), context, storage)?;
            if val != Value::Null
                && (self.min == Value::Null || val.compare(&self.min)? == std::cmp::Ordering::Less)
            {
                self.min = val;
            }
        }
        Ok(())
    }

    fn finalize(self: Box<Self>) -> Result<Value> {
        Ok(self.min)
    }
}

/// MAX aggregate accumulator
struct MaxAccumulator {
    max: Value,
}

impl Accumulator for MaxAccumulator {
    fn add(
        &mut self,
        row: &Arc<Vec<Value>>,
        agg: &AggregateFunc,
        context: &TransactionContext,
        storage: &MvccStorage,
    ) -> Result<()> {
        if let AggregateFunc::Max(expr) = agg {
            let val = evaluate_expression(expr, Some(row), context, storage)?;
            if val != Value::Null
                && (self.max == Value::Null
                    || val.compare(&self.max)? == std::cmp::Ordering::Greater)
            {
                self.max = val;
            }
        }
        Ok(())
    }

    fn finalize(self: Box<Self>) -> Result<Value> {
        Ok(self.max)
    }
}

/// Create an accumulator for an aggregate function
fn create_accumulator(agg: &AggregateFunc) -> Box<dyn Accumulator> {
    match agg {
        AggregateFunc::Count(_) => Box::new(CountAccumulator { count: 0 }),
        AggregateFunc::Sum(_) => Box::new(SumAccumulator { sum: Value::Null }),
        AggregateFunc::Avg(_) => Box::new(AvgAccumulator {
            sum: Value::Null,
            count: 0,
        }),
        AggregateFunc::Min(_) => Box::new(MinAccumulator { min: Value::Null }),
        AggregateFunc::Max(_) => Box::new(MaxAccumulator { max: Value::Null }),
    }
}

/// Evaluate an expression with MVCC context
/// For now, we use a simplified version that handles common cases
/// TODO: Factor out expression evaluation into a shared module
pub(crate) fn evaluate_expression(
    expr: &Expression,
    row: Option<&Arc<Vec<Value>>>,
    _context: &TransactionContext,
    _storage: &MvccStorage,
) -> Result<Value> {
    match expr {
        Expression::Constant(val) => Ok(val.clone()),

        Expression::Column(index) => {
            if let Some(row) = row {
                row.get(*index).cloned().ok_or_else(|| {
                    Error::InvalidValue(format!("Column index {} out of bounds", index))
                })
            } else {
                Err(Error::InvalidValue(
                    "No row context for column reference".into(),
                ))
            }
        }

        // COUNT(*) is typically represented as Count(Constant(Integer(1))) or similar
        // We'll handle it at the aggregate level

        // Basic arithmetic operations
        Expression::Add(left, right) => {
            let l = evaluate_expression(left, row, _context, _storage)?;
            let r = evaluate_expression(right, row, _context, _storage)?;
            l.add(&r)
        }

        Expression::Subtract(left, right) => {
            let l = evaluate_expression(left, row, _context, _storage)?;
            let r = evaluate_expression(right, row, _context, _storage)?;
            l.subtract(&r)
        }

        Expression::Multiply(left, right) => {
            let l = evaluate_expression(left, row, _context, _storage)?;
            let r = evaluate_expression(right, row, _context, _storage)?;
            l.multiply(&r)
        }

        Expression::Divide(left, right) => {
            let l = evaluate_expression(left, row, _context, _storage)?;
            let r = evaluate_expression(right, row, _context, _storage)?;
            l.divide(&r)
        }

        _ => Err(Error::InvalidValue(
            "Complex expressions in aggregates not yet implemented".into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::mvcc::MvccStorage;
    use proven_hlc::{HlcClock, NodeId};

    fn setup_test() -> (MvccStorage, TransactionContext) {
        let storage = MvccStorage::new();
        let clock = HlcClock::new(NodeId::new(1));
        let timestamp = clock.now();
        let context = TransactionContext::new(timestamp);
        (storage, context)
    }

    #[test]
    fn test_count_aggregation() -> Result<()> {
        let (storage, context) = setup_test();

        let mut aggregator = Aggregator::new(
            vec![],
            vec![AggregateFunc::Count(Expression::Constant(Value::Integer(
                1,
            )))],
        );

        // Add some rows
        aggregator.add(&Arc::new(vec![Value::Integer(1)]), &context, &storage)?;
        aggregator.add(&Arc::new(vec![Value::Integer(2)]), &context, &storage)?;
        aggregator.add(&Arc::new(vec![Value::Integer(3)]), &context, &storage)?;

        let results = aggregator.finalize()?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0][0], Value::Integer(3));

        Ok(())
    }

    #[test]
    fn test_sum_aggregation() -> Result<()> {
        let (storage, context) = setup_test();

        let mut aggregator =
            Aggregator::new(vec![], vec![AggregateFunc::Sum(Expression::Column(0))]);

        aggregator.add(&Arc::new(vec![Value::Integer(10)]), &context, &storage)?;
        aggregator.add(&Arc::new(vec![Value::Integer(20)]), &context, &storage)?;
        aggregator.add(&Arc::new(vec![Value::Integer(30)]), &context, &storage)?;

        let results = aggregator.finalize()?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0][0], Value::Integer(60));

        Ok(())
    }

    #[test]
    fn test_group_by_aggregation() -> Result<()> {
        let (storage, context) = setup_test();

        // GROUP BY category (column 0), COUNT(*)
        let mut aggregator = Aggregator::new(
            vec![Expression::Column(0)],
            vec![AggregateFunc::Count(Expression::Constant(Value::Integer(
                1,
            )))],
        );

        // Add rows: [category, value]
        aggregator.add(
            &Arc::new(vec![Value::String("A".into()), Value::Integer(10)]),
            &context,
            &storage,
        )?;
        aggregator.add(
            &Arc::new(vec![Value::String("B".into()), Value::Integer(20)]),
            &context,
            &storage,
        )?;
        aggregator.add(
            &Arc::new(vec![Value::String("A".into()), Value::Integer(30)]),
            &context,
            &storage,
        )?;
        aggregator.add(
            &Arc::new(vec![Value::String("B".into()), Value::Integer(40)]),
            &context,
            &storage,
        )?;
        aggregator.add(
            &Arc::new(vec![Value::String("A".into()), Value::Integer(50)]),
            &context,
            &storage,
        )?;

        let mut results = aggregator.finalize()?;
        results.sort_by(|a, b| a[0].compare(&b[0]).unwrap());

        assert_eq!(results.len(), 2);
        // Group A: 3 rows
        assert_eq!(results[0][0], Value::String("A".into()));
        assert_eq!(results[0][1], Value::Integer(3));
        // Group B: 2 rows
        assert_eq!(results[1][0], Value::String("B".into()));
        assert_eq!(results[1][1], Value::Integer(2));

        Ok(())
    }

    #[test]
    fn test_min_max_aggregation() -> Result<()> {
        let (storage, context) = setup_test();

        let mut aggregator = Aggregator::new(
            vec![],
            vec![
                AggregateFunc::Min(Expression::Column(0)),
                AggregateFunc::Max(Expression::Column(0)),
            ],
        );

        aggregator.add(&Arc::new(vec![Value::Integer(5)]), &context, &storage)?;
        aggregator.add(&Arc::new(vec![Value::Integer(2)]), &context, &storage)?;
        aggregator.add(&Arc::new(vec![Value::Integer(8)]), &context, &storage)?;
        aggregator.add(&Arc::new(vec![Value::Integer(1)]), &context, &storage)?;

        let results = aggregator.finalize()?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0][0], Value::Integer(1)); // MIN
        assert_eq!(results[0][1], Value::Integer(8)); // MAX

        Ok(())
    }

    #[test]
    fn test_avg_aggregation() -> Result<()> {
        let (storage, context) = setup_test();

        let mut aggregator =
            Aggregator::new(vec![], vec![AggregateFunc::Avg(Expression::Column(0))]);

        aggregator.add(&Arc::new(vec![Value::Integer(10)]), &context, &storage)?;
        aggregator.add(&Arc::new(vec![Value::Integer(20)]), &context, &storage)?;
        aggregator.add(&Arc::new(vec![Value::Integer(30)]), &context, &storage)?;

        let results = aggregator.finalize()?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0][0], Value::Integer(20)); // (10+20+30)/3 = 20

        Ok(())
    }
}
