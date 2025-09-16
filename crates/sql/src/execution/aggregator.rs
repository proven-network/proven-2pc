//! MVCC-aware aggregation execution for GROUP BY queries
//!
//! This module handles aggregation operations with proper MVCC transaction context,
//! ensuring all expression evaluation respects transaction visibility rules.

use crate::error::{Error, Result};
use crate::planning::plan::AggregateFunc;
use crate::storage::MvccStorage;
use crate::stream::transaction::TransactionContext;
use crate::types::evaluator;
use crate::types::expression::Expression;
use crate::types::value::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Wrapper for Value to make it hashable for DISTINCT tracking
#[derive(Debug, Clone)]
struct HashableValue(Value);

impl PartialEq for HashableValue {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for HashableValue {}

impl std::hash::Hash for HashableValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match &self.0 {
            Value::Null => 0u8.hash(state),
            Value::Bool(b) => (1u8, b).hash(state),
            Value::I8(v) => (2u8, v).hash(state),
            Value::I16(v) => (3u8, v).hash(state),
            Value::I32(v) => (4u8, v).hash(state),
            Value::I64(v) => (5u8, v).hash(state),
            Value::I128(v) => (6u8, v).hash(state),
            Value::U8(v) => (7u8, v).hash(state),
            Value::U16(v) => (8u8, v).hash(state),
            Value::U32(v) => (9u8, v).hash(state),
            Value::U64(v) => (10u8, v).hash(state),
            Value::U128(v) => (11u8, v).hash(state),
            // For floats, convert to bits for hashing
            Value::F32(f) => (12u8, f.to_bits()).hash(state),
            Value::F64(f) => (13u8, f.to_bits()).hash(state),
            Value::Str(s) => (14u8, s).hash(state),
            Value::List(v) => {
                15u8.hash(state);
                for item in v {
                    HashableValue(item.clone()).hash(state);
                }
            }
            // Add more types as needed
            _ => {
                // For complex types, use debug representation
                (255u8, format!("{:?}", self.0)).hash(state);
            }
        }
    }
}

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
    distinct: bool,
    seen_values: HashSet<HashableValue>,
}

impl Accumulator for CountAccumulator {
    fn add(
        &mut self,
        row: &Arc<Vec<Value>>,
        agg: &AggregateFunc,
        context: &TransactionContext,
        storage: &MvccStorage,
    ) -> Result<()> {
        let expr = match agg {
            AggregateFunc::Count(expr) | AggregateFunc::CountDistinct(expr) => expr,
            _ => return Ok(()),
        };

        // Special handling for COUNT(DISTINCT *)
        if matches!(expr, Expression::All) && self.distinct {
            // For COUNT(DISTINCT *), hash the entire row
            let row_hash = HashableValue(Value::List(row.as_ref().clone()));
            if self.seen_values.insert(row_hash) {
                self.count += 1;
            }
        } else {
            let val = evaluate_expression(expr, Some(row), context, storage)?;
            if val != Value::Null {
                if self.distinct {
                    if self.seen_values.insert(HashableValue(val)) {
                        self.count += 1;
                    }
                } else {
                    self.count += 1;
                }
            }
        }
        Ok(())
    }

    fn finalize(self: Box<Self>) -> Result<Value> {
        Ok(Value::integer(self.count))
    }
}

/// SUM aggregate accumulator
struct SumAccumulator {
    sum: Value,
    distinct: bool,
    seen_values: HashSet<HashableValue>,
}

impl Accumulator for SumAccumulator {
    fn add(
        &mut self,
        row: &Arc<Vec<Value>>,
        agg: &AggregateFunc,
        context: &TransactionContext,
        storage: &MvccStorage,
    ) -> Result<()> {
        let expr = match agg {
            AggregateFunc::Sum(expr) | AggregateFunc::SumDistinct(expr) => expr,
            _ => return Ok(()),
        };

        let val = evaluate_expression(expr, Some(row), context, storage)?;
        if val != Value::Null {
            if self.distinct {
                if self.seen_values.insert(HashableValue(val.clone())) {
                    self.sum = if self.sum == Value::Null {
                        val
                    } else {
                        evaluator::add(&self.sum, &val)?
                    };
                }
            } else {
                self.sum = if self.sum == Value::Null {
                    val
                } else {
                    evaluator::add(&self.sum, &val)?
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
    distinct: bool,
    seen_values: HashSet<HashableValue>,
}

impl Accumulator for AvgAccumulator {
    fn add(
        &mut self,
        row: &Arc<Vec<Value>>,
        agg: &AggregateFunc,
        context: &TransactionContext,
        storage: &MvccStorage,
    ) -> Result<()> {
        let expr = match agg {
            AggregateFunc::Avg(expr) | AggregateFunc::AvgDistinct(expr) => expr,
            _ => return Ok(()),
        };

        let val = evaluate_expression(expr, Some(row), context, storage)?;
        if val != Value::Null {
            if self.distinct {
                if self.seen_values.insert(HashableValue(val.clone())) {
                    self.sum = if self.sum == Value::Null {
                        val
                    } else {
                        evaluator::add(&self.sum, &val)?
                    };
                    self.count += 1;
                }
            } else {
                self.sum = if self.sum == Value::Null {
                    val
                } else {
                    evaluator::add(&self.sum, &val)?
                };
                self.count += 1;
            }
        }
        Ok(())
    }

    fn finalize(self: Box<Self>) -> Result<Value> {
        if self.count > 0 {
            evaluator::divide(&self.sum, &Value::integer(self.count))
        } else {
            Ok(Value::Null)
        }
    }
}

/// MIN aggregate accumulator
struct MinAccumulator {
    min: Value,
    distinct: bool,
    seen_values: HashSet<HashableValue>,
}

impl Accumulator for MinAccumulator {
    fn add(
        &mut self,
        row: &Arc<Vec<Value>>,
        agg: &AggregateFunc,
        context: &TransactionContext,
        storage: &MvccStorage,
    ) -> Result<()> {
        let expr = match agg {
            AggregateFunc::Min(expr) | AggregateFunc::MinDistinct(expr) => expr,
            _ => return Ok(()),
        };

        let val = evaluate_expression(expr, Some(row), context, storage)?;
        if val != Value::Null {
            // For MIN with DISTINCT, we still need to track seen values but
            // MIN itself naturally handles duplicates (min of duplicates is same)
            if self.distinct {
                self.seen_values.insert(HashableValue(val.clone()));
            }
            if self.min == Value::Null
                || evaluator::compare(&val, &self.min)? == std::cmp::Ordering::Less
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
    distinct: bool,
    seen_values: HashSet<HashableValue>,
}

impl Accumulator for MaxAccumulator {
    fn add(
        &mut self,
        row: &Arc<Vec<Value>>,
        agg: &AggregateFunc,
        context: &TransactionContext,
        storage: &MvccStorage,
    ) -> Result<()> {
        let expr = match agg {
            AggregateFunc::Max(expr) | AggregateFunc::MaxDistinct(expr) => expr,
            _ => return Ok(()),
        };

        let val = evaluate_expression(expr, Some(row), context, storage)?;
        if val != Value::Null {
            // For MAX with DISTINCT, we still need to track seen values but
            // MAX itself naturally handles duplicates (max of duplicates is same)
            if self.distinct {
                self.seen_values.insert(HashableValue(val.clone()));
            }
            if self.max == Value::Null
                || evaluator::compare(&val, &self.max)? == std::cmp::Ordering::Greater
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

/// STDEV aggregate accumulator
struct StDevAccumulator {
    values: Vec<f64>,
    distinct: bool,
    seen_values: HashSet<HashableValue>,
}

impl Accumulator for StDevAccumulator {
    fn add(
        &mut self,
        row: &Arc<Vec<Value>>,
        agg: &AggregateFunc,
        context: &TransactionContext,
        storage: &MvccStorage,
    ) -> Result<()> {
        let expr = match agg {
            AggregateFunc::StDev(expr) | AggregateFunc::StDevDistinct(expr) => expr,
            _ => return Ok(()),
        };

        let val = evaluate_expression(expr, Some(row), context, storage)?;
        if val != Value::Null {
            // Convert to f64
            let num_val = match &val {
                Value::I8(n) => *n as f64,
                Value::I16(n) => *n as f64,
                Value::I32(n) => *n as f64,
                Value::I64(n) => *n as f64,
                Value::I128(n) => *n as f64,
                Value::U8(n) => *n as f64,
                Value::U16(n) => *n as f64,
                Value::U32(n) => *n as f64,
                Value::U64(n) => *n as f64,
                Value::U128(n) => *n as f64,
                Value::F32(n) => *n as f64,
                Value::F64(n) => *n,
                Value::Decimal(d) => {
                    // Convert decimal to f64
                    d.to_string().parse::<f64>().unwrap_or(0.0)
                }
                _ => return Ok(()), // Skip non-numeric values
            };

            if self.distinct {
                if self.seen_values.insert(HashableValue(val)) {
                    self.values.push(num_val);
                }
            } else {
                self.values.push(num_val);
            }
        }
        Ok(())
    }

    fn finalize(self: Box<Self>) -> Result<Value> {
        if self.values.len() <= 1 {
            // Standard deviation requires at least 2 values
            return Ok(Value::Null);
        }

        let n = self.values.len() as f64;
        let mean = self.values.iter().sum::<f64>() / n;

        // Calculate sample standard deviation (n-1 denominator)
        let variance = self.values.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (n - 1.0);

        let stdev = variance.sqrt();
        Ok(Value::F64(stdev))
    }
}

/// VARIANCE aggregate accumulator
struct VarianceAccumulator {
    values: Vec<f64>,
    distinct: bool,
    seen_values: HashSet<HashableValue>,
}

impl Accumulator for VarianceAccumulator {
    fn add(
        &mut self,
        row: &Arc<Vec<Value>>,
        agg: &AggregateFunc,
        context: &TransactionContext,
        storage: &MvccStorage,
    ) -> Result<()> {
        let expr = match agg {
            AggregateFunc::Variance(expr) | AggregateFunc::VarianceDistinct(expr) => expr,
            _ => return Ok(()),
        };

        let val = evaluate_expression(expr, Some(row), context, storage)?;
        if val != Value::Null {
            // Convert to f64
            let num_val = match &val {
                Value::I8(n) => *n as f64,
                Value::I16(n) => *n as f64,
                Value::I32(n) => *n as f64,
                Value::I64(n) => *n as f64,
                Value::I128(n) => *n as f64,
                Value::U8(n) => *n as f64,
                Value::U16(n) => *n as f64,
                Value::U32(n) => *n as f64,
                Value::U64(n) => *n as f64,
                Value::U128(n) => *n as f64,
                Value::F32(n) => *n as f64,
                Value::F64(n) => *n,
                Value::Decimal(d) => {
                    // Convert decimal to f64
                    d.to_string().parse::<f64>().unwrap_or(0.0)
                }
                _ => return Ok(()), // Skip non-numeric values
            };

            if self.distinct {
                if self.seen_values.insert(HashableValue(val)) {
                    self.values.push(num_val);
                }
            } else {
                self.values.push(num_val);
            }
        }
        Ok(())
    }

    fn finalize(self: Box<Self>) -> Result<Value> {
        if self.values.len() <= 1 {
            // Variance requires at least 2 values
            return Ok(Value::Null);
        }

        let n = self.values.len() as f64;
        let mean = self.values.iter().sum::<f64>() / n;

        // Calculate sample variance (n-1 denominator)
        let variance = self.values.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (n - 1.0);

        Ok(Value::F64(variance))
    }
}

/// Create an accumulator for an aggregate function
fn create_accumulator(agg: &AggregateFunc) -> Box<dyn Accumulator> {
    match agg {
        AggregateFunc::Count(_) => Box::new(CountAccumulator {
            count: 0,
            distinct: false,
            seen_values: HashSet::new(),
        }),
        AggregateFunc::CountDistinct(_) => Box::new(CountAccumulator {
            count: 0,
            distinct: true,
            seen_values: HashSet::new(),
        }),
        AggregateFunc::Sum(_) => Box::new(SumAccumulator {
            sum: Value::Null,
            distinct: false,
            seen_values: HashSet::new(),
        }),
        AggregateFunc::SumDistinct(_) => Box::new(SumAccumulator {
            sum: Value::Null,
            distinct: true,
            seen_values: HashSet::new(),
        }),
        AggregateFunc::Avg(_) => Box::new(AvgAccumulator {
            sum: Value::Null,
            count: 0,
            distinct: false,
            seen_values: HashSet::new(),
        }),
        AggregateFunc::AvgDistinct(_) => Box::new(AvgAccumulator {
            sum: Value::Null,
            count: 0,
            distinct: true,
            seen_values: HashSet::new(),
        }),
        AggregateFunc::Min(_) => Box::new(MinAccumulator {
            min: Value::Null,
            distinct: false,
            seen_values: HashSet::new(),
        }),
        AggregateFunc::MinDistinct(_) => Box::new(MinAccumulator {
            min: Value::Null,
            distinct: true,
            seen_values: HashSet::new(),
        }),
        AggregateFunc::Max(_) => Box::new(MaxAccumulator {
            max: Value::Null,
            distinct: false,
            seen_values: HashSet::new(),
        }),
        AggregateFunc::MaxDistinct(_) => Box::new(MaxAccumulator {
            max: Value::Null,
            distinct: true,
            seen_values: HashSet::new(),
        }),
        AggregateFunc::StDev(_) => Box::new(StDevAccumulator {
            values: Vec::new(),
            distinct: false,
            seen_values: HashSet::new(),
        }),
        AggregateFunc::StDevDistinct(_) => Box::new(StDevAccumulator {
            values: Vec::new(),
            distinct: true,
            seen_values: HashSet::new(),
        }),
        AggregateFunc::Variance(_) => Box::new(VarianceAccumulator {
            values: Vec::new(),
            distinct: false,
            seen_values: HashSet::new(),
        }),
        AggregateFunc::VarianceDistinct(_) => Box::new(VarianceAccumulator {
            values: Vec::new(),
            distinct: true,
            seen_values: HashSet::new(),
        }),
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

        Expression::All => {
            // Expression::All should only be used in COUNT(DISTINCT *)
            // and is handled specially in the accumulator
            Ok(Value::integer(1))
        }

        // Basic arithmetic operations
        Expression::Add(left, right) => {
            let l = evaluate_expression(left, row, _context, _storage)?;
            let r = evaluate_expression(right, row, _context, _storage)?;
            evaluator::add(&l, &r)
        }

        Expression::Subtract(left, right) => {
            let l = evaluate_expression(left, row, _context, _storage)?;
            let r = evaluate_expression(right, row, _context, _storage)?;
            evaluator::subtract(&l, &r)
        }

        Expression::Multiply(left, right) => {
            let l = evaluate_expression(left, row, _context, _storage)?;
            let r = evaluate_expression(right, row, _context, _storage)?;
            evaluator::multiply(&l, &r)
        }

        Expression::Divide(left, right) => {
            let l = evaluate_expression(left, row, _context, _storage)?;
            let r = evaluate_expression(right, row, _context, _storage)?;
            evaluator::divide(&l, &r)
        }

        Expression::Function(name, args) => {
            // Evaluate function arguments
            let arg_values: Result<Vec<_>> = args
                .iter()
                .map(|arg| evaluate_expression(arg, row, _context, _storage))
                .collect();
            // Call the function evaluator
            crate::types::functions::evaluate_function(name, &arg_values?, _context)
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
            vec![AggregateFunc::Count(Expression::Constant(Value::integer(
                1,
            )))],
        );

        // Add some rows
        aggregator.add(&Arc::new(vec![Value::integer(1)]), &context, &storage)?;
        aggregator.add(&Arc::new(vec![Value::integer(2)]), &context, &storage)?;
        aggregator.add(&Arc::new(vec![Value::integer(3)]), &context, &storage)?;

        let results = aggregator.finalize()?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0][0], Value::integer(3));

        Ok(())
    }

    #[test]
    fn test_sum_aggregation() -> Result<()> {
        let (storage, context) = setup_test();

        let mut aggregator =
            Aggregator::new(vec![], vec![AggregateFunc::Sum(Expression::Column(0))]);

        aggregator.add(&Arc::new(vec![Value::integer(10)]), &context, &storage)?;
        aggregator.add(&Arc::new(vec![Value::integer(20)]), &context, &storage)?;
        aggregator.add(&Arc::new(vec![Value::integer(30)]), &context, &storage)?;

        let results = aggregator.finalize()?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0][0], Value::integer(60));

        Ok(())
    }

    #[test]
    fn test_group_by_aggregation() -> Result<()> {
        let (storage, context) = setup_test();

        // GROUP BY category (column 0), COUNT(*)
        let mut aggregator = Aggregator::new(
            vec![Expression::Column(0)],
            vec![AggregateFunc::Count(Expression::Constant(Value::integer(
                1,
            )))],
        );

        // Add rows: [category, value]
        aggregator.add(
            &Arc::new(vec![Value::string("A"), Value::integer(10)]),
            &context,
            &storage,
        )?;
        aggregator.add(
            &Arc::new(vec![Value::string("B"), Value::integer(20)]),
            &context,
            &storage,
        )?;
        aggregator.add(
            &Arc::new(vec![Value::string("A"), Value::integer(30)]),
            &context,
            &storage,
        )?;
        aggregator.add(
            &Arc::new(vec![Value::string("B"), Value::integer(40)]),
            &context,
            &storage,
        )?;
        aggregator.add(
            &Arc::new(vec![Value::string("A"), Value::integer(50)]),
            &context,
            &storage,
        )?;

        let mut results = aggregator.finalize()?;
        results.sort_by(|a, b| evaluator::compare(&a[0], &b[0]).unwrap());

        assert_eq!(results.len(), 2);
        // Group A: 3 rows
        assert_eq!(results[0][0], Value::string("A"));
        assert_eq!(results[0][1], Value::integer(3));
        // Group B: 2 rows
        assert_eq!(results[1][0], Value::string("B"));
        assert_eq!(results[1][1], Value::integer(2));

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

        aggregator.add(&Arc::new(vec![Value::integer(5)]), &context, &storage)?;
        aggregator.add(&Arc::new(vec![Value::integer(2)]), &context, &storage)?;
        aggregator.add(&Arc::new(vec![Value::integer(8)]), &context, &storage)?;
        aggregator.add(&Arc::new(vec![Value::integer(1)]), &context, &storage)?;

        let results = aggregator.finalize()?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0][0], Value::integer(1)); // MIN
        assert_eq!(results[0][1], Value::integer(8)); // MAX

        Ok(())
    }

    #[test]
    fn test_avg_aggregation() -> Result<()> {
        let (storage, context) = setup_test();

        let mut aggregator =
            Aggregator::new(vec![], vec![AggregateFunc::Avg(Expression::Column(0))]);

        aggregator.add(&Arc::new(vec![Value::integer(10)]), &context, &storage)?;
        aggregator.add(&Arc::new(vec![Value::integer(20)]), &context, &storage)?;
        aggregator.add(&Arc::new(vec![Value::integer(30)]), &context, &storage)?;

        let results = aggregator.finalize()?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0][0], Value::integer(20)); // (10+20+30)/3 = 20

        Ok(())
    }
}
