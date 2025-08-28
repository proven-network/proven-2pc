//! Aggregation execution for GROUP BY queries

use crate::error::Result;
use crate::sql::types::value::{Row, Value};
use std::collections::HashMap;
use std::sync::Arc;

/// An aggregator that groups rows by key and computes aggregate functions
pub struct Aggregator {
    /// The group key expressions (empty for no grouping)
    group_by: Vec<crate::sql::types::expression::Expression>,
    /// The aggregate functions to compute
    aggregates: Vec<Aggregate>,
    /// Buckets for each group key, containing accumulators
    buckets: HashMap<Vec<Value>, Vec<Box<dyn Accumulator>>>,
    /// Whether we've seen any rows (for empty aggregates)
    has_rows: bool,
}

impl Aggregator {
    /// Create a new aggregator
    pub fn new(
        group_by: Vec<crate::sql::types::expression::Expression>,
        aggregates: Vec<Aggregate>,
    ) -> Self {
        Self {
            group_by,
            aggregates,
            buckets: HashMap::new(),
            has_rows: false,
        }
    }

    /// Add a row to the aggregator with context
    pub fn add(
        &mut self,
        row: Arc<Row>,
        context: &crate::transaction_id::TransactionContext,
    ) -> Result<()> {
        self.has_rows = true;

        // Evaluate the group key
        let key = self
            .group_by
            .iter()
            .map(|expr| expr.evaluate(Some(&row), context))
            .collect::<Result<Vec<_>>>()?;

        // Get or create the bucket for this key
        let bucket = self.buckets.entry(key).or_insert_with(|| {
            self.aggregates
                .iter()
                .map(|agg| agg.accumulator())
                .collect()
        });

        // Add the row to each accumulator
        for (acc, agg) in bucket.iter_mut().zip(&self.aggregates) {
            acc.add(&agg.expression().evaluate(Some(&row), context)?)?;
        }

        Ok(())
    }

    /// Finalize and return all aggregated rows
    pub fn finalize(self) -> Result<Vec<Arc<Row>>> {
        // If no GROUP BY and no rows, return single row with NULL aggregates
        if self.group_by.is_empty() && !self.has_rows {
            let row = self
                .aggregates
                .iter()
                .map(|agg| agg.accumulator().finalize())
                .collect::<Result<Vec<_>>>()?;
            return Ok(vec![Arc::new(row)]);
        }

        // Convert buckets to rows
        let mut rows = Vec::new();
        for (key, bucket) in self.buckets {
            let mut row = key;
            for acc in bucket {
                row.push(acc.finalize()?);
            }
            rows.push(Arc::new(row));
        }

        Ok(rows)
    }
}

/// An aggregate function
#[derive(Debug, Clone)]
pub enum Aggregate {
    Count(crate::sql::types::expression::Expression),
    Sum(crate::sql::types::expression::Expression),
    Avg(crate::sql::types::expression::Expression),
    Min(crate::sql::types::expression::Expression),
    Max(crate::sql::types::expression::Expression),
}

impl Aggregate {
    /// Get the expression being aggregated
    pub fn expression(&self) -> &crate::sql::types::expression::Expression {
        match self {
            Self::Count(e) | Self::Sum(e) | Self::Avg(e) | Self::Min(e) | Self::Max(e) => e,
        }
    }

    /// Create an accumulator for this aggregate
    fn accumulator(&self) -> Box<dyn Accumulator> {
        match self {
            Self::Count(_) => Box::new(CountAccumulator::new()),
            Self::Sum(_) => Box::new(SumAccumulator::new()),
            Self::Avg(_) => Box::new(AvgAccumulator::new()),
            Self::Min(_) => Box::new(MinAccumulator::new()),
            Self::Max(_) => Box::new(MaxAccumulator::new()),
        }
    }
}

/// An accumulator for a single aggregate function
trait Accumulator: Send {
    /// Add a value to the accumulator
    fn add(&mut self, value: &Value) -> Result<()>;

    /// Finalize and return the result
    fn finalize(self: Box<Self>) -> Result<Value>;
}

/// COUNT accumulator
struct CountAccumulator {
    count: u64,
}

impl CountAccumulator {
    fn new() -> Self {
        Self { count: 0 }
    }
}

impl Accumulator for CountAccumulator {
    fn add(&mut self, value: &Value) -> Result<()> {
        // COUNT(*) counts all rows, COUNT(expr) skips NULLs
        if !matches!(value, Value::Null) {
            self.count += 1;
        }
        Ok(())
    }

    fn finalize(self: Box<Self>) -> Result<Value> {
        Ok(Value::Integer(self.count as i64))
    }
}

/// SUM accumulator
struct SumAccumulator {
    sum: Option<Value>,
}

impl SumAccumulator {
    fn new() -> Self {
        Self { sum: None }
    }
}

impl Accumulator for SumAccumulator {
    fn add(&mut self, value: &Value) -> Result<()> {
        if matches!(value, Value::Null) {
            return Ok(());
        }

        self.sum = match (&self.sum, value) {
            (None, v) => Some(v.clone()),
            (Some(Value::Integer(a)), Value::Integer(b)) => {
                Some(Value::Integer(a.saturating_add(*b)))
            }
            (Some(Value::Decimal(a)), Value::Decimal(b)) => Some(Value::Decimal(a + b)),
            (Some(Value::Integer(a)), Value::Decimal(b)) => {
                Some(Value::Decimal(rust_decimal::Decimal::from(*a) + b))
            }
            (Some(Value::Decimal(b)), Value::Integer(a)) => {
                Some(Value::Decimal(b + rust_decimal::Decimal::from(*a)))
            }
            _ => {
                return Err(crate::error::Error::ExecutionError(
                    "Cannot sum non-numeric values".into(),
                ));
            }
        };

        Ok(())
    }

    fn finalize(self: Box<Self>) -> Result<Value> {
        Ok(self.sum.unwrap_or(Value::Null))
    }
}

/// AVG accumulator
struct AvgAccumulator {
    sum: rust_decimal::Decimal,
    count: u64,
}

impl AvgAccumulator {
    fn new() -> Self {
        Self {
            sum: rust_decimal::Decimal::ZERO,
            count: 0,
        }
    }
}

impl Accumulator for AvgAccumulator {
    fn add(&mut self, value: &Value) -> Result<()> {
        match value {
            Value::Null => {}
            Value::Integer(n) => {
                self.sum += rust_decimal::Decimal::from(*n);
                self.count += 1;
            }
            Value::Decimal(n) => {
                self.sum += n;
                self.count += 1;
            }
            _ => {
                return Err(crate::error::Error::ExecutionError(
                    "Cannot average non-numeric values".into(),
                ));
            }
        }
        Ok(())
    }

    fn finalize(self: Box<Self>) -> Result<Value> {
        if self.count == 0 {
            Ok(Value::Null)
        } else {
            Ok(Value::Decimal(
                self.sum / rust_decimal::Decimal::from(self.count),
            ))
        }
    }
}

/// MIN accumulator
struct MinAccumulator {
    min: Option<Value>,
}

impl MinAccumulator {
    fn new() -> Self {
        Self { min: None }
    }
}

impl Accumulator for MinAccumulator {
    fn add(&mut self, value: &Value) -> Result<()> {
        if matches!(value, Value::Null) {
            return Ok(());
        }

        self.min = match &self.min {
            None => Some(value.clone()),
            Some(current) if value < current => Some(value.clone()),
            _ => self.min.clone(),
        };

        Ok(())
    }

    fn finalize(self: Box<Self>) -> Result<Value> {
        Ok(self.min.unwrap_or(Value::Null))
    }
}

/// MAX accumulator
struct MaxAccumulator {
    max: Option<Value>,
}

impl MaxAccumulator {
    fn new() -> Self {
        Self { max: None }
    }
}

impl Accumulator for MaxAccumulator {
    fn add(&mut self, value: &Value) -> Result<()> {
        if matches!(value, Value::Null) {
            return Ok(());
        }

        self.max = match &self.max {
            None => Some(value.clone()),
            Some(current) if value > current => Some(value.clone()),
            _ => self.max.clone(),
        };

        Ok(())
    }

    fn finalize(self: Box<Self>) -> Result<Value> {
        Ok(self.max.unwrap_or(Value::Null))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::types::expression::Expression;

    #[test]
    fn test_count() {
        let mut agg = Aggregator::new(vec![], vec![Aggregate::Count(Expression::Column(0))]);

        let ctx = crate::transaction_id::TransactionContext::new(
            crate::hlc::HlcTimestamp::from_physical_time(0, crate::hlc::NodeId::new(1)),
        );
        agg.add(Arc::new(vec![Value::Integer(1)]), &ctx).unwrap();
        agg.add(Arc::new(vec![Value::Integer(2)]), &ctx).unwrap();
        agg.add(Arc::new(vec![Value::Null]), &ctx).unwrap();

        let rows = agg.finalize().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Integer(2)); // COUNT skips NULLs
    }

    #[test]
    fn test_sum() {
        let mut agg = Aggregator::new(vec![], vec![Aggregate::Sum(Expression::Column(0))]);

        let ctx = crate::transaction_id::TransactionContext::new(
            crate::hlc::HlcTimestamp::from_physical_time(0, crate::hlc::NodeId::new(1)),
        );
        agg.add(Arc::new(vec![Value::Integer(10)]), &ctx).unwrap();
        agg.add(Arc::new(vec![Value::Integer(20)]), &ctx).unwrap();
        agg.add(Arc::new(vec![Value::Integer(30)]), &ctx).unwrap();

        let rows = agg.finalize().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Integer(60));
    }

    #[test]
    fn test_group_by() {
        let mut agg = Aggregator::new(
            vec![Expression::Column(0)], // GROUP BY first column
            vec![Aggregate::Count(Expression::Column(1))],
        );

        let ctx = crate::transaction_id::TransactionContext::new(
            crate::hlc::HlcTimestamp::from_physical_time(0, crate::hlc::NodeId::new(1)),
        );
        agg.add(
            Arc::new(vec![Value::String("a".into()), Value::Integer(1)]),
            &ctx,
        )
        .unwrap();
        agg.add(
            Arc::new(vec![Value::String("b".into()), Value::Integer(2)]),
            &ctx,
        )
        .unwrap();
        agg.add(
            Arc::new(vec![Value::String("a".into()), Value::Integer(3)]),
            &ctx,
        )
        .unwrap();

        let mut rows = agg.finalize().unwrap();
        rows.sort_by_key(|r| r[0].clone());

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], Value::String("a".into()));
        assert_eq!(rows[0][1], Value::Integer(2)); // 2 rows with "a"
        assert_eq!(rows[1][0], Value::String("b".into()));
        assert_eq!(rows[1][1], Value::Integer(1)); // 1 row with "b"
    }
}
