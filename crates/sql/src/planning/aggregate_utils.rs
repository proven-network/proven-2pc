//! Aggregate function utilities
//!
//! This module provides utilities for building and working with aggregate functions,
//! eliminating code duplication across the planner.

use crate::error::{Error, Result};
use crate::parsing::ast::Expression as AstExpression;
use crate::types::expression::Expression;
use crate::types::plan::AggregateFunc;

/// Helper for building aggregate functions
pub struct AggregateBuilder;

impl AggregateBuilder {
    /// Parse the argument for an aggregate function
    ///
    /// Handles special cases:
    /// - Empty args -> Constant(1)
    /// - Single wildcard -> All or Constant(1) depending on DISTINCT
    /// - Normal expression -> resolve it
    pub fn parse_aggregate_arg<F>(
        args: &[AstExpression],
        func_name: &str,
        resolve_fn: F,
    ) -> Result<Expression>
    where
        F: FnOnce(&AstExpression) -> Result<Expression>,
    {
        if args.is_empty() {
            Ok(Expression::Constant(crate::types::Value::integer(1)))
        } else if args.len() == 1 && matches!(args[0], AstExpression::All) {
            if func_name.ends_with("_DISTINCT") {
                Ok(Expression::All)
            } else {
                Ok(Expression::Constant(crate::types::Value::integer(1)))
            }
        } else {
            resolve_fn(&args[0])
        }
    }

    /// Build an AggregateFunc from a function name and argument
    ///
    /// Returns an error if the function name is not a recognized aggregate
    pub fn build_aggregate_func(func_name: &str, arg: Expression) -> Result<AggregateFunc> {
        let func_name_upper = func_name.to_uppercase();

        match func_name_upper.as_str() {
            "COUNT" => Ok(AggregateFunc::Count(arg)),
            "COUNT_DISTINCT" => Ok(AggregateFunc::CountDistinct(arg)),
            "SUM" => Ok(AggregateFunc::Sum(arg)),
            "SUM_DISTINCT" => Ok(AggregateFunc::SumDistinct(arg)),
            "AVG" => Ok(AggregateFunc::Avg(arg)),
            "AVG_DISTINCT" => Ok(AggregateFunc::AvgDistinct(arg)),
            "MIN" => Ok(AggregateFunc::Min(arg)),
            "MIN_DISTINCT" => Ok(AggregateFunc::MinDistinct(arg)),
            "MAX" => Ok(AggregateFunc::Max(arg)),
            "MAX_DISTINCT" => Ok(AggregateFunc::MaxDistinct(arg)),
            "STDEV" => Ok(AggregateFunc::StDev(arg)),
            "STDEV_DISTINCT" => Ok(AggregateFunc::StDevDistinct(arg)),
            "VARIANCE" => Ok(AggregateFunc::Variance(arg)),
            "VARIANCE_DISTINCT" => Ok(AggregateFunc::VarianceDistinct(arg)),
            _ => Err(Error::ExecutionError(format!(
                "Unknown aggregate function: {}",
                func_name
            ))),
        }
    }

    /// Check if a function name is a recognized aggregate function
    pub fn is_aggregate_name(func_name: &str) -> bool {
        let func_name_upper = func_name.to_uppercase();
        matches!(
            func_name_upper.as_str(),
            "COUNT"
                | "COUNT_DISTINCT"
                | "SUM"
                | "SUM_DISTINCT"
                | "AVG"
                | "AVG_DISTINCT"
                | "MIN"
                | "MIN_DISTINCT"
                | "MAX"
                | "MAX_DISTINCT"
                | "STDEV"
                | "STDEV_DISTINCT"
                | "VARIANCE"
                | "VARIANCE_DISTINCT"
        )
    }
}
