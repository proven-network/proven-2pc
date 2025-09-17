//! Parameter handling for execution
//!
//! This module provides parameter binding and evaluation support
//! for parameterized queries during execution.

use crate::types::expression::Expression;
use crate::types::value::{Value, Row};
use crate::error::{Error, Result};

/// Context for parameter evaluation during execution
#[derive(Debug, Clone)]
pub struct ParameterContext {
    /// Parameter values in order
    values: Vec<Value>,
}

impl ParameterContext {
    /// Create a new parameter context
    pub fn new(values: Vec<Value>) -> Self {
        Self { values }
    }

    /// Create an empty context (no parameters)
    pub fn empty() -> Self {
        Self { values: Vec::new() }
    }

    /// Get a parameter value by index
    pub fn get(&self, index: usize) -> Result<&Value> {
        self.values.get(index).ok_or_else(|| {
            Error::ExecutionError(format!("Parameter {} out of bounds", index))
        })
    }

    /// Get the number of parameters
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Check if there are no parameters
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

/// Evaluate an expression with parameter support
pub fn evaluate_with_params(
    expr: &Expression,
    row: Option<&Row>,
    params: &ParameterContext,
) -> Result<Value> {
    match expr {
        Expression::Constant(value) => Ok(value.clone()),

        Expression::Column(index) => {
            row.and_then(|r| r.0.get(*index))
                .cloned()
                .ok_or_else(|| Error::ExecutionError(format!("Column {} not found", index)))
        }

        Expression::Parameter(index) => params.get(*index).cloned(),

        Expression::All => {
            // Return the entire row for COUNT(*) etc.
            Ok(Value::Struct(row.map(|r| r.0.clone()).unwrap_or_default()))
        }

        // Logical operators
        Expression::And(left, right) => {
            let left_val = evaluate_with_params(left, row, params)?;
            // Short-circuit evaluation
            if !left_val.to_bool()? {
                return Ok(Value::Bool(false));
            }
            let right_val = evaluate_with_params(right, row, params)?;
            Ok(Value::Bool(left_val.to_bool()? && right_val.to_bool()?))
        }

        Expression::Or(left, right) => {
            let left_val = evaluate_with_params(left, row, params)?;
            // Short-circuit evaluation
            if left_val.to_bool()? {
                return Ok(Value::Bool(true));
            }
            let right_val = evaluate_with_params(right, row, params)?;
            Ok(Value::Bool(left_val.to_bool()? || right_val.to_bool()?))
        }

        Expression::Not(expr) => {
            let val = evaluate_with_params(expr, row, params)?;
            Ok(Value::Bool(!val.to_bool()?))
        }

        // Comparison operators
        Expression::Equal(left, right) => {
            let left_val = evaluate_with_params(left, row, params)?;
            let right_val = evaluate_with_params(right, row, params)?;
            Ok(Value::Bool(left_val == right_val))
        }

        Expression::NotEqual(left, right) => {
            let left_val = evaluate_with_params(left, row, params)?;
            let right_val = evaluate_with_params(right, row, params)?;
            Ok(Value::Bool(left_val != right_val))
        }

        Expression::LessThan(left, right) => {
            let left_val = evaluate_with_params(left, row, params)?;
            let right_val = evaluate_with_params(right, row, params)?;
            Ok(Value::Bool(left_val < right_val))
        }

        Expression::LessThanOrEqual(left, right) => {
            let left_val = evaluate_with_params(left, row, params)?;
            let right_val = evaluate_with_params(right, row, params)?;
            Ok(Value::Bool(left_val <= right_val))
        }

        Expression::GreaterThan(left, right) => {
            let left_val = evaluate_with_params(left, row, params)?;
            let right_val = evaluate_with_params(right, row, params)?;
            Ok(Value::Bool(left_val > right_val))
        }

        Expression::GreaterThanOrEqual(left, right) => {
            let left_val = evaluate_with_params(left, row, params)?;
            let right_val = evaluate_with_params(right, row, params)?;
            Ok(Value::Bool(left_val >= right_val))
        }

        Expression::Is(expr, value) => {
            let val = evaluate_with_params(expr, row, params)?;
            Ok(Value::Bool(val == *value))
        }

        // Arithmetic operators
        Expression::Add(left, right) => {
            let left_val = evaluate_with_params(left, row, params)?;
            let right_val = evaluate_with_params(right, row, params)?;
            left_val.add(&right_val)
        }

        Expression::Subtract(left, right) => {
            let left_val = evaluate_with_params(left, row, params)?;
            let right_val = evaluate_with_params(right, row, params)?;
            left_val.subtract(&right_val)
        }

        Expression::Multiply(left, right) => {
            let left_val = evaluate_with_params(left, row, params)?;
            let right_val = evaluate_with_params(right, row, params)?;
            left_val.multiply(&right_val)
        }

        Expression::Divide(left, right) => {
            let left_val = evaluate_with_params(left, row, params)?;
            let right_val = evaluate_with_params(right, row, params)?;
            left_val.divide(&right_val)
        }

        Expression::Remainder(left, right) => {
            let left_val = evaluate_with_params(left, row, params)?;
            let right_val = evaluate_with_params(right, row, params)?;
            left_val.remainder(&right_val)
        }

        Expression::Exponentiate(left, right) => {
            let left_val = evaluate_with_params(left, row, params)?;
            let right_val = evaluate_with_params(right, row, params)?;
            left_val.exponentiate(&right_val)
        }

        // Unary operators
        Expression::Negate(expr) => {
            let val = evaluate_with_params(expr, row, params)?;
            val.negate()
        }

        Expression::Identity(expr) => {
            evaluate_with_params(expr, row, params)
        }

        Expression::Factorial(expr) => {
            let val = evaluate_with_params(expr, row, params)?;
            val.factorial()
        }

        // Pattern matching
        Expression::Like(expr, pattern) => {
            let val = evaluate_with_params(expr, row, params)?;
            let pattern_val = evaluate_with_params(pattern, row, params)?;

            // Convert to strings
            let text = val.to_string();
            let pattern_str = pattern_val.to_string();

            // Simple LIKE implementation (% = any chars, _ = one char)
            Ok(Value::Bool(like_match(&text, &pattern_str)))
        }

        // List operations
        Expression::InList { expr, list, negated } => {
            let val = evaluate_with_params(expr, row, params)?;

            for item in list {
                let item_val = evaluate_with_params(item, row, params)?;
                if val == item_val {
                    return Ok(Value::Bool(!negated));
                }
            }

            Ok(Value::Bool(*negated))
        }

        Expression::Between { expr, low, high, negated } => {
            let val = evaluate_with_params(expr, row, params)?;
            let low_val = evaluate_with_params(low, row, params)?;
            let high_val = evaluate_with_params(high, row, params)?;

            let in_range = val >= low_val && val <= high_val;
            Ok(Value::Bool(if *negated { !in_range } else { in_range }))
        }
    }
}

/// Simple LIKE pattern matching
fn like_match(text: &str, pattern: &str) -> bool {
    // Convert SQL LIKE pattern to regex-like matching
    let mut pattern_chars = pattern.chars().peekable();
    let mut text_chars = text.chars();

    while let Some(p) = pattern_chars.next() {
        match p {
            '%' => {
                // % matches any sequence of characters
                if pattern_chars.peek().is_none() {
                    return true; // % at end matches everything
                }

                // Try to match rest of pattern at each position
                let remaining_pattern: String = pattern_chars.collect();
                for i in 0..=text.len() {
                    if like_match(&text[i..], &remaining_pattern) {
                        return true;
                    }
                }
                return false;
            }
            '_' => {
                // _ matches exactly one character
                if text_chars.next().is_none() {
                    return false;
                }
            }
            c => {
                // Regular character must match exactly
                if text_chars.next() != Some(c) {
                    return false;
                }
            }
        }
    }

    // Pattern exhausted - check if text is also exhausted
    text_chars.next().is_none()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parameter_evaluation() {
        let params = ParameterContext::new(vec![
            Value::I32(42),
            Value::Str("hello".to_string()),
        ]);

        // Test parameter access
        let expr = Expression::Parameter(0);
        let result = evaluate_with_params(&expr, None, &params).unwrap();
        assert_eq!(result, Value::I32(42));

        // Test parameter in comparison
        let expr = Expression::Equal(
            Box::new(Expression::Parameter(0)),
            Box::new(Expression::Constant(Value::I32(42))),
        );
        let result = evaluate_with_params(&expr, None, &params).unwrap();
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn test_like_matching() {
        assert!(like_match("hello world", "hello%"));
        assert!(like_match("hello world", "%world"));
        assert!(like_match("hello world", "%o w%"));
        assert!(like_match("hello", "h_llo"));
        assert!(!like_match("hello", "h_lo"));
        assert!(like_match("hello", "_____"));
    }
}