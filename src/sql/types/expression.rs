//! SQL expression evaluation with transaction context for deterministic functions
//!
//! Adapted from toydb for proven-sql's PCC architecture.
//! Key changes:
//! - Added TransactionContext for deterministic function evaluation
//! - Integrated with our Value types (including UUID, Timestamp, Blob)

use super::value::{Row, Value};
use crate::context::TransactionContext;
use crate::error::{Error, Result};
use regex::Regex;
use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// An expression, made up of nested operations and values. Values are either
/// constants, or numeric column references which are looked up in rows.
/// Evaluated to a final value during query execution.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Expression {
    /// A constant value.
    Constant(Value),
    /// A column reference. Looks up the value in a row during evaluation.
    Column(usize),

    /// a AND b: logical AND of two booleans.
    And(Box<Expression>, Box<Expression>),
    /// a OR b: logical OR of two booleans.
    Or(Box<Expression>, Box<Expression>),
    /// NOT a: logical NOT of a boolean.
    Not(Box<Expression>),

    /// a = b: equality comparison of two values.
    Equal(Box<Expression>, Box<Expression>),
    /// a > b: greater than comparison of two values.
    GreaterThan(Box<Expression>, Box<Expression>),
    /// a < b: less than comparison of two values.
    LessThan(Box<Expression>, Box<Expression>),
    /// a >= b: greater than or equal comparison.
    GreaterThanOrEqual(Box<Expression>, Box<Expression>),
    /// a <= b: less than or equal comparison.
    LessThanOrEqual(Box<Expression>, Box<Expression>),
    /// a != b: not equal comparison.
    NotEqual(Box<Expression>, Box<Expression>),
    /// a IS NULL or a IS NAN: checks for the given value.
    Is(Box<Expression>, Value),

    /// a + b: adds two numbers or concatenates strings.
    Add(Box<Expression>, Box<Expression>),
    /// a - b: subtracts two numbers.
    Subtract(Box<Expression>, Box<Expression>),
    /// a * b: multiplies two numbers.
    Multiply(Box<Expression>, Box<Expression>),
    /// a / b: divides two numbers.
    Divide(Box<Expression>, Box<Expression>),
    /// a % b: remainder of two numbers.
    Remainder(Box<Expression>, Box<Expression>),
    /// a ^ b: exponentiates two numbers.
    Exponentiate(Box<Expression>, Box<Expression>),
    /// a!: takes the factorial of a number.
    Factorial(Box<Expression>),
    /// +a: the identity function, returns the same number.
    Identity(Box<Expression>),
    /// -a: negates a number.
    Negate(Box<Expression>),

    /// a LIKE pattern: SQL pattern matching.
    Like(Box<Expression>, Box<Expression>),

    /// Function call with deterministic evaluation.
    Function(String, Vec<Expression>),
}

impl Expression {
    /// Evaluates the expression to a value, using a row for column lookups
    /// and a transaction context for deterministic functions.
    pub fn evaluate(&self, row: Option<&Row>, context: &TransactionContext) -> Result<Value> {
        use Expression::*;
        Ok(match self {
            Constant(value) => value.clone(),

            Column(i) => row
                .and_then(|row| row.get(*i))
                .cloned()
                .ok_or_else(|| Error::ExecutionError(format!("column {i} not found")))?,

            // Logical operations
            And(lhs, rhs) => {
                let lhs = lhs.evaluate(row, context)?;
                let rhs = rhs.evaluate(row, context)?;
                lhs.and(&rhs)?
            }
            Or(lhs, rhs) => {
                let lhs = lhs.evaluate(row, context)?;
                let rhs = rhs.evaluate(row, context)?;
                lhs.or(&rhs)?
            }
            Not(expr) => {
                let value = expr.evaluate(row, context)?;
                value.not()?
            }

            // Comparison operations
            Equal(lhs, rhs) => {
                let lhs = lhs.evaluate(row, context)?;
                let rhs = rhs.evaluate(row, context)?;
                Value::Boolean(lhs == rhs)
            }
            NotEqual(lhs, rhs) => {
                let lhs = lhs.evaluate(row, context)?;
                let rhs = rhs.evaluate(row, context)?;
                Value::Boolean(lhs != rhs)
            }
            GreaterThan(lhs, rhs) => {
                let lhs = lhs.evaluate(row, context)?;
                let rhs = rhs.evaluate(row, context)?;
                Value::Boolean(lhs > rhs)
            }
            GreaterThanOrEqual(lhs, rhs) => {
                let lhs = lhs.evaluate(row, context)?;
                let rhs = rhs.evaluate(row, context)?;
                Value::Boolean(lhs >= rhs)
            }
            LessThan(lhs, rhs) => {
                let lhs = lhs.evaluate(row, context)?;
                let rhs = rhs.evaluate(row, context)?;
                Value::Boolean(lhs < rhs)
            }
            LessThanOrEqual(lhs, rhs) => {
                let lhs = lhs.evaluate(row, context)?;
                let rhs = rhs.evaluate(row, context)?;
                Value::Boolean(lhs <= rhs)
            }
            Is(expr, check_value) => {
                let value = expr.evaluate(row, context)?;
                Value::Boolean(value == *check_value)
            }

            // Arithmetic operations
            Add(lhs, rhs) => {
                let lhs = lhs.evaluate(row, context)?;
                let rhs = rhs.evaluate(row, context)?;
                lhs.add(&rhs)?
            }
            Subtract(lhs, rhs) => {
                let lhs = lhs.evaluate(row, context)?;
                let rhs = rhs.evaluate(row, context)?;
                lhs.subtract(&rhs)?
            }
            Multiply(lhs, rhs) => {
                let lhs = lhs.evaluate(row, context)?;
                let rhs = rhs.evaluate(row, context)?;
                lhs.multiply(&rhs)?
            }
            Divide(lhs, rhs) => {
                let lhs = lhs.evaluate(row, context)?;
                let rhs = rhs.evaluate(row, context)?;
                lhs.divide(&rhs)?
            }
            Remainder(lhs, rhs) => {
                let lhs = lhs.evaluate(row, context)?;
                let rhs = rhs.evaluate(row, context)?;
                // TODO: Implement remainder in Value
                match (&lhs, &rhs) {
                    (Value::Integer(a), Value::Integer(b)) => {
                        if *b == 0 {
                            return Err(Error::InvalidValue("Division by zero".into()));
                        }
                        Value::Integer(a % b)
                    }
                    _ => {
                        return Err(Error::TypeMismatch {
                            expected: "integers".into(),
                            found: format!("{:?} and {:?}", lhs, rhs),
                        });
                    }
                }
            }
            Exponentiate(lhs, rhs) => {
                let lhs = lhs.evaluate(row, context)?;
                let rhs = rhs.evaluate(row, context)?;
                // TODO: Implement exponentiation in Value
                match (&lhs, &rhs) {
                    (Value::Integer(a), Value::Integer(b)) => {
                        if *b < 0 {
                            return Err(Error::InvalidValue("Negative exponent".into()));
                        }
                        let result = (*a as f64).powi(*b as i32);
                        Value::Integer(result as i64)
                    }
                    (Value::Decimal(a), Value::Integer(b)) => {
                        // Convert to f64 for exponentiation, then back to Decimal
                        let base = a.to_f64().ok_or_else(|| {
                            Error::InvalidValue("Decimal conversion failed".into())
                        })?;
                        let result = base.powi(*b as i32);
                        use rust_decimal::Decimal;
                        use std::str::FromStr;
                        Value::Decimal(
                            Decimal::from_str(&result.to_string()).map_err(|_| {
                                Error::InvalidValue("Decimal conversion failed".into())
                            })?,
                        )
                    }
                    _ => {
                        return Err(Error::TypeMismatch {
                            expected: "numeric".into(),
                            found: format!("{:?} and {:?}", lhs, rhs),
                        });
                    }
                }
            }
            Factorial(expr) => {
                let value = expr.evaluate(row, context)?;
                match value {
                    Value::Integer(n) if n >= 0 && n <= 20 => {
                        let mut result = 1i64;
                        for i in 2..=n {
                            result = result.saturating_mul(i);
                        }
                        Value::Integer(result)
                    }
                    Value::Integer(_) => {
                        return Err(Error::InvalidValue("Factorial out of range".into()));
                    }
                    _ => {
                        return Err(Error::TypeMismatch {
                            expected: "integer".into(),
                            found: format!("{:?}", value),
                        });
                    }
                }
            }
            Identity(expr) => expr.evaluate(row, context)?,
            Negate(expr) => {
                let value = expr.evaluate(row, context)?;
                match value {
                    Value::Integer(i) => Value::Integer(-i),
                    Value::Decimal(d) => Value::Decimal(-d),
                    _ => {
                        return Err(Error::TypeMismatch {
                            expected: "numeric".into(),
                            found: format!("{:?}", value),
                        });
                    }
                }
            }

            // Pattern matching
            Like(expr, pattern) => {
                let value = expr.evaluate(row, context)?;
                let pattern_value = pattern.evaluate(row, context)?;

                match (&value, &pattern_value) {
                    (Value::String(s), Value::String(p)) => {
                        // Convert SQL LIKE pattern to regex
                        let regex_pattern = p.replace('%', ".*").replace('_', ".");
                        let regex = Regex::new(&format!("^{}$", regex_pattern)).map_err(|e| {
                            Error::ExecutionError(format!("Invalid pattern: {}", e))
                        })?;
                        Value::Boolean(regex.is_match(s))
                    }
                    (Value::Null, _) | (_, Value::Null) => Value::Null,
                    _ => {
                        return Err(Error::TypeMismatch {
                            expected: "strings".into(),
                            found: format!("{:?} and {:?}", value, pattern_value),
                        });
                    }
                }
            }

            // Function calls - use our deterministic function evaluator
            Function(name, args) => {
                let arg_values: Result<Vec<_>> =
                    args.iter().map(|arg| arg.evaluate(row, context)).collect();
                crate::sql::types::functions::evaluate_function(name, &arg_values?, context)?
            }
        })
    }

    /// Walks the expression tree, calling the visitor function for each node.
    pub fn walk(&self, visitor: &mut impl FnMut(&Expression) -> bool) -> bool {
        use Expression::*;

        if !visitor(self) {
            return false;
        }

        match self {
            Constant(_) | Column(_) => true,

            And(lhs, rhs)
            | Or(lhs, rhs)
            | Equal(lhs, rhs)
            | NotEqual(lhs, rhs)
            | GreaterThan(lhs, rhs)
            | GreaterThanOrEqual(lhs, rhs)
            | LessThan(lhs, rhs)
            | LessThanOrEqual(lhs, rhs)
            | Add(lhs, rhs)
            | Subtract(lhs, rhs)
            | Multiply(lhs, rhs)
            | Divide(lhs, rhs)
            | Remainder(lhs, rhs)
            | Exponentiate(lhs, rhs)
            | Like(lhs, rhs) => lhs.walk(visitor) && rhs.walk(visitor),

            Not(expr) | Factorial(expr) | Identity(expr) | Negate(expr) | Is(expr, _) => {
                expr.walk(visitor)
            }

            Function(_, args) => args.iter().all(|arg| arg.walk(visitor)),
        }
    }

    /// Replaces column references using the given map.
    pub fn remap_columns(self, map: &[Option<usize>]) -> Self {
        use Expression::*;
        match self {
            Column(i) => map
                .get(i)
                .and_then(|i| i.map(Column))
                .unwrap_or(Constant(Value::Null)),

            And(lhs, rhs) => And(
                Box::new(lhs.remap_columns(map)),
                Box::new(rhs.remap_columns(map)),
            ),
            Or(lhs, rhs) => Or(
                Box::new(lhs.remap_columns(map)),
                Box::new(rhs.remap_columns(map)),
            ),
            Not(expr) => Not(Box::new(expr.remap_columns(map))),

            Equal(lhs, rhs) => Equal(
                Box::new(lhs.remap_columns(map)),
                Box::new(rhs.remap_columns(map)),
            ),
            NotEqual(lhs, rhs) => NotEqual(
                Box::new(lhs.remap_columns(map)),
                Box::new(rhs.remap_columns(map)),
            ),
            GreaterThan(lhs, rhs) => GreaterThan(
                Box::new(lhs.remap_columns(map)),
                Box::new(rhs.remap_columns(map)),
            ),
            GreaterThanOrEqual(lhs, rhs) => GreaterThanOrEqual(
                Box::new(lhs.remap_columns(map)),
                Box::new(rhs.remap_columns(map)),
            ),
            LessThan(lhs, rhs) => LessThan(
                Box::new(lhs.remap_columns(map)),
                Box::new(rhs.remap_columns(map)),
            ),
            LessThanOrEqual(lhs, rhs) => LessThanOrEqual(
                Box::new(lhs.remap_columns(map)),
                Box::new(rhs.remap_columns(map)),
            ),
            Is(expr, value) => Is(Box::new(expr.remap_columns(map)), value),

            Add(lhs, rhs) => Add(
                Box::new(lhs.remap_columns(map)),
                Box::new(rhs.remap_columns(map)),
            ),
            Subtract(lhs, rhs) => Subtract(
                Box::new(lhs.remap_columns(map)),
                Box::new(rhs.remap_columns(map)),
            ),
            Multiply(lhs, rhs) => Multiply(
                Box::new(lhs.remap_columns(map)),
                Box::new(rhs.remap_columns(map)),
            ),
            Divide(lhs, rhs) => Divide(
                Box::new(lhs.remap_columns(map)),
                Box::new(rhs.remap_columns(map)),
            ),
            Remainder(lhs, rhs) => Remainder(
                Box::new(lhs.remap_columns(map)),
                Box::new(rhs.remap_columns(map)),
            ),
            Exponentiate(lhs, rhs) => Exponentiate(
                Box::new(lhs.remap_columns(map)),
                Box::new(rhs.remap_columns(map)),
            ),

            Factorial(expr) => Factorial(Box::new(expr.remap_columns(map))),
            Identity(expr) => Identity(Box::new(expr.remap_columns(map))),
            Negate(expr) => Negate(Box::new(expr.remap_columns(map))),

            Like(lhs, rhs) => Like(
                Box::new(lhs.remap_columns(map)),
                Box::new(rhs.remap_columns(map)),
            ),

            Function(name, args) => Function(
                name,
                args.into_iter().map(|arg| arg.remap_columns(map)).collect(),
            ),

            Constant(value) => Constant(value),
        }
    }
}

impl Display for Expression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use Expression::*;
        match self {
            Constant(value) => write!(f, "{}", value),
            Column(i) => write!(f, "#{}", i),

            And(lhs, rhs) => write!(f, "({} AND {})", lhs, rhs),
            Or(lhs, rhs) => write!(f, "({} OR {})", lhs, rhs),
            Not(expr) => write!(f, "(NOT {})", expr),

            Equal(lhs, rhs) => write!(f, "({} = {})", lhs, rhs),
            NotEqual(lhs, rhs) => write!(f, "({} != {})", lhs, rhs),
            GreaterThan(lhs, rhs) => write!(f, "({} > {})", lhs, rhs),
            GreaterThanOrEqual(lhs, rhs) => write!(f, "({} >= {})", lhs, rhs),
            LessThan(lhs, rhs) => write!(f, "({} < {})", lhs, rhs),
            LessThanOrEqual(lhs, rhs) => write!(f, "({} <= {})", lhs, rhs),
            Is(expr, value) => write!(f, "({} IS {})", expr, value),

            Add(lhs, rhs) => write!(f, "({} + {})", lhs, rhs),
            Subtract(lhs, rhs) => write!(f, "({} - {})", lhs, rhs),
            Multiply(lhs, rhs) => write!(f, "({} * {})", lhs, rhs),
            Divide(lhs, rhs) => write!(f, "({} / {})", lhs, rhs),
            Remainder(lhs, rhs) => write!(f, "({} % {})", lhs, rhs),
            Exponentiate(lhs, rhs) => write!(f, "({} ^ {})", lhs, rhs),
            Factorial(expr) => write!(f, "({}!)", expr),
            Identity(expr) => write!(f, "(+{})", expr),
            Negate(expr) => write!(f, "(-{})", expr),

            Like(lhs, rhs) => write!(f, "({} LIKE {})", lhs, rhs),

            Function(name, args) => {
                write!(f, "{}(", name)?;
                for (i, arg) in args.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", arg)?;
                }
                write!(f, ")")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::value::{Row, Value};
    use super::Expression;
    use crate::context::{TransactionContext, TransactionId};
    use crate::hlc::{HlcTimestamp, NodeId};
    use rust_decimal::Decimal;
    use std::str::FromStr;

    fn test_context() -> TransactionContext {
        TransactionContext {
            tx_id: TransactionId {
                global_id: HlcTimestamp::new(1000, 0, NodeId::new(1)),
                sub_seq: 1,
            },
            read_only: false,
        }
    }

    #[test]
    fn test_constant_evaluation() {
        let ctx = test_context();
        let expr = Expression::Constant(Value::Integer(42));
        assert_eq!(expr.evaluate(None, &ctx).unwrap(), Value::Integer(42));

        let expr = Expression::Constant(Value::String("hello".to_string()));
        assert_eq!(
            expr.evaluate(None, &ctx).unwrap(),
            Value::String("hello".to_string())
        );

        let expr = Expression::Constant(Value::Boolean(true));
        assert_eq!(expr.evaluate(None, &ctx).unwrap(), Value::Boolean(true));
    }

    #[test]
    fn test_column_evaluation() {
        let ctx = test_context();
        let row: Row = vec![
            Value::Integer(1),
            Value::String("test".to_string()),
            Value::Boolean(false),
        ];

        let expr = Expression::Column(0);
        assert_eq!(expr.evaluate(Some(&row), &ctx).unwrap(), Value::Integer(1));

        let expr = Expression::Column(1);
        assert_eq!(
            expr.evaluate(Some(&row), &ctx).unwrap(),
            Value::String("test".to_string())
        );

        let expr = Expression::Column(2);
        assert_eq!(
            expr.evaluate(Some(&row), &ctx).unwrap(),
            Value::Boolean(false)
        );

        // Test column out of bounds
        let expr = Expression::Column(3);
        assert!(expr.evaluate(Some(&row), &ctx).is_err());
    }

    #[test]
    fn test_logical_operations() {
        let ctx = test_context();

        // AND
        let expr = Expression::And(
            Box::new(Expression::Constant(Value::Boolean(true))),
            Box::new(Expression::Constant(Value::Boolean(false))),
        );
        assert_eq!(expr.evaluate(None, &ctx).unwrap(), Value::Boolean(false));

        // OR
        let expr = Expression::Or(
            Box::new(Expression::Constant(Value::Boolean(true))),
            Box::new(Expression::Constant(Value::Boolean(false))),
        );
        assert_eq!(expr.evaluate(None, &ctx).unwrap(), Value::Boolean(true));

        // NOT
        let expr = Expression::Not(Box::new(Expression::Constant(Value::Boolean(true))));
        assert_eq!(expr.evaluate(None, &ctx).unwrap(), Value::Boolean(false));
    }

    #[test]
    fn test_comparison_operations() {
        let ctx = test_context();

        // Equal
        let expr = Expression::Equal(
            Box::new(Expression::Constant(Value::Integer(5))),
            Box::new(Expression::Constant(Value::Integer(5))),
        );
        assert_eq!(expr.evaluate(None, &ctx).unwrap(), Value::Boolean(true));

        // NotEqual
        let expr = Expression::NotEqual(
            Box::new(Expression::Constant(Value::Integer(5))),
            Box::new(Expression::Constant(Value::Integer(3))),
        );
        assert_eq!(expr.evaluate(None, &ctx).unwrap(), Value::Boolean(true));

        // GreaterThan
        let expr = Expression::GreaterThan(
            Box::new(Expression::Constant(Value::Integer(5))),
            Box::new(Expression::Constant(Value::Integer(3))),
        );
        assert_eq!(expr.evaluate(None, &ctx).unwrap(), Value::Boolean(true));

        // LessThanOrEqual
        let expr = Expression::LessThanOrEqual(
            Box::new(Expression::Constant(Value::Integer(3))),
            Box::new(Expression::Constant(Value::Integer(5))),
        );
        assert_eq!(expr.evaluate(None, &ctx).unwrap(), Value::Boolean(true));
    }

    #[test]
    fn test_arithmetic_operations() {
        let ctx = test_context();

        // Add
        let expr = Expression::Add(
            Box::new(Expression::Constant(Value::Integer(10))),
            Box::new(Expression::Constant(Value::Integer(5))),
        );
        assert_eq!(expr.evaluate(None, &ctx).unwrap(), Value::Integer(15));

        // Subtract
        let expr = Expression::Subtract(
            Box::new(Expression::Constant(Value::Integer(10))),
            Box::new(Expression::Constant(Value::Integer(3))),
        );
        assert_eq!(expr.evaluate(None, &ctx).unwrap(), Value::Integer(7));

        // Multiply
        let expr = Expression::Multiply(
            Box::new(Expression::Constant(Value::Integer(4))),
            Box::new(Expression::Constant(Value::Integer(5))),
        );
        assert_eq!(expr.evaluate(None, &ctx).unwrap(), Value::Integer(20));

        // Divide
        let expr = Expression::Divide(
            Box::new(Expression::Constant(Value::Integer(20))),
            Box::new(Expression::Constant(Value::Integer(4))),
        );
        assert_eq!(expr.evaluate(None, &ctx).unwrap(), Value::Integer(5));

        // Remainder
        let expr = Expression::Remainder(
            Box::new(Expression::Constant(Value::Integer(17))),
            Box::new(Expression::Constant(Value::Integer(5))),
        );
        assert_eq!(expr.evaluate(None, &ctx).unwrap(), Value::Integer(2));
    }

    #[test]
    fn test_string_operations() {
        let ctx = test_context();

        // String concatenation
        let expr = Expression::Add(
            Box::new(Expression::Constant(Value::String("hello".to_string()))),
            Box::new(Expression::Constant(Value::String(" world".to_string()))),
        );
        assert_eq!(
            expr.evaluate(None, &ctx).unwrap(),
            Value::String("hello world".to_string())
        );
    }

    #[test]
    fn test_like_pattern() {
        let ctx = test_context();

        // Basic pattern matching
        let expr = Expression::Like(
            Box::new(Expression::Constant(Value::String(
                "hello world".to_string(),
            ))),
            Box::new(Expression::Constant(Value::String("hello%".to_string()))),
        );
        assert_eq!(expr.evaluate(None, &ctx).unwrap(), Value::Boolean(true));

        let expr = Expression::Like(
            Box::new(Expression::Constant(Value::String(
                "hello world".to_string(),
            ))),
            Box::new(Expression::Constant(Value::String("%world".to_string()))),
        );
        assert_eq!(expr.evaluate(None, &ctx).unwrap(), Value::Boolean(true));

        let expr = Expression::Like(
            Box::new(Expression::Constant(Value::String("hello".to_string()))),
            Box::new(Expression::Constant(Value::String("h_llo".to_string()))),
        );
        assert_eq!(expr.evaluate(None, &ctx).unwrap(), Value::Boolean(true));

        let expr = Expression::Like(
            Box::new(Expression::Constant(Value::String("hello".to_string()))),
            Box::new(Expression::Constant(Value::String("goodbye".to_string()))),
        );
        assert_eq!(expr.evaluate(None, &ctx).unwrap(), Value::Boolean(false));
    }

    #[test]
    fn test_null_handling() {
        let ctx = test_context();

        // NULL in AND
        let expr = Expression::And(
            Box::new(Expression::Constant(Value::Null)),
            Box::new(Expression::Constant(Value::Boolean(true))),
        );
        assert_eq!(expr.evaluate(None, &ctx).unwrap(), Value::Null);

        // NULL in OR with false
        let expr = Expression::Or(
            Box::new(Expression::Constant(Value::Null)),
            Box::new(Expression::Constant(Value::Boolean(false))),
        );
        assert_eq!(expr.evaluate(None, &ctx).unwrap(), Value::Null);

        // NULL in OR with true
        let expr = Expression::Or(
            Box::new(Expression::Constant(Value::Null)),
            Box::new(Expression::Constant(Value::Boolean(true))),
        );
        assert_eq!(expr.evaluate(None, &ctx).unwrap(), Value::Boolean(true));

        // IS NULL check
        let expr = Expression::Is(Box::new(Expression::Constant(Value::Null)), Value::Null);
        assert_eq!(expr.evaluate(None, &ctx).unwrap(), Value::Boolean(true));
    }

    #[test]
    fn test_deterministic_functions() {
        let ctx = test_context();

        // Test NOW() function - should return the transaction timestamp
        let expr = Expression::Function("NOW".to_string(), vec![]);
        let result = expr.evaluate(None, &ctx).unwrap();
        assert_eq!(result, Value::Timestamp(1000)); // Should match ctx.tx_id.global_id.physical

        // Test UUID() function - should be deterministic based on tx_id
        let expr1 = Expression::Function("UUID".to_string(), vec![]);
        let uuid1 = expr1.evaluate(None, &ctx).unwrap();

        // Same function with same args should produce same UUID
        let expr2 = Expression::Function("UUID".to_string(), vec![]);
        let uuid2 = expr2.evaluate(None, &ctx).unwrap();
        assert_eq!(uuid1, uuid2);

        // Different sequence should produce different UUID
        let expr3 = Expression::Function(
            "UUID".to_string(),
            vec![Expression::Constant(Value::Integer(1))],
        );
        let uuid3 = expr3.evaluate(None, &ctx).unwrap();
        assert_ne!(uuid1, uuid3);
    }

    #[test]
    fn test_complex_expression() {
        let ctx = test_context();
        let row: Row = vec![
            Value::Integer(10),
            Value::Integer(5),
            Value::String("test".to_string()),
        ];

        // (column[0] + column[1]) * 2 > 20
        let expr = Expression::GreaterThan(
            Box::new(Expression::Multiply(
                Box::new(Expression::Add(
                    Box::new(Expression::Column(0)),
                    Box::new(Expression::Column(1)),
                )),
                Box::new(Expression::Constant(Value::Integer(2))),
            )),
            Box::new(Expression::Constant(Value::Integer(20))),
        );

        // (10 + 5) * 2 = 30, which is > 20
        assert_eq!(
            expr.evaluate(Some(&row), &ctx).unwrap(),
            Value::Boolean(true)
        );
    }

    #[test]
    fn test_decimal_operations() {
        let ctx = test_context();

        let expr = Expression::Add(
            Box::new(Expression::Constant(Value::Decimal(
                Decimal::from_str("10.5").unwrap(),
            ))),
            Box::new(Expression::Constant(Value::Decimal(
                Decimal::from_str("5.3").unwrap(),
            ))),
        );
        assert_eq!(
            expr.evaluate(None, &ctx).unwrap(),
            Value::Decimal(Decimal::from_str("15.8").unwrap())
        );

        // Mixed integer and decimal
        let expr = Expression::Multiply(
            Box::new(Expression::Constant(Value::Integer(3))),
            Box::new(Expression::Constant(Value::Decimal(
                Decimal::from_str("2.5").unwrap(),
            ))),
        );
        assert_eq!(
            expr.evaluate(None, &ctx).unwrap(),
            Value::Decimal(Decimal::from_str("7.5").unwrap())
        );
    }
}
