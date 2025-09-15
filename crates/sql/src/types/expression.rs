//! SQL expression evaluation with transaction context for deterministic functions
//!
//! Adapted from toydb for proven-sql's PCC architecture.
//! Key changes:
//! - Added TransactionContext for deterministic function evaluation
//! - Integrated with our Value types (including UUID, Timestamp, Blob)

use super::evaluator;
use super::value::{Row, Value};
use crate::error::{Error, Result};
use crate::stream::transaction::TransactionContext;
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
    /// A parameter placeholder for prepared statements (0-indexed).
    Parameter(usize),
    /// All columns - used for COUNT(DISTINCT *)
    All,

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

    /// a IN (list): checks if value is in list.
    InList(Box<Expression>, Vec<Expression>, bool), // expr, list, negated

    /// a BETWEEN low AND high: checks if value is in range.
    Between(Box<Expression>, Box<Expression>, Box<Expression>, bool), // expr, low, high, negated

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
            All => Value::integer(1), // Placeholder - should be handled at higher level

            Column(i) => row
                .and_then(|row| row.get(*i))
                .cloned()
                .ok_or_else(|| Error::ExecutionError(format!("column {i} not found")))?,

            Parameter(idx) => {
                // Parameters should be bound before evaluation
                return Err(Error::ExecutionError(format!(
                    "unbound parameter at position {}",
                    idx
                )));
            }

            // Logical operations
            And(lhs, rhs) => {
                let lhs = lhs.evaluate(row, context)?;
                let rhs = rhs.evaluate(row, context)?;
                evaluator::and(&lhs, &rhs)?
            }
            Or(lhs, rhs) => {
                let lhs = lhs.evaluate(row, context)?;
                let rhs = rhs.evaluate(row, context)?;
                evaluator::or(&lhs, &rhs)?
            }
            Not(expr) => {
                let value = expr.evaluate(row, context)?;
                evaluator::not(&value)?
            }

            // Comparison operations
            Equal(lhs, rhs) => {
                let lhs = lhs.evaluate(row, context)?;
                let rhs = rhs.evaluate(row, context)?;
                Value::boolean(lhs == rhs)
            }
            NotEqual(lhs, rhs) => {
                let lhs = lhs.evaluate(row, context)?;
                let rhs = rhs.evaluate(row, context)?;
                Value::boolean(lhs != rhs)
            }
            GreaterThan(lhs, rhs) => {
                let lhs = lhs.evaluate(row, context)?;
                let rhs = rhs.evaluate(row, context)?;
                Value::boolean(lhs > rhs)
            }
            GreaterThanOrEqual(lhs, rhs) => {
                let lhs = lhs.evaluate(row, context)?;
                let rhs = rhs.evaluate(row, context)?;
                Value::boolean(lhs >= rhs)
            }
            LessThan(lhs, rhs) => {
                let lhs = lhs.evaluate(row, context)?;
                let rhs = rhs.evaluate(row, context)?;
                Value::boolean(lhs < rhs)
            }
            LessThanOrEqual(lhs, rhs) => {
                let lhs = lhs.evaluate(row, context)?;
                let rhs = rhs.evaluate(row, context)?;
                Value::boolean(lhs <= rhs)
            }
            Is(expr, check_value) => {
                let value = expr.evaluate(row, context)?;
                Value::boolean(value == *check_value)
            }

            // Arithmetic operations
            Add(lhs, rhs) => {
                let lhs = lhs.evaluate(row, context)?;
                let rhs = rhs.evaluate(row, context)?;
                evaluator::add(&lhs, &rhs)?
            }
            Subtract(lhs, rhs) => {
                let lhs = lhs.evaluate(row, context)?;
                let rhs = rhs.evaluate(row, context)?;
                evaluator::subtract(&lhs, &rhs)?
            }
            Multiply(lhs, rhs) => {
                let lhs = lhs.evaluate(row, context)?;
                let rhs = rhs.evaluate(row, context)?;
                evaluator::multiply(&lhs, &rhs)?
            }
            Divide(lhs, rhs) => {
                let lhs = lhs.evaluate(row, context)?;
                let rhs = rhs.evaluate(row, context)?;
                evaluator::divide(&lhs, &rhs)?
            }
            Remainder(lhs, rhs) => {
                let lhs = lhs.evaluate(row, context)?;
                let rhs = rhs.evaluate(row, context)?;
                // TODO: Implement remainder in Value
                match (&lhs, &rhs) {
                    (Value::I64(a), Value::I64(b)) => {
                        if *b == 0 {
                            return Err(Error::InvalidValue("Division by zero".into()));
                        }
                        Value::I64(a % b)
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
                    (Value::I64(a), Value::I64(b)) => {
                        if *b < 0 {
                            return Err(Error::InvalidValue("Negative exponent".into()));
                        }
                        let result = (*a as f64).powi(*b as i32);
                        Value::I64(result as i64)
                    }
                    (Value::Decimal(a), Value::I64(b)) => {
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
                    Value::I64(n) if (0..=20).contains(&n) => {
                        let mut result = 1i64;
                        for i in 2..=n {
                            result = result.saturating_mul(i);
                        }
                        Value::I64(result)
                    }
                    Value::I64(_) => {
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
                    Value::I8(i) => Value::I8(-i),
                    Value::I16(i) => Value::I16(-i),
                    Value::I32(i) => Value::I32(-i),
                    Value::I64(i) => Value::I64(-i),
                    Value::I128(i) => Value::I128(-i),
                    Value::F32(f) => Value::F32(-f),
                    Value::F64(f) => Value::F64(-f),
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
                    (Value::Str(s), Value::Str(p)) => {
                        // Convert SQL LIKE pattern to regex
                        let regex_pattern = p.replace('%', ".*").replace('_', ".");
                        let regex = Regex::new(&format!("^{}$", regex_pattern)).map_err(|e| {
                            Error::ExecutionError(format!("Invalid pattern: {}", e))
                        })?;
                        Value::boolean(regex.is_match(s))
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
                crate::types::functions::evaluate_function(name, &arg_values?, context)?
            }

            // IN list operator
            InList(expr, list, negated) => {
                let value = expr.evaluate(row, context)?;

                // If value is NULL, return NULL
                if value == Value::Null {
                    return Ok(Value::Null);
                }

                let mut found = false;
                let mut has_null = false;

                for item in list {
                    let item_value = item.evaluate(row, context)?;
                    if item_value == Value::Null {
                        has_null = true;
                    } else {
                        // Use evaluator::compare for type-aware comparison
                        // Two values are equal if compare returns Ordering::Equal
                        if let Ok(std::cmp::Ordering::Equal) =
                            evaluator::compare(&value, &item_value)
                        {
                            found = true;
                            break;
                        }
                    }
                }

                // SQL three-valued logic:
                // - If found, return !negated
                // - If not found and list has NULL, return NULL
                // - Otherwise return negated
                if found {
                    Value::boolean(!negated)
                } else if has_null {
                    Value::Null
                } else {
                    Value::boolean(*negated)
                }
            }

            // BETWEEN operator
            Between(expr, low, high, negated) => {
                let value = expr.evaluate(row, context)?;
                let low_value = low.evaluate(row, context)?;
                let high_value = high.evaluate(row, context)?;

                // If any value is NULL, return NULL
                if value == Value::Null || low_value == Value::Null || high_value == Value::Null {
                    return Ok(Value::Null);
                }

                // Check if value is between low and high (inclusive)
                let in_range = value >= low_value && value <= high_value;

                if *negated {
                    Value::boolean(!in_range)
                } else {
                    Value::boolean(in_range)
                }
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
            Constant(_) | Column(_) | Parameter(_) | All => true,

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

            InList(expr, list, _) => expr.walk(visitor) && list.iter().all(|e| e.walk(visitor)),

            Between(expr, low, high, _) => {
                expr.walk(visitor) && low.walk(visitor) && high.walk(visitor)
            }
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
            Parameter(idx) => Parameter(idx),
            All => All,

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

            InList(expr, list, negated) => InList(
                Box::new(expr.remap_columns(map)),
                list.into_iter().map(|e| e.remap_columns(map)).collect(),
                negated,
            ),

            Between(expr, low, high, negated) => Between(
                Box::new(expr.remap_columns(map)),
                Box::new(low.remap_columns(map)),
                Box::new(high.remap_columns(map)),
                negated,
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
            Parameter(idx) => write!(f, "?{}", idx),
            All => write!(f, "*"),

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

            InList(expr, list, negated) => {
                write!(f, "{}", expr)?;
                if *negated {
                    write!(f, " NOT")?;
                }
                write!(f, " IN (")?;
                for (i, item) in list.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", item)?;
                }
                write!(f, ")")
            }

            Between(expr, low, high, negated) => {
                write!(f, "{}", expr)?;
                if *negated {
                    write!(f, " NOT")?;
                }
                write!(f, " BETWEEN {} AND {}", low, high)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::value::{Row, Value};
    use super::Expression;
    use crate::stream::transaction::TransactionContext;
    use proven_hlc::{HlcTimestamp, NodeId};
    use rust_decimal::Decimal;
    use std::str::FromStr;

    fn test_context() -> TransactionContext {
        TransactionContext::new(HlcTimestamp::new(1000, 0, NodeId::new(1)))
    }

    #[test]
    fn test_constant_evaluation() {
        let ctx = test_context();
        let expr = Expression::Constant(Value::I64(42));
        assert_eq!(expr.evaluate(None, &ctx).unwrap(), Value::I64(42));

        let expr = Expression::Constant(Value::string("hello".to_string()));
        assert_eq!(
            expr.evaluate(None, &ctx).unwrap(),
            Value::string("hello".to_string())
        );

        let expr = Expression::Constant(Value::boolean(true));
        assert_eq!(expr.evaluate(None, &ctx).unwrap(), Value::boolean(true));
    }

    #[test]
    fn test_column_evaluation() {
        let ctx = test_context();
        let row: Row = vec![
            Value::I64(1),
            Value::string("test".to_string()),
            Value::boolean(false),
        ];

        let expr = Expression::Column(0);
        assert_eq!(expr.evaluate(Some(&row), &ctx).unwrap(), Value::I64(1));

        let expr = Expression::Column(1);
        assert_eq!(
            expr.evaluate(Some(&row), &ctx).unwrap(),
            Value::string("test".to_string())
        );

        let expr = Expression::Column(2);
        assert_eq!(
            expr.evaluate(Some(&row), &ctx).unwrap(),
            Value::boolean(false)
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
            Box::new(Expression::Constant(Value::boolean(true))),
            Box::new(Expression::Constant(Value::boolean(false))),
        );
        assert_eq!(expr.evaluate(None, &ctx).unwrap(), Value::boolean(false));

        // OR
        let expr = Expression::Or(
            Box::new(Expression::Constant(Value::boolean(true))),
            Box::new(Expression::Constant(Value::boolean(false))),
        );
        assert_eq!(expr.evaluate(None, &ctx).unwrap(), Value::boolean(true));

        // NOT
        let expr = Expression::Not(Box::new(Expression::Constant(Value::boolean(true))));
        assert_eq!(expr.evaluate(None, &ctx).unwrap(), Value::boolean(false));
    }

    #[test]
    fn test_comparison_operations() {
        let ctx = test_context();

        // Equal
        let expr = Expression::Equal(
            Box::new(Expression::Constant(Value::I64(5))),
            Box::new(Expression::Constant(Value::I64(5))),
        );
        assert_eq!(expr.evaluate(None, &ctx).unwrap(), Value::boolean(true));

        // NotEqual
        let expr = Expression::NotEqual(
            Box::new(Expression::Constant(Value::I64(5))),
            Box::new(Expression::Constant(Value::I64(3))),
        );
        assert_eq!(expr.evaluate(None, &ctx).unwrap(), Value::boolean(true));

        // GreaterThan
        let expr = Expression::GreaterThan(
            Box::new(Expression::Constant(Value::I64(5))),
            Box::new(Expression::Constant(Value::I64(3))),
        );
        assert_eq!(expr.evaluate(None, &ctx).unwrap(), Value::boolean(true));

        // LessThanOrEqual
        let expr = Expression::LessThanOrEqual(
            Box::new(Expression::Constant(Value::I64(3))),
            Box::new(Expression::Constant(Value::I64(5))),
        );
        assert_eq!(expr.evaluate(None, &ctx).unwrap(), Value::boolean(true));
    }

    #[test]
    fn test_arithmetic_operations() {
        let ctx = test_context();

        // Add
        let expr = Expression::Add(
            Box::new(Expression::Constant(Value::I64(10))),
            Box::new(Expression::Constant(Value::I64(5))),
        );
        assert_eq!(expr.evaluate(None, &ctx).unwrap(), Value::I64(15));

        // Subtract
        let expr = Expression::Subtract(
            Box::new(Expression::Constant(Value::I64(10))),
            Box::new(Expression::Constant(Value::I64(3))),
        );
        assert_eq!(expr.evaluate(None, &ctx).unwrap(), Value::I64(7));

        // Multiply
        let expr = Expression::Multiply(
            Box::new(Expression::Constant(Value::I64(4))),
            Box::new(Expression::Constant(Value::I64(5))),
        );
        assert_eq!(expr.evaluate(None, &ctx).unwrap(), Value::I64(20));

        // Divide
        let expr = Expression::Divide(
            Box::new(Expression::Constant(Value::I64(20))),
            Box::new(Expression::Constant(Value::I64(4))),
        );
        assert_eq!(expr.evaluate(None, &ctx).unwrap(), Value::I64(5));

        // Remainder
        let expr = Expression::Remainder(
            Box::new(Expression::Constant(Value::I64(17))),
            Box::new(Expression::Constant(Value::I64(5))),
        );
        assert_eq!(expr.evaluate(None, &ctx).unwrap(), Value::I64(2));
    }

    #[test]
    fn test_string_operations() {
        let ctx = test_context();

        // String concatenation
        let expr = Expression::Add(
            Box::new(Expression::Constant(Value::string("hello".to_string()))),
            Box::new(Expression::Constant(Value::string(" world".to_string()))),
        );
        assert_eq!(
            expr.evaluate(None, &ctx).unwrap(),
            Value::string("hello world".to_string())
        );
    }

    #[test]
    fn test_like_pattern() {
        let ctx = test_context();

        // Basic pattern matching
        let expr = Expression::Like(
            Box::new(Expression::Constant(Value::string(
                "hello world".to_string(),
            ))),
            Box::new(Expression::Constant(Value::string("hello%".to_string()))),
        );
        assert_eq!(expr.evaluate(None, &ctx).unwrap(), Value::boolean(true));

        let expr = Expression::Like(
            Box::new(Expression::Constant(Value::string(
                "hello world".to_string(),
            ))),
            Box::new(Expression::Constant(Value::string("%world".to_string()))),
        );
        assert_eq!(expr.evaluate(None, &ctx).unwrap(), Value::boolean(true));

        let expr = Expression::Like(
            Box::new(Expression::Constant(Value::string("hello".to_string()))),
            Box::new(Expression::Constant(Value::string("h_llo".to_string()))),
        );
        assert_eq!(expr.evaluate(None, &ctx).unwrap(), Value::boolean(true));

        let expr = Expression::Like(
            Box::new(Expression::Constant(Value::string("hello".to_string()))),
            Box::new(Expression::Constant(Value::string("goodbye".to_string()))),
        );
        assert_eq!(expr.evaluate(None, &ctx).unwrap(), Value::boolean(false));
    }

    #[test]
    fn test_null_handling() {
        let ctx = test_context();

        // NULL in AND
        let expr = Expression::And(
            Box::new(Expression::Constant(Value::Null)),
            Box::new(Expression::Constant(Value::boolean(true))),
        );
        assert_eq!(expr.evaluate(None, &ctx).unwrap(), Value::Null);

        // NULL in OR with false
        let expr = Expression::Or(
            Box::new(Expression::Constant(Value::Null)),
            Box::new(Expression::Constant(Value::boolean(false))),
        );
        assert_eq!(expr.evaluate(None, &ctx).unwrap(), Value::Null);

        // NULL in OR with true
        let expr = Expression::Or(
            Box::new(Expression::Constant(Value::Null)),
            Box::new(Expression::Constant(Value::boolean(true))),
        );
        assert_eq!(expr.evaluate(None, &ctx).unwrap(), Value::boolean(true));

        // IS NULL check
        let expr = Expression::Is(Box::new(Expression::Constant(Value::Null)), Value::Null);
        assert_eq!(expr.evaluate(None, &ctx).unwrap(), Value::boolean(true));
    }

    #[test]
    fn test_deterministic_functions() {
        let ctx = test_context();

        // Test NOW() function - should return the transaction timestamp
        let expr = Expression::Function("NOW".to_string(), vec![]);
        let result = expr.evaluate(None, &ctx).unwrap();
        // Convert the expected timestamp to NaiveDateTime
        use chrono::DateTime;
        let expected = DateTime::from_timestamp(0, 1_000_000).unwrap().naive_utc();
        assert_eq!(result, Value::Timestamp(expected)); // Should match ctx.tx_id.global_id.physical (1000 microseconds = 0 secs + 1000000 nanos)

        // Test UUID() function - should be deterministic based on tx_id
        let expr1 = Expression::Function("UUID".to_string(), vec![]);
        let uuid1 = expr1.evaluate(None, &ctx).unwrap();

        // Calling UUID again should produce a different UUID (auto-incrementing sequence)
        let expr2 = Expression::Function("UUID".to_string(), vec![]);
        let uuid2 = expr2.evaluate(None, &ctx).unwrap();
        assert_ne!(uuid1, uuid2);

        // And a third call should be different from both
        let expr3 = Expression::Function("UUID".to_string(), vec![]);
        let uuid3 = expr3.evaluate(None, &ctx).unwrap();
        assert_ne!(uuid1, uuid3);
        assert_ne!(uuid2, uuid3);
    }

    #[test]
    fn test_complex_expression() {
        let ctx = test_context();
        let row: Row = vec![
            Value::I64(10),
            Value::I64(5),
            Value::string("test".to_string()),
        ];

        // (column[0] + column[1]) * 2 > 20
        let expr = Expression::GreaterThan(
            Box::new(Expression::Multiply(
                Box::new(Expression::Add(
                    Box::new(Expression::Column(0)),
                    Box::new(Expression::Column(1)),
                )),
                Box::new(Expression::Constant(Value::I64(2))),
            )),
            Box::new(Expression::Constant(Value::I64(20))),
        );

        // (10 + 5) * 2 = 30, which is > 20
        assert_eq!(
            expr.evaluate(Some(&row), &ctx).unwrap(),
            Value::boolean(true)
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
            Box::new(Expression::Constant(Value::I64(3))),
            Box::new(Expression::Constant(Value::Decimal(
                Decimal::from_str("2.5").unwrap(),
            ))),
        );
        assert_eq!(
            expr.evaluate(None, &ctx).unwrap(),
            Value::Decimal(Decimal::from_str("7.5").unwrap())
        );
    }

    #[test]
    fn test_in_list_evaluation() {
        let ctx = test_context();

        // Test: 1 IN (1, 2, 3) should return true
        // Use I32 since that's what the parser creates for small integers
        let expr = Expression::InList(
            Box::new(Expression::Constant(Value::I32(1))),
            vec![
                Expression::Constant(Value::I32(1)),
                Expression::Constant(Value::I32(2)),
                Expression::Constant(Value::I32(3)),
            ],
            false,
        );
        assert_eq!(expr.evaluate(None, &ctx).unwrap(), Value::Bool(true));

        // Test with mixed types - should match thanks to type coercion
        let expr = Expression::InList(
            Box::new(Expression::Constant(Value::I32(1))),
            vec![
                Expression::Constant(Value::I64(1)),
                Expression::Constant(Value::I64(2)),
                Expression::Constant(Value::I64(3)),
            ],
            false,
        );
        // This should return true because evaluator::compare handles type coercion
        assert_eq!(expr.evaluate(None, &ctx).unwrap(), Value::Bool(true));

        // Test: 4 IN (1, 2, 3) should return false
        let expr = Expression::InList(
            Box::new(Expression::Constant(Value::I64(4))),
            vec![
                Expression::Constant(Value::I64(1)),
                Expression::Constant(Value::I64(2)),
                Expression::Constant(Value::I64(3)),
            ],
            false,
        );
        assert_eq!(expr.evaluate(None, &ctx).unwrap(), Value::Bool(false));

        // Test: 1 NOT IN (2, 3, 4) should return true
        let expr = Expression::InList(
            Box::new(Expression::Constant(Value::I64(1))),
            vec![
                Expression::Constant(Value::I64(2)),
                Expression::Constant(Value::I64(3)),
                Expression::Constant(Value::I64(4)),
            ],
            true, // negated
        );
        assert_eq!(expr.evaluate(None, &ctx).unwrap(), Value::Bool(true));

        // Test: NULL IN (1, 2, 3) should return NULL
        let expr = Expression::InList(
            Box::new(Expression::Constant(Value::Null)),
            vec![
                Expression::Constant(Value::I64(1)),
                Expression::Constant(Value::I64(2)),
                Expression::Constant(Value::I64(3)),
            ],
            false,
        );
        assert_eq!(expr.evaluate(None, &ctx).unwrap(), Value::Null);

        // Test: 4 IN (1, NULL, 3) should return NULL (not found, but list has NULL)
        let expr = Expression::InList(
            Box::new(Expression::Constant(Value::I64(4))),
            vec![
                Expression::Constant(Value::I64(1)),
                Expression::Constant(Value::Null),
                Expression::Constant(Value::I64(3)),
            ],
            false,
        );
        assert_eq!(expr.evaluate(None, &ctx).unwrap(), Value::Null);
    }

    #[test]
    fn test_between_evaluation() {
        let ctx = test_context();

        // Test: 2 BETWEEN 1 AND 3 should return true
        let expr = Expression::Between(
            Box::new(Expression::Constant(Value::I64(2))),
            Box::new(Expression::Constant(Value::I64(1))),
            Box::new(Expression::Constant(Value::I64(3))),
            false,
        );
        assert_eq!(expr.evaluate(None, &ctx).unwrap(), Value::Bool(true));

        // Test: 4 BETWEEN 1 AND 3 should return false
        let expr = Expression::Between(
            Box::new(Expression::Constant(Value::I64(4))),
            Box::new(Expression::Constant(Value::I64(1))),
            Box::new(Expression::Constant(Value::I64(3))),
            false,
        );
        assert_eq!(expr.evaluate(None, &ctx).unwrap(), Value::Bool(false));

        // Test: 4 NOT BETWEEN 1 AND 3 should return true
        let expr = Expression::Between(
            Box::new(Expression::Constant(Value::I64(4))),
            Box::new(Expression::Constant(Value::I64(1))),
            Box::new(Expression::Constant(Value::I64(3))),
            true, // negated
        );
        assert_eq!(expr.evaluate(None, &ctx).unwrap(), Value::Bool(true));

        // Test: NULL BETWEEN 1 AND 3 should return NULL
        let expr = Expression::Between(
            Box::new(Expression::Constant(Value::Null)),
            Box::new(Expression::Constant(Value::I64(1))),
            Box::new(Expression::Constant(Value::I64(3))),
            false,
        );
        assert_eq!(expr.evaluate(None, &ctx).unwrap(), Value::Null);
    }
}
