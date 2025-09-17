//! Expression evaluation for SQL execution
//!
//! This module handles the evaluation of expressions during query execution,
//! converting Expression trees into concrete Values using row data and
//! transaction context.

use crate::error::{Error, Result};
use crate::operators;
use crate::semantic::BoundParameters;
use crate::stream::transaction::TransactionContext;
use crate::types::expression::Expression;
use crate::types::value::{Row, Value};
use std::sync::Arc;

/// Evaluate an expression to a value, using a row for column lookups,
/// a transaction context for deterministic functions, and optional parameters.
pub fn evaluate(
    expr: &Expression,
    row: Option<&Row>,
    context: &TransactionContext,
    params: Option<&BoundParameters>,
) -> Result<Value> {
    use Expression::*;
    Ok(match expr {
        Constant(value) => value.clone(),
        All => Value::integer(1), // Placeholder - should be handled at higher level

        Column(i) => row
            .and_then(|row| row.get(*i))
            .cloned()
            .ok_or_else(|| Error::InvalidValue(format!("Column {} not found", i)))?,

        Parameter(idx) => params.and_then(|p| p.get(*idx)).cloned().ok_or_else(|| {
            Error::ExecutionError(format!("unbound parameter at position {}", idx))
        })?,

        // Logical operations
        And(lhs, rhs) => {
            let l = evaluate(lhs, row, context, params)?;
            let r = evaluate(rhs, row, context, params)?;
            operators::execute_and(&l, &r)?
        }
        Or(lhs, rhs) => {
            let l = evaluate(lhs, row, context, params)?;
            let r = evaluate(rhs, row, context, params)?;
            operators::execute_or(&l, &r)?
        }
        Not(expr) => {
            let v = evaluate(expr, row, context, params)?;
            operators::execute_not(&v)?
        }

        // Comparison operations with SQL NULL semantics and NaN handling
        Equal(lhs, rhs) => {
            let l = evaluate(lhs, row, context, params)?;
            let mut r = evaluate(rhs, row, context, params)?;

            // Special case: if comparing collections with JSON strings, parse the strings
            if matches!(l, Value::Map(_)) && matches!(r, Value::Str(_)) {
                if let Value::Str(s) = &r
                    && s.starts_with('{')
                    && s.ends_with('}')
                    && let Ok(parsed) = Value::parse_json_object(s)
                    && matches!(parsed, Value::Map(_))
                {
                    r = parsed;
                }
            } else if matches!(r, Value::Map(_)) && matches!(l, Value::Str(_)) {
                if let Value::Str(s) = &l
                    && s.starts_with('{')
                    && s.ends_with('}')
                    && let Ok(parsed) = Value::parse_json_object(s)
                    && matches!(parsed, Value::Map(_))
                {
                    return evaluate(
                        &Expression::Equal(
                            Box::new(Expression::Constant(parsed)),
                            Box::new(Expression::Constant(r)),
                        ),
                        row,
                        context,
                        params,
                    );
                }
            }
            // Handle Array/List comparison
            else if (matches!(l, Value::Array(_)) || matches!(l, Value::List(_)))
                && matches!(r, Value::Str(_))
            {
                if let Value::Str(s) = &r
                    && s.starts_with('[')
                    && s.ends_with(']')
                    && let Ok(parsed) = Value::parse_json_array(s)
                {
                    r = parsed;
                }
            } else if (matches!(r, Value::Array(_)) || matches!(r, Value::List(_)))
                && matches!(l, Value::Str(_))
            {
                if let Value::Str(s) = &l
                    && s.starts_with('[')
                    && s.ends_with(']')
                    && let Ok(parsed) = Value::parse_json_array(s)
                {
                    return evaluate(
                        &Expression::Equal(
                            Box::new(Expression::Constant(parsed)),
                            Box::new(Expression::Constant(r)),
                        ),
                        row,
                        context,
                        params,
                    );
                }
            }
            // Handle Struct comparison
            else if matches!(l, Value::Struct(_)) && matches!(r, Value::Str(_)) {
                if let Value::Str(s) = &r
                    && s.starts_with('{')
                    && s.ends_with('}')
                    && let Ok(parsed) = Value::parse_json_object(s)
                    && let Value::Struct(fields) = &l
                {
                    let schema: Vec<(String, crate::types::DataType)> = fields
                        .iter()
                        .map(|(name, val)| (name.clone(), val.data_type()))
                        .collect();
                    if let Ok(coerced) = crate::coercion::coerce_value(
                        parsed,
                        &crate::types::DataType::Struct(schema),
                    ) {
                        r = coerced;
                    }
                }
            } else if matches!(r, Value::Struct(_))
                && matches!(l, Value::Str(_))
                && let Value::Str(s) = &l
                && s.starts_with('{')
                && s.ends_with('}')
                && let Ok(parsed) = Value::parse_json_object(s)
                && let Value::Struct(fields) = &r
            {
                let schema: Vec<(String, crate::types::DataType)> = fields
                    .iter()
                    .map(|(name, val)| (name.clone(), val.data_type()))
                    .collect();
                if let Ok(coerced) =
                    crate::coercion::coerce_value(parsed, &crate::types::DataType::Struct(schema))
                {
                    return evaluate(
                        &Expression::Equal(
                            Box::new(Expression::Constant(coerced)),
                            Box::new(Expression::Constant(r)),
                        ),
                        row,
                        context,
                        params,
                    );
                }
            }

            // SQL semantics: any comparison with NULL returns NULL
            if l.is_null() || r.is_null() {
                return Ok(Value::Null);
            }

            // IEEE 754 semantics: NaN is never equal to anything, including itself
            let has_nan = match (&l, &r) {
                (Value::F32(a), _) if a.is_nan() => true,
                (_, Value::F32(b)) if b.is_nan() => true,
                (Value::F64(a), _) if a.is_nan() => true,
                (_, Value::F64(b)) if b.is_nan() => true,
                _ => false,
            };
            if has_nan {
                return Ok(Value::boolean(false));
            }

            // Use operators::compare for type-aware comparison
            match operators::compare(&l, &r) {
                Ok(std::cmp::Ordering::Equal) => Value::boolean(true),
                Ok(_) => Value::boolean(false),
                Err(_) => Value::boolean(false), // Type mismatch means not equal
            }
        }

        NotEqual(lhs, rhs) => {
            let l = evaluate(lhs, row, context, params)?;
            let r = evaluate(rhs, row, context, params)?;
            // SQL semantics: any comparison with NULL returns NULL
            if l.is_null() || r.is_null() {
                return Ok(Value::Null);
            }
            // IEEE 754 semantics: NaN is never equal to anything, so always not equal
            let has_nan = match (&l, &r) {
                (Value::F32(a), _) if a.is_nan() => true,
                (_, Value::F32(b)) if b.is_nan() => true,
                (Value::F64(a), _) if a.is_nan() => true,
                (_, Value::F64(b)) if b.is_nan() => true,
                _ => false,
            };
            if has_nan {
                return Ok(Value::boolean(true));
            }
            match operators::compare(&l, &r) {
                Ok(std::cmp::Ordering::Equal) => Value::boolean(false),
                Ok(_) => Value::boolean(true),
                Err(_) => Value::boolean(true), // Type mismatch means not equal
            }
        }

        LessThan(lhs, rhs) => {
            let l = evaluate(lhs, row, context, params)?;
            let r = evaluate(rhs, row, context, params)?;
            if l.is_null() || r.is_null() {
                return Ok(Value::Null);
            }
            let has_nan = match (&l, &r) {
                (Value::F32(a), _) if a.is_nan() => true,
                (_, Value::F32(b)) if b.is_nan() => true,
                (Value::F64(a), _) if a.is_nan() => true,
                (_, Value::F64(b)) if b.is_nan() => true,
                _ => false,
            };
            if has_nan {
                return Ok(Value::boolean(false));
            }
            match operators::compare(&l, &r) {
                Ok(std::cmp::Ordering::Less) => Value::boolean(true),
                Ok(_) => Value::boolean(false),
                Err(_) => Value::boolean(false),
            }
        }

        LessThanOrEqual(lhs, rhs) => {
            let l = evaluate(lhs, row, context, params)?;
            let r = evaluate(rhs, row, context, params)?;
            if l.is_null() || r.is_null() {
                return Ok(Value::Null);
            }
            let has_nan = match (&l, &r) {
                (Value::F32(a), _) if a.is_nan() => true,
                (_, Value::F32(b)) if b.is_nan() => true,
                (Value::F64(a), _) if a.is_nan() => true,
                (_, Value::F64(b)) if b.is_nan() => true,
                _ => false,
            };
            if has_nan {
                return Ok(Value::boolean(false));
            }
            match operators::compare(&l, &r) {
                Ok(std::cmp::Ordering::Less | std::cmp::Ordering::Equal) => Value::boolean(true),
                Ok(_) => Value::boolean(false),
                Err(_) => Value::boolean(false),
            }
        }

        GreaterThan(lhs, rhs) => {
            let l = evaluate(lhs, row, context, params)?;
            let r = evaluate(rhs, row, context, params)?;
            if l.is_null() || r.is_null() {
                return Ok(Value::Null);
            }
            let has_nan = match (&l, &r) {
                (Value::F32(a), _) if a.is_nan() => true,
                (_, Value::F32(b)) if b.is_nan() => true,
                (Value::F64(a), _) if a.is_nan() => true,
                (_, Value::F64(b)) if b.is_nan() => true,
                _ => false,
            };
            if has_nan {
                return Ok(Value::boolean(false));
            }
            match operators::compare(&l, &r) {
                Ok(std::cmp::Ordering::Greater) => Value::boolean(true),
                Ok(_) => Value::boolean(false),
                Err(_) => Value::boolean(false),
            }
        }

        GreaterThanOrEqual(lhs, rhs) => {
            let l = evaluate(lhs, row, context, params)?;
            let r = evaluate(rhs, row, context, params)?;
            if l.is_null() || r.is_null() {
                return Ok(Value::Null);
            }
            let has_nan = match (&l, &r) {
                (Value::F32(a), _) if a.is_nan() => true,
                (_, Value::F32(b)) if b.is_nan() => true,
                (Value::F64(a), _) if a.is_nan() => true,
                (_, Value::F64(b)) if b.is_nan() => true,
                _ => false,
            };
            if has_nan {
                return Ok(Value::boolean(false));
            }
            match operators::compare(&l, &r) {
                Ok(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal) => Value::boolean(true),
                Ok(_) => Value::boolean(false),
                Err(_) => Value::boolean(false),
            }
        }

        Is(expr, check_value) => {
            let v = evaluate(expr, row, context, params)?;
            Value::boolean(v == *check_value)
        }

        // Arithmetic operations
        Add(lhs, rhs) => {
            let l = evaluate(lhs, row, context, params)?;
            let r = evaluate(rhs, row, context, params)?;
            operators::execute_add(&l, &r)?
        }
        Subtract(lhs, rhs) => {
            let l = evaluate(lhs, row, context, params)?;
            let r = evaluate(rhs, row, context, params)?;
            operators::execute_subtract(&l, &r)?
        }
        Multiply(lhs, rhs) => {
            let l = evaluate(lhs, row, context, params)?;
            let r = evaluate(rhs, row, context, params)?;
            operators::execute_multiply(&l, &r)?
        }
        Divide(lhs, rhs) => {
            let l = evaluate(lhs, row, context, params)?;
            let r = evaluate(rhs, row, context, params)?;
            operators::execute_divide(&l, &r)?
        }
        Remainder(lhs, rhs) => {
            let l = evaluate(lhs, row, context, params)?;
            let r = evaluate(rhs, row, context, params)?;
            operators::execute_remainder(&l, &r)?
        }

        Negate(expr) => {
            let value = evaluate(expr, row, context, params)?;
            operators::execute_negate(&value)?
        }

        Identity(expr) => {
            let value = evaluate(expr, row, context, params)?;
            operators::execute_identity(&value)?
        }

        // Function calls
        Function(name, args) => {
            let arg_values: Result<Vec<_>> = args
                .iter()
                .map(|arg| evaluate(arg, row, context, params))
                .collect();
            crate::functions::execute_function(name, &arg_values?, context)?
        }

        // IN list operator
        InList(expr, list, negated) => {
            let value = evaluate(expr, row, context, params)?;

            if value == Value::Null {
                return Ok(Value::Null);
            }

            let mut found = false;
            let mut has_null = false;

            for item in list {
                let item_value = evaluate(item, row, context, params)?;
                if item_value == Value::Null {
                    has_null = true;
                } else if let Ok(std::cmp::Ordering::Equal) =
                    operators::compare(&value, &item_value)
                {
                    found = true;
                    break;
                }
            }

            // SQL three-valued logic
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
            let value = evaluate(expr, row, context, params)?;
            let low_value = evaluate(low, row, context, params)?;
            let high_value = evaluate(high, row, context, params)?;

            if value == Value::Null || low_value == Value::Null || high_value == Value::Null {
                return Ok(Value::Null);
            }

            let low_cmp =
                operators::compare(&value, &low_value).unwrap_or(std::cmp::Ordering::Less);
            let high_cmp =
                operators::compare(&value, &high_value).unwrap_or(std::cmp::Ordering::Greater);

            let in_range =
                low_cmp != std::cmp::Ordering::Less && high_cmp != std::cmp::Ordering::Greater;

            if *negated {
                Value::boolean(!in_range)
            } else {
                Value::boolean(in_range)
            }
        }

        // Collection access expressions
        ArrayAccess(base, index) => {
            let collection = evaluate(base, row, context, params)?;
            let idx = evaluate(index, row, context, params)?;

            match (collection, idx) {
                (Value::Array(arr), Value::I32(i)) if i >= 0 => {
                    arr.get(i as usize).cloned().unwrap_or(Value::Null)
                }
                (Value::Array(arr), Value::I64(i)) if i >= 0 => {
                    arr.get(i as usize).cloned().unwrap_or(Value::Null)
                }
                (Value::List(list), Value::I32(i)) if i >= 0 => {
                    list.get(i as usize).cloned().unwrap_or(Value::Null)
                }
                (Value::List(list), Value::I64(i)) if i >= 0 => {
                    list.get(i as usize).cloned().unwrap_or(Value::Null)
                }
                (Value::Map(map), Value::Str(key)) => map.get(&key).cloned().unwrap_or(Value::Null),
                _ => Value::Null,
            }
        }

        FieldAccess(base, field) => {
            let struct_val = evaluate(base, row, context, params)?;
            match struct_val {
                Value::Struct(fields) => fields
                    .iter()
                    .find(|(name, _)| name == field)
                    .map(|(_, val)| val.clone())
                    .unwrap_or(Value::Null),
                _ => Value::Null,
            }
        }

        ArrayLiteral(elements) => {
            let values: Result<Vec<_>> = elements
                .iter()
                .map(|e| evaluate(e, row, context, params))
                .collect();
            Value::List(values?)
        }

        MapLiteral(pairs) => {
            let mut map = std::collections::HashMap::new();
            for (k, v) in pairs {
                let key = evaluate(k, row, context, params)?;
                let value = evaluate(v, row, context, params)?;
                if let Value::Str(key_str) = key {
                    map.insert(key_str, value);
                }
            }
            Value::Map(map)
        }

        // Other expression types
        _ => Value::Null,
    })
}

/// Helper to evaluate an expression with an Arc<Vec<Value>> row (used by executor)
pub fn evaluate_with_arc(
    expr: &Expression,
    row: Option<&Arc<Vec<Value>>>,
    context: &TransactionContext,
    params: Option<&BoundParameters>,
) -> Result<Value> {
    evaluate(expr, row.map(|r| r.as_ref()), context, params)
}
