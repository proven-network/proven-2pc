//! Expression evaluation for SQL execution
//!
//! This module handles the evaluation of expressions during query execution,
//! converting Expression trees into concrete Values using row data and
//! transaction context.

use crate::error::{Error, Result};
use crate::operators;
use crate::storage::SqlStorage;
use crate::types::context::ExecutionContext;
use crate::types::expression::Expression;
use crate::types::{Row, Value};
use std::sync::Arc;

/// Evaluate an expression to a value, using a row for column lookups,
/// a transaction context for deterministic functions, and optional parameters.
pub fn evaluate(
    expr: &Expression,
    row: Option<&Row>,
    context: &ExecutionContext,
    params: Option<&Vec<Value>>,
) -> Result<Value> {
    evaluate_with_storage(expr, row, context, params, None)
}

/// Evaluate an expression with optional storage access (for subqueries)
/// and optional outer row (for correlated subqueries)
pub fn evaluate_with_storage(
    expr: &Expression,
    row: Option<&Row>,
    context: &ExecutionContext,
    params: Option<&Vec<Value>>,
    storage: Option<&SqlStorage>,
) -> Result<Value> {
    evaluate_with_storage_and_outer(expr, row, None, context, params, storage)
}

/// Evaluate an expression with storage, params, and outer row context
fn evaluate_with_storage_and_outer(
    expr: &Expression,
    row: Option<&Row>,
    outer_row: Option<&Row>,
    context: &ExecutionContext,
    params: Option<&Vec<Value>>,
    storage: Option<&SqlStorage>,
) -> Result<Value> {
    use Expression::*;
    Ok(match expr {
        Constant(value) => value.clone(),
        All => Value::integer(1), // Placeholder - should be handled at higher level

        Column(i) => {
            // For correlated subqueries, we may have an outer_row
            // Create a combined row: [current_row values... | outer_row values...]
            // Inner query columns come first, then outer query columns
            if let Some(outer) = outer_row {
                let combined_row: Row = if let Some(inner) = row {
                    inner.iter().chain(outer.iter()).cloned().collect()
                } else {
                    outer.clone()
                };
                combined_row
                    .get(*i)
                    .cloned()
                    .ok_or_else(|| Error::InvalidValue(format!("Column {} not found", i)))?
            } else {
                row.and_then(|row| row.get(*i))
                    .cloned()
                    .ok_or_else(|| Error::InvalidValue(format!("Column {} not found", i)))?
            }
        }

        Parameter(idx) => params.and_then(|p| p.get(*idx)).cloned().ok_or_else(|| {
            Error::ExecutionError(format!("unbound parameter at position {}", idx))
        })?,

        // Logical operations
        And(lhs, rhs) => {
            let l = evaluate_with_storage_and_outer(lhs, row, outer_row, context, params, storage)?;
            let r = evaluate_with_storage_and_outer(rhs, row, outer_row, context, params, storage)?;
            operators::execute_and(&l, &r)?
        }
        Or(lhs, rhs) => {
            let l = evaluate_with_storage_and_outer(lhs, row, outer_row, context, params, storage)?;
            let r = evaluate_with_storage_and_outer(rhs, row, outer_row, context, params, storage)?;
            operators::execute_or(&l, &r)?
        }
        Xor(lhs, rhs) => {
            let l = evaluate_with_storage_and_outer(lhs, row, outer_row, context, params, storage)?;
            let r = evaluate_with_storage_and_outer(rhs, row, outer_row, context, params, storage)?;
            operators::execute_xor(&l, &r)?
        }
        Not(expr) => {
            let v =
                evaluate_with_storage_and_outer(expr, row, outer_row, context, params, storage)?;
            operators::execute_not(&v)?
        }

        // Comparison operations
        Equal(lhs, rhs) => {
            let l = evaluate_with_storage_and_outer(lhs, row, outer_row, context, params, storage)?;
            let r = evaluate_with_storage_and_outer(rhs, row, outer_row, context, params, storage)?;
            operators::execute_equal(&l, &r)?
        }

        NotEqual(lhs, rhs) => {
            let l = evaluate_with_storage_and_outer(lhs, row, outer_row, context, params, storage)?;
            let r = evaluate_with_storage_and_outer(rhs, row, outer_row, context, params, storage)?;
            operators::execute_not_equal(&l, &r)?
        }

        LessThan(lhs, rhs) => {
            let l = evaluate_with_storage_and_outer(lhs, row, outer_row, context, params, storage)?;
            let r = evaluate_with_storage_and_outer(rhs, row, outer_row, context, params, storage)?;
            operators::execute_less_than(&l, &r)?
        }

        LessThanOrEqual(lhs, rhs) => {
            let l = evaluate_with_storage_and_outer(lhs, row, outer_row, context, params, storage)?;
            let r = evaluate_with_storage_and_outer(rhs, row, outer_row, context, params, storage)?;
            operators::execute_less_than_equal(&l, &r)?
        }

        GreaterThan(lhs, rhs) => {
            let l = evaluate_with_storage_and_outer(lhs, row, outer_row, context, params, storage)?;
            let r = evaluate_with_storage_and_outer(rhs, row, outer_row, context, params, storage)?;
            operators::execute_greater_than(&l, &r)?
        }

        GreaterThanOrEqual(lhs, rhs) => {
            let l = evaluate_with_storage_and_outer(lhs, row, outer_row, context, params, storage)?;
            let r = evaluate_with_storage_and_outer(rhs, row, outer_row, context, params, storage)?;
            operators::execute_greater_than_equal(&l, &r)?
        }

        Is(expr, check_value) => {
            let v =
                evaluate_with_storage_and_outer(expr, row, outer_row, context, params, storage)?;
            Value::boolean(v == *check_value)
        }

        // Arithmetic operations
        Add(lhs, rhs) => {
            let l = evaluate_with_storage_and_outer(lhs, row, outer_row, context, params, storage)?;
            let r = evaluate_with_storage_and_outer(rhs, row, outer_row, context, params, storage)?;
            operators::execute_add(&l, &r)?
        }
        Concat(lhs, rhs) => {
            let l = evaluate_with_storage_and_outer(lhs, row, outer_row, context, params, storage)?;
            let r = evaluate_with_storage_and_outer(rhs, row, outer_row, context, params, storage)?;
            operators::execute_concat(&l, &r)?
        }
        Subtract(lhs, rhs) => {
            let l = evaluate_with_storage_and_outer(lhs, row, outer_row, context, params, storage)?;
            let r = evaluate_with_storage_and_outer(rhs, row, outer_row, context, params, storage)?;
            operators::execute_subtract(&l, &r)?
        }
        Multiply(lhs, rhs) => {
            let l = evaluate_with_storage_and_outer(lhs, row, outer_row, context, params, storage)?;
            let r = evaluate_with_storage_and_outer(rhs, row, outer_row, context, params, storage)?;
            operators::execute_multiply(&l, &r)?
        }
        Divide(lhs, rhs) => {
            let l = evaluate_with_storage_and_outer(lhs, row, outer_row, context, params, storage)?;
            let r = evaluate_with_storage_and_outer(rhs, row, outer_row, context, params, storage)?;
            operators::execute_divide(&l, &r)?
        }
        Remainder(lhs, rhs) => {
            let l = evaluate_with_storage_and_outer(lhs, row, outer_row, context, params, storage)?;
            let r = evaluate_with_storage_and_outer(rhs, row, outer_row, context, params, storage)?;
            operators::execute_remainder(&l, &r)?
        }

        Negate(expr) => {
            let value =
                evaluate_with_storage_and_outer(expr, row, outer_row, context, params, storage)?;
            operators::execute_negate(&value)?
        }

        Identity(expr) => {
            let value =
                evaluate_with_storage_and_outer(expr, row, outer_row, context, params, storage)?;
            operators::execute_identity(&value)?
        }

        Factorial(expr) => {
            let value =
                evaluate_with_storage_and_outer(expr, row, outer_row, context, params, storage)?;
            operators::execute_factorial(&value)?
        }

        Exponentiate(lhs, rhs) => {
            let l = evaluate_with_storage_and_outer(lhs, row, outer_row, context, params, storage)?;
            let r = evaluate_with_storage_and_outer(rhs, row, outer_row, context, params, storage)?;
            operators::execute_exponentiate(&l, &r)?
        }

        // Bitwise operations
        BitwiseAnd(lhs, rhs) => {
            let l = evaluate_with_storage_and_outer(lhs, row, outer_row, context, params, storage)?;
            let r = evaluate_with_storage_and_outer(rhs, row, outer_row, context, params, storage)?;
            operators::execute_bitwise_and(&l, &r)?
        }
        BitwiseOr(lhs, rhs) => {
            let l = evaluate_with_storage_and_outer(lhs, row, outer_row, context, params, storage)?;
            let r = evaluate_with_storage_and_outer(rhs, row, outer_row, context, params, storage)?;
            operators::execute_bitwise_or(&l, &r)?
        }
        BitwiseXor(lhs, rhs) => {
            let l = evaluate_with_storage_and_outer(lhs, row, outer_row, context, params, storage)?;
            let r = evaluate_with_storage_and_outer(rhs, row, outer_row, context, params, storage)?;
            operators::execute_bitwise_xor(&l, &r)?
        }
        BitwiseShiftLeft(lhs, rhs) => {
            let l = evaluate_with_storage_and_outer(lhs, row, outer_row, context, params, storage)?;
            let r = evaluate_with_storage_and_outer(rhs, row, outer_row, context, params, storage)?;
            operators::execute_bitwise_shift_left(&l, &r)?
        }
        BitwiseShiftRight(lhs, rhs) => {
            let l = evaluate_with_storage_and_outer(lhs, row, outer_row, context, params, storage)?;
            let r = evaluate_with_storage_and_outer(rhs, row, outer_row, context, params, storage)?;
            operators::execute_bitwise_shift_right(&l, &r)?
        }

        BitwiseNot(expr) => {
            let value =
                evaluate_with_storage_and_outer(expr, row, outer_row, context, params, storage)?;
            operators::execute_bitwise_not(&value)?
        }

        ILike(expr, pattern, negated) => {
            let value =
                evaluate_with_storage_and_outer(expr, row, outer_row, context, params, storage)?;
            let pattern_value =
                evaluate_with_storage_and_outer(pattern, row, outer_row, context, params, storage)?;
            let result = operators::execute_ilike(&value, &pattern_value)?;
            if *negated {
                operators::execute_not(&result)?
            } else {
                result
            }
        }

        Like(expr, pattern, negated) => {
            let value =
                evaluate_with_storage_and_outer(expr, row, outer_row, context, params, storage)?;
            let pattern_value =
                evaluate_with_storage_and_outer(pattern, row, outer_row, context, params, storage)?;
            let result = operators::execute_like(&value, &pattern_value)?;
            if *negated {
                operators::execute_not(&result)?
            } else {
                result
            }
        }

        // Function calls
        Function(name, args) => {
            let arg_values: Result<Vec<_>> = args
                .iter()
                .map(|arg| {
                    evaluate_with_storage_and_outer(arg, row, outer_row, context, params, storage)
                })
                .collect();
            crate::functions::execute_function(name, &arg_values?, context)?
        }

        // IN list operator
        InList(expr, list, negated) => {
            let value =
                evaluate_with_storage_and_outer(expr, row, outer_row, context, params, storage)?;

            if value == Value::Null {
                return Ok(Value::Null);
            }

            let mut found = false;
            let mut has_null = false;

            for item in list {
                let item_value = evaluate_with_storage_and_outer(
                    item, row, outer_row, context, params, storage,
                )?;
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
            let value =
                evaluate_with_storage_and_outer(expr, row, outer_row, context, params, storage)?;
            let low_value =
                evaluate_with_storage_and_outer(low, row, outer_row, context, params, storage)?;
            let high_value =
                evaluate_with_storage_and_outer(high, row, outer_row, context, params, storage)?;

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
            let collection =
                evaluate_with_storage_and_outer(base, row, outer_row, context, params, storage)?;
            let idx =
                evaluate_with_storage_and_outer(index, row, outer_row, context, params, storage)?;

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
            let struct_val =
                evaluate_with_storage_and_outer(base, row, outer_row, context, params, storage)?;
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
                .map(|e| {
                    evaluate_with_storage_and_outer(e, row, outer_row, context, params, storage)
                })
                .collect();
            Value::Array(values?)
        }

        MapLiteral(pairs) => {
            let mut map = std::collections::HashMap::new();
            for (k, v) in pairs {
                let key =
                    evaluate_with_storage_and_outer(k, row, outer_row, context, params, storage)?;
                let value =
                    evaluate_with_storage_and_outer(v, row, outer_row, context, params, storage)?;
                if let Value::Str(key_str) = key {
                    map.insert(key_str, value);
                }
            }
            Value::Map(map)
        }

        // Subquery operators
        InSubquery(expr, plan, negated) => {
            let value =
                evaluate_with_storage_and_outer(expr, row, outer_row, context, params, storage)?;

            if value == Value::Null {
                return Ok(Value::Null);
            }

            // We need storage to execute the subquery
            let storage = storage.ok_or_else(|| {
                Error::ExecutionError("Subquery evaluation requires storage access".to_string())
            })?;

            // Clone context to make it mutable for subquery execution
            let mut subquery_ctx = context.clone();

            // Execute the subquery plan
            let node = match plan.as_ref() {
                crate::types::plan::Plan::Query { root, .. } => root.as_ref().clone(),
                _ => {
                    return Err(Error::ExecutionError(
                        "IN subquery must be a Query".to_string(),
                    ));
                }
            };
            // For correlated subqueries, pass the current row as the outer row
            let outer_row_arc = row.map(|r| Arc::new(r.clone()));
            let mut rows = super::executor::execute_node_read_with_outer(
                node,
                storage,
                &mut subquery_ctx,
                params,
                outer_row_arc.as_ref(),
            )?;

            // Check if value is in the result set
            let mut found = false;
            let mut has_null = false;

            while let Some(Ok(row_values)) = rows.next() {
                if row_values.is_empty() {
                    continue;
                }
                let item_value = &row_values[0]; // Take first column of subquery result
                if *item_value == Value::Null {
                    has_null = true;
                } else if let Ok(std::cmp::Ordering::Equal) = operators::compare(&value, item_value)
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

        Exists(plan, negated) => {
            // We need storage to execute the subquery
            let storage = storage.ok_or_else(|| {
                Error::ExecutionError("Subquery evaluation requires storage access".to_string())
            })?;

            // Clone context to make it mutable for subquery execution
            let mut subquery_ctx = context.clone();

            // Execute the subquery plan
            let node = match plan.as_ref() {
                crate::types::plan::Plan::Query { root, .. } => root.as_ref().clone(),
                _ => {
                    return Err(Error::ExecutionError(
                        "EXISTS subquery must be a Query".to_string(),
                    ));
                }
            };
            // For correlated subqueries, pass the current row as the outer row
            let outer_row_arc = row.map(|r| Arc::new(r.clone()));
            let mut rows = super::executor::execute_node_read_with_outer(
                node,
                storage,
                &mut subquery_ctx,
                params,
                outer_row_arc.as_ref(),
            )?;

            // Check if any rows exist
            let exists = rows.next().is_some();

            if *negated {
                Value::boolean(!exists)
            } else {
                Value::boolean(exists)
            }
        }

        Subquery(plan) => {
            // We need storage to execute the subquery
            let storage = storage.ok_or_else(|| {
                Error::ExecutionError("Subquery evaluation requires storage access".to_string())
            })?;

            // Clone context to make it mutable for subquery execution
            let mut subquery_ctx = context.clone();

            // Execute the subquery plan
            let node = match plan.as_ref() {
                crate::types::plan::Plan::Query { root, .. } => root.as_ref().clone(),
                _ => {
                    return Err(Error::ExecutionError(
                        "Scalar subquery must be a Query".to_string(),
                    ));
                }
            };
            // For correlated subqueries, pass the current row as the outer row
            let outer_row_arc = row.map(|r| Arc::new(r.clone()));
            let mut rows = super::executor::execute_node_read_with_outer(
                node,
                storage,
                &mut subquery_ctx,
                params,
                outer_row_arc.as_ref(),
            )?;

            // Get the first row
            match rows.next() {
                Some(Ok(row_values)) => {
                    // Check if there are more rows (error case for scalar subquery)
                    if rows.next().is_some() {
                        return Err(Error::ExecutionError(
                            "Scalar subquery returned more than one row".to_string(),
                        ));
                    }
                    // Return the first column of the first row
                    row_values.first().cloned().unwrap_or(Value::Null)
                }
                _ => Value::Null, // No rows returned
            }
        }

        Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            // Evaluate operand once if present
            let operand_value = if let Some(op) = operand {
                evaluate_with_storage_and_outer(op, row, outer_row, context, params, storage)?
            } else {
                Value::boolean(true)
            };

            // Iterate through when/then clauses
            for (when_expr, then_expr) in when_clauses {
                let when_value = evaluate_with_storage_and_outer(
                    when_expr, row, outer_row, context, params, storage,
                )?;

                // For simple CASE (operand present), compare operand == when_value
                // For searched CASE (no operand), check if when_value is true
                let matches = if operand.is_some() {
                    // Simple CASE: compare values for equality
                    operators::execute_equal(&operand_value, &when_value)? == Value::boolean(true)
                } else {
                    // Searched CASE: evaluate when as boolean condition
                    when_value == Value::boolean(true)
                };

                if matches {
                    return evaluate_with_storage_and_outer(
                        then_expr, row, outer_row, context, params, storage,
                    );
                }
            }

            // No match found, evaluate else clause or return NULL
            if let Some(else_expr) = else_clause {
                evaluate_with_storage_and_outer(
                    else_expr, row, outer_row, context, params, storage,
                )?
            } else {
                Value::Null
            }
        }
    })
}

/// Evaluate an expression with Arc row and optional storage access (for subqueries)
pub fn evaluate_with_arc_and_storage(
    expr: &Expression,
    row: Option<&Arc<Vec<Value>>>,
    context: &ExecutionContext,
    params: Option<&Vec<Value>>,
    storage: Option<&SqlStorage>,
) -> Result<Value> {
    evaluate_with_storage(expr, row.map(|r| r.as_ref()), context, params, storage)
}

/// Evaluate with Arc row, storage, and outer row (for correlated subqueries)
pub fn evaluate_with_arc_storage_and_outer(
    expr: &Expression,
    row: Option<&Arc<Vec<Value>>>,
    outer_row: Option<&Arc<Vec<Value>>>,
    context: &ExecutionContext,
    params: Option<&Vec<Value>>,
    storage: Option<&SqlStorage>,
) -> Result<Value> {
    evaluate_with_storage_and_outer(
        expr,
        row.map(|r| r.as_ref()),
        outer_row.map(|r| r.as_ref()),
        context,
        params,
        storage,
    )
}
