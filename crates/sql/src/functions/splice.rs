//! SPLICE function - remove and/or insert elements in a list
//!
//! Similar to JavaScript's Array.splice(), this function:
//! - Removes elements from a list starting at a given index
//! - Optionally inserts new elements at that position
//!
//! Signature: SPLICE(list, start_index, delete_count[, items_list])

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::coercion::coerce_value;
use crate::error::{Error, Result};
use crate::types::context::ExecutionContext;
use crate::types::data_type::DataType;
use crate::types::{Value, ValueExt};

pub struct SpliceFunction;

impl Function for SpliceFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "SPLICE",
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        // SPLICE takes 3 or 4 arguments
        if arg_types.len() < 3 || arg_types.len() > 4 {
            return Err(Error::ExecutionError(format!(
                "SPLICE takes 3 or 4 arguments, got {}",
                arg_types.len()
            )));
        }

        // First argument must be a list or array
        let element_type = match &arg_types[0] {
            DataType::List(inner) => inner.as_ref().clone(),
            DataType::Array(inner, _size) => inner.as_ref().clone(),
            DataType::Nullable(inner) => match inner.as_ref() {
                DataType::List(list_inner) => list_inner.as_ref().clone(),
                DataType::Array(array_inner, _size) => array_inner.as_ref().clone(),
                _ => {
                    return Err(Error::TypeMismatch {
                        expected: "list or array".into(),
                        found: arg_types[0].to_string(),
                    });
                }
            },
            _ => {
                return Err(Error::TypeMismatch {
                    expected: "list or array".into(),
                    found: arg_types[0].to_string(),
                });
            }
        };

        // Second and third arguments must be integers (start index and delete count)
        match &arg_types[1] {
            DataType::I8
            | DataType::I16
            | DataType::I32
            | DataType::I64
            | DataType::I128
            | DataType::U8
            | DataType::U16
            | DataType::U32
            | DataType::U64
            | DataType::U128 => {}
            _ => {
                return Err(Error::TypeMismatch {
                    expected: "integer for start index".into(),
                    found: arg_types[1].to_string(),
                });
            }
        }

        match &arg_types[2] {
            DataType::I8
            | DataType::I16
            | DataType::I32
            | DataType::I64
            | DataType::I128
            | DataType::U8
            | DataType::U16
            | DataType::U32
            | DataType::U64
            | DataType::U128 => {}
            _ => {
                return Err(Error::TypeMismatch {
                    expected: "integer for delete count".into(),
                    found: arg_types[2].to_string(),
                });
            }
        }

        // If 4th argument exists, it must be a list or array
        if arg_types.len() == 4 {
            match &arg_types[3] {
                DataType::List(_) | DataType::Array(_, _) => {}
                DataType::Nullable(inner) => match inner.as_ref() {
                    DataType::List(_) | DataType::Array(_, _) => {}
                    _ => {
                        return Err(Error::TypeMismatch {
                            expected: "list or array for items to insert".into(),
                            found: arg_types[3].to_string(),
                        });
                    }
                },
                _ => {
                    return Err(Error::TypeMismatch {
                        expected: "list or array for items to insert".into(),
                        found: arg_types[3].to_string(),
                    });
                }
            }
        }

        // Return a list with the same element type
        Ok(DataType::List(Box::new(element_type)))
    }

    fn execute(&self, args: &[Value], _context: &ExecutionContext) -> Result<Value> {
        if args.len() < 3 || args.len() > 4 {
            return Err(Error::ExecutionError(format!(
                "SPLICE takes 3 or 4 arguments, got {}",
                args.len()
            )));
        }

        // Handle NULL list
        if matches!(args[0], Value::Null) {
            return Ok(Value::Null);
        }

        // Extract the list (convert arrays to lists for processing)
        let list = match &args[0] {
            Value::List(l) => l.clone(),
            Value::Array(a) => a.clone(), // Arrays can be spliced too
            _ => {
                return Err(Error::TypeMismatch {
                    expected: "list or array".into(),
                    found: args[0].data_type().to_string(),
                });
            }
        };

        // Extract start index
        let start_index = match &args[1] {
            Value::I8(n) => *n as i64,
            Value::I16(n) => *n as i64,
            Value::I32(n) => *n as i64,
            Value::I64(n) => *n,
            Value::I128(n) => *n as i64,
            Value::U8(n) => *n as i64,
            Value::U16(n) => *n as i64,
            Value::U32(n) => *n as i64,
            Value::U64(n) => *n as i64,
            Value::U128(n) => *n as i64,
            _ => {
                return Err(Error::TypeMismatch {
                    expected: "integer for start index".into(),
                    found: args[1].data_type().to_string(),
                });
            }
        };

        // Extract delete count
        let delete_count = match &args[2] {
            Value::I8(n) => *n as i64,
            Value::I16(n) => *n as i64,
            Value::I32(n) => *n as i64,
            Value::I64(n) => *n,
            Value::I128(n) => *n as i64,
            Value::U8(n) => *n as i64,
            Value::U16(n) => *n as i64,
            Value::U32(n) => *n as i64,
            Value::U64(n) => *n as i64,
            Value::U128(n) => *n as i64,
            _ => {
                return Err(Error::TypeMismatch {
                    expected: "integer for delete count".into(),
                    found: args[2].data_type().to_string(),
                });
            }
        };

        // Handle negative indices
        if start_index < 0 {
            return Err(Error::ExecutionError(
                "SPLICE does not support negative indices".into(),
            ));
        }

        let start = start_index as usize;
        let count = delete_count.max(0) as usize;

        // Extract items to insert (if provided)
        let items_to_insert = if args.len() == 4 {
            match &args[3] {
                Value::List(items) => items.clone(),
                Value::Array(items) => items.clone(),
                Value::Null => Vec::new(),
                _ => {
                    return Err(Error::TypeMismatch {
                        expected: "list or array for items to insert".into(),
                        found: args[3].data_type().to_string(),
                    });
                }
            }
        } else {
            Vec::new()
        };

        // Perform the splice operation
        let mut result = list;

        // Clamp start to list bounds
        let actual_start = start.min(result.len());

        // Clamp delete count to remaining elements
        let actual_count = count.min(result.len().saturating_sub(actual_start));

        // Remove elements
        result.drain(actual_start..actual_start + actual_count);

        // Determine element type for coercion
        let element_type = if !result.is_empty() {
            result[0].data_type()
        } else if !items_to_insert.is_empty() {
            items_to_insert[0].data_type()
        } else {
            DataType::Nullable(Box::new(DataType::Text)) // Default type for empty list
        };

        // Insert new elements at the position, coercing to match list type
        for (i, item) in items_to_insert.iter().enumerate() {
            let coerced_item = coerce_value(item.clone(), &element_type)?;
            result.insert(actual_start + i, coerced_item);
        }

        Ok(Value::List(result))
    }
}

/// Register the function
pub fn register(registry: &mut FunctionRegistry) {
    registry.register(Box::new(SpliceFunction));
}

#[cfg(test)]
mod tests {
    use super::*;
    use proven_hlc::{HlcTimestamp, NodeId};

    #[test]
    fn test_splice_remove_only() {
        let func = SpliceFunction;
        let context = ExecutionContext::new(HlcTimestamp::new(1000, 0, NodeId::new(1)), 0);

        // SPLICE([1, 2, 3, 4, 5], 1, 3) should remove 3 elements starting at index 1
        let result = func
            .execute(
                &[
                    Value::List(vec![
                        Value::I64(1),
                        Value::I64(2),
                        Value::I64(3),
                        Value::I64(4),
                        Value::I64(5),
                    ]),
                    Value::I64(1),
                    Value::I64(3),
                ],
                &context,
            )
            .unwrap();

        match result {
            Value::List(list) => {
                assert_eq!(list.len(), 2);
                assert_eq!(list[0], Value::I64(1));
                assert_eq!(list[1], Value::I64(5));
            }
            _ => panic!("Expected List value"),
        }
    }

    #[test]
    fn test_splice_remove_and_insert() {
        let func = SpliceFunction;
        let context = ExecutionContext::new(HlcTimestamp::new(1000, 0, NodeId::new(1)), 0);

        // SPLICE([1, 2, 3, 4, 5], 1, 3, [100, 99])
        let result = func
            .execute(
                &[
                    Value::List(vec![
                        Value::I64(1),
                        Value::I64(2),
                        Value::I64(3),
                        Value::I64(4),
                        Value::I64(5),
                    ]),
                    Value::I64(1),
                    Value::I64(3),
                    Value::List(vec![Value::I64(100), Value::I64(99)]),
                ],
                &context,
            )
            .unwrap();

        match result {
            Value::List(list) => {
                assert_eq!(list.len(), 4);
                assert_eq!(list[0], Value::I64(1));
                assert_eq!(list[1], Value::I64(100));
                assert_eq!(list[2], Value::I64(99));
                assert_eq!(list[3], Value::I64(5));
            }
            _ => panic!("Expected List value"),
        }
    }

    #[test]
    fn test_splice_zero_count() {
        let func = SpliceFunction;
        let context = ExecutionContext::new(HlcTimestamp::new(1000, 0, NodeId::new(1)), 0);

        // SPLICE([1, 2, 3], 1, 0, [10]) - insert without removing
        let result = func
            .execute(
                &[
                    Value::List(vec![Value::I64(1), Value::I64(2), Value::I64(3)]),
                    Value::I64(1),
                    Value::I64(0),
                    Value::List(vec![Value::I64(10)]),
                ],
                &context,
            )
            .unwrap();

        match result {
            Value::List(list) => {
                assert_eq!(list.len(), 4);
                assert_eq!(list[0], Value::I64(1));
                assert_eq!(list[1], Value::I64(10));
                assert_eq!(list[2], Value::I64(2));
                assert_eq!(list[3], Value::I64(3));
            }
            _ => panic!("Expected List value"),
        }
    }
}
