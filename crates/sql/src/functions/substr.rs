//! SUBSTR function - extracts substring from a string

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::types::context::ExecutionContext;
use crate::types::data_type::DataType;
use crate::types::{Value, ValueExt};

pub struct SubstrFunction;

impl Function for SubstrFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "SUBSTR",
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        // SUBSTR(string, start) or SUBSTR(string, start, length)
        if arg_types.len() < 2 || arg_types.len() > 3 {
            return Err(Error::ExecutionError(format!(
                "SUBSTR takes 2 or 3 arguments, got {}",
                arg_types.len()
            )));
        }

        // First argument must be string
        match &arg_types[0] {
            DataType::Text | DataType::Str => {}
            DataType::Nullable(inner) => match inner.as_ref() {
                DataType::Text | DataType::Str => {
                    return Ok(DataType::Nullable(Box::new(DataType::Text)));
                }
                _ => {
                    return Err(Error::TypeMismatch {
                        expected: "string type".into(),
                        found: arg_types[0].to_string(),
                    });
                }
            },
            _ => {
                return Err(Error::TypeMismatch {
                    expected: "string type".into(),
                    found: arg_types[0].to_string(),
                });
            }
        }

        // Second argument (start position) must be integer
        if !matches!(
            arg_types[1],
            DataType::I8
                | DataType::I16
                | DataType::I32
                | DataType::I64
                | DataType::U8
                | DataType::U16
                | DataType::U32
                | DataType::U64
        ) {
            return Err(Error::TypeMismatch {
                expected: "integer".into(),
                found: arg_types[1].to_string(),
            });
        }

        // Third argument (length) if present must be integer or NULL
        if arg_types.len() == 3 {
            match &arg_types[2] {
                DataType::I8
                | DataType::I16
                | DataType::I32
                | DataType::I64
                | DataType::U8
                | DataType::U16
                | DataType::U32
                | DataType::U64
                | DataType::Null => {}
                DataType::Nullable(_) => {}
                _ => {
                    return Err(Error::TypeMismatch {
                        expected: "integer or NULL".into(),
                        found: arg_types[2].to_string(),
                    });
                }
            }
        }

        Ok(DataType::Text)
    }

    fn execute(&self, args: &[Value], _context: &ExecutionContext) -> Result<Value> {
        if args.len() < 2 || args.len() > 3 {
            return Err(Error::ExecutionError(
                "SUBSTR takes 2 or 3 arguments".into(),
            ));
        }

        // Handle NULL inputs
        if args.iter().any(|v| matches!(v, Value::Null)) {
            return Ok(Value::Null);
        }

        // Get string value
        let s = match &args[0] {
            Value::Str(s) => s.as_str(),
            _ => {
                return Err(Error::TypeMismatch {
                    expected: "string".into(),
                    found: args[0].data_type().to_string(),
                });
            }
        };

        // Get start position (1-based index in SQL)
        let start = match &args[1] {
            Value::I8(v) => *v as i64,
            Value::I16(v) => *v as i64,
            Value::I32(v) => *v as i64,
            Value::I64(v) => *v,
            Value::U8(v) => *v as i64,
            Value::U16(v) => *v as i64,
            Value::U32(v) => *v as i64,
            Value::U64(v) => *v as i64,
            _ => {
                return Err(Error::TypeMismatch {
                    expected: "integer".into(),
                    found: args[1].data_type().to_string(),
                });
            }
        };

        // Get length if provided
        let length = if args.len() == 3 {
            Some(match &args[2] {
                Value::I8(v) => *v as i64,
                Value::I16(v) => *v as i64,
                Value::I32(v) => *v as i64,
                Value::I64(v) => *v,
                Value::U8(v) => *v as i64,
                Value::U16(v) => *v as i64,
                Value::U32(v) => *v as i64,
                Value::U64(v) => *v as i64,
                _ => {
                    return Err(Error::TypeMismatch {
                        expected: "integer".into(),
                        found: args[2].data_type().to_string(),
                    });
                }
            })
        } else {
            None
        };

        // Convert 1-based SQL index to 0-based Rust index
        // Handle negative positions (count from end)
        let chars: Vec<char> = s.chars().collect();
        let len = chars.len() as i64;

        let actual_start = if start > 0 {
            (start - 1) as usize // Convert 1-based to 0-based
        } else if start < 0 {
            // Negative positions count from end
            ((len + start).max(0)) as usize
        } else {
            // start == 0 is treated as 1 in SQL
            0
        };

        // If start is beyond string length, return empty string
        if actual_start >= chars.len() {
            return Ok(Value::string(""));
        }

        // Extract substring
        let result = if let Some(length) = length {
            // PostgreSQL behavior: error on negative length
            if length < 0 {
                return Err(Error::ExecutionError(
                    "SUBSTR length must be non-negative".into(),
                ));
            }
            if length == 0 {
                String::new()
            } else {
                let end = (actual_start + length as usize).min(chars.len());
                chars[actual_start..end].iter().collect()
            }
        } else {
            chars[actual_start..].iter().collect()
        };

        Ok(Value::string(result))
    }
}

/// Register the SUBSTR function
pub fn register(registry: &mut FunctionRegistry) {
    registry.register(Box::new(SubstrFunction));
}

#[cfg(test)]
mod tests {
    use super::*;
    use proven_hlc::{HlcTimestamp, NodeId};

    #[test]
    fn test_substr_signature() {
        let func = SubstrFunction;
        let sig = func.signature();
        assert_eq!(sig.name, "SUBSTR");
        assert!(!sig.is_aggregate);
    }

    #[test]
    fn test_substr_validate() {
        let func = SubstrFunction;

        // Valid 2-arg call
        assert_eq!(
            func.validate(&[DataType::Text, DataType::I32]).unwrap(),
            DataType::Text
        );

        // Valid 3-arg call
        assert_eq!(
            func.validate(&[DataType::Text, DataType::I32, DataType::I32])
                .unwrap(),
            DataType::Text
        );

        // Wrong number of arguments
        assert!(func.validate(&[DataType::Text]).is_err());
        assert!(
            func.validate(&[DataType::Text, DataType::I32, DataType::I32, DataType::I32])
                .is_err()
        );

        // Wrong types
        assert!(func.validate(&[DataType::I32, DataType::I32]).is_err());
        assert!(func.validate(&[DataType::Text, DataType::Text]).is_err());
    }

    #[test]
    fn test_substr_execute() {
        let func = SubstrFunction;
        let context = ExecutionContext::new(HlcTimestamp::new(1000, 0, NodeId::new(1)), 0);

        // Basic substring with start only
        let result = func
            .execute(&[Value::string("hello world"), Value::I32(7)], &context)
            .unwrap();
        assert_eq!(result, Value::string("world"));

        // Substring with start and length
        let result = func
            .execute(
                &[Value::string("hello world"), Value::I32(1), Value::I32(5)],
                &context,
            )
            .unwrap();
        assert_eq!(result, Value::string("hello"));

        // Start position beyond string length
        let result = func
            .execute(&[Value::string("hello"), Value::I32(10)], &context)
            .unwrap();
        assert_eq!(result, Value::string(""));

        // Negative start position (from end)
        let result = func
            .execute(&[Value::string("hello"), Value::I32(-2)], &context)
            .unwrap();
        assert_eq!(result, Value::string("lo"));

        // Zero length
        let result = func
            .execute(
                &[Value::string("hello"), Value::I32(1), Value::I32(0)],
                &context,
            )
            .unwrap();
        assert_eq!(result, Value::string(""));

        // NULL handling
        let result = func
            .execute(&[Value::Null, Value::I32(1)], &context)
            .unwrap();
        assert_eq!(result, Value::Null);
    }
}
