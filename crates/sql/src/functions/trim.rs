//! TRIM, LTRIM, RTRIM functions - remove whitespace from strings

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::types::context::ExecutionContext;
use crate::types::data_type::DataType;
use crate::types::value::Value;

pub struct TrimFunction;
pub struct LtrimFunction;
pub struct RtrimFunction;

impl Function for TrimFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "TRIM",
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return Err(Error::ExecutionError(format!(
                "TRIM takes exactly 1 argument, got {}",
                arg_types.len()
            )));
        }

        match &arg_types[0] {
            DataType::Text | DataType::Str => Ok(DataType::Text),
            DataType::Nullable(inner) => match inner.as_ref() {
                DataType::Text | DataType::Str => Ok(DataType::Nullable(Box::new(DataType::Text))),
                _ => Err(Error::TypeMismatch {
                    expected: "string type".into(),
                    found: arg_types[0].to_string(),
                }),
            },
            _ => Err(Error::TypeMismatch {
                expected: "string type".into(),
                found: arg_types[0].to_string(),
            }),
        }
    }

    fn execute(&self, args: &[Value], _context: &ExecutionContext) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::ExecutionError(
                "TRIM takes exactly 1 argument".into(),
            ));
        }

        match &args[0] {
            Value::Str(s) => Ok(Value::string(s.trim())),
            Value::Null => Ok(Value::Null),
            _ => Err(Error::TypeMismatch {
                expected: "string".into(),
                found: args[0].data_type().to_string(),
            }),
        }
    }
}

impl Function for LtrimFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "LTRIM",
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return Err(Error::ExecutionError(format!(
                "LTRIM takes exactly 1 argument, got {}",
                arg_types.len()
            )));
        }

        match &arg_types[0] {
            DataType::Text | DataType::Str => Ok(DataType::Text),
            DataType::Nullable(inner) => match inner.as_ref() {
                DataType::Text | DataType::Str => Ok(DataType::Nullable(Box::new(DataType::Text))),
                _ => Err(Error::TypeMismatch {
                    expected: "string type".into(),
                    found: arg_types[0].to_string(),
                }),
            },
            _ => Err(Error::TypeMismatch {
                expected: "string type".into(),
                found: arg_types[0].to_string(),
            }),
        }
    }

    fn execute(&self, args: &[Value], _context: &ExecutionContext) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::ExecutionError(
                "LTRIM takes exactly 1 argument".into(),
            ));
        }

        match &args[0] {
            Value::Str(s) => Ok(Value::string(s.trim_start())),
            Value::Null => Ok(Value::Null),
            _ => Err(Error::TypeMismatch {
                expected: "string".into(),
                found: args[0].data_type().to_string(),
            }),
        }
    }
}

impl Function for RtrimFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "RTRIM",
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return Err(Error::ExecutionError(format!(
                "RTRIM takes exactly 1 argument, got {}",
                arg_types.len()
            )));
        }

        match &arg_types[0] {
            DataType::Text | DataType::Str => Ok(DataType::Text),
            DataType::Nullable(inner) => match inner.as_ref() {
                DataType::Text | DataType::Str => Ok(DataType::Nullable(Box::new(DataType::Text))),
                _ => Err(Error::TypeMismatch {
                    expected: "string type".into(),
                    found: arg_types[0].to_string(),
                }),
            },
            _ => Err(Error::TypeMismatch {
                expected: "string type".into(),
                found: arg_types[0].to_string(),
            }),
        }
    }

    fn execute(&self, args: &[Value], _context: &ExecutionContext) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::ExecutionError(
                "RTRIM takes exactly 1 argument".into(),
            ));
        }

        match &args[0] {
            Value::Str(s) => Ok(Value::string(s.trim_end())),
            Value::Null => Ok(Value::Null),
            _ => Err(Error::TypeMismatch {
                expected: "string".into(),
                found: args[0].data_type().to_string(),
            }),
        }
    }
}

/// Register the TRIM functions
pub fn register(registry: &mut FunctionRegistry) {
    registry.register(Box::new(TrimFunction));
    registry.register(Box::new(LtrimFunction));
    registry.register(Box::new(RtrimFunction));
}

#[cfg(test)]
mod tests {
    use super::*;
    use proven_hlc::{HlcTimestamp, NodeId};

    #[test]
    fn test_trim_execute() {
        let func = TrimFunction;
        let context = ExecutionContext::new(HlcTimestamp::new(1000, 0, NodeId::new(1)), 0);

        // Trim both sides
        let result = func
            .execute(&[Value::string("  hello world  ")], &context)
            .unwrap();
        assert_eq!(result, Value::string("hello world"));

        // No whitespace
        let result = func.execute(&[Value::string("hello")], &context).unwrap();
        assert_eq!(result, Value::string("hello"));

        // Only whitespace
        let result = func.execute(&[Value::string("   ")], &context).unwrap();
        assert_eq!(result, Value::string(""));

        // NULL handling
        let result = func.execute(&[Value::Null], &context).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_ltrim_execute() {
        let func = LtrimFunction;
        let context = ExecutionContext::new(HlcTimestamp::new(1000, 0, NodeId::new(1)), 0);

        // Trim left side only
        let result = func
            .execute(&[Value::string("  hello world  ")], &context)
            .unwrap();
        assert_eq!(result, Value::string("hello world  "));

        // No left whitespace
        let result = func.execute(&[Value::string("hello  ")], &context).unwrap();
        assert_eq!(result, Value::string("hello  "));

        // NULL handling
        let result = func.execute(&[Value::Null], &context).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_rtrim_execute() {
        let func = RtrimFunction;
        let context = ExecutionContext::new(HlcTimestamp::new(1000, 0, NodeId::new(1)), 0);

        // Trim right side only
        let result = func
            .execute(&[Value::string("  hello world  ")], &context)
            .unwrap();
        assert_eq!(result, Value::string("  hello world"));

        // No right whitespace
        let result = func.execute(&[Value::string("  hello")], &context).unwrap();
        assert_eq!(result, Value::string("  hello"));

        // NULL handling
        let result = func.execute(&[Value::Null], &context).unwrap();
        assert_eq!(result, Value::Null);
    }
}
