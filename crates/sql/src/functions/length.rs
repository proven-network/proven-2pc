//! LENGTH function - returns length of string or collection

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::stream::transaction::TransactionContext;
use crate::types::data_type::DataType;
use crate::types::value::Value;

pub struct LengthFunction;

impl Function for LengthFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "LENGTH",
            min_args: 1,
            max_args: Some(1),
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return Err(Error::ExecutionError(format!(
                "LENGTH takes exactly 1 argument, got {}",
                arg_types.len()
            )));
        }

        match &arg_types[0] {
            DataType::Text
            | DataType::Str
            | DataType::Bytea
            | DataType::List(_)
            | DataType::Map(_, _)
            | DataType::Struct(_) => Ok(DataType::I64),

            DataType::Nullable(inner) => match inner.as_ref() {
                DataType::Text
                | DataType::Str
                | DataType::Bytea
                | DataType::List(_)
                | DataType::Map(_, _)
                | DataType::Struct(_) => Ok(DataType::Nullable(Box::new(DataType::I64))),
                _ => Err(Error::TypeMismatch {
                    expected: "string, bytea, or collection type".into(),
                    found: arg_types[0].to_string(),
                }),
            },
            _ => Err(Error::TypeMismatch {
                expected: "string, bytea, or collection type".into(),
                found: arg_types[0].to_string(),
            }),
        }
    }

    fn execute(&self, args: &[Value], _context: &TransactionContext) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::ExecutionError(
                "LENGTH takes exactly 1 argument".into(),
            ));
        }

        match &args[0] {
            Value::Str(s) => Ok(Value::integer(s.chars().count() as i64)),
            Value::Bytea(b) => Ok(Value::integer(b.len() as i64)),
            Value::Array(a) | Value::List(a) => Ok(Value::integer(a.len() as i64)),
            Value::Map(m) => Ok(Value::integer(m.len() as i64)),
            Value::Struct(fields) => Ok(Value::integer(fields.len() as i64)),
            Value::Null => Ok(Value::Null),
            _ => Err(Error::TypeMismatch {
                expected: "string, blob, or collection".into(),
                found: args[0].data_type().to_string(),
            }),
        }
    }
}

/// Register the LENGTH function
pub fn register(registry: &mut FunctionRegistry) {
    registry.register(Box::new(LengthFunction));
}

#[cfg(test)]
mod tests {
    use super::*;
    use proven_hlc::{HlcTimestamp, NodeId};
    use std::collections::HashMap;

    #[test]
    fn test_length_strings() {
        let func = LengthFunction;
        let context = TransactionContext::new(HlcTimestamp::new(1000, 0, NodeId::new(1)));

        // String length
        let result = func.execute(&[Value::string("hello")], &context).unwrap();
        assert_eq!(result, Value::integer(5));

        // Empty string
        let result = func.execute(&[Value::string("")], &context).unwrap();
        assert_eq!(result, Value::integer(0));

        // Unicode string
        let result = func
            .execute(&[Value::string("こんにちは")], &context)
            .unwrap();
        assert_eq!(result, Value::integer(5));
    }

    #[test]
    fn test_length_collections() {
        let func = LengthFunction;
        let context = TransactionContext::new(HlcTimestamp::new(1000, 0, NodeId::new(1)));

        // List length
        let list = Value::List(vec![Value::I64(1), Value::I64(2), Value::I64(3)]);
        let result = func.execute(&[list], &context).unwrap();
        assert_eq!(result, Value::integer(3));

        // Map length
        let mut map = HashMap::new();
        map.insert("a".to_string(), Value::I64(1));
        map.insert("b".to_string(), Value::I64(2));
        let map_val = Value::Map(map);
        let result = func.execute(&[map_val], &context).unwrap();
        assert_eq!(result, Value::integer(2));

        // Struct length
        let struct_val = Value::Struct(vec![
            ("name".to_string(), Value::string("Alice")),
            ("age".to_string(), Value::I64(30)),
        ]);
        let result = func.execute(&[struct_val], &context).unwrap();
        assert_eq!(result, Value::integer(2));

        // NULL handling
        let result = func.execute(&[Value::Null], &context).unwrap();
        assert_eq!(result, Value::Null);
    }
}
