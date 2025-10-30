//! GET_Y function - extracts Y coordinate from a Point

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::types::context::ExecutionContext;
use crate::types::data_type::DataType;
use crate::types::{Value, ValueExt};

pub struct GetYFunction;

impl Function for GetYFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "GET_Y",
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return Err(Error::ExecutionError(format!(
                "GET_Y takes exactly 1 argument, got {}",
                arg_types.len()
            )));
        }

        match &arg_types[0] {
            DataType::Point => {
                // GET_Y returns F64
                Ok(DataType::F64)
            }
            DataType::Null => {
                // NULL literal returns nullable F64
                Ok(DataType::Nullable(Box::new(DataType::F64)))
            }
            DataType::Nullable(inner) => {
                // Check inner type is Point or null
                match inner.as_ref() {
                    DataType::Null => Ok(DataType::Nullable(Box::new(DataType::F64))),
                    DataType::Point => Ok(DataType::Nullable(Box::new(DataType::F64))),
                    _ => Err(Error::TypeMismatch {
                        expected: "Point".into(),
                        found: arg_types[0].to_string(),
                    }),
                }
            }
            _ => Err(Error::TypeMismatch {
                expected: "Point".into(),
                found: arg_types[0].to_string(),
            }),
        }
    }

    fn execute(&self, args: &[Value], _context: &ExecutionContext) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::ExecutionError(
                "GET_Y takes exactly 1 argument".into(),
            ));
        }

        match &args[0] {
            Value::Point(point) => Ok(Value::F64(point.y)),
            Value::Null => Ok(Value::Null),
            _ => Err(Error::TypeMismatch {
                expected: "Point".into(),
                found: args[0].data_type().to_string(),
            }),
        }
    }
}

/// Register the GET_Y function
pub fn register(registry: &mut FunctionRegistry) {
    registry.register(Box::new(GetYFunction));
}

#[cfg(test)]
mod tests {
    use super::*;
    use proven_common::TransactionId;
    use proven_value::Point;
    use uuid::Uuid;

    #[test]
    fn test_get_y_execute() {
        let func = GetYFunction;
        let context = ExecutionContext::new(TransactionId::from_uuid(Uuid::from_u128(0)), 0);

        // Extract Y coordinate
        let point = Point { x: 10.5, y: 20.3 };
        assert_eq!(
            func.execute(&[Value::Point(point)], &context).unwrap(),
            Value::F64(20.3)
        );

        // Negative Y coordinate
        let point = Point {
            x: -122.4194,
            y: 37.7749,
        };
        assert_eq!(
            func.execute(&[Value::Point(point)], &context).unwrap(),
            Value::F64(37.7749)
        );

        // Zero Y coordinate
        let point = Point { x: 5.0, y: 0.0 };
        assert_eq!(
            func.execute(&[Value::Point(point)], &context).unwrap(),
            Value::F64(0.0)
        );

        // NULL handling
        assert_eq!(func.execute(&[Value::Null], &context).unwrap(), Value::Null);
    }

    #[test]
    fn test_get_y_validate() {
        let func = GetYFunction;

        // Valid Point type
        assert_eq!(func.validate(&[DataType::Point]).unwrap(), DataType::F64);

        // Nullable Point
        assert_eq!(
            func.validate(&[DataType::Nullable(Box::new(DataType::Point))])
                .unwrap(),
            DataType::Nullable(Box::new(DataType::F64))
        );

        // NULL
        assert_eq!(
            func.validate(&[DataType::Null]).unwrap(),
            DataType::Nullable(Box::new(DataType::F64))
        );

        // Invalid types should error
        assert!(func.validate(&[DataType::I32]).is_err());
        assert!(func.validate(&[DataType::Str]).is_err());
        assert!(func.validate(&[DataType::F64]).is_err());

        // Wrong number of arguments
        assert!(func.validate(&[]).is_err());
        assert!(func.validate(&[DataType::Point, DataType::Point]).is_err());
    }
}
