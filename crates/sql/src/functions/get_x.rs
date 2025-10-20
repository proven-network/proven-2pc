//! GET_X function - extracts X coordinate from a Point

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::types::context::ExecutionContext;
use crate::types::data_type::DataType;
use crate::types::{Value, ValueExt};

pub struct GetXFunction;

impl Function for GetXFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "GET_X",
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return Err(Error::ExecutionError(format!(
                "GET_X takes exactly 1 argument, got {}",
                arg_types.len()
            )));
        }

        match &arg_types[0] {
            DataType::Point => {
                // GET_X returns F64
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
                "GET_X takes exactly 1 argument".into(),
            ));
        }

        match &args[0] {
            Value::Point(point) => Ok(Value::F64(point.x)),
            Value::Null => Ok(Value::Null),
            _ => Err(Error::TypeMismatch {
                expected: "Point".into(),
                found: args[0].data_type().to_string(),
            }),
        }
    }
}

/// Register the GET_X function
pub fn register(registry: &mut FunctionRegistry) {
    registry.register(Box::new(GetXFunction));
}

#[cfg(test)]
mod tests {
    use super::*;
    use proven_hlc::{HlcTimestamp, NodeId};
    use proven_value::Point;

    #[test]
    fn test_get_x_execute() {
        let func = GetXFunction;
        let context = ExecutionContext::new(HlcTimestamp::new(1000, 0, NodeId::new(1)), 0);

        // Extract X coordinate
        let point = Point { x: 10.5, y: 20.3 };
        assert_eq!(
            func.execute(&[Value::Point(point)], &context).unwrap(),
            Value::F64(10.5)
        );

        // Negative X coordinate
        let point = Point {
            x: -71.064544,
            y: 42.28787,
        };
        assert_eq!(
            func.execute(&[Value::Point(point)], &context).unwrap(),
            Value::F64(-71.064544)
        );

        // Zero X coordinate
        let point = Point { x: 0.0, y: 5.0 };
        assert_eq!(
            func.execute(&[Value::Point(point)], &context).unwrap(),
            Value::F64(0.0)
        );

        // NULL handling
        assert_eq!(func.execute(&[Value::Null], &context).unwrap(), Value::Null);
    }

    #[test]
    fn test_get_x_validate() {
        let func = GetXFunction;

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
