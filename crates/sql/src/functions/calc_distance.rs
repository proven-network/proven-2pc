//! CALC_DISTANCE function - calculates Euclidean distance between two Points

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::types::context::ExecutionContext;
use crate::types::data_type::DataType;
use crate::types::{Value, ValueExt};

pub struct CalcDistanceFunction;

impl Function for CalcDistanceFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "CALC_DISTANCE",
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 {
            return Err(Error::ExecutionError(format!(
                "CALC_DISTANCE takes exactly 2 arguments, got {}",
                arg_types.len()
            )));
        }

        let mut has_null = false;

        // Both arguments must be Point or null
        for arg_type in arg_types {
            match arg_type {
                DataType::Point => {}
                DataType::Null => {
                    has_null = true;
                }
                DataType::Nullable(inner) => {
                    has_null = true;
                    match inner.as_ref() {
                        DataType::Null | DataType::Point => {}
                        _ => {
                            return Err(Error::TypeMismatch {
                                expected: "Point".into(),
                                found: arg_type.to_string(),
                            });
                        }
                    }
                }
                _ => {
                    return Err(Error::TypeMismatch {
                        expected: "Point".into(),
                        found: arg_type.to_string(),
                    });
                }
            }
        }

        // CALC_DISTANCE returns nullable F64 if any argument is/can be null, otherwise F64
        if has_null {
            Ok(DataType::Nullable(Box::new(DataType::F64)))
        } else {
            Ok(DataType::F64)
        }
    }

    fn execute(&self, args: &[Value], _context: &ExecutionContext) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::ExecutionError(
                "CALC_DISTANCE takes exactly 2 arguments".into(),
            ));
        }

        // Handle NULLs
        if args.iter().any(|v| matches!(v, Value::Null)) {
            return Ok(Value::Null);
        }

        let point1 = match &args[0] {
            Value::Point(p) => p,
            _ => {
                return Err(Error::TypeMismatch {
                    expected: "Point".into(),
                    found: args[0].data_type().to_string(),
                });
            }
        };

        let point2 = match &args[1] {
            Value::Point(p) => p,
            _ => {
                return Err(Error::TypeMismatch {
                    expected: "Point".into(),
                    found: args[1].data_type().to_string(),
                });
            }
        };

        // Calculate Euclidean distance: sqrt((x2 - x1)^2 + (y2 - y1)^2)
        let dx = point2.x - point1.x;
        let dy = point2.y - point1.y;
        let distance = (dx * dx + dy * dy).sqrt();

        Ok(Value::F64(distance))
    }
}

/// Register the CALC_DISTANCE function
pub fn register(registry: &mut FunctionRegistry) {
    registry.register(Box::new(CalcDistanceFunction));
}

#[cfg(test)]
mod tests {
    use super::*;
    use proven_common::TransactionId;
    use proven_value::Point;
    use uuid::Uuid;

    #[test]
    fn test_calc_distance_execute() {
        let func = CalcDistanceFunction;
        let context = ExecutionContext::new(TransactionId::from_uuid(Uuid::from_u128(0)), 0);

        // Distance between (0, 0) and (3, 4) = 5.0
        let p1 = Point { x: 0.0, y: 0.0 };
        let p2 = Point { x: 3.0, y: 4.0 };
        assert_eq!(
            func.execute(&[Value::Point(p1), Value::Point(p2)], &context)
                .unwrap(),
            Value::F64(5.0)
        );

        // Distance between (1, 1) and (4, 5) = 5.0
        let p1 = Point { x: 1.0, y: 1.0 };
        let p2 = Point { x: 4.0, y: 5.0 };
        assert_eq!(
            func.execute(&[Value::Point(p1), Value::Point(p2)], &context)
                .unwrap(),
            Value::F64(5.0)
        );

        // Distance from point to itself = 0.0
        let p1 = Point { x: 5.5, y: 7.3 };
        let p2 = Point { x: 5.5, y: 7.3 };
        assert_eq!(
            func.execute(&[Value::Point(p1), Value::Point(p2)], &context)
                .unwrap(),
            Value::F64(0.0)
        );

        // Distance with negative coordinates
        let p1 = Point { x: -3.0, y: -4.0 };
        let p2 = Point { x: 0.0, y: 0.0 };
        assert_eq!(
            func.execute(&[Value::Point(p1), Value::Point(p2)], &context)
                .unwrap(),
            Value::F64(5.0)
        );

        // Horizontal line (same y)
        let p1 = Point { x: 0.0, y: 5.0 };
        let p2 = Point { x: 8.0, y: 5.0 };
        assert_eq!(
            func.execute(&[Value::Point(p1), Value::Point(p2)], &context)
                .unwrap(),
            Value::F64(8.0)
        );

        // Vertical line (same x)
        let p1 = Point { x: 3.0, y: 0.0 };
        let p2 = Point { x: 3.0, y: 6.0 };
        assert_eq!(
            func.execute(&[Value::Point(p1), Value::Point(p2)], &context)
                .unwrap(),
            Value::F64(6.0)
        );

        // NULL handling
        let p3 = Point { x: 1.0, y: 1.0 };
        let p4 = Point { x: 2.0, y: 2.0 };
        assert_eq!(
            func.execute(&[Value::Null, Value::Point(p3)], &context)
                .unwrap(),
            Value::Null
        );
        assert_eq!(
            func.execute(&[Value::Point(p4), Value::Null], &context)
                .unwrap(),
            Value::Null
        );
        assert_eq!(
            func.execute(&[Value::Null, Value::Null], &context).unwrap(),
            Value::Null
        );
    }

    #[test]
    fn test_calc_distance_validate() {
        let func = CalcDistanceFunction;

        // Valid Point types
        assert_eq!(
            func.validate(&[DataType::Point, DataType::Point]).unwrap(),
            DataType::F64
        );

        // Nullable Point
        assert_eq!(
            func.validate(&[
                DataType::Point,
                DataType::Nullable(Box::new(DataType::Point))
            ])
            .unwrap(),
            DataType::Nullable(Box::new(DataType::F64))
        );

        // Both nullable
        assert_eq!(
            func.validate(&[
                DataType::Nullable(Box::new(DataType::Point)),
                DataType::Nullable(Box::new(DataType::Point))
            ])
            .unwrap(),
            DataType::Nullable(Box::new(DataType::F64))
        );

        // NULL
        assert_eq!(
            func.validate(&[DataType::Null, DataType::Point]).unwrap(),
            DataType::Nullable(Box::new(DataType::F64))
        );

        // Invalid types should error
        assert!(func.validate(&[DataType::I32, DataType::Point]).is_err());
        assert!(func.validate(&[DataType::Point, DataType::F64]).is_err());
        assert!(func.validate(&[DataType::Str, DataType::Str]).is_err());

        // Wrong number of arguments
        assert!(func.validate(&[]).is_err());
        assert!(func.validate(&[DataType::Point]).is_err());
        assert!(
            func.validate(&[DataType::Point, DataType::Point, DataType::Point])
                .is_err()
        );
    }
}
