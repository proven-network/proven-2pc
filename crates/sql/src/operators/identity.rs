//! Identity (unary plus) operator implementation

use super::helpers::{unwrap_nullable, wrap_nullable};
use super::traits::UnaryOperator;
use crate::error::{Error, Result};
use crate::types::{DataType, Value};

pub struct IdentityOperator;

impl UnaryOperator for IdentityOperator {
    fn name(&self) -> &'static str {
        "identity"
    }

    fn symbol(&self) -> &'static str {
        "+"
    }

    fn validate(&self, operand: &DataType) -> Result<DataType> {
        use DataType::*;

        let (inner, nullable) = unwrap_nullable(operand);

        // Identity operator just returns the same type
        match inner {
            // All numeric types are valid
            I8
            | I16
            | I32
            | I64
            | I128
            | U8
            | U16
            | U32
            | U64
            | U128
            | F32
            | F64
            | Decimal(_, _) => Ok(wrap_nullable(inner.clone(), nullable)),

            // Interval can have identity applied
            Interval => Ok(wrap_nullable(inner.clone(), nullable)),

            // Unknown type (NULL) can have identity applied (result is NULL)
            Unknown => Ok(wrap_nullable(Unknown, nullable)),

            _ => Err(Error::InvalidValue(format!(
                "Cannot apply unary plus to non-numeric type: {}",
                operand
            ))),
        }
    }

    fn execute(&self, operand: &Value) -> Result<Value> {
        use Value::*;

        match operand {
            // NULL handling
            Null => Ok(Null),

            // For all numeric types, identity just returns the value unchanged
            I8(_) | I16(_) | I32(_) | I64(_) | I128(_) | U8(_) | U16(_) | U32(_) | U64(_)
            | U128(_) | F32(_) | F64(_) | Decimal(_) | Interval(_) => Ok(operand.clone()),

            _ => Err(Error::InvalidValue(format!(
                "Cannot apply unary plus to non-numeric value: {:?}",
                operand
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_identity_numeric() {
        let op = IdentityOperator;

        // Type validation
        assert_eq!(op.validate(&DataType::I32).unwrap(), DataType::I32);
        assert_eq!(op.validate(&DataType::F64).unwrap(), DataType::F64);

        // Execution - values remain unchanged
        assert_eq!(op.execute(&Value::I32(5)).unwrap(), Value::I32(5));
        assert_eq!(op.execute(&Value::I32(-3)).unwrap(), Value::I32(-3));
        assert_eq!(
            op.execute(&Value::F64(std::f64::consts::PI)).unwrap(),
            Value::F64(std::f64::consts::PI)
        );
    }
}
