//! Bitwise NOT operator implementation

use super::helpers::{unwrap_nullable, wrap_nullable};
use super::traits::UnaryOperator;
use crate::error::{Error, Result};
use crate::types::{DataType, Value};

pub struct BitwiseNotOperator;

impl UnaryOperator for BitwiseNotOperator {
    fn name(&self) -> &'static str {
        "bitwise NOT"
    }

    fn symbol(&self) -> &'static str {
        "~"
    }

    fn validate(&self, operand: &DataType) -> Result<DataType> {
        use DataType::*;

        let (inner, nullable) = unwrap_nullable(operand);

        let result = match inner {
            // All integer types stay the same type
            I8 | I16 | I32 | I64 | I128 | U8 | U16 | U32 | U64 | U128 => inner.clone(),

            // Null type can be bitwise NOT'd (result is NULL)
            Null => Null,

            _ => {
                return Err(Error::InvalidValue(format!(
                    "Cannot apply bitwise NOT to non-integer type: {}",
                    operand
                )));
            }
        };

        Ok(wrap_nullable(result, nullable))
    }

    fn execute(&self, operand: &Value) -> Result<Value> {
        use Value::*;

        match operand {
            // NULL handling
            Null => Ok(Null),

            // Integer types
            I8(n) => Ok(I8(!n)),
            I16(n) => Ok(I16(!n)),
            I32(n) => Ok(I32(!n)),
            I64(n) => Ok(I64(!n)),
            I128(n) => Ok(I128(!n)),
            U8(n) => Ok(U8(!n)),
            U16(n) => Ok(U16(!n)),
            U32(n) => Ok(U32(!n)),
            U64(n) => Ok(U64(!n)),
            U128(n) => Ok(U128(!n)),

            _ => Err(Error::InvalidValue(format!(
                "Cannot apply bitwise NOT to non-integer value: {:?}",
                operand
            ))),
        }
    }
}
