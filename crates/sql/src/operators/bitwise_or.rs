//! Bitwise OR operator implementation

use super::helpers::*;
use super::traits::BinaryOperator;
use crate::error::{Error, Result};
use crate::types::{DataType, Value};

pub struct BitwiseOrOperator;

impl BinaryOperator for BitwiseOrOperator {
    fn name(&self) -> &'static str {
        "bitwise OR"
    }

    fn symbol(&self) -> &'static str {
        "|"
    }

    fn is_commutative(&self, _left: &DataType, _right: &DataType) -> bool {
        true // Bitwise OR is commutative
    }

    fn validate(&self, left: &DataType, right: &DataType) -> Result<DataType> {
        use DataType::*;

        let (left_inner, right_inner, nullable) = unwrap_nullable_pair(left, right);

        let result = match (left_inner, right_inner) {
            // Unknown (NULL) with anything returns Unknown
            (Null, _) | (_, Null) => Null,

            // Both operands must be integers
            (I8, I8) => I8,
            (I16, I16) => I16,
            (I32, I32) => I32,
            (I64, I64) => I64,
            (I128, I128) => I128,
            (U8, U8) => U8,
            (U16, U16) => U16,
            (U32, U32) => U32,
            (U64, U64) => U64,
            (U128, U128) => U128,

            // Mixed integer types - promote to wider type
            (I8, I16) | (I16, I8) => I16,
            (I8, I32) | (I32, I8) => I32,
            (I8, I64) | (I64, I8) => I64,
            (I8, I128) | (I128, I8) => I128,
            (I16, I32) | (I32, I16) => I32,
            (I16, I64) | (I64, I16) => I64,
            (I16, I128) | (I128, I16) => I128,
            (I32, I64) | (I64, I32) => I64,
            (I32, I128) | (I128, I32) => I128,
            (I64, I128) | (I128, I64) => I128,

            (U8, U16) | (U16, U8) => U16,
            (U8, U32) | (U32, U8) => U32,
            (U8, U64) | (U64, U8) => U64,
            (U8, U128) | (U128, U8) => U128,
            (U16, U32) | (U32, U16) => U32,
            (U16, U64) | (U64, U16) => U64,
            (U16, U128) | (U128, U16) => U128,
            (U32, U64) | (U64, U32) => U64,
            (U32, U128) | (U128, U32) => U128,
            (U64, U128) | (U128, U64) => U128,

            _ => {
                return Err(Error::InvalidOperation(format!(
                    "Cannot perform {} on {} and {}",
                    self.name(),
                    left,
                    right,
                )));
            }
        };

        Ok(wrap_nullable(result, nullable))
    }

    fn execute(&self, left: &Value, right: &Value) -> Result<Value> {
        use Value::*;

        // NULL handling
        if matches!(left, Null) || matches!(right, Null) {
            return Ok(Null);
        }

        Ok(match (left, right) {
            (I8(a), I8(b)) => I8(a | b),
            (I16(a), I16(b)) => I16(a | b),
            (I32(a), I32(b)) => I32(a | b),
            (I64(a), I64(b)) => I64(a | b),
            (I128(a), I128(b)) => I128(a | b),
            (U8(a), U8(b)) => U8(a | b),
            (U16(a), U16(b)) => U16(a | b),
            (U32(a), U32(b)) => U32(a | b),
            (U64(a), U64(b)) => U64(a | b),
            (U128(a), U128(b)) => U128(a | b),

            // Mixed integer types - convert to wider type
            (I8(a), I16(b)) => I16((*a as i16) | b),
            (I16(a), I8(b)) => I16(a | (*b as i16)),
            (I8(a), I32(b)) => I32((*a as i32) | b),
            (I32(a), I8(b)) => I32(a | (*b as i32)),
            (I8(a), I64(b)) => I64((*a as i64) | b),
            (I64(a), I8(b)) => I64(a | (*b as i64)),
            (I8(a), I128(b)) => I128((*a as i128) | b),
            (I128(a), I8(b)) => I128(a | (*b as i128)),

            (I16(a), I32(b)) => I32((*a as i32) | b),
            (I32(a), I16(b)) => I32(a | (*b as i32)),
            (I16(a), I64(b)) => I64((*a as i64) | b),
            (I64(a), I16(b)) => I64(a | (*b as i64)),
            (I16(a), I128(b)) => I128((*a as i128) | b),
            (I128(a), I16(b)) => I128(a | (*b as i128)),

            (I32(a), I64(b)) => I64((*a as i64) | b),
            (I64(a), I32(b)) => I64(a | (*b as i64)),
            (I32(a), I128(b)) => I128((*a as i128) | b),
            (I128(a), I32(b)) => I128(a | (*b as i128)),

            (I64(a), I128(b)) => I128((*a as i128) | b),
            (I128(a), I64(b)) => I128(a | (*b as i128)),

            (U8(a), U16(b)) => U16((*a as u16) | b),
            (U16(a), U8(b)) => U16(a | (*b as u16)),
            (U8(a), U32(b)) => U32((*a as u32) | b),
            (U32(a), U8(b)) => U32(a | (*b as u32)),
            (U8(a), U64(b)) => U64((*a as u64) | b),
            (U64(a), U8(b)) => U64(a | (*b as u64)),
            (U8(a), U128(b)) => U128((*a as u128) | b),
            (U128(a), U8(b)) => U128(a | (*b as u128)),

            (U16(a), U32(b)) => U32((*a as u32) | b),
            (U32(a), U16(b)) => U32(a | (*b as u32)),
            (U16(a), U64(b)) => U64((*a as u64) | b),
            (U64(a), U16(b)) => U64(a | (*b as u64)),
            (U16(a), U128(b)) => U128((*a as u128) | b),
            (U128(a), U16(b)) => U128(a | (*b as u128)),

            (U32(a), U64(b)) => U64((*a as u64) | b),
            (U64(a), U32(b)) => U64(a | (*b as u64)),
            (U32(a), U128(b)) => U128((*a as u128) | b),
            (U128(a), U32(b)) => U128(a | (*b as u128)),

            (U64(a), U128(b)) => U128((*a as u128) | b),
            (U128(a), U64(b)) => U128(a | (*b as u128)),

            _ => {
                return Err(Error::InvalidOperation(format!(
                    "Cannot perform {} on {} and {}",
                    self.name(),
                    left,
                    right,
                )));
            }
        })
    }
}
