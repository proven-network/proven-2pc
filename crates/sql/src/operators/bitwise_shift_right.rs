//! Bitwise right shift operator implementation

use super::helpers::*;
use super::traits::BinaryOperator;
use crate::error::{Error, Result};
use crate::types::{DataType, Value};

pub struct BitwiseShiftRightOperator;

impl BinaryOperator for BitwiseShiftRightOperator {
    fn name(&self) -> &'static str {
        "bitwise right shift"
    }

    fn symbol(&self) -> &'static str {
        ">>"
    }

    fn is_commutative(&self, _left: &DataType, _right: &DataType) -> bool {
        false // Shift operations are NOT commutative
    }

    fn validate(&self, left: &DataType, right: &DataType) -> Result<DataType> {
        use DataType::*;

        let (left_inner, right_inner, nullable) = unwrap_nullable_pair(left, right);

        let result = match (left_inner, right_inner) {
            // Unknown (NULL) with anything returns Unknown
            (Null, _) | (_, Null) => Null,

            // Left operand must be an integer, right operand must be an integer (shift amount)
            (I8, _) if right_inner.is_integer() => I8,
            (I16, _) if right_inner.is_integer() => I16,
            (I32, _) if right_inner.is_integer() => I32,
            (I64, _) if right_inner.is_integer() => I64,
            (I128, _) if right_inner.is_integer() => I128,
            (U8, _) if right_inner.is_integer() => U8,
            (U16, _) if right_inner.is_integer() => U16,
            (U32, _) if right_inner.is_integer() => U32,
            (U64, _) if right_inner.is_integer() => U64,
            (U128, _) if right_inner.is_integer() => U128,

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

        // Convert right operand to u32 (shift amount)
        let shift_amount = match right {
            I8(n) if *n >= 0 => *n as u32,
            I16(n) if *n >= 0 => *n as u32,
            I32(n) if *n >= 0 => *n as u32,
            I64(n) if *n >= 0 && *n <= u32::MAX as i64 => *n as u32,
            I128(n) if *n >= 0 && *n <= u32::MAX as i128 => *n as u32,
            U8(n) => *n as u32,
            U16(n) => *n as u32,
            U32(n) => *n,
            U64(n) if *n <= u32::MAX as u64 => *n as u32,
            U128(n) if *n <= u32::MAX as u128 => *n as u32,
            _ => {
                return Err(Error::InvalidValue(
                    "Shift amount must be a non-negative integer that fits in u32".into(),
                ));
            }
        };

        // Perform the shift operation with overflow check
        // For signed integers, use arithmetic right shift (sign-extending)
        // For unsigned integers, use logical right shift (zero-filling)
        Ok(match left {
            I8(n) => {
                if shift_amount >= 8 {
                    return Err(Error::InvalidValue(format!(
                        "Shift amount {} exceeds bit width of I8 (8 bits)",
                        shift_amount
                    )));
                }
                I8(n.wrapping_shr(shift_amount))
            }
            I16(n) => {
                if shift_amount >= 16 {
                    return Err(Error::InvalidValue(format!(
                        "Shift amount {} exceeds bit width of I16 (16 bits)",
                        shift_amount
                    )));
                }
                I16(n.wrapping_shr(shift_amount))
            }
            I32(n) => {
                if shift_amount >= 32 {
                    return Err(Error::InvalidValue(format!(
                        "Shift amount {} exceeds bit width of I32 (32 bits)",
                        shift_amount
                    )));
                }
                I32(n.wrapping_shr(shift_amount))
            }
            I64(n) => {
                if shift_amount >= 64 {
                    return Err(Error::InvalidValue(format!(
                        "Shift amount {} exceeds bit width of I64 (64 bits)",
                        shift_amount
                    )));
                }
                I64(n.wrapping_shr(shift_amount))
            }
            I128(n) => {
                if shift_amount >= 128 {
                    return Err(Error::InvalidValue(format!(
                        "Shift amount {} exceeds bit width of I128 (128 bits)",
                        shift_amount
                    )));
                }
                I128(n.wrapping_shr(shift_amount))
            }
            U8(n) => {
                if shift_amount >= 8 {
                    return Err(Error::InvalidValue(format!(
                        "Shift amount {} exceeds bit width of U8 (8 bits)",
                        shift_amount
                    )));
                }
                U8(n.wrapping_shr(shift_amount))
            }
            U16(n) => {
                if shift_amount >= 16 {
                    return Err(Error::InvalidValue(format!(
                        "Shift amount {} exceeds bit width of U16 (16 bits)",
                        shift_amount
                    )));
                }
                U16(n.wrapping_shr(shift_amount))
            }
            U32(n) => {
                if shift_amount >= 32 {
                    return Err(Error::InvalidValue(format!(
                        "Shift amount {} exceeds bit width of U32 (32 bits)",
                        shift_amount
                    )));
                }
                U32(n.wrapping_shr(shift_amount))
            }
            U64(n) => {
                if shift_amount >= 64 {
                    return Err(Error::InvalidValue(format!(
                        "Shift amount {} exceeds bit width of U64 (64 bits)",
                        shift_amount
                    )));
                }
                U64(n.wrapping_shr(shift_amount))
            }
            U128(n) => {
                if shift_amount >= 128 {
                    return Err(Error::InvalidValue(format!(
                        "Shift amount {} exceeds bit width of U128 (128 bits)",
                        shift_amount
                    )));
                }
                U128(n.wrapping_shr(shift_amount))
            }
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
