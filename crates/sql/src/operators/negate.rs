//! Negate (unary minus) operator implementation

use super::helpers::{unwrap_nullable, wrap_nullable};
use super::traits::UnaryOperator;
use crate::error::{Error, Result};
use crate::types::{DataType, Value};

pub struct NegateOperator;

impl UnaryOperator for NegateOperator {
    fn name(&self) -> &'static str {
        "negation"
    }

    fn symbol(&self) -> &'static str {
        "-"
    }

    fn validate(&self, operand: &DataType) -> Result<DataType> {
        use DataType::*;

        let (inner, nullable) = unwrap_nullable(operand);

        let result = match inner {
            // Signed integers - stay the same type
            I8 | I16 | I32 | I64 | I128 => inner.clone(),

            // Unsigned integers - convert to signed of next larger size
            U8 => I16,
            U16 => I32,
            U32 => I64,
            U64 => I128,
            U128 => {
                return Err(Error::InvalidValue(
                    "Cannot negate U128 (would overflow)".into(),
                ));
            }

            // Floats and decimal - stay the same
            F32 => F32,
            F64 => F64,
            Decimal(p, s) => Decimal(*p, *s),

            // Interval can be negated
            Interval => Interval,

            // Unknown type (NULL) can be negated (result is NULL)
            Unknown => Unknown,

            _ => {
                return Err(Error::InvalidValue(format!(
                    "Cannot negate non-numeric type: {}",
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

            // Signed integers with overflow checking
            I8(n) => n
                .checked_neg()
                .map(I8)
                .ok_or_else(|| Error::InvalidValue("I8 negation overflow".into())),
            I16(n) => n
                .checked_neg()
                .map(I16)
                .ok_or_else(|| Error::InvalidValue("I16 negation overflow".into())),
            I32(n) => n
                .checked_neg()
                .map(I32)
                .ok_or_else(|| Error::InvalidValue("I32 negation overflow".into())),
            I64(n) => n
                .checked_neg()
                .map(I64)
                .ok_or_else(|| Error::InvalidValue("I64 negation overflow".into())),
            I128(n) => n
                .checked_neg()
                .map(I128)
                .ok_or_else(|| Error::InvalidValue("I128 negation overflow".into())),

            // Unsigned integers - convert to signed
            U8(n) => Ok(I16(-(*n as i16))),
            U16(n) => Ok(I32(-(*n as i32))),
            U32(n) => Ok(I64(-(*n as i64))),
            U64(n) => Ok(I128(-(*n as i128))),
            U128(n) => {
                if *n > i128::MAX as u128 {
                    Err(Error::InvalidValue("U128 negation overflow".into()))
                } else {
                    Ok(I128(-(*n as i128)))
                }
            }

            // Floats
            F32(n) => Ok(F32(-n)),
            F64(n) => Ok(F64(-n)),

            // Decimal
            Decimal(d) => Ok(Decimal(-d)),

            // Interval
            Interval(i) => Ok(Interval(crate::types::data_type::Interval {
                months: -i.months,
                days: -i.days,
                microseconds: -i.microseconds,
            })),

            _ => Err(Error::InvalidValue(format!(
                "Cannot negate non-numeric value: {:?}",
                operand
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_negate_numeric() {
        let op = NegateOperator;

        // Type validation
        assert_eq!(op.validate(&DataType::I32).unwrap(), DataType::I32);
        assert_eq!(op.validate(&DataType::U8).unwrap(), DataType::I16);

        // Execution
        assert_eq!(op.execute(&Value::I32(5)).unwrap(), Value::I32(-5));
        assert_eq!(op.execute(&Value::I32(-3)).unwrap(), Value::I32(3));
        assert_eq!(op.execute(&Value::U8(5)).unwrap(), Value::I16(-5));

        // Overflow
        assert!(op.execute(&Value::I8(-128)).is_err());
    }
}
