//! XOR logical operator implementation

use super::helpers::*;
use super::traits::BinaryOperator;
use crate::error::{Error, Result};
use crate::types::{DataType, Value};

pub struct XorOperator;

impl BinaryOperator for XorOperator {
    fn name(&self) -> &'static str {
        "logical XOR"
    }

    fn symbol(&self) -> &'static str {
        "XOR"
    }

    fn is_commutative(&self, _left: &DataType, _right: &DataType) -> bool {
        true // XOR is commutative
    }

    fn validate(&self, left: &DataType, right: &DataType) -> Result<DataType> {
        use DataType::*;

        let (left_inner, right_inner, nullable) = unwrap_nullable_pair(left, right);

        // Both operands must be boolean
        match (left_inner, right_inner) {
            (Bool, Bool) => Ok(wrap_nullable(Bool, nullable)),
            _ => Err(Error::TypeMismatch {
                expected: "BOOLEAN".into(),
                found: format!("{:?} XOR {:?}", left, right),
            }),
        }
    }

    fn execute(&self, left: &Value, right: &Value) -> Result<Value> {
        use Value::*;

        // Three-valued logic for XOR:
        // TRUE XOR TRUE = FALSE
        // TRUE XOR FALSE = TRUE
        // FALSE XOR TRUE = TRUE
        // FALSE XOR FALSE = FALSE
        // TRUE XOR NULL = NULL
        // FALSE XOR NULL = NULL
        // NULL XOR TRUE = NULL
        // NULL XOR FALSE = NULL
        // NULL XOR NULL = NULL

        match (left, right) {
            // Both are boolean values - do XOR
            (Bool(a), Bool(b)) => Ok(Bool(a ^ b)),
            // Any NULL results in NULL
            (Null, _) | (_, Null) => Ok(Null),
            _ => Err(Error::TypeMismatch {
                expected: "BOOLEAN".into(),
                found: format!("{:?} XOR {:?}", left, right),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_xor() {
        let op = XorOperator;

        // Type validation
        assert_eq!(
            op.validate(&DataType::Bool, &DataType::Bool).unwrap(),
            DataType::Bool
        );

        // Execution - truth table
        assert_eq!(
            op.execute(&Value::Bool(true), &Value::Bool(true)).unwrap(),
            Value::Bool(false)
        );
        assert_eq!(
            op.execute(&Value::Bool(true), &Value::Bool(false)).unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            op.execute(&Value::Bool(false), &Value::Bool(true)).unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            op.execute(&Value::Bool(false), &Value::Bool(false))
                .unwrap(),
            Value::Bool(false)
        );

        // Three-valued logic
        assert_eq!(
            op.execute(&Value::Bool(true), &Value::Null).unwrap(),
            Value::Null
        );
        assert_eq!(
            op.execute(&Value::Bool(false), &Value::Null).unwrap(),
            Value::Null
        );
        assert_eq!(op.execute(&Value::Null, &Value::Null).unwrap(), Value::Null);
    }
}
