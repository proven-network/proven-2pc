//! OR logical operator implementation

use super::helpers::*;
use super::traits::BinaryOperator;
use crate::error::{Error, Result};
use crate::types::{DataType, Value};

pub struct OrOperator;

impl BinaryOperator for OrOperator {
    fn name(&self) -> &'static str {
        "logical OR"
    }

    fn symbol(&self) -> &'static str {
        "OR"
    }

    fn is_commutative(&self, _left: &DataType, _right: &DataType) -> bool {
        true // OR is commutative
    }

    fn validate(&self, left: &DataType, right: &DataType) -> Result<DataType> {
        use DataType::*;

        let (left_inner, right_inner, nullable) = unwrap_nullable_pair(left, right);

        // Both operands must be boolean
        match (left_inner, right_inner) {
            (Bool, Bool) => Ok(wrap_nullable(Bool, nullable)),
            _ => Err(Error::TypeMismatch {
                expected: "BOOLEAN".into(),
                found: format!("{:?} OR {:?}", left, right),
            }),
        }
    }

    fn execute(&self, left: &Value, right: &Value) -> Result<Value> {
        use Value::*;

        // Three-valued logic for OR:
        // TRUE OR TRUE = TRUE
        // TRUE OR FALSE = TRUE
        // FALSE OR TRUE = TRUE
        // FALSE OR FALSE = FALSE
        // TRUE OR NULL = TRUE
        // FALSE OR NULL = NULL
        // NULL OR TRUE = TRUE
        // NULL OR FALSE = NULL
        // NULL OR NULL = NULL

        match (left, right) {
            // If either is true, result is true
            (Bool(true), _) | (_, Bool(true)) => Ok(Bool(true)),
            // Both false
            (Bool(false), Bool(false)) => Ok(Bool(false)),
            // One false, one null
            (Bool(false), Null) | (Null, Bool(false)) => Ok(Null),
            // Both null
            (Null, Null) => Ok(Null),
            _ => Err(Error::TypeMismatch {
                expected: "BOOLEAN".into(),
                found: format!("{:?} OR {:?}", left, right),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_or() {
        let op = OrOperator;

        // Type validation
        assert_eq!(
            op.validate(&DataType::Bool, &DataType::Bool).unwrap(),
            DataType::Bool
        );

        // Execution - truth table
        assert_eq!(
            op.execute(&Value::Bool(true), &Value::Bool(true)).unwrap(),
            Value::Bool(true)
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
            Value::Bool(true)
        );
        assert_eq!(
            op.execute(&Value::Bool(false), &Value::Null).unwrap(),
            Value::Null
        );
        assert_eq!(op.execute(&Value::Null, &Value::Null).unwrap(), Value::Null);
    }
}
