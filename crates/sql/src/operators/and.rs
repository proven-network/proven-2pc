//! AND logical operator implementation

use super::helpers::*;
use super::traits::BinaryOperator;
use crate::error::{Error, Result};
use crate::types::{DataType, Value};

pub struct AndOperator;

impl BinaryOperator for AndOperator {
    fn name(&self) -> &'static str {
        "logical AND"
    }

    fn symbol(&self) -> &'static str {
        "AND"
    }

    fn is_commutative(&self, _left: &DataType, _right: &DataType) -> bool {
        true // AND is commutative
    }

    fn validate(&self, left: &DataType, right: &DataType) -> Result<DataType> {
        use DataType::*;

        let (left_inner, right_inner, nullable) = unwrap_nullable_pair(left, right);

        // Both operands must be boolean
        match (left_inner, right_inner) {
            (Bool, Bool) => Ok(wrap_nullable(Bool, nullable)),
            _ => Err(Error::TypeMismatch {
                expected: "BOOLEAN".into(),
                found: format!("{:?} AND {:?}", left, right),
            }),
        }
    }

    fn execute(&self, left: &Value, right: &Value) -> Result<Value> {
        use Value::*;

        // Three-valued logic for AND:
        // TRUE AND TRUE = TRUE
        // TRUE AND FALSE = FALSE
        // FALSE AND TRUE = FALSE
        // FALSE AND FALSE = FALSE
        // TRUE AND NULL = NULL
        // FALSE AND NULL = FALSE
        // NULL AND TRUE = NULL
        // NULL AND FALSE = FALSE
        // NULL AND NULL = NULL

        match (left, right) {
            // If either is false, result is false
            (Bool(false), _) | (_, Bool(false)) => Ok(Bool(false)),
            // Both true
            (Bool(true), Bool(true)) => Ok(Bool(true)),
            // One true, one null
            (Bool(true), Null) | (Null, Bool(true)) => Ok(Null),
            // Both null
            (Null, Null) => Ok(Null),
            _ => Err(Error::TypeMismatch {
                expected: "BOOLEAN".into(),
                found: format!("{:?} AND {:?}", left, right),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_and() {
        let op = AndOperator;

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
            Value::Bool(false)
        );
        assert_eq!(
            op.execute(&Value::Bool(false), &Value::Bool(true)).unwrap(),
            Value::Bool(false)
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
            Value::Bool(false)
        );
        assert_eq!(op.execute(&Value::Null, &Value::Null).unwrap(), Value::Null);
    }
}
