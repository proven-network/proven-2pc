//! NOT logical operator implementation

use super::helpers::{unwrap_nullable, wrap_nullable};
use super::traits::UnaryOperator;
use crate::error::{Error, Result};
use crate::types::{DataType, Value};

pub struct NotOperator;

impl UnaryOperator for NotOperator {
    fn name(&self) -> &'static str {
        "logical NOT"
    }

    fn symbol(&self) -> &'static str {
        "NOT"
    }

    fn validate(&self, operand: &DataType) -> Result<DataType> {
        use DataType::*;

        let (inner, nullable) = unwrap_nullable(operand);

        // Operand must be boolean
        match inner {
            Bool => Ok(wrap_nullable(Bool, nullable)),
            _ => Err(Error::TypeMismatch {
                expected: "BOOLEAN".into(),
                found: format!("{:?}", operand),
            }),
        }
    }

    fn execute(&self, operand: &Value) -> Result<Value> {
        use Value::*;

        // Three-valued logic for NOT:
        // NOT TRUE = FALSE
        // NOT FALSE = TRUE
        // NOT NULL = NULL

        match operand {
            Bool(true) => Ok(Bool(false)),
            Bool(false) => Ok(Bool(true)),
            Null => Ok(Null),
            _ => Err(Error::TypeMismatch {
                expected: "BOOLEAN".into(),
                found: format!("{:?}", operand),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_not() {
        let op = NotOperator;

        // Type validation
        assert_eq!(op.validate(&DataType::Bool).unwrap(), DataType::Bool);

        // Execution
        assert_eq!(op.execute(&Value::Bool(true)).unwrap(), Value::Bool(false));
        assert_eq!(op.execute(&Value::Bool(false)).unwrap(), Value::Bool(true));

        // Three-valued logic
        assert_eq!(op.execute(&Value::Null).unwrap(), Value::Null);
    }
}
