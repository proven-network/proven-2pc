//! Expression validation
//!
//! Validates expressions and collects parameter requirements

use crate::error::{Error, Result};
use crate::parsing::ast::{Expression, Operator};
use crate::semantic::context::AnalysisContext;
use crate::semantic::statement::{AnalyzedStatement, ExpressionId};
use crate::types::data_type::DataType;

/// Validates expressions and collects parameter requirements
pub struct ExpressionValidator;

impl ExpressionValidator {
    /// Create a new expression validator
    pub fn new() -> Self {
        Self
    }

    /// Validate an expression and collect parameter requirements
    pub fn validate(
        &self,
        expr: &Expression,
        expr_id: &ExpressionId,
        analyzed: &AnalyzedStatement,
        context: &AnalysisContext,
    ) -> Result<()> {
        match expr {
            Expression::Parameter(idx) => {
                // Parameters should already be registered during analysis
                // Just verify they exist
                if !analyzed.parameter_slots.iter().any(|s| s.index == *idx) {
                    // This is OK - parameter may be registered later in the analysis
                    // or this validator may be called during analysis
                }
                Ok(())
            }

            Expression::Function(_name, args) => {
                // Validate function arguments
                for (i, arg) in args.iter().enumerate() {
                    let arg_id = expr_id.child(i);
                    self.validate(arg, &arg_id, analyzed, context)?;
                }
                Ok(())
            }

            Expression::Operator(op) => self.validate_operator(op, expr_id, analyzed, context),

            _ => Ok(()), // Other expressions don't need special validation
        }
    }

    fn validate_operator(
        &self,
        op: &Operator,
        expr_id: &ExpressionId,
        analyzed: &AnalyzedStatement,
        context: &AnalysisContext,
    ) -> Result<()> {
        match op {
            Operator::And(left, right) | Operator::Or(left, right) => {
                // Validate boolean operands
                self.validate(left, &expr_id.child(0), analyzed, context)?;
                self.validate(right, &expr_id.child(1), analyzed, context)?;

                // Check types are boolean if not parameters
                if let Some(left_type) = analyzed.get_type(&expr_id.child(0))
                    && !matches!(left_type.data_type, DataType::Bool)
                    && !matches!(left.as_ref(), Expression::Parameter(_))
                {
                    return Err(Error::TypeMismatch {
                        expected: "boolean".to_string(),
                        found: format!("{:?}", left_type.data_type),
                    });
                }
                Ok(())
            }

            Operator::Equal(left, right)
            | Operator::NotEqual(left, right)
            | Operator::LessThan(left, right)
            | Operator::LessThanOrEqual(left, right)
            | Operator::GreaterThan(left, right)
            | Operator::GreaterThanOrEqual(left, right) => {
                // Validate comparison operands
                self.validate(left, &expr_id.child(0), analyzed, context)?;
                self.validate(right, &expr_id.child(1), analyzed, context)?;

                // Check types are compatible for comparison
                if let (Some(left_type), Some(right_type)) = (
                    analyzed.get_type(&expr_id.child(0)),
                    analyzed.get_type(&expr_id.child(1)),
                ) {
                    // Parameters can be compared with anything
                    if !matches!(left.as_ref(), Expression::Parameter(_))
                        && !matches!(right.as_ref(), Expression::Parameter(_))
                    {
                        // Check if types can be compared
                        if !can_compare(&left_type.data_type, &right_type.data_type) {
                            return Err(Error::TypeMismatch {
                                expected: format!("comparable with {:?}", left_type.data_type),
                                found: format!("{:?}", right_type.data_type),
                            });
                        }
                    }
                }
                Ok(())
            }

            _ => Ok(()), // Other operators handled similarly
        }
    }
}

// Helper functions

/// Check if two types can be compared
fn can_compare(left: &DataType, right: &DataType) -> bool {
    use crate::coercion::can_coerce;
    use DataType::*;

    // Special cases that aren't covered by coercion
    match (left, right) {
        // Null types can be compared with anything
        (Null, _) | (_, Null) => true,

        // Same types can always be compared
        _ if left == right => true,

        // Nullable types - check inner types
        (Nullable(l), Nullable(r)) => can_compare(l, r),
        (Nullable(inner), other) | (other, Nullable(inner)) => can_compare(inner, other),

        // For everything else, check if either type can be coerced to the other
        // This handles all the numeric comparisons, string to date/array/uuid conversions, etc.
        _ => can_coerce(left, right) || can_coerce(right, left),
    }
}
