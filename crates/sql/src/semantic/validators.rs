//! Validators for zero-copy semantic analysis
//!
//! These validators work with AnalyzedStatement and Arc<Statement>,
//! collecting requirements into ParameterSlots rather than validating values.

use crate::error::{Error, Result};
use crate::parsing::ast::{DdlStatement, DmlStatement, Expression, Operator, Statement};
use crate::semantic::context::AnalysisContext;
use crate::semantic::statement::{AnalyzedStatement, ExpressionId, SqlContext};
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

/// Validates statement-level requirements
pub struct StatementValidator;

impl StatementValidator {
    /// Create a new statement validator
    pub fn new() -> Self {
        Self
    }

    /// Validate a statement (read-only validation)
    pub fn validate(&self, analyzed: &AnalyzedStatement, _context: &AnalysisContext) -> Result<()> {
        match analyzed.ast.as_ref() {
            Statement::Dml(dml) => self.validate_dml(dml, analyzed),
            Statement::Ddl(ddl) => self.validate_ddl(ddl),
            _ => Ok(()),
        }
    }

    fn validate_dml(&self, stmt: &DmlStatement, analyzed: &AnalyzedStatement) -> Result<()> {
        match stmt {
            DmlStatement::Select(select) => {
                // Validate GROUP BY / aggregate rules
                if !select.group_by.is_empty() || analyzed.metadata.has_aggregates {
                    // Runtime validation will be needed for parameters in SELECT list
                    // This is just a structural check
                }

                // HAVING requires GROUP BY or aggregates
                if select.having.is_some()
                    && select.group_by.is_empty()
                    && !analyzed.metadata.has_aggregates
                {
                    return Err(Error::ExecutionError(
                        "HAVING requires GROUP BY or aggregate functions".to_string(),
                    ));
                }
                Ok(())
            }

            DmlStatement::Insert { .. } => {
                // Constraint validation happens in ConstraintValidator
                Ok(())
            }

            DmlStatement::Update { .. } | DmlStatement::Delete { .. } => {
                // No special validation needed
                Ok(())
            }
        }
    }

    fn validate_ddl(&self, _stmt: &DdlStatement) -> Result<()> {
        // DDL statements don't typically have parameters
        Ok(())
    }
}

/// Validates constraints and updates parameter requirements
pub struct ConstraintValidator;

impl ConstraintValidator {
    /// Create a new constraint validator
    pub fn new() -> Self {
        Self
    }

    /// Validate constraints and update parameter requirements
    pub fn validate(
        &self,
        analyzed: &mut AnalyzedStatement,
        context: &AnalysisContext,
    ) -> Result<()> {
        match analyzed.ast.as_ref() {
            Statement::Dml(DmlStatement::Insert { table, .. }) => {
                // Get table schema
                if let Some(schema) = context.schemas().get(table) {
                    // Check NOT NULL constraints for parameters
                    for slot in &mut analyzed.parameter_slots {
                        if let SqlContext::InsertValue { column_index } =
                            slot.coercion_context.sql_context
                            && column_index < schema.columns.len()
                        {
                            let col = &schema.columns[column_index];
                            if !col.nullable {
                                slot.coercion_context.nullable = false;
                                slot.description =
                                    format!("Parameter for non-nullable column '{}'", col.name);
                            }
                        }
                    }
                }
                Ok(())
            }

            Statement::Dml(DmlStatement::Update { table, .. }) => {
                // Similar validation for UPDATE
                if let Some(schema) = context.schemas().get(table) {
                    for slot in &mut analyzed.parameter_slots {
                        if let SqlContext::UpdateAssignment { ref column_name } =
                            slot.coercion_context.sql_context
                        {
                            // Find column by name
                            for col in &schema.columns {
                                if &col.name == column_name {
                                    if !col.nullable {
                                        slot.coercion_context.nullable = false;
                                    }
                                    slot.acceptable_types = vec![col.datatype.clone()];
                                    break;
                                }
                            }
                        }
                    }
                }
                Ok(())
            }

            _ => Ok(()),
        }
    }
}

/// Validates functions with parameters
///
/// This validator simply updates parameter descriptions for better error messages.
/// Actual function validation is delegated to the functions module.
pub struct FunctionValidator;

impl FunctionValidator {
    /// Create a new function validator
    pub fn new() -> Self {
        Self
    }

    /// Update parameter descriptions for function arguments
    ///
    /// The actual validation happens in the functions module when we have
    /// concrete parameter values. This just improves error messages.
    pub fn validate(
        &self,
        analyzed: &mut AnalyzedStatement,
        _context: &AnalysisContext,
    ) -> Result<()> {
        // Just update descriptions for better error messages
        for slot in &mut analyzed.parameter_slots {
            if let SqlContext::FunctionArgument {
                ref function_name,
                arg_index,
            } = slot.coercion_context.sql_context
            {
                // Get function to check if it's aggregate
                if let Some(func) = crate::functions::get_function(function_name) {
                    let sig = func.signature();

                    slot.description = if sig.is_aggregate {
                        format!(
                            "Parameter {} for aggregate function '{}'",
                            arg_index + 1,
                            function_name
                        )
                    } else {
                        format!(
                            "Parameter {} for function '{}'",
                            arg_index + 1,
                            function_name
                        )
                    };
                }
            }
        }
        Ok(())
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
