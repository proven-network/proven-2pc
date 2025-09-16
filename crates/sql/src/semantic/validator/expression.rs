//! Expression validation

use crate::error::{Error, Result};
use crate::parsing::ast::Expression;
use crate::semantic::annotated_ast::AnnotatedStatement;
use crate::semantic::context::AnalysisContext;

/// Validator for expressions
pub struct ExpressionValidator {
    /// Whether to allow subqueries in expressions
    allow_subqueries: bool,
}

impl ExpressionValidator {
    /// Create a new expression validator
    pub fn new() -> Self {
        Self {
            allow_subqueries: true,
        }
    }

    /// Validate all expressions in a statement
    pub fn validate_statement(&self, statement: &AnnotatedStatement, context: &mut AnalysisContext) -> Result<()> {
        // TODO: Implement expression validation
        // - Check for invalid expression combinations
        // - Validate aggregate function usage
        // - Check for column references outside scope
        // - Validate subquery usage
        Ok(())
    }

    /// Validate a single expression
    pub fn validate_expression(&self, expr: &Expression, context: &mut AnalysisContext) -> Result<()> {
        match expr {
            Expression::Column(table, column) => {
                // Column validation is handled by the resolver
                context.resolve_column(table.as_deref(), column)?;
            }
            Expression::Function(name, args) => {
                self.validate_function_call(name, args, context)?;
            }
            Expression::Operator(op) => {
                self.validate_operator(op, context)?;
            }
            Expression::Case { .. } => {
                // TODO: Validate CASE expression
            }
            _ => {
                // Literals, placeholders, etc. are always valid
            }
        }
        Ok(())
    }

    /// Validate operator
    fn validate_operator(&self, op: &crate::parsing::ast::Operator, context: &mut AnalysisContext) -> Result<()> {
        use crate::parsing::ast::Operator;

        match op {
            Operator::And(left, right) | Operator::Or(left, right)
            | Operator::Equal(left, right) | Operator::NotEqual(left, right)
            | Operator::LessThan(left, right) | Operator::LessThanOrEqual(left, right)
            | Operator::GreaterThan(left, right) | Operator::GreaterThanOrEqual(left, right)
            | Operator::Add(left, right) | Operator::Subtract(left, right)
            | Operator::Multiply(left, right) | Operator::Divide(left, right)
            | Operator::Remainder(left, right) | Operator::Exponentiate(left, right)
            | Operator::Like(left, right) => {
                self.validate_expression(left, context)?;
                self.validate_expression(right, context)?;
            }
            Operator::Not(expr) | Operator::Negate(expr) | Operator::Identity(expr)
            | Operator::Factorial(expr) => {
                self.validate_expression(expr, context)?;
            }
            Operator::Is(expr, _) => {
                self.validate_expression(expr, context)?;
            }
            Operator::InList { expr, list, .. } => {
                self.validate_expression(expr, context)?;
                for item in list {
                    self.validate_expression(item, context)?;
                }
            }
            Operator::Between { expr, low, high, .. } => {
                self.validate_expression(expr, context)?;
                self.validate_expression(low, context)?;
                self.validate_expression(high, context)?;
            }
        }
        Ok(())
    }

    /// Validate function call
    fn validate_function_call(
        &self,
        name: &str,
        args: &[Expression],
        context: &mut AnalysisContext,
    ) -> Result<()> {
        // Validate each argument
        for arg in args {
            self.validate_expression(arg, context)?;
        }

        // Check if it's an aggregate function using the registry
        let is_aggregate = crate::functions::get_function(name)
            .map(|func| func.signature().is_aggregate)
            .unwrap_or(false);

        if is_aggregate {
            // TODO: Track that we're in an aggregate context
            // This affects what columns can be referenced
        }

        Ok(())
    }
}