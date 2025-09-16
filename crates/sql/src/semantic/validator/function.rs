//! Function signature validation

use crate::error::{Error, Result};
use crate::parsing::ast::Expression;
use crate::semantic::annotated_ast::AnnotatedStatement;
use crate::semantic::context::AnalysisContext;
use crate::types::data_type::DataType;

/// Validator for function calls
pub struct FunctionValidator {}

impl FunctionValidator {
    /// Create a new function validator
    pub fn new() -> Self {
        Self {}
    }

    /// Validate all function calls in a statement
    pub fn validate_statement(
        &self,
        statement: &AnnotatedStatement,
        context: &mut AnalysisContext,
    ) -> Result<()> {
        // Get the original statement to walk through expressions
        let original = match statement {
            AnnotatedStatement::Select(s) => &s.statement,
            AnnotatedStatement::Insert(i) => &i.statement,
            AnnotatedStatement::Update(u) => &u.statement,
            AnnotatedStatement::Delete(d) => &d.statement,
            AnnotatedStatement::CreateTable(c) => &c.statement,
            AnnotatedStatement::Ddl(s) => s,
        };

        // Walk through the statement and validate function calls
        match original {
            crate::parsing::ast::Statement::Dml(dml) => match dml {
                crate::parsing::ast::DmlStatement::Select(select) => {
                    // Validate functions in SELECT projections
                    for (expr, _alias) in &select.select {
                        self.validate_expression(expr, context)?;
                    }

                    // Validate functions in WHERE clause
                    if let Some(where_clause) = &select.r#where {
                        self.validate_expression(where_clause, context)?;
                    }

                    // Validate functions in GROUP BY
                    for expr in &select.group_by {
                        self.validate_expression(expr, context)?;
                    }

                    // Validate functions in HAVING
                    if let Some(having) = &select.having {
                        self.validate_expression(having, context)?;
                    }

                    // Validate functions in ORDER BY
                    for (expr, _) in &select.order_by {
                        self.validate_expression(expr, context)?;
                    }

                    // Validate LIMIT and OFFSET if they're expressions
                    if let Some(limit) = &select.limit {
                        self.validate_expression(limit, context)?;
                    }
                    if let Some(offset) = &select.offset {
                        self.validate_expression(offset, context)?;
                    }
                }
                crate::parsing::ast::DmlStatement::Insert { source, .. } => {
                    // Validate functions in INSERT source
                    use crate::parsing::ast::dml::InsertSource;
                    match source {
                        InsertSource::Values(rows) => {
                            for row in rows {
                                for expr in row {
                                    self.validate_expression(expr, context)?;
                                }
                            }
                        }
                        InsertSource::Select(select) => {
                            // Recursively validate the SELECT statement
                            let select_stmt = crate::parsing::ast::Statement::Dml(
                                crate::parsing::ast::DmlStatement::Select(Box::new(
                                    select.as_ref().clone(),
                                )),
                            );
                            let annotated = AnnotatedStatement::Ddl(select_stmt);
                            self.validate_statement(&annotated, context)?;
                        }
                        InsertSource::DefaultValues => {
                            // Nothing to validate
                        }
                    }
                }
                crate::parsing::ast::DmlStatement::Update { set, r#where, .. } => {
                    // Validate functions in SET assignments
                    for (_column, expr_opt) in set {
                        if let Some(expr) = expr_opt {
                            self.validate_expression(expr, context)?;
                        }
                    }

                    // Validate functions in WHERE clause
                    if let Some(where_clause) = r#where {
                        self.validate_expression(where_clause, context)?;
                    }
                }
                crate::parsing::ast::DmlStatement::Delete { r#where, .. } => {
                    // Validate functions in WHERE clause
                    if let Some(where_clause) = r#where {
                        self.validate_expression(where_clause, context)?;
                    }
                }
            },
            _ => {
                // DDL statements typically don't have function calls
            }
        }

        Ok(())
    }

    /// Validate all function calls in an expression
    fn validate_expression(&self, expr: &Expression, context: &mut AnalysisContext) -> Result<()> {
        match expr {
            Expression::Function(name, args) => {
                // First validate nested expressions in arguments
                for arg in args {
                    self.validate_expression(arg, context)?;
                }

                // Then validate the function call itself
                self.validate_function(name, args, context)?;
                Ok(())
            }
            Expression::Operator(op) => {
                // Recursively validate operator expressions
                use crate::parsing::ast::Operator;
                match op {
                    Operator::And(left, right)
                    | Operator::Or(left, right)
                    | Operator::Equal(left, right)
                    | Operator::NotEqual(left, right)
                    | Operator::LessThan(left, right)
                    | Operator::LessThanOrEqual(left, right)
                    | Operator::GreaterThan(left, right)
                    | Operator::GreaterThanOrEqual(left, right)
                    | Operator::Add(left, right)
                    | Operator::Subtract(left, right)
                    | Operator::Multiply(left, right)
                    | Operator::Divide(left, right)
                    | Operator::Remainder(left, right)
                    | Operator::Exponentiate(left, right)
                    | Operator::Like(left, right) => {
                        self.validate_expression(left, context)?;
                        self.validate_expression(right, context)?;
                    }
                    Operator::Not(expr)
                    | Operator::Negate(expr)
                    | Operator::Identity(expr)
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
                    Operator::Between {
                        expr, low, high, ..
                    } => {
                        self.validate_expression(expr, context)?;
                        self.validate_expression(low, context)?;
                        self.validate_expression(high, context)?;
                    }
                }
                Ok(())
            }
            Expression::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                // Validate CASE expression components
                if let Some(op) = operand {
                    self.validate_expression(op, context)?;
                }
                for (condition, result) in when_clauses {
                    self.validate_expression(condition, context)?;
                    self.validate_expression(result, context)?;
                }
                if let Some(else_expr) = else_clause {
                    self.validate_expression(else_expr, context)?;
                }
                Ok(())
            }
            _ => {
                // Literals, columns, parameters don't contain function calls
                Ok(())
            }
        }
    }

    /// Validate a function call
    pub fn validate_function(
        &self,
        name: &str,
        args: &[Expression],
        context: &mut AnalysisContext,
    ) -> Result<DataType> {
        // Use the function registry to get the function
        let func = crate::functions::get_function(name);

        if let Some(func) = func {
            // Get the signature from the function
            let signature = func.signature();

            // Check argument count
            if args.len() < signature.min_args {
                return Err(Error::ExecutionError(format!(
                    "Function {} requires at least {} arguments, got {}",
                    name,
                    signature.min_args,
                    args.len()
                )));
            }

            if let Some(max) = signature.max_args {
                if args.len() > max {
                    return Err(Error::ExecutionError(format!(
                        "Function {} accepts at most {} arguments, got {}",
                        name,
                        max,
                        args.len()
                    )));
                }
            }

            // Track if this is an aggregate function
            if signature.is_aggregate {
                context.set_deterministic(false);
            }

            // Infer argument types from the expressions
            let mut arg_types = Vec::new();
            for arg in args {
                // Simple type inference for literals
                let arg_type = match arg {
                    Expression::Literal(lit) => match lit {
                        crate::parsing::ast::Literal::Null => DataType::Nullable(Box::new(DataType::Text)),
                        crate::parsing::ast::Literal::Boolean(_) => DataType::Bool,
                        crate::parsing::ast::Literal::Integer(_) => DataType::I64,
                        crate::parsing::ast::Literal::Float(_) => DataType::F64,
                        crate::parsing::ast::Literal::String(_) => DataType::Text,
                        crate::parsing::ast::Literal::Bytea(_) => DataType::Bytea,
                        crate::parsing::ast::Literal::Date(_) => DataType::Date,
                        crate::parsing::ast::Literal::Time(_) => DataType::Time,
                        crate::parsing::ast::Literal::Timestamp(_) => DataType::Timestamp,
                        crate::parsing::ast::Literal::Interval(_) => DataType::Interval,
                    },
                    Expression::Column(_, _) => {
                        // For columns, we'd need context, so use a generic type
                        DataType::Text
                    },
                    _ => {
                        // For complex expressions, use a generic type
                        DataType::Nullable(Box::new(DataType::Text))
                    }
                };
                arg_types.push(arg_type);
            }

            // Use validate to get the return type
            func.validate(&arg_types)
                .or_else(|_| Ok(DataType::Nullable(Box::new(DataType::Text))))
        } else {
            // Unknown function - we'll allow it but can't validate
            Ok(DataType::Nullable(Box::new(DataType::Text)))
        }
    }

    /// Check if a function is an aggregate
    pub fn is_aggregate(&self, name: &str) -> bool {
        crate::functions::get_function(name)
            .map(|func| func.signature().is_aggregate)
            .unwrap_or(false)
    }

    /// Get the return type of a function
    pub fn return_type(&self, name: &str, arg_types: &[DataType]) -> DataType {
        crate::functions::get_function(name)
            .and_then(|func| func.validate(arg_types).ok())
            .unwrap_or(DataType::Nullable(Box::new(DataType::Text)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parsing::ast::{DmlStatement, SelectStatement, Statement};
    use crate::semantic::annotated_ast::{AnnotatedSelect, AnnotatedStatement};

    #[test]
    fn test_validate_function_calls() {
        let validator = FunctionValidator::new();
        let mut context = AnalysisContext::new(std::collections::HashMap::new());

        // Test validating UPPER function
        let upper_args = vec![Expression::Literal(crate::parsing::ast::Literal::String(
            "test".to_string(),
        ))];
        let result = validator.validate_function("UPPER", &upper_args, &mut context);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), DataType::Text);

        // Test validating unknown function (should still succeed but return nullable text)
        let unknown_args = vec![Expression::Literal(crate::parsing::ast::Literal::Integer(
            42,
        ))];
        let result = validator.validate_function("UNKNOWN_FUNC", &unknown_args, &mut context);
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            DataType::Nullable(Box::new(DataType::Text))
        );

        // Test validating function with wrong number of args
        let wrong_args = vec![
            Expression::Literal(crate::parsing::ast::Literal::String("test".to_string())),
            Expression::Literal(crate::parsing::ast::Literal::String("extra".to_string())),
        ];
        let result = validator.validate_function("UPPER", &wrong_args, &mut context);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_statement_with_functions() {
        let validator = FunctionValidator::new();
        let mut context = AnalysisContext::new(std::collections::HashMap::new());

        // Create a SELECT statement with function calls
        let select = SelectStatement {
            select: vec![
                (
                    Expression::Function(
                        "UPPER".to_string(),
                        vec![Expression::Column(None, "name".to_string())],
                    ),
                    None,
                ),
                (
                    Expression::Function(
                        "LENGTH".to_string(),
                        vec![Expression::Column(None, "description".to_string())],
                    ),
                    None,
                ),
            ],
            from: vec![],
            r#where: Some(Expression::Operator(
                crate::parsing::ast::Operator::GreaterThan(
                    Box::new(Expression::Function(
                        "COUNT".to_string(),
                        vec![Expression::Column(None, "id".to_string())],
                    )),
                    Box::new(Expression::Literal(crate::parsing::ast::Literal::Integer(
                        5,
                    ))),
                ),
            )),
            group_by: vec![],
            having: None,
            order_by: vec![],
            offset: None,
            limit: None,
        };

        let statement = Statement::Dml(DmlStatement::Select(Box::new(select.clone())));
        let annotated = AnnotatedStatement::Select(AnnotatedSelect {
            statement: statement.clone(),
            projection_types: vec![],
            where_type: None,
            group_by_types: vec![],
            having_type: None,
            order_by_types: vec![],
        });

        // Should validate successfully
        let result = validator.validate_statement(&annotated, &mut context);
        assert!(result.is_ok());
    }
}
