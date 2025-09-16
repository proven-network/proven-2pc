//! Type checking and inference

use super::annotated_ast::AnnotatedStatement;
use super::context::AnalysisContext;
use super::types::TypedExpression;
use crate::error::{Error, Result};
use crate::parsing::ast::{Expression, Statement};
use crate::types::data_type::DataType;

/// Type checker for SQL statements and expressions
pub struct TypeChecker {
    /// Whether to allow implicit type conversions
    allow_implicit_conversions: bool,
}

impl TypeChecker {
    /// Create a new type checker
    pub fn new() -> Self {
        Self {
            allow_implicit_conversions: true,
        }
    }

    /// Type check a statement
    pub fn check_statement(&mut self, statement: Statement, context: &mut AnalysisContext) -> Result<AnnotatedStatement> {
        match &statement {
            Statement::Dml(dml) => match dml {
                crate::parsing::ast::DmlStatement::Select(_) => self.check_select(statement, context),
                crate::parsing::ast::DmlStatement::Insert { .. } => self.check_insert(statement, context),
                crate::parsing::ast::DmlStatement::Update { .. } => self.check_update(statement, context),
                crate::parsing::ast::DmlStatement::Delete { .. } => self.check_delete(statement, context),
            },
            Statement::Ddl(ddl) => match ddl {
                crate::parsing::ast::DdlStatement::CreateTable { .. } => self.check_create_table(statement, context),
                _ => Ok(AnnotatedStatement::Ddl(statement)),
            },
            _ => Ok(AnnotatedStatement::Ddl(statement)),
        }
    }

    /// Type check a SELECT statement
    fn check_select(&mut self, statement: Statement, context: &mut AnalysisContext) -> Result<AnnotatedStatement> {
        // TODO: Implement SELECT type checking
        // - Check projection expressions
        // - Check WHERE clause is boolean
        // - Check GROUP BY expressions
        // - Check HAVING clause is boolean
        // - Check ORDER BY expressions
        Ok(AnnotatedStatement::Ddl(statement))
    }

    /// Type check an INSERT statement
    fn check_insert(&mut self, statement: Statement, context: &mut AnalysisContext) -> Result<AnnotatedStatement> {
        // TODO: Implement INSERT type checking
        // - Check value types match column types
        // - Handle DEFAULT values
        // - Check for nullable columns
        Ok(AnnotatedStatement::Ddl(statement))
    }

    /// Type check an UPDATE statement
    fn check_update(&mut self, statement: Statement, context: &mut AnalysisContext) -> Result<AnnotatedStatement> {
        // TODO: Implement UPDATE type checking
        // - Check assignment value types
        // - Check WHERE clause is boolean
        Ok(AnnotatedStatement::Ddl(statement))
    }

    /// Type check a DELETE statement
    fn check_delete(&mut self, statement: Statement, context: &mut AnalysisContext) -> Result<AnnotatedStatement> {
        // TODO: Implement DELETE type checking
        // - Check WHERE clause is boolean
        Ok(AnnotatedStatement::Ddl(statement))
    }

    /// Type check a CREATE TABLE statement
    fn check_create_table(&mut self, statement: Statement, context: &mut AnalysisContext) -> Result<AnnotatedStatement> {
        // TODO: Implement CREATE TABLE type checking
        // - Validate column types
        // - Check DEFAULT values match column types
        // - Validate constraints
        Ok(AnnotatedStatement::Ddl(statement))
    }

    /// Type check an expression
    pub fn check_expression(&mut self, expr: &Expression, context: &mut AnalysisContext) -> Result<TypedExpression> {
        let (data_type, nullable) = self.infer_type(expr, context)?;

        Ok(TypedExpression {
            expr: expr.clone(),
            data_type,
            nullable,
        })
    }

    /// Infer the type of an expression
    fn infer_type(&self, expr: &Expression, context: &mut AnalysisContext) -> Result<(DataType, bool)> {
        match expr {
            Expression::Literal(lit) => Ok((self.literal_type(lit)?, false)),
            Expression::Column(table, column) => {
                let col_info = context.resolve_column(table.as_deref(), column)?;
                Ok((col_info.data_type, col_info.nullable))
            }
            Expression::Operator(op) => self.infer_operator_type(op, context),
            Expression::Parameter(_index) => {
                // For placeholders, we'll need to track expectations
                // Use a nullable text type as placeholder
                Ok((DataType::Nullable(Box::new(DataType::Text)), true))
            }
            Expression::Function(_name, _args) => {
                // TODO: Infer function return type based on name and args
                // Use a nullable text type as placeholder
                Ok((DataType::Nullable(Box::new(DataType::Text)), true))
            }
            _ => Err(Error::ExecutionError("Type inference not implemented for this expression".to_string())),
        }
    }

    /// Infer the type of an operator expression
    fn infer_operator_type(&self, op: &crate::parsing::ast::Operator, context: &mut AnalysisContext) -> Result<(DataType, bool)> {
        use crate::parsing::ast::Operator;

        match op {
            Operator::And(left, right) | Operator::Or(left, right) => {
                let (left_type, left_nullable) = self.infer_type(left, context)?;
                let (right_type, right_nullable) = self.infer_type(right, context)?;

                if !matches!(left_type, DataType::Bool) {
                    return Err(Error::TypeMismatch {
                        expected: "BOOL".to_string(),
                        found: format!("{:?}", left_type),
                    });
                }
                if !matches!(right_type, DataType::Bool) {
                    return Err(Error::TypeMismatch {
                        expected: "BOOL".to_string(),
                        found: format!("{:?}", right_type),
                    });
                }
                Ok((DataType::Bool, left_nullable || right_nullable))
            }
            Operator::Not(expr) => {
                let (expr_type, nullable) = self.infer_type(expr, context)?;
                if !matches!(expr_type, DataType::Bool) {
                    return Err(Error::TypeMismatch {
                        expected: "BOOL".to_string(),
                        found: format!("{:?}", expr_type),
                    });
                }
                Ok((DataType::Bool, nullable))
            }
            Operator::Equal(left, right) | Operator::NotEqual(left, right)
            | Operator::LessThan(left, right) | Operator::LessThanOrEqual(left, right)
            | Operator::GreaterThan(left, right) | Operator::GreaterThanOrEqual(left, right) => {
                let (left_type, left_nullable) = self.infer_type(left, context)?;
                let (right_type, right_nullable) = self.infer_type(right, context)?;
                // Comparison operators return boolean
                Ok((DataType::Bool, left_nullable || right_nullable))
            }
            Operator::Add(left, right) | Operator::Subtract(left, right)
            | Operator::Multiply(left, right) | Operator::Divide(left, right)
            | Operator::Remainder(left, right) | Operator::Exponentiate(left, right) => {
                let (left_type, left_nullable) = self.infer_type(left, context)?;
                let (right_type, right_nullable) = self.infer_type(right, context)?;

                if !self.are_numeric_types_compatible(&left_type, &right_type) {
                    return Err(Error::TypeMismatch {
                        expected: format!("{:?}", left_type),
                        found: format!("{:?}", right_type),
                    });
                }
                let result_type = self.wider_numeric_type(&left_type, &right_type)?;
                Ok((result_type, left_nullable || right_nullable))
            }
            Operator::Negate(expr) | Operator::Identity(expr) | Operator::Factorial(expr) => {
                let (expr_type, nullable) = self.infer_type(expr, context)?;
                if !self.is_numeric_type(&expr_type) {
                    return Err(Error::TypeMismatch {
                        expected: "numeric type".to_string(),
                        found: format!("{:?}", expr_type),
                    });
                }
                Ok((expr_type, nullable))
            }
            Operator::Like(left, right) => {
                // LIKE requires string types
                let (left_type, left_nullable) = self.infer_type(left, context)?;
                let (right_type, right_nullable) = self.infer_type(right, context)?;
                // Returns boolean
                Ok((DataType::Bool, left_nullable || right_nullable))
            }
            Operator::Is(expr, lit) => {
                let (_expr_type, nullable) = self.infer_type(expr, context)?;
                // IS NULL/IS NOT NULL always returns boolean
                Ok((DataType::Bool, false))
            }
            Operator::InList { expr, list, .. } => {
                let (_expr_type, nullable) = self.infer_type(expr, context)?;
                // IN returns boolean
                Ok((DataType::Bool, nullable))
            }
            Operator::Between { expr, .. } => {
                let (_expr_type, nullable) = self.infer_type(expr, context)?;
                // BETWEEN returns boolean
                Ok((DataType::Bool, nullable))
            }
        }
    }

    /// Get the type of a literal
    fn literal_type(&self, lit: &crate::parsing::ast::Literal) -> Result<DataType> {
        use crate::parsing::ast::Literal;

        match lit {
            Literal::Null => Ok(DataType::Nullable(Box::new(DataType::Text))),
            Literal::Boolean(_) => Ok(DataType::Bool),
            Literal::Integer(_) => Ok(DataType::I64),
            Literal::Float(_) => Ok(DataType::F64),
            Literal::String(_) => Ok(DataType::Text),
            Literal::Bytea(_) => Ok(DataType::Bytea),
            Literal::Date(_) => Ok(DataType::Date),
            Literal::Time(_) => Ok(DataType::Time),
            Literal::Timestamp(_) => Ok(DataType::Timestamp),
            Literal::Interval(_) => Ok(DataType::Interval),
        }
    }


    /// Check if two numeric types are compatible
    fn are_numeric_types_compatible(&self, left: &DataType, right: &DataType) -> bool {
        self.is_numeric_type(left) && self.is_numeric_type(right)
    }

    /// Check if a type is numeric
    fn is_numeric_type(&self, data_type: &DataType) -> bool {
        matches!(
            data_type,
            DataType::I8
                | DataType::I16
                | DataType::I32
                | DataType::I64
                | DataType::I128
                | DataType::U8
                | DataType::U16
                | DataType::U32
                | DataType::U64
                | DataType::U128
                | DataType::F32
                | DataType::F64
                | DataType::Decimal(_, _)
        )
    }

    /// Get the wider of two numeric types
    fn wider_numeric_type(&self, left: &DataType, right: &DataType) -> Result<DataType> {
        // Simple implementation - can be enhanced
        use DataType::*;

        match (left, right) {
            // Float types are wider than integer types
            (F64, _) | (_, F64) => Ok(F64),
            (F32, _) | (_, F32) => Ok(F32),
            (Decimal(_, _), _) | (_, Decimal(_, _)) => Ok(Decimal(None, None)),

            // Among integers, use the larger size
            (I128, _) | (_, I128) => Ok(I128),
            (U128, _) | (_, U128) => Ok(U128),
            (I64, _) | (_, I64) => Ok(I64),
            (U64, _) | (_, U64) => Ok(U64),
            (I32, _) | (_, I32) => Ok(I32),
            (U32, _) | (_, U32) => Ok(U32),
            (I16, _) | (_, I16) => Ok(I16),
            (U16, _) | (_, U16) => Ok(U16),
            (I8, _) | (_, I8) => Ok(I8),
            (U8, _) | (_, U8) => Ok(U8),

            _ => Ok(left.clone()),
        }
    }
}