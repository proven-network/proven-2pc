//! Column name resolution

use crate::error::Result;
use crate::parsing::ast::{DmlStatement, Expression, SelectStatement, Statement};
use crate::semantic::context::AnalysisContext;

/// Resolver for column references
pub struct ColumnResolver;

impl ColumnResolver {
    /// Resolve columns in a statement
    pub fn resolve_statement(
        &mut self,
        statement: &Statement,
        context: &mut AnalysisContext,
    ) -> Result<()> {
        match statement {
            Statement::Dml(dml) => self.resolve_dml_statement(dml, context),
            _ => Ok(()), // DDL and other statements don't have column references to resolve
        }
    }

    /// Resolve columns in a DML statement
    fn resolve_dml_statement(
        &mut self,
        dml: &DmlStatement,
        context: &mut AnalysisContext,
    ) -> Result<()> {
        match dml {
            DmlStatement::Select(select) => self.resolve_select_statement(select, context),
            DmlStatement::Insert { source, .. } => {
                // Resolve expressions in insert source
                match source {
                    super::super::super::parsing::ast::InsertSource::Values(values) => {
                        for value_row in values {
                            for expr in value_row {
                                self.resolve_expression(expr, context)?;
                            }
                        }
                    }
                    super::super::super::parsing::ast::InsertSource::Select(select) => {
                        self.resolve_select_statement(select, context)?;
                    }
                    super::super::super::parsing::ast::InsertSource::DefaultValues => {}
                }
                Ok(())
            }
            DmlStatement::Update { set, r#where, .. } => {
                // Resolve assignment values
                for expr in set.values().flatten() {
                    self.resolve_expression(expr, context)?;
                }

                // Resolve WHERE clause
                if let Some(where_expr) = r#where {
                    self.resolve_expression(where_expr, context)?;
                }
                Ok(())
            }
            DmlStatement::Delete { r#where, .. } => {
                // Resolve WHERE clause
                if let Some(where_expr) = r#where {
                    self.resolve_expression(where_expr, context)?;
                }
                Ok(())
            }
        }
    }

    /// Resolve a select statement
    fn resolve_select_statement(
        &mut self,
        select: &SelectStatement,
        context: &mut AnalysisContext,
    ) -> Result<()> {
        // Resolve projection expressions
        for (expr, _alias) in &select.select {
            self.resolve_expression(expr, context)?;
        }

        // Resolve WHERE clause
        if let Some(where_expr) = &select.r#where {
            self.resolve_expression(where_expr, context)?;
        }

        // Resolve GROUP BY
        for expr in &select.group_by {
            self.resolve_expression(expr, context)?;
        }

        // Resolve HAVING
        if let Some(having) = &select.having {
            self.resolve_expression(having, context)?;
        }

        // Resolve ORDER BY
        for (expr, _direction) in &select.order_by {
            self.resolve_expression(expr, context)?;
        }

        // Resolve LIMIT and OFFSET
        if let Some(limit) = &select.limit {
            self.resolve_expression(limit, context)?;
        }
        if let Some(offset) = &select.offset {
            self.resolve_expression(offset, context)?;
        }

        Ok(())
    }

    /// Resolve column references in an expression
    fn resolve_expression(
        &mut self,
        expr: &Expression,
        context: &mut AnalysisContext,
    ) -> Result<()> {
        match expr {
            Expression::Column(table, column) => {
                // Resolve the column reference
                context.resolve_column(table.as_deref(), column)?;
            }
            Expression::Function(_, args) => {
                for arg in args {
                    self.resolve_expression(arg, context)?;
                }
            }
            Expression::Operator(op) => {
                self.resolve_operator(op, context)?;
            }
            Expression::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                if let Some(operand) = operand {
                    self.resolve_expression(operand, context)?;
                }
                for (when_expr, then_expr) in when_clauses {
                    self.resolve_expression(when_expr, context)?;
                    self.resolve_expression(then_expr, context)?;
                }
                if let Some(else_expr) = else_clause {
                    self.resolve_expression(else_expr, context)?;
                }
            }
            Expression::ArrayAccess { base, index } => {
                self.resolve_expression(base, context)?;
                self.resolve_expression(index, context)?;
            }
            Expression::FieldAccess { base, .. } => {
                self.resolve_expression(base, context)?;
            }
            Expression::ArrayLiteral(exprs) => {
                for expr in exprs {
                    self.resolve_expression(expr, context)?;
                }
            }
            Expression::MapLiteral(entries) => {
                for (key, value) in entries {
                    self.resolve_expression(key, context)?;
                    self.resolve_expression(value, context)?;
                }
            }
            _ => {
                // All, Literal, Parameter don't need resolution
            }
        }
        Ok(())
    }

    /// Resolve column references in an operator
    fn resolve_operator(
        &mut self,
        op: &crate::parsing::ast::Operator,
        context: &mut AnalysisContext,
    ) -> Result<()> {
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
                self.resolve_expression(left, context)?;
                self.resolve_expression(right, context)?;
            }
            Operator::Not(expr)
            | Operator::Negate(expr)
            | Operator::Identity(expr)
            | Operator::Factorial(expr) => {
                self.resolve_expression(expr, context)?;
            }
            Operator::Is(expr, _) => {
                self.resolve_expression(expr, context)?;
            }
            Operator::InList { expr, list, .. } => {
                self.resolve_expression(expr, context)?;
                for item in list {
                    self.resolve_expression(item, context)?;
                }
            }
            Operator::Between {
                expr, low, high, ..
            } => {
                self.resolve_expression(expr, context)?;
                self.resolve_expression(low, context)?;
                self.resolve_expression(high, context)?;
            }
        }
        Ok(())
    }
}
