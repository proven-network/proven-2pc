//! Statement-level validation
//!
//! Validates statement structures and rules like HAVING requires GROUP BY

use crate::error::{Error, Result};
use crate::parsing::ast::{DdlStatement, DmlStatement, Expression, Statement};
use crate::semantic::context::AnalysisContext;
use crate::semantic::statement::AnalyzedStatement;
use std::collections::HashSet;

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
                    // When we have GROUP BY or aggregates, validate non-aggregate columns
                    self.validate_group_by_columns(select, analyzed)?;
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

    /// Validate that non-aggregate columns in SELECT appear in GROUP BY
    fn validate_group_by_columns(
        &self,
        select: &crate::parsing::ast::SelectStatement,
        analyzed: &AnalyzedStatement,
    ) -> Result<()> {
        // If no aggregates, GROUP BY acts like DISTINCT - all columns are allowed
        if !analyzed.metadata.has_aggregates {
            return Ok(());
        }

        // Collect columns that appear in GROUP BY
        let mut group_by_columns = HashSet::new();
        for expr in &select.group_by {
            Self::collect_column_refs(expr, &mut group_by_columns);
        }

        // Check each SELECT expression
        for (expr, _alias) in &select.select {
            // Check if this expression contains non-aggregate columns not in GROUP BY
            Self::validate_expression_group_by(expr, &group_by_columns, analyzed)?;
        }

        Ok(())
    }

    /// Collect all column references from an expression
    fn collect_column_refs(expr: &Expression, columns: &mut HashSet<String>) {
        use crate::parsing::ast::Operator;

        match expr {
            Expression::Column(_table, name) => {
                columns.insert(name.clone());
            }
            Expression::Operator(op) => {
                match op {
                    // Binary operators
                    Operator::And(left, right)
                    | Operator::Or(left, right)
                    | Operator::Equal(left, right)
                    | Operator::NotEqual(left, right)
                    | Operator::GreaterThan(left, right)
                    | Operator::GreaterThanOrEqual(left, right)
                    | Operator::LessThan(left, right)
                    | Operator::LessThanOrEqual(left, right)
                    | Operator::Add(left, right)
                    | Operator::Subtract(left, right)
                    | Operator::Multiply(left, right)
                    | Operator::Divide(left, right)
                    | Operator::Remainder(left, right)
                    | Operator::Exponentiate(left, right)
                    | Operator::Like(left, right) => {
                        Self::collect_column_refs(left, columns);
                        Self::collect_column_refs(right, columns);
                    }
                    // Unary operators
                    Operator::Not(expr)
                    | Operator::Negate(expr)
                    | Operator::Identity(expr)
                    | Operator::Factorial(expr) => {
                        Self::collect_column_refs(expr, columns);
                    }
                    Operator::Is(expr, _) => {
                        Self::collect_column_refs(expr, columns);
                    }
                    Operator::InList { expr, list, .. } => {
                        Self::collect_column_refs(expr, columns);
                        for item in list {
                            Self::collect_column_refs(item, columns);
                        }
                    }
                    Operator::Between {
                        expr, low, high, ..
                    } => {
                        Self::collect_column_refs(expr, columns);
                        Self::collect_column_refs(low, columns);
                        Self::collect_column_refs(high, columns);
                    }
                }
            }
            Expression::Function(_name, args) => {
                for arg in args {
                    Self::collect_column_refs(arg, columns);
                }
            }
            Expression::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                if let Some(op) = operand {
                    Self::collect_column_refs(op, columns);
                }
                for (when_expr, then_expr) in when_clauses {
                    Self::collect_column_refs(when_expr, columns);
                    Self::collect_column_refs(then_expr, columns);
                }
                if let Some(else_expr) = else_clause {
                    Self::collect_column_refs(else_expr, columns);
                }
            }
            _ => {}
        }
    }

    /// Validate that an expression only uses columns from GROUP BY or aggregates
    fn validate_expression_group_by(
        expr: &Expression,
        group_by_columns: &HashSet<String>,
        _analyzed: &AnalyzedStatement,
    ) -> Result<()> {
        use crate::parsing::ast::Operator;

        match expr {
            Expression::Column(_table, name) => {
                if !group_by_columns.contains(name) {
                    return Err(Error::ExecutionError(format!(
                        "Column '{}' must appear in GROUP BY clause or be used in an aggregate function",
                        name
                    )));
                }
            }
            Expression::Function(name, args) => {
                // Check if this is an aggregate function (including DISTINCT variants)
                let lower_name = name.to_lowercase();
                let is_aggregate = matches!(
                    lower_name.as_str(),
                    "sum"
                        | "avg"
                        | "count"
                        | "min"
                        | "max"
                        | "stdev"
                        | "variance"
                        | "sum_distinct"
                        | "avg_distinct"
                        | "count_distinct"
                        | "min_distinct"
                        | "max_distinct"
                        | "stdev_distinct"
                        | "variance_distinct"
                );

                if !is_aggregate {
                    // Non-aggregate function - check its arguments
                    for arg in args {
                        Self::validate_expression_group_by(arg, group_by_columns, _analyzed)?;
                    }
                }
                // Aggregate functions are allowed, and their arguments don't need to be in GROUP BY
            }
            Expression::Operator(op) => {
                match op {
                    // Binary operators
                    Operator::And(left, right)
                    | Operator::Or(left, right)
                    | Operator::Equal(left, right)
                    | Operator::NotEqual(left, right)
                    | Operator::GreaterThan(left, right)
                    | Operator::GreaterThanOrEqual(left, right)
                    | Operator::LessThan(left, right)
                    | Operator::LessThanOrEqual(left, right)
                    | Operator::Add(left, right)
                    | Operator::Subtract(left, right)
                    | Operator::Multiply(left, right)
                    | Operator::Divide(left, right)
                    | Operator::Remainder(left, right)
                    | Operator::Exponentiate(left, right)
                    | Operator::Like(left, right) => {
                        Self::validate_expression_group_by(left, group_by_columns, _analyzed)?;
                        Self::validate_expression_group_by(right, group_by_columns, _analyzed)?;
                    }
                    // Unary operators
                    Operator::Not(expr)
                    | Operator::Negate(expr)
                    | Operator::Identity(expr)
                    | Operator::Factorial(expr) => {
                        Self::validate_expression_group_by(expr, group_by_columns, _analyzed)?;
                    }
                    Operator::Is(expr, _) => {
                        Self::validate_expression_group_by(expr, group_by_columns, _analyzed)?;
                    }
                    Operator::InList { expr, list, .. } => {
                        Self::validate_expression_group_by(expr, group_by_columns, _analyzed)?;
                        for item in list {
                            Self::validate_expression_group_by(item, group_by_columns, _analyzed)?;
                        }
                    }
                    Operator::Between {
                        expr, low, high, ..
                    } => {
                        Self::validate_expression_group_by(expr, group_by_columns, _analyzed)?;
                        Self::validate_expression_group_by(low, group_by_columns, _analyzed)?;
                        Self::validate_expression_group_by(high, group_by_columns, _analyzed)?;
                    }
                }
            }
            Expression::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                if let Some(op) = operand {
                    Self::validate_expression_group_by(op, group_by_columns, _analyzed)?;
                }
                for (when_expr, then_expr) in when_clauses {
                    Self::validate_expression_group_by(when_expr, group_by_columns, _analyzed)?;
                    Self::validate_expression_group_by(then_expr, group_by_columns, _analyzed)?;
                }
                if let Some(else_expr) = else_clause {
                    Self::validate_expression_group_by(else_expr, group_by_columns, _analyzed)?;
                }
            }
            Expression::Parameter(_) => {
                // Parameters need runtime validation
                // We can't know at compile time if they reference a column
            }
            Expression::Literal(_) => {
                // Literals are always allowed
            }
            _ => {}
        }
        Ok(())
    }
}
