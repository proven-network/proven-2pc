//! Scope management for nested queries

use crate::error::Result;
use crate::parsing::ast::{Expression, Statement};
use crate::semantic::context::AnalysisContext;
use std::collections::HashMap;

/// Scope information
#[derive(Debug, Clone)]
struct Scope {
    /// Level in the scope hierarchy (0 = outermost)
    level: usize,
    /// Available columns in this scope
    columns: HashMap<String, Vec<String>>, // table -> columns
    /// Parent scope
    parent: Option<Box<Scope>>,
}

/// Manager for query scopes (subqueries, CTEs, etc.)
pub struct ScopeManager {
    /// Current scope
    current_scope: Option<Scope>,
}

impl ScopeManager {
    /// Create a new scope manager
    pub fn new() -> Self {
        Self {
            current_scope: None,
        }
    }

    /// Analyze scopes in a statement
    pub fn analyze_statement(
        &mut self,
        statement: &Statement,
        context: &mut AnalysisContext,
    ) -> Result<()> {
        // Push a new scope for the statement
        self.push_scope();

        // Analyze the statement
        self.analyze_statement_internal(statement, context)?;

        // Pop the scope
        self.pop_scope();

        Ok(())
    }

    /// Internal statement analysis
    fn analyze_statement_internal(
        &mut self,
        statement: &Statement,
        context: &mut AnalysisContext,
    ) -> Result<()> {
        match statement {
            Statement::Dml(crate::parsing::ast::DmlStatement::Select(select)) => {
                // Process WHERE clause
                if let Some(where_expr) = &select.r#where {
                    self.analyze_expression(where_expr, context)?;
                }

                // Process HAVING clause
                if let Some(having_expr) = &select.having {
                    self.analyze_expression(having_expr, context)?;
                }
            }
            _ => {
                // Other statement types
            }
        }
        Ok(())
    }

    /// Analyze scopes in an expression
    fn analyze_expression(
        &mut self,
        expr: &Expression,
        context: &mut AnalysisContext,
    ) -> Result<()> {
        match expr {
            Expression::Operator(op) => {
                self.analyze_operator(op, context)?;
            }
            Expression::Function(_, args) => {
                for arg in args {
                    self.analyze_expression(arg, context)?;
                }
            }
            Expression::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                if let Some(operand) = operand {
                    self.analyze_expression(operand, context)?;
                }
                for (when_expr, then_expr) in when_clauses {
                    self.analyze_expression(when_expr, context)?;
                    self.analyze_expression(then_expr, context)?;
                }
                if let Some(else_expr) = else_clause {
                    self.analyze_expression(else_expr, context)?;
                }
            }
            _ => {
                // Other expression types don't create new scopes
            }
        }
        Ok(())
    }

    /// Analyze scopes in an operator
    fn analyze_operator(
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
                self.analyze_expression(left, context)?;
                self.analyze_expression(right, context)?;
            }
            Operator::Not(expr)
            | Operator::Negate(expr)
            | Operator::Identity(expr)
            | Operator::Factorial(expr) => {
                self.analyze_expression(expr, context)?;
            }
            Operator::Is(expr, _) => {
                self.analyze_expression(expr, context)?;
            }
            Operator::InList { expr, list, .. } => {
                self.analyze_expression(expr, context)?;
                for item in list {
                    self.analyze_expression(item, context)?;
                }
            }
            Operator::Between {
                expr, low, high, ..
            } => {
                self.analyze_expression(expr, context)?;
                self.analyze_expression(low, context)?;
                self.analyze_expression(high, context)?;
            }
        }
        Ok(())
    }

    /// Push a new scope
    fn push_scope(&mut self) {
        let level = self
            .current_scope
            .as_ref()
            .map(|s| s.level + 1)
            .unwrap_or(0);

        let new_scope = Scope {
            level,
            columns: HashMap::new(),
            parent: self.current_scope.clone().map(Box::new),
        };

        self.current_scope = Some(new_scope);
    }

    /// Pop the current scope
    fn pop_scope(&mut self) {
        if let Some(scope) = &self.current_scope {
            self.current_scope = scope.parent.as_ref().map(|p| (**p).clone());
        }
    }

    /// Get the current scope level
    pub fn current_level(&self) -> usize {
        self.current_scope.as_ref().map(|s| s.level).unwrap_or(0)
    }

    /// Check if we're in a subquery
    pub fn in_subquery(&self) -> bool {
        self.current_level() > 0
    }

    /// Add columns to the current scope
    pub fn add_columns(&mut self, table: String, columns: Vec<String>) {
        if let Some(scope) = &mut self.current_scope {
            scope.columns.insert(table, columns);
        }
    }

    /// Check if a column is available in the current scope
    pub fn column_in_scope(&self, table: Option<&str>, column: &str) -> bool {
        let mut scope = self.current_scope.as_ref();

        while let Some(s) = scope {
            // If table is specified, look for it
            if let Some(table) = table {
                if let Some(columns) = s.columns.get(table)
                    && columns.contains(&column.to_string())
                {
                    return true;
                }
            } else {
                // No table specified - search all tables in scope
                for columns in s.columns.values() {
                    if columns.contains(&column.to_string()) {
                        return true;
                    }
                }
            }

            // Check parent scope
            scope = s.parent.as_deref();
        }

        false
    }
}
