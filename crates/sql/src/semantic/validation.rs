//! Semantic validation phase
//!
//! This module validates semantic rules that aren't handled by
//! name resolution or type checking, including:
//! - GROUP BY validation
//! - Aggregate function usage rules
//! - ORDER BY validation
//! - DISTINCT usage
//! - DEFAULT value validation

use super::statement::ExpressionId;
use crate::error::{Error, Result};
use crate::parsing::ast::{DmlStatement, Expression, Literal, SelectStatement, Statement};
use std::collections::HashSet;
use std::sync::Arc;

/// Handles semantic validation rules
pub struct SemanticValidator {
    // Could add configuration for validation strictness
}

impl SemanticValidator {
    /// Create a new semantic validator
    pub fn new() -> Self {
        Self {}
    }

    /// Validate GROUP BY rules
    fn validate_group_by(&self, select: &SelectStatement) -> Result<()> {
        // Collect all GROUP BY expressions
        let group_by_exprs: HashSet<String> = select
            .group_by
            .iter()
            .map(|e| self.expression_to_string(e))
            .collect();

        // Check SELECT list for non-aggregate, non-GROUP BY expressions
        for (expr, _alias) in &select.select {
            self.validate_group_by_expr(expr, &group_by_exprs)?;
        }

        // Check ORDER BY for non-aggregate, non-GROUP BY expressions
        for (expr, _) in &select.order_by {
            self.validate_group_by_expr(expr, &group_by_exprs)?;
        }

        Ok(())
    }

    /// Validate an expression in GROUP BY context
    fn validate_group_by_expr(
        &self,
        expr: &Expression,
        group_by_exprs: &HashSet<String>,
    ) -> Result<()> {
        // If it's an aggregate, it's always ok
        if Self::is_aggregate_expression(expr) {
            return Ok(());
        }

        // If it's in the GROUP BY list, it's ok
        let expr_str = self.expression_to_string(expr);
        if group_by_exprs.contains(&expr_str) {
            return Ok(());
        }

        // If it's a literal, it's ok
        if matches!(expr, Expression::Literal(_)) {
            return Ok(());
        }

        // Check sub-expressions
        match expr {
            Expression::Operator(op) => {
                use crate::parsing::ast::Operator;
                match op {
                    Operator::And(l, r)
                    | Operator::Or(l, r)
                    | Operator::Equal(l, r)
                    | Operator::NotEqual(l, r)
                    | Operator::GreaterThan(l, r)
                    | Operator::GreaterThanOrEqual(l, r)
                    | Operator::LessThan(l, r)
                    | Operator::LessThanOrEqual(l, r)
                    | Operator::Add(l, r)
                    | Operator::Subtract(l, r)
                    | Operator::Multiply(l, r)
                    | Operator::Divide(l, r)
                    | Operator::Remainder(l, r)
                    | Operator::Exponentiate(l, r)
                    | Operator::Like(l, r) => {
                        self.validate_group_by_expr(l, group_by_exprs)?;
                        self.validate_group_by_expr(r, group_by_exprs)?;
                    }
                    Operator::Not(e)
                    | Operator::Negate(e)
                    | Operator::Identity(e)
                    | Operator::Factorial(e) => {
                        self.validate_group_by_expr(e, group_by_exprs)?;
                    }
                    Operator::Between {
                        expr,
                        low,
                        high,
                        negated: _,
                    } => {
                        self.validate_group_by_expr(expr, group_by_exprs)?;
                        self.validate_group_by_expr(low, group_by_exprs)?;
                        self.validate_group_by_expr(high, group_by_exprs)?;
                    }
                    Operator::InList {
                        expr,
                        list,
                        negated: _,
                    } => {
                        self.validate_group_by_expr(expr, group_by_exprs)?;
                        for item in list {
                            self.validate_group_by_expr(item, group_by_exprs)?;
                        }
                    }
                    Operator::Is(e, _) => {
                        self.validate_group_by_expr(e, group_by_exprs)?;
                    }
                    Operator::InSubquery { expr, subquery, .. } => {
                        self.validate_group_by_expr(expr, group_by_exprs)?;
                        self.validate_group_by_expr(subquery, group_by_exprs)?;
                    }
                    Operator::Exists { subquery, .. } => {
                        self.validate_group_by_expr(subquery, group_by_exprs)?;
                    }
                }
                Ok(())
            }
            // CAST is handled as a function
            Expression::Function(name, args) if name.to_uppercase() == "CAST" => {
                // CAST function - check the expression being cast
                if !args.is_empty() {
                    self.validate_group_by_expr(&args[0], group_by_exprs)?;
                }
                Ok(())
            }
            Expression::Function(name, args) => {
                // If it's not an aggregate function, check args
                if !crate::functions::is_aggregate(&name.to_uppercase()) {
                    for arg in args {
                        self.validate_group_by_expr(arg, group_by_exprs)?;
                    }
                }
                Ok(())
            }
            Expression::Column(_, _) => {
                // Non-grouped column reference
                Err(Error::ExecutionError(format!(
                    "Column '{}' must appear in GROUP BY clause or be used in an aggregate function",
                    expr_str
                )))
            }
            _ => Ok(()),
        }
    }

    /// Validate ORDER BY clause
    fn validate_order_by(&self, _select: &SelectStatement) -> Result<()> {
        // ORDER BY validation could be enhanced here
        Ok(())
    }

    /// Check if an expression is an aggregate
    fn is_aggregate_expression(expr: &Expression) -> bool {
        match expr {
            Expression::Function(name, _) => crate::functions::is_aggregate(&name.to_uppercase()),
            Expression::Operator(op) => {
                // Check if any sub-expression is an aggregate
                use crate::parsing::ast::Operator;
                match op {
                    Operator::And(l, r)
                    | Operator::Or(l, r)
                    | Operator::Equal(l, r)
                    | Operator::NotEqual(l, r)
                    | Operator::GreaterThan(l, r)
                    | Operator::GreaterThanOrEqual(l, r)
                    | Operator::LessThan(l, r)
                    | Operator::LessThanOrEqual(l, r)
                    | Operator::Add(l, r)
                    | Operator::Subtract(l, r)
                    | Operator::Multiply(l, r)
                    | Operator::Divide(l, r)
                    | Operator::Remainder(l, r)
                    | Operator::Exponentiate(l, r)
                    | Operator::Like(l, r) => {
                        Self::is_aggregate_expression(l) || Self::is_aggregate_expression(r)
                    }
                    Operator::Not(e)
                    | Operator::Negate(e)
                    | Operator::Identity(e)
                    | Operator::Factorial(e) => Self::is_aggregate_expression(e),
                    Operator::Between {
                        expr,
                        low,
                        high,
                        negated: _,
                    } => {
                        Self::is_aggregate_expression(expr)
                            || Self::is_aggregate_expression(low)
                            || Self::is_aggregate_expression(high)
                    }
                    Operator::InList {
                        expr,
                        list,
                        negated: _,
                    } => {
                        Self::is_aggregate_expression(expr)
                            || list.iter().any(Self::is_aggregate_expression)
                    }
                    Operator::Is(e, _) => Self::is_aggregate_expression(e),
                    Operator::InSubquery { expr, subquery, .. } => {
                        Self::is_aggregate_expression(expr)
                            || Self::is_aggregate_expression(subquery)
                    }
                    Operator::Exists { subquery, .. } => Self::is_aggregate_expression(subquery),
                }
            }
            _ => false,
        }
    }

    /// Validate aggregates without GROUP BY
    fn validate_aggregates_without_group_by(&self, select: &SelectStatement) -> Result<()> {
        // When using aggregates without GROUP BY, non-aggregate columns are not allowed
        // Only literals, parameters, and aggregate functions are permitted

        for (expr, _) in &select.select {
            self.validate_aggregate_context(expr)?;
        }

        if let Some(having) = &select.having {
            // HAVING is allowed with aggregates even without GROUP BY
            self.validate_aggregate_context(having)?;
        }

        for (expr, _) in &select.order_by {
            self.validate_aggregate_context(expr)?;
        }

        Ok(())
    }

    /// Validate expression in aggregate context (no GROUP BY)
    fn validate_aggregate_context(&self, expr: &Expression) -> Result<()> {
        // If the entire expression is an aggregate, it's valid
        if Self::is_aggregate_expression(expr) {
            // But check for nested aggregates
            return self.validate_no_nested_aggregates_in_aggregate(expr);
        }

        match expr {
            Expression::Column(_, col) => {
                // Bare columns not allowed when using aggregates without GROUP BY
                Err(Error::ExecutionError(format!(
                    "Column '{}' must appear in GROUP BY clause or be used in an aggregate function",
                    col
                )))
            }
            Expression::Function(_name, args) => {
                // Non-aggregate function - check arguments
                for arg in args {
                    self.validate_aggregate_context(arg)?;
                }
                Ok(())
            }
            Expression::Operator(op) => {
                use crate::parsing::ast::Operator;
                match op {
                    Operator::And(l, r)
                    | Operator::Or(l, r)
                    | Operator::Equal(l, r)
                    | Operator::NotEqual(l, r)
                    | Operator::GreaterThan(l, r)
                    | Operator::GreaterThanOrEqual(l, r)
                    | Operator::LessThan(l, r)
                    | Operator::LessThanOrEqual(l, r)
                    | Operator::Add(l, r)
                    | Operator::Subtract(l, r)
                    | Operator::Multiply(l, r)
                    | Operator::Divide(l, r)
                    | Operator::Remainder(l, r)
                    | Operator::Exponentiate(l, r)
                    | Operator::Like(l, r) => {
                        self.validate_aggregate_context(l)?;
                        self.validate_aggregate_context(r)?;
                    }
                    Operator::Not(e)
                    | Operator::Negate(e)
                    | Operator::Identity(e)
                    | Operator::Factorial(e) => {
                        self.validate_aggregate_context(e)?;
                    }
                    Operator::Between {
                        expr,
                        low,
                        high,
                        negated: _,
                    } => {
                        self.validate_aggregate_context(expr)?;
                        self.validate_aggregate_context(low)?;
                        self.validate_aggregate_context(high)?;
                    }
                    Operator::InList {
                        expr,
                        list,
                        negated: _,
                    } => {
                        self.validate_aggregate_context(expr)?;
                        for item in list {
                            self.validate_aggregate_context(item)?;
                        }
                    }
                    Operator::Is(e, _) => {
                        self.validate_aggregate_context(e)?;
                    }
                    Operator::InSubquery { expr, subquery, .. } => {
                        self.validate_aggregate_context(expr)?;
                        self.validate_aggregate_context(subquery)?;
                    }
                    Operator::Exists { subquery, .. } => {
                        self.validate_aggregate_context(subquery)?;
                    }
                }
                Ok(())
            }
            Expression::Literal(_) | Expression::Parameter(_) => {
                // Literals and parameters are always valid
                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// Validate that an aggregate function doesn't have nested aggregates
    fn validate_no_nested_aggregates_in_aggregate(&self, expr: &Expression) -> Result<()> {
        match expr {
            Expression::Function(name, args) => {
                if crate::functions::is_aggregate(&name.to_uppercase()) {
                    // This is the outer aggregate - check its arguments for nested aggregates
                    for arg in args {
                        Self::validate_no_nested_aggregates(arg)?;
                    }
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// Check for nested aggregate functions
    fn validate_no_nested_aggregates(expr: &Expression) -> Result<()> {
        match expr {
            Expression::Function(name, args) => {
                if crate::functions::is_aggregate(&name.to_uppercase()) {
                    // Found a nested aggregate!
                    return Err(Error::ExecutionError(format!(
                        "Nested aggregate functions are not allowed: {}",
                        name
                    )));
                }
                // Check arguments recursively
                for arg in args {
                    Self::validate_no_nested_aggregates(arg)?;
                }
                Ok(())
            }
            Expression::Operator(op) => {
                use crate::parsing::ast::Operator;
                match op {
                    Operator::And(l, r)
                    | Operator::Or(l, r)
                    | Operator::Equal(l, r)
                    | Operator::NotEqual(l, r)
                    | Operator::GreaterThan(l, r)
                    | Operator::GreaterThanOrEqual(l, r)
                    | Operator::LessThan(l, r)
                    | Operator::LessThanOrEqual(l, r)
                    | Operator::Add(l, r)
                    | Operator::Subtract(l, r)
                    | Operator::Multiply(l, r)
                    | Operator::Divide(l, r)
                    | Operator::Remainder(l, r)
                    | Operator::Exponentiate(l, r)
                    | Operator::Like(l, r) => {
                        Self::validate_no_nested_aggregates(l)?;
                        Self::validate_no_nested_aggregates(r)?;
                    }
                    Operator::Not(e)
                    | Operator::Negate(e)
                    | Operator::Identity(e)
                    | Operator::Factorial(e) => {
                        Self::validate_no_nested_aggregates(e)?;
                    }
                    Operator::Between {
                        expr,
                        low,
                        high,
                        negated: _,
                    } => {
                        Self::validate_no_nested_aggregates(expr)?;
                        Self::validate_no_nested_aggregates(low)?;
                        Self::validate_no_nested_aggregates(high)?;
                    }
                    Operator::InList {
                        expr,
                        list,
                        negated: _,
                    } => {
                        Self::validate_no_nested_aggregates(expr)?;
                        for item in list {
                            Self::validate_no_nested_aggregates(item)?;
                        }
                    }
                    Operator::Is(e, _) => {
                        Self::validate_no_nested_aggregates(e)?;
                    }
                    Operator::InSubquery { expr, subquery, .. } => {
                        Self::validate_no_nested_aggregates(expr)?;
                        Self::validate_no_nested_aggregates(subquery)?;
                    }
                    Operator::Exists { subquery, .. } => {
                        Self::validate_no_nested_aggregates(subquery)?;
                    }
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// Convert expression to string for comparison
    fn expression_to_string(&self, expr: &Expression) -> String {
        match expr {
            Expression::Column(table, col) => {
                if let Some(t) = table {
                    format!("{}.{}", t, col)
                } else {
                    col.clone()
                }
            }
            Expression::Literal(lit) => match lit {
                Literal::Null => "NULL".to_string(),
                Literal::Boolean(b) => b.to_string(),
                Literal::Integer(i) => i.to_string(),
                Literal::Float(f) => f.to_string(),
                Literal::String(s) => format!("'{}'", s),
                _ => "literal".to_string(),
            },
            Expression::Function(name, _) => name.clone(),
            _ => "expr".to_string(),
        }
    }

    /// Validate all semantic rules (for new phase architecture)
    pub fn validate_all(
        &self,
        statement: &Arc<Statement>,
        input: &super::analyzer::ValidationInput,
    ) -> Result<Vec<String>> {
        let violations = Vec::new();

        if let Statement::Dml(dml) = statement.as_ref() {
            // Validate and return error immediately if validation fails
            self.validate_dml_new(dml, input)?;
        }

        Ok(violations)
    }

    /// Validate DML for new architecture
    fn validate_dml_new(
        &self,
        statement: &DmlStatement,
        input: &super::analyzer::ValidationInput,
    ) -> Result<()> {
        match statement {
            DmlStatement::Select(select) => self.validate_select_new(select, input),
            DmlStatement::Insert {
                table,
                columns,
                source,
            } => self.validate_insert_new(table, columns.as_ref(), source, input),
            DmlStatement::Update { table, set, .. } => self.validate_update_new(table, set, input),
            DmlStatement::Delete { .. } => Ok(()),
        }
    }

    /// Validate SELECT for new architecture
    fn validate_select_new(
        &self,
        select: &SelectStatement,
        input: &super::analyzer::ValidationInput,
    ) -> Result<()> {
        // Check if SELECT contains aggregates by examining type annotations
        let has_aggregates = select.select.iter().enumerate().any(|(idx, _)| {
            let expr_id = ExpressionId::from_path(vec![idx]);
            input
                .expression_types
                .get(&expr_id)
                .map(|t| t.is_aggregate)
                .unwrap_or(false)
        });

        // Validate GROUP BY if present
        if !select.group_by.is_empty() {
            self.validate_group_by(select)?;
        } else if has_aggregates {
            self.validate_aggregates_without_group_by(select)?;
        }

        // Validate ORDER BY
        self.validate_order_by(select)?;

        // Validate HAVING
        if select.having.is_some() && select.group_by.is_empty() && !has_aggregates {
            return Err(Error::ExecutionError(
                "HAVING clause requires GROUP BY or aggregate functions".into(),
            ));
        }

        Ok(())
    }

    /// Validate INSERT for new architecture
    fn validate_insert_new(
        &self,
        table: &str,
        columns: Option<&Vec<String>>,
        source: &crate::parsing::ast::InsertSource,
        input: &super::analyzer::ValidationInput,
    ) -> Result<()> {
        // Get table schema
        let schema = input
            .schemas
            .get(table)
            .ok_or_else(|| Error::TableNotFound(table.to_string()))?;

        // Validate foreign key references
        for col in &schema.columns {
            if let Some(ref referenced_table) = col.references {
                if !input.schemas.contains_key(referenced_table) {
                    return Err(Error::ExecutionError(format!(
                        "Foreign key references non-existent table '{}'",
                        referenced_table
                    )));
                }

                // Also check type compatibility with referenced table's primary key
                if let Some(ref_schema) = input.schemas.get(referenced_table)
                    && let Some(pk_idx) = ref_schema.primary_key
                {
                    let pk_col = &ref_schema.columns[pk_idx];
                    if col.datatype != pk_col.datatype {
                        return Err(Error::ExecutionError(format!(
                            "Foreign key '{}' type {} doesn't match referenced primary key type {}",
                            col.name, col.datatype, pk_col.datatype
                        )));
                    }
                }
            }
        }

        use crate::parsing::ast::InsertSource;
        match source {
            InsertSource::Values(rows) => {
                // If columns are specified, check that all required columns are included
                if let Some(cols) = columns {
                    for schema_col in &schema.columns {
                        // Check if this column is required (non-nullable, no default)
                        if !schema_col.nullable && schema_col.default.is_none() {
                            // Check if this required column is in the provided columns list
                            if !cols.contains(&schema_col.name) {
                                return Err(Error::ExecutionError(format!(
                                    "Column '{}' is required but not provided (not nullable and has no default)",
                                    schema_col.name
                                )));
                            }
                        }
                    }
                }

                // Check column count
                let expected_cols = columns
                    .as_ref()
                    .map(|c| c.len())
                    .unwrap_or(schema.columns.len());

                for row in rows {
                    if row.len() != expected_cols {
                        return Err(Error::ExecutionError(format!(
                            "{} values but {} columns",
                            row.len(),
                            expected_cols
                        )));
                    }
                }

                // Type checking for literals
                let target_columns = if let Some(cols) = columns {
                    // Map column names to their schema definitions
                    cols.iter()
                        .map(|name| schema.columns.iter().find(|c| &c.name == name))
                        .collect::<Option<Vec<_>>>()
                        .ok_or_else(|| Error::ExecutionError("Invalid column name".to_string()))?
                } else {
                    // Use all columns in order
                    schema.columns.iter().collect()
                };

                // Check type compatibility for literal values
                use crate::coercion;
                use crate::parsing::ast::{Expression, Literal};

                for row in rows {
                    for (value_expr, target_col) in row.iter().zip(&target_columns) {
                        // Check literal types
                        if let Expression::Literal(lit) = value_expr {
                            let value_type = match lit {
                                Literal::String(_) => crate::types::data_type::DataType::Str,
                                Literal::Integer(_) => crate::types::data_type::DataType::I64,
                                Literal::Float(_) => crate::types::data_type::DataType::F64,
                                Literal::Boolean(_) => crate::types::data_type::DataType::Bool,
                                Literal::Null => crate::types::data_type::DataType::Null,
                                _ => continue, // Skip other literals for now
                            };

                            // Check NULL constraint first
                            if matches!(lit, Literal::Null) {
                                if !target_col.nullable {
                                    return Err(Error::ExecutionError(format!(
                                        "Cannot insert NULL into non-nullable column '{}'",
                                        target_col.name
                                    )));
                                }
                                // NULL is allowed for nullable columns regardless of type
                                continue;
                            }

                            // Check if value can be coerced to target type (for non-NULL values)
                            if !coercion::can_coerce(&value_type, &target_col.datatype) {
                                return Err(Error::TypeMismatch {
                                    expected: format!("{:?}", target_col.datatype),
                                    found: format!("{:?}", value_type),
                                });
                            }
                        }
                    }
                }
            }
            InsertSource::Select(select) => {
                // Check that SELECT produces correct column count
                // Handle SELECT * specially
                let select_cols =
                    if select.select.len() == 1 && matches!(&select.select[0].0, Expression::All) {
                        // SELECT * - count columns from the FROM clause tables
                        // Get the first table from the FROM clause to count columns
                        if !select.from.is_empty() {
                            // Extract table name from the first FROM clause entry
                            use crate::parsing::ast::FromClause;
                            let table_name = match &select.from[0] {
                                FromClause::Table { name, .. } => name,
                                FromClause::Join { left, .. } => {
                                    if let FromClause::Table { name, .. } = left.as_ref() {
                                        name
                                    } else {
                                        // Complex join, skip validation for now
                                        return Ok(());
                                    }
                                }
                            };

                            // Look up the table schema
                            if let Some(from_schema) = input.schemas.get(table_name) {
                                from_schema.columns.len()
                            } else {
                                // Table not found, let it fail later with proper error
                                return Ok(());
                            }
                        } else {
                            // No FROM clause for SELECT *, can't determine column count
                            return Ok(());
                        }
                    } else {
                        select.select.len()
                    };

                let expected_cols = columns
                    .as_ref()
                    .map(|c| c.len())
                    .unwrap_or(schema.columns.len());

                if select_cols != expected_cols {
                    return Err(Error::ExecutionError(format!(
                        "SELECT column count mismatch: expected {}, got {}",
                        expected_cols, select_cols
                    )));
                }
            }
            InsertSource::DefaultValues => {
                // Check that all columns have defaults or are nullable
                for col in &schema.columns {
                    if col.default.is_none() && !col.nullable {
                        return Err(Error::ExecutionError(format!(
                            "Column '{}' has no default value and is NOT NULL",
                            col.name
                        )));
                    }
                }
            }
        }

        Ok(())
    }

    /// Validate UPDATE for new architecture
    fn validate_update_new(
        &self,
        table: &str,
        set: &std::collections::BTreeMap<String, Option<Expression>>,
        input: &super::analyzer::ValidationInput,
    ) -> Result<()> {
        // Get table schema
        let schema = input
            .schemas
            .get(table)
            .ok_or_else(|| Error::TableNotFound(table.to_string()))?;

        // Check that columns exist and validate NULL constraints
        use crate::parsing::ast::{Expression, Literal};

        for (col_name, value_expr) in set {
            let column = schema
                .columns
                .iter()
                .find(|c| &c.name == col_name)
                .ok_or_else(|| Error::ColumnNotFound(col_name.clone()))?;

            // Check NULL constraint
            if let Some(expr) = value_expr
                && let Expression::Literal(Literal::Null) = expr
                && !column.nullable
            {
                return Err(Error::ExecutionError(format!(
                    "Cannot set non-nullable column '{}' to NULL",
                    col_name
                )));
            }
        }

        Ok(())
    }
}
