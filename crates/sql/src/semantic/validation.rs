//! Semantic validation phase
//!
//! This module validates semantic rules that aren't handled by
//! name resolution or type checking, including:
//! - GROUP BY validation
//! - Aggregate function usage rules
//! - ORDER BY validation
//! - DISTINCT usage
//! - DEFAULT value validation

use super::analyzer::ValidationInput;
use super::statement::ExpressionId;
use crate::error::{Error, Result};
use crate::parsing::ast::ddl::{ForeignKeyConstraint, ReferentialAction};
use crate::parsing::ast::{
    DdlStatement, DmlStatement, Expression, Literal, SelectStatement, Statement,
};
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
                    | Operator::Xor(l, r)
                    | Operator::Equal(l, r)
                    | Operator::NotEqual(l, r)
                    | Operator::GreaterThan(l, r)
                    | Operator::GreaterThanOrEqual(l, r)
                    | Operator::LessThan(l, r)
                    | Operator::LessThanOrEqual(l, r)
                    | Operator::Add(l, r)
                    | Operator::Concat(l, r)
                    | Operator::Subtract(l, r)
                    | Operator::Multiply(l, r)
                    | Operator::Divide(l, r)
                    | Operator::Remainder(l, r)
                    | Operator::Exponentiate(l, r)
                    | Operator::ILike(l, r)
                    | Operator::Like(l, r)
                    | Operator::BitwiseAnd(l, r)
                    | Operator::BitwiseOr(l, r)
                    | Operator::BitwiseXor(l, r)
                    | Operator::BitwiseShiftLeft(l, r)
                    | Operator::BitwiseShiftRight(l, r) => {
                        self.validate_group_by_expr(l, group_by_exprs)?;
                        self.validate_group_by_expr(r, group_by_exprs)?;
                    }
                    Operator::Not(e)
                    | Operator::Negate(e)
                    | Operator::Identity(e)
                    | Operator::Factorial(e)
                    | Operator::BitwiseNot(e) => {
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
                    | Operator::Xor(l, r)
                    | Operator::Equal(l, r)
                    | Operator::NotEqual(l, r)
                    | Operator::GreaterThan(l, r)
                    | Operator::GreaterThanOrEqual(l, r)
                    | Operator::LessThan(l, r)
                    | Operator::LessThanOrEqual(l, r)
                    | Operator::Add(l, r)
                    | Operator::Concat(l, r)
                    | Operator::Subtract(l, r)
                    | Operator::Multiply(l, r)
                    | Operator::Divide(l, r)
                    | Operator::Remainder(l, r)
                    | Operator::Exponentiate(l, r)
                    | Operator::ILike(l, r)
                    | Operator::Like(l, r)
                    | Operator::BitwiseAnd(l, r)
                    | Operator::BitwiseOr(l, r)
                    | Operator::BitwiseXor(l, r)
                    | Operator::BitwiseShiftLeft(l, r)
                    | Operator::BitwiseShiftRight(l, r) => {
                        Self::is_aggregate_expression(l) || Self::is_aggregate_expression(r)
                    }
                    Operator::Not(e)
                    | Operator::Negate(e)
                    | Operator::Identity(e)
                    | Operator::Factorial(e)
                    | Operator::BitwiseNot(e) => Self::is_aggregate_expression(e),
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
                    | Operator::Xor(l, r)
                    | Operator::Equal(l, r)
                    | Operator::NotEqual(l, r)
                    | Operator::GreaterThan(l, r)
                    | Operator::GreaterThanOrEqual(l, r)
                    | Operator::LessThan(l, r)
                    | Operator::LessThanOrEqual(l, r)
                    | Operator::Add(l, r)
                    | Operator::Concat(l, r)
                    | Operator::Subtract(l, r)
                    | Operator::Multiply(l, r)
                    | Operator::Divide(l, r)
                    | Operator::Remainder(l, r)
                    | Operator::Exponentiate(l, r)
                    | Operator::ILike(l, r)
                    | Operator::Like(l, r)
                    | Operator::BitwiseAnd(l, r)
                    | Operator::BitwiseOr(l, r)
                    | Operator::BitwiseXor(l, r)
                    | Operator::BitwiseShiftLeft(l, r)
                    | Operator::BitwiseShiftRight(l, r) => {
                        self.validate_aggregate_context(l)?;
                        self.validate_aggregate_context(r)?;
                    }
                    Operator::Not(e)
                    | Operator::Negate(e)
                    | Operator::Identity(e)
                    | Operator::Factorial(e)
                    | Operator::BitwiseNot(e) => {
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
                    | Operator::Xor(l, r)
                    | Operator::Equal(l, r)
                    | Operator::NotEqual(l, r)
                    | Operator::GreaterThan(l, r)
                    | Operator::GreaterThanOrEqual(l, r)
                    | Operator::LessThan(l, r)
                    | Operator::LessThanOrEqual(l, r)
                    | Operator::Add(l, r)
                    | Operator::Concat(l, r)
                    | Operator::Subtract(l, r)
                    | Operator::Multiply(l, r)
                    | Operator::Divide(l, r)
                    | Operator::Remainder(l, r)
                    | Operator::Exponentiate(l, r)
                    | Operator::ILike(l, r)
                    | Operator::Like(l, r)
                    | Operator::BitwiseAnd(l, r)
                    | Operator::BitwiseOr(l, r)
                    | Operator::BitwiseXor(l, r)
                    | Operator::BitwiseShiftLeft(l, r)
                    | Operator::BitwiseShiftRight(l, r) => {
                        Self::validate_no_nested_aggregates(l)?;
                        Self::validate_no_nested_aggregates(r)?;
                    }
                    Operator::Not(e)
                    | Operator::Negate(e)
                    | Operator::Identity(e)
                    | Operator::Factorial(e)
                    | Operator::BitwiseNot(e) => {
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

        match statement.as_ref() {
            Statement::Dml(dml) => {
                // Validate and return error immediately if validation fails
                self.validate_dml_new(dml, input)?;
            }
            Statement::Ddl(ddl) => {
                // Validate DDL statements
                self.validate_ddl(ddl, input)?;
            }
            _ => {} // Other statements don't need semantic validation
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
            DmlStatement::Values(values) => self.validate_values_new(values, input),
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
                    if col.data_type != pk_col.data_type {
                        return Err(Error::ExecutionError(format!(
                            "Foreign key '{}' type {} doesn't match referenced primary key type {}",
                            col.name, col.data_type, pk_col.data_type
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
                                return Err(Error::NullConstraintViolation(
                                    schema_col.name.clone(),
                                ));
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
                    let mut mapped_columns = Vec::new();
                    for name in cols {
                        let col = schema
                            .columns
                            .iter()
                            .find(|c| &c.name == name)
                            .ok_or_else(|| Error::ColumnNotFound(name.clone()))?;
                        mapped_columns.push(col);
                    }
                    mapped_columns
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
                        match value_expr {
                            Expression::Literal(lit) => {
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
                                if !coercion::can_coerce(&value_type, &target_col.data_type) {
                                    return Err(Error::TypeMismatch {
                                        expected: format!("{:?}", target_col.data_type),
                                        found: format!("{:?}", value_type),
                                    });
                                }
                            }
                            Expression::ArrayLiteral(elements) => {
                                // For array literals, check if it can be coerced to the target type
                                // Arrays can be coerced to LIST types
                                if let crate::types::data_type::DataType::List(elem_type) =
                                    &target_col.data_type
                                {
                                    // Check each element can be coerced to the element type
                                    for elem in elements {
                                        if let Expression::Literal(lit) = elem {
                                            let elem_value_type = match lit {
                                                Literal::String(_) => {
                                                    crate::types::data_type::DataType::Str
                                                }
                                                Literal::Integer(_) => {
                                                    crate::types::data_type::DataType::I64
                                                }
                                                Literal::Float(_) => {
                                                    crate::types::data_type::DataType::F64
                                                }
                                                Literal::Boolean(_) => {
                                                    crate::types::data_type::DataType::Bool
                                                }
                                                Literal::Null => {
                                                    crate::types::data_type::DataType::Null
                                                }
                                                _ => continue,
                                            };

                                            if !matches!(lit, Literal::Null)
                                                && !coercion::can_coerce(
                                                    &elem_value_type,
                                                    elem_type,
                                                )
                                            {
                                                return Err(Error::TypeMismatch {
                                                    expected: format!("{:?}", elem_type),
                                                    found: format!("{:?}", elem_value_type),
                                                });
                                            }
                                        }
                                        // For non-literal expressions in array, skip validation for now
                                    }
                                } else if let crate::types::data_type::DataType::Array(
                                    elem_type,
                                    size,
                                ) = &target_col.data_type
                                {
                                    // Check array size if specified
                                    if let Some(expected_size) = size
                                        && elements.len() != *expected_size
                                    {
                                        return Err(Error::ExecutionError(format!(
                                            "Array size mismatch: expected {} elements, got {}",
                                            expected_size,
                                            elements.len()
                                        )));
                                    }
                                    // Check element types
                                    for elem in elements {
                                        if let Expression::Literal(lit) = elem {
                                            let elem_value_type = match lit {
                                                Literal::String(_) => {
                                                    crate::types::data_type::DataType::Str
                                                }
                                                Literal::Integer(_) => {
                                                    crate::types::data_type::DataType::I64
                                                }
                                                Literal::Float(_) => {
                                                    crate::types::data_type::DataType::F64
                                                }
                                                Literal::Boolean(_) => {
                                                    crate::types::data_type::DataType::Bool
                                                }
                                                Literal::Null => {
                                                    crate::types::data_type::DataType::Null
                                                }
                                                _ => continue,
                                            };

                                            if !matches!(lit, Literal::Null)
                                                && !coercion::can_coerce(
                                                    &elem_value_type,
                                                    elem_type,
                                                )
                                            {
                                                return Err(Error::TypeMismatch {
                                                    expected: format!("{:?}", elem_type),
                                                    found: format!("{:?}", elem_value_type),
                                                });
                                            }
                                        }
                                    }
                                } else {
                                    return Err(Error::TypeMismatch {
                                        expected: format!("{:?}", target_col.data_type),
                                        found: "ARRAY".to_string(),
                                    });
                                }
                            }
                            _ => {
                                // Skip validation for complex expressions
                                continue;
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
                                FromClause::Subquery { .. } => {
                                    // Skip validation for subqueries for now
                                    return Ok(());
                                }
                                FromClause::Series { .. } => {
                                    // Skip validation for SERIES
                                    return Ok(());
                                }
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

    /// Validate VALUES statement for new architecture
    fn validate_values_new(
        &self,
        values: &crate::parsing::ast::dml::ValuesStatement,
        _input: &ValidationInput,
    ) -> Result<()> {
        if values.rows.is_empty() {
            return Err(Error::ExecutionError(
                "VALUES must have at least one row".into(),
            ));
        }

        // Check that all rows have the same number of columns
        let num_columns = values.rows[0].len();
        for row in values.rows.iter() {
            if row.len() != num_columns {
                return Err(Error::ExecutionError(format!(
                    "{} values but {} columns",
                    row.len(),
                    num_columns
                )));
            }
        }

        // Type inference will handle type compatibility across rows
        Ok(())
    }

    /// Validate DDL statements
    fn validate_ddl(&self, statement: &DdlStatement, input: &ValidationInput) -> Result<()> {
        match statement {
            DdlStatement::CreateTable {
                name,
                columns,
                foreign_keys,
                if_not_exists,
            } => {
                self.validate_create_table(name, columns, foreign_keys, *if_not_exists, input)?;
            }
            DdlStatement::DropTable { names, cascade, .. } => {
                self.validate_drop_table(names, *cascade, input)?;
            }
            _ => {} // Other DDL statements don't need special validation yet
        }
        Ok(())
    }

    /// Validate CREATE TABLE statement
    fn validate_create_table(
        &self,
        table_name: &str,
        columns: &[crate::parsing::ast::Column],
        foreign_keys: &[ForeignKeyConstraint],
        if_not_exists: bool,
        input: &ValidationInput,
    ) -> Result<()> {
        // Check if table already exists
        if input.schemas.contains_key(table_name) {
            if if_not_exists {
                // IF NOT EXISTS specified, silently ignore the duplicate
                return Ok(());
            }
            return Err(Error::DuplicateTable(table_name.to_string()));
        }

        // Validate each foreign key constraint
        for fk in foreign_keys {
            self.validate_foreign_key_constraint(fk, columns, table_name, input)?;
        }

        // Also validate inline foreign key references in columns
        for col in columns {
            if let Some(ref referenced_table) = col.references {
                // Check if referenced table exists (allow self-references)
                let is_self_reference =
                    referenced_table.to_lowercase() == table_name.to_lowercase();
                if !is_self_reference && !input.schemas.contains_key(referenced_table) {
                    return Err(Error::ExecutionError(format!(
                        "Foreign key references non-existent table '{}'",
                        referenced_table
                    )));
                }

                // For self-references, we can't validate the schema yet
                if !is_self_reference {
                    // Check if referenced table has a primary key
                    let ref_schema = &input.schemas[referenced_table];
                    if ref_schema.primary_key.is_none() {
                        return Err(Error::ExecutionError(format!(
                            "Foreign key references table '{}' which has no primary key",
                            referenced_table
                        )));
                    }

                    // Check type compatibility with primary key
                    if let Some(pk_idx) = ref_schema.primary_key {
                        let pk_col = &ref_schema.columns[pk_idx];
                        if col.data_type != pk_col.data_type {
                            return Err(Error::ExecutionError(format!(
                                "Foreign key column '{}' type {:?} doesn't match referenced primary key '{}' type {:?}",
                                col.name, col.data_type, pk_col.name, pk_col.data_type
                            )));
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Validate a foreign key constraint
    fn validate_foreign_key_constraint(
        &self,
        fk: &ForeignKeyConstraint,
        columns: &[crate::parsing::ast::Column],
        table_name: &str,
        input: &ValidationInput,
    ) -> Result<()> {
        // Check that all referencing columns exist in the table
        for col_name in &fk.columns {
            if !columns.iter().any(|c| &c.name == col_name) {
                return Err(Error::ExecutionError(format!(
                    "Foreign key column not found: '{}'",
                    col_name
                )));
            }
        }

        // Check that referenced table exists (allow self-references)
        let is_self_reference = fk.referenced_table.to_lowercase() == table_name.to_lowercase();
        if !is_self_reference && !input.schemas.contains_key(&fk.referenced_table) {
            return Err(Error::ExecutionError(format!(
                "Foreign key references non-existent table '{}'",
                fk.referenced_table
            )));
        }

        // For self-references, we can't validate against the schema yet
        // as the table is still being created
        if is_self_reference {
            // Just validate that the columns exist and have matching types
            if fk.columns.len() == 1 && fk.referenced_columns.len() == 1 {
                let col_name = &fk.columns[0];
                let ref_col_name = &fk.referenced_columns[0];

                let col = columns
                    .iter()
                    .find(|c| &c.name == col_name)
                    .ok_or_else(|| {
                        Error::ExecutionError(format!(
                            "Foreign key column '{}' not found",
                            col_name
                        ))
                    })?;

                let ref_col = columns
                    .iter()
                    .find(|c| &c.name == ref_col_name)
                    .ok_or_else(|| {
                        Error::ExecutionError(format!(
                            "Referenced column '{}' not found in self-referencing table",
                            ref_col_name
                        ))
                    })?;

                if col.data_type != ref_col.data_type {
                    return Err(Error::ExecutionError(format!(
                        "Self-referencing foreign key column '{}' type {:?} doesn't match referenced column '{}' type {:?}",
                        col_name, col.data_type, ref_col_name, ref_col.data_type
                    )));
                }
            }
            // Skip further validation for self-references
            return Ok(());
        }

        let ref_schema = &input.schemas[&fk.referenced_table];

        // If referenced columns are specified, validate them
        if !fk.referenced_columns.is_empty() {
            // Check that all referenced columns exist
            for col_name in &fk.referenced_columns {
                if !ref_schema.columns.iter().any(|c| &c.name == col_name) {
                    return Err(Error::ExecutionError(format!(
                        "Referenced column not found: '{}' in table '{}'",
                        col_name, fk.referenced_table
                    )));
                }
            }

            // For now, we require that referenced columns form the primary key
            // (Later we could support UNIQUE constraints too)
            if fk.referenced_columns.len() == 1 {
                let ref_col_name = &fk.referenced_columns[0];
                let pk_idx = ref_schema.primary_key.ok_or_else(|| {
                    Error::ExecutionError(format!(
                        "Foreign key references table '{}' which has no primary key",
                        fk.referenced_table
                    ))
                })?;

                let pk_col = &ref_schema.columns[pk_idx];
                if &pk_col.name != ref_col_name {
                    return Err(Error::ExecutionError(format!(
                        "Foreign key must reference primary key column, but '{}' is not the primary key of '{}'",
                        ref_col_name, fk.referenced_table
                    )));
                }
            } else {
                // Multi-column foreign keys not yet supported
                return Err(Error::ExecutionError(
                    "Multi-column foreign keys are not yet supported".into(),
                ));
            }
        } else {
            // No referenced columns specified - must reference primary key
            if ref_schema.primary_key.is_none() {
                return Err(Error::ExecutionError(format!(
                    "Foreign key references table '{}' which has no primary key",
                    fk.referenced_table
                )));
            }
        }

        // Check type compatibility
        if fk.columns.len() == 1 && fk.referenced_columns.len() <= 1 {
            let col_name = &fk.columns[0];
            let col = columns
                .iter()
                .find(|c| &c.name == col_name)
                .ok_or_else(|| {
                    Error::ExecutionError(format!("Foreign key column '{}' not found", col_name))
                })?;

            let pk_idx = ref_schema.primary_key.ok_or_else(|| {
                Error::ExecutionError(format!(
                    "Referenced table '{}' has no primary key",
                    fk.referenced_table
                ))
            })?;
            let pk_col = &ref_schema.columns[pk_idx];

            if col.data_type != pk_col.data_type {
                return Err(Error::ExecutionError(format!(
                    "Foreign key type mismatch: column '{}' type {:?} doesn't match referenced primary key type {:?}",
                    col.name, col.data_type, pk_col.data_type
                )));
            }
        }

        // Validate referential actions
        self.validate_referential_action(fk.on_delete, "ON DELETE")?;
        self.validate_referential_action(fk.on_update, "ON UPDATE")?;

        Ok(())
    }

    /// Validate referential action
    fn validate_referential_action(&self, action: ReferentialAction, _context: &str) -> Result<()> {
        // All referential actions are now supported
        match action {
            ReferentialAction::NoAction
            | ReferentialAction::Restrict
            | ReferentialAction::Cascade
            | ReferentialAction::SetNull
            | ReferentialAction::SetDefault => Ok(()),
        }
    }

    /// Validate DROP TABLE statement
    fn validate_drop_table(
        &self,
        table_names: &[String],
        cascade: bool,
        input: &ValidationInput,
    ) -> Result<()> {
        for table_name in table_names {
            // Check if table exists
            if !input.schemas.contains_key(table_name) {
                // This might be OK with IF EXISTS, so we don't error here
                continue;
            }

            // Check if any other tables reference this one via foreign keys
            if !cascade {
                for (other_table_name, other_schema) in input.schemas.iter() {
                    if other_table_name == table_name {
                        continue;
                    }

                    // Check inline foreign key references
                    for col in &other_schema.columns {
                        if let Some(ref ref_table) = col.references
                            && ref_table == table_name
                        {
                            return Err(Error::ExecutionError(format!(
                                "Cannot drop table '{}' because table '{}' references it. Use CASCADE to drop dependent objects.",
                                table_name, other_table_name
                            )));
                        }
                    }

                    // Check table-level foreign key constraints
                    for fk in &other_schema.foreign_keys {
                        if fk.referenced_table.to_lowercase() == table_name.to_lowercase() {
                            return Err(Error::ExecutionError(format!(
                                "Cannot drop table '{}' because table '{}' references it. Use CASCADE to drop dependent objects.",
                                table_name, other_table_name
                            )));
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
