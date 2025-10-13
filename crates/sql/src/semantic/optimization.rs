//! Optimization metadata building phase
//!
//! This module builds immutable metadata that helps the planner
//! optimize query execution without re-analyzing the query:
//! - Predicate templates for conflict detection
//! - Join structure analysis
//! - Index applicability information
//! - Expression templates (future)

use super::analyzer::JoinHint;
use super::statement::{ExpressionId, PredicateTemplate, PredicateValue};
use crate::error::Result;
use crate::parsing::ast::{
    DmlStatement, Expression, FromClause, InsertSource, Literal, Operator, SelectStatement,
    Statement,
};
use crate::types::Value;
use crate::types::schema::Table;
use std::collections::HashMap;
use std::sync::Arc;

/// Builds optimization metadata for the planner
pub struct MetadataBuilder {
    schemas: HashMap<String, Table>,
}

/// Helper struct for range extraction
struct RangeInfo {
    table: String,
    column: String,
    lower: Option<(PredicateValue, bool)>,
    upper: Option<(PredicateValue, bool)>,
}

impl MetadataBuilder {
    /// Create a new metadata builder
    pub fn new(schemas: HashMap<String, Table>) -> Self {
        Self { schemas }
    }

    /// Convert an expression to a PredicateValue
    fn expression_to_predicate_value(&self, expr: &Expression) -> PredicateValue {
        match expr {
            Expression::Literal(lit) => {
                let value = match lit {
                    Literal::Null => Value::Null,
                    Literal::Boolean(b) => Value::Bool(*b),
                    Literal::Integer(i) => {
                        if *i >= i32::MIN as i128 && *i <= i32::MAX as i128 {
                            Value::I32(*i as i32)
                        } else if *i >= i64::MIN as i128 && *i <= i64::MAX as i128 {
                            Value::I64(*i as i64)
                        } else {
                            Value::I128(*i)
                        }
                    }
                    Literal::Float(f) => Value::F64(*f),
                    Literal::String(s) => Value::string(s.clone()),
                    Literal::Bytea(b) => Value::Bytea(b.clone()),
                    Literal::Date(d) => Value::Date(*d),
                    Literal::Time(t) => Value::Time(*t),
                    Literal::Timestamp(ts) => Value::Timestamp(*ts),
                    Literal::Interval(i) => Value::Interval(i.clone()),
                };
                PredicateValue::Constant(value)
            }
            Expression::Parameter(idx) => PredicateValue::Parameter(*idx),
            _ => PredicateValue::Expression(ExpressionId::from_path(vec![])),
        }
    }

    /// Build predicates for new phase architecture
    pub fn build_predicates(
        &self,
        statement: &Arc<Statement>,
        input: &super::analyzer::OptimizationInput,
    ) -> Result<Vec<PredicateTemplate>> {
        let mut templates = Vec::new();

        if let Statement::Dml(dml) = statement.as_ref() {
            match dml {
                DmlStatement::Select(select) => {
                    // Count predicates before extracting from WHERE
                    let initial_count = templates.len();

                    // Extract predicates from WHERE clause (including subqueries)
                    if let Some(where_expr) = &select.r#where {
                        self.extract_templates_new(where_expr, &mut templates, input);
                    }

                    // Check if any predicates were added for the main query tables
                    // (not counting subquery predicates)
                    let mut has_main_table_predicates = false;
                    for source in input.tables {
                        let table_name = match source {
                            super::resolution::TableSource::Table { name, .. } => name,
                            super::resolution::TableSource::Subquery { alias, .. } => alias,
                        };
                        for template in &templates[initial_count..] {
                            if self.template_covers_table(template, table_name) {
                                has_main_table_predicates = true;
                                break;
                            }
                        }
                        if has_main_table_predicates {
                            break;
                        }
                    }

                    // If no predicates cover the main tables, add FullTable predicates
                    if !has_main_table_predicates {
                        for source in input.tables {
                            let table_name = match source {
                                super::resolution::TableSource::Table { name, .. } => name.clone(),
                                super::resolution::TableSource::Subquery { alias, .. } => {
                                    alias.clone()
                                }
                            };
                            templates.push(PredicateTemplate::FullTable { table: table_name });
                        }
                    }
                }
                DmlStatement::Insert {
                    table,
                    columns,
                    source,
                } => {
                    self.build_insert_templates(
                        table,
                        columns.as_ref(),
                        source,
                        &mut templates,
                        input,
                    )?;
                }
                DmlStatement::Update { table, r#where, .. } => {
                    if let Some(where_expr) = r#where {
                        self.extract_templates_new(where_expr, &mut templates, input);
                    }

                    // Fall back to FullTable if no predicates extracted
                    if templates.is_empty() {
                        templates.push(PredicateTemplate::FullTable {
                            table: table.clone(),
                        });
                    }
                }
                DmlStatement::Delete { table, r#where } => {
                    if let Some(where_expr) = r#where {
                        self.extract_templates_new(where_expr, &mut templates, input);
                    }

                    // Fall back to FullTable if no predicates extracted
                    if templates.is_empty() {
                        templates.push(PredicateTemplate::FullTable {
                            table: table.clone(),
                        });
                    }
                }
                DmlStatement::Values(_) => {
                    // VALUES statements don't have predicates
                }
            }
        }

        Ok(templates)
    }

    /// Analyze joins for new phase architecture
    pub fn analyze_joins(
        &self,
        statement: &Arc<Statement>,
        input: &super::analyzer::OptimizationInput,
    ) -> Result<Vec<JoinHint>> {
        let mut hints = Vec::new();

        if let Statement::Dml(DmlStatement::Select(select)) = statement.as_ref() {
            // Analyze each join in the FROM clause
            for from_clause in &select.from {
                self.analyze_from_joins(from_clause, &mut hints, input)?;
            }
        }

        Ok(hints)
    }

    /// Recursively analyze joins in FROM clause
    fn analyze_from_joins(
        &self,
        from: &crate::parsing::ast::FromClause,
        hints: &mut Vec<JoinHint>,
        input: &super::analyzer::OptimizationInput,
    ) -> Result<()> {
        use crate::parsing::ast::FromClause;

        if let FromClause::Join {
            left,
            right,
            r#type: _,
            predicate,
        } = from
        {
            // Recursively analyze nested joins
            self.analyze_from_joins(left, hints, input)?;
            self.analyze_from_joins(right, hints, input)?;

            // Extract table names from left and right
            let left_table = Self::extract_table_from_clause(left);
            let right_table = Self::extract_table_from_clause(right);

            if let (Some(left_table), Some(right_table)) = (left_table, right_table) {
                // Estimate selectivity based on join predicate
                let selectivity = if let Some(pred) = predicate {
                    self.estimate_join_selectivity(pred, &left_table, &right_table, input)
                } else {
                    1.0 // Cross join has selectivity 1.0
                };

                // Join type was here but is currently unused
                // Could be re-added when join type optimization is implemented

                hints.push(JoinHint {
                    left_table,
                    right_table,
                    selectivity_estimate: selectivity,
                });
            }
        }

        Ok(())
    }

    /// Extract table name from FROM clause
    fn extract_table_from_clause(from: &crate::parsing::ast::FromClause) -> Option<String> {
        use crate::parsing::ast::FromClause;

        match from {
            FromClause::Table { name, .. } => Some(name.clone()),
            FromClause::Subquery { alias, .. } => Some(alias.name.clone()),
            FromClause::Join { left, .. } => Self::extract_table_from_clause(left),
        }
    }

    /// Estimate selectivity of join predicate
    fn estimate_join_selectivity(
        &self,
        pred: &Expression,
        left_table: &str,
        right_table: &str,
        input: &super::analyzer::OptimizationInput,
    ) -> f64 {
        use crate::parsing::ast::Operator;

        match pred {
            Expression::Operator(Operator::Equal(left, right)) => {
                // Check if it's an equi-join on indexed columns
                if let (Expression::Column(_lt, lc), Expression::Column(_rt, rc)) =
                    (left.as_ref(), right.as_ref())
                {
                    let left_indexed = self.is_column_indexed(left_table, lc, input);
                    let right_indexed = self.is_column_indexed(right_table, rc, input);

                    if left_indexed && right_indexed {
                        // Both columns indexed - very selective
                        0.001
                    } else if left_indexed || right_indexed {
                        // One column indexed - moderately selective
                        0.01
                    } else {
                        // No indexes - less selective
                        0.1
                    }
                } else {
                    // Not a simple column comparison
                    0.2
                }
            }
            Expression::Operator(Operator::And(left, right)) => {
                // AND of predicates - multiply selectivities
                let left_sel = self.estimate_join_selectivity(left, left_table, right_table, input);
                let right_sel =
                    self.estimate_join_selectivity(right, left_table, right_table, input);
                left_sel * right_sel
            }
            Expression::Operator(Operator::Or(left, right)) => {
                // OR of predicates - use inclusion-exclusion principle
                let left_sel = self.estimate_join_selectivity(left, left_table, right_table, input);
                let right_sel =
                    self.estimate_join_selectivity(right, left_table, right_table, input);
                left_sel + right_sel - (left_sel * right_sel)
            }
            _ => {
                // Conservative estimate for complex predicates
                0.3
            }
        }
    }

    /// Check if a column is indexed
    fn is_column_indexed(
        &self,
        table: &str,
        column: &str,
        _input: &super::analyzer::OptimizationInput,
    ) -> bool {
        if let Some(schema) = self.schemas.get(table) {
            schema
                .columns
                .iter()
                .any(|col| col.name == column && col.index)
        } else {
            false
        }
    }

    /// Extract predicate templates from WHERE clause (new architecture)
    fn extract_templates_new(
        &self,
        expr: &Expression,
        templates: &mut Vec<PredicateTemplate>,
        input: &super::analyzer::OptimizationInput,
    ) {
        if let Expression::Operator(op) = expr {
            use Operator::*;
            match op {
                Equal(left, right) => {
                    if let Expression::Column(table, col) = left.as_ref() {
                        let table_name = self.resolve_table_name(table.as_deref(), col, input);
                        let value = self.expression_to_predicate_value(right);

                        // Check for primary key equality
                        if let Some(schema) =
                            self.get_schema_for_column(table.as_deref(), col, input)
                            && let Some(pk_idx) = schema.primary_key
                            && schema.columns[pk_idx].name == *col
                        {
                            templates.push(PredicateTemplate::PrimaryKey {
                                table: table_name,
                                value,
                            });
                            return;
                        }

                        // Check for indexed column
                        if self.is_indexed_new(table.as_deref(), col, input) {
                            templates.push(PredicateTemplate::IndexedColumn {
                                table: table_name,
                                column: col.clone(),
                                value,
                            });
                        } else {
                            // Regular equality predicate (non-indexed)
                            templates.push(PredicateTemplate::Equality {
                                table: table_name,
                                column_name: col.clone(),
                                value_expr: value,
                            });
                        }
                    }
                }
                And(left, right) => {
                    // Check if both sides are range conditions on the same column
                    // This handles BETWEEN-style queries: col >= X AND col <= Y
                    if let (Some(range1), Some(range2)) = (
                        self.try_extract_range(left, input),
                        self.try_extract_range(right, input),
                    ) && range1.table == range2.table
                        && range1.column == range2.column
                    {
                        // Merge the two ranges
                        let merged = PredicateTemplate::Range {
                            table: range1.table,
                            column_name: range1.column,
                            lower: range1.lower.or(range2.lower),
                            upper: range1.upper.or(range2.upper),
                        };
                        templates.push(merged);
                        return;
                    }

                    // Otherwise, extract separately
                    self.extract_templates_new(left, templates, input);
                    self.extract_templates_new(right, templates, input);
                }

                // Range predicates
                GreaterThan(left, right) => {
                    // Check for subqueries in the comparison
                    if let Expression::Subquery(select) = right.as_ref() {
                        self.extract_subquery_predicates(select, templates);
                    }
                    if let Expression::Subquery(select) = left.as_ref() {
                        self.extract_subquery_predicates(select, templates);
                    }

                    if let Expression::Column(table, col) = left.as_ref() {
                        let table_name = self.resolve_table_name(table.as_deref(), col, input);
                        let value = self.expression_to_predicate_value(right);
                        templates.push(PredicateTemplate::Range {
                            table: table_name,
                            column_name: col.clone(),
                            lower: Some((value, false)), // exclusive
                            upper: None,
                        });
                    }
                }

                GreaterThanOrEqual(left, right) => {
                    if let Expression::Column(table, col) = left.as_ref() {
                        let table_name = self.resolve_table_name(table.as_deref(), col, input);
                        let value = self.expression_to_predicate_value(right);
                        templates.push(PredicateTemplate::Range {
                            table: table_name,
                            column_name: col.clone(),
                            lower: Some((value, true)), // inclusive
                            upper: None,
                        });
                    }
                }

                LessThan(left, right) => {
                    if let Expression::Column(table, col) = left.as_ref() {
                        let table_name = self.resolve_table_name(table.as_deref(), col, input);
                        let value = self.expression_to_predicate_value(right);
                        templates.push(PredicateTemplate::Range {
                            table: table_name,
                            column_name: col.clone(),
                            lower: None,
                            upper: Some((value, false)), // exclusive
                        });
                    }
                }

                LessThanOrEqual(left, right) => {
                    if let Expression::Column(table, col) = left.as_ref() {
                        let table_name = self.resolve_table_name(table.as_deref(), col, input);
                        let value = self.expression_to_predicate_value(right);
                        templates.push(PredicateTemplate::Range {
                            table: table_name,
                            column_name: col.clone(),
                            lower: None,
                            upper: Some((value, true)), // inclusive
                        });
                    }
                }

                // ILIKE predicates (case-insensitive)
                ILike(left, right) => {
                    if let Expression::Column(table, col) = left.as_ref() {
                        let table_name = self.resolve_table_name(table.as_deref(), col, input);

                        // Extract pattern as string value
                        if let Expression::Literal(Literal::String(pattern)) = right.as_ref() {
                            // For now, treat ILIKE same as LIKE in templates
                            // In a real index implementation, we'd handle case-insensitive matching
                            templates.push(PredicateTemplate::Like {
                                table: table_name,
                                column_name: col.clone(),
                                pattern: pattern.clone(),
                            });
                        } else {
                            templates.push(PredicateTemplate::Like {
                                table: table_name,
                                column_name: col.clone(),
                                pattern: String::new(),
                            });
                        }
                    }
                }

                // LIKE predicates
                Like(left, right) => {
                    if let Expression::Column(table, col) = left.as_ref() {
                        let table_name = self.resolve_table_name(table.as_deref(), col, input);

                        // Extract pattern as string value
                        if let Expression::Literal(Literal::String(pattern)) = right.as_ref() {
                            templates.push(PredicateTemplate::Like {
                                table: table_name,
                                column_name: col.clone(),
                                pattern: pattern.clone(),
                            });
                        } else {
                            // Pattern comes from parameter - for now just use empty pattern
                            // In a real implementation, we'd handle parameter substitution
                            templates.push(PredicateTemplate::Like {
                                table: table_name,
                                column_name: col.clone(),
                                pattern: String::new(), // Will be filled from params
                            });
                        }
                    }
                }

                // IS NULL / IS NOT NULL predicates
                Is(expr, literal) => {
                    if let Expression::Column(table, col) = expr.as_ref() {
                        let table_name = self.resolve_table_name(table.as_deref(), col, input);

                        // Check if it's IS NULL or IS NOT NULL based on the literal
                        if let Literal::Null = literal {
                            templates.push(PredicateTemplate::IsNull {
                                table: table_name,
                                column_name: col.clone(),
                            });
                        }
                    }
                }

                // Handle NOT operator for IS NOT NULL
                Not(expr) => {
                    // Check if it's NOT (col IS NULL) which is equivalent to IS NOT NULL
                    if let Expression::Operator(Operator::Is(inner, Literal::Null)) = expr.as_ref()
                    {
                        if let Expression::Column(table, col) = inner.as_ref() {
                            let table_name = self.resolve_table_name(table.as_deref(), col, input);
                            templates.push(PredicateTemplate::IsNotNull {
                                table: table_name,
                                column_name: col.clone(),
                            });
                        }
                    } else {
                        // For other NOT expressions, just recurse
                        self.extract_templates_new(expr, templates, input);
                    }
                }

                InList { expr, list, .. } => {
                    if let Expression::Column(table, col) = expr.as_ref() {
                        let table_name = self.resolve_table_name(table.as_deref(), col, input);
                        let values: Vec<PredicateValue> = list
                            .iter()
                            .map(|v| self.expression_to_predicate_value(v))
                            .collect();

                        // Check if it's on the primary key
                        if let Some(schema) =
                            self.get_schema_for_column(table.as_deref(), col, input)
                            && let Some(pk_idx) = schema.primary_key
                            && schema.columns[pk_idx].name == *col
                        {
                            // For primary key IN lists, create individual PK predicates
                            for value in values {
                                templates.push(PredicateTemplate::PrimaryKey {
                                    table: table_name.clone(),
                                    value,
                                });
                            }
                        } else {
                            // Regular IN list predicate
                            templates.push(PredicateTemplate::InList {
                                table: table_name,
                                column_name: col.clone(),
                                values,
                            });
                        }
                    }
                }

                // Handle subqueries - extract predicates from nested SELECT
                InSubquery { subquery, .. } => {
                    // InSubquery contains an Expression that should be Expression::Subquery
                    if let Expression::Subquery(select_stmt) = subquery.as_ref() {
                        self.extract_subquery_predicates(select_stmt, templates);
                    }
                }

                Exists { subquery, .. } => {
                    // Exists contains an Expression that should be Expression::Subquery
                    if let Expression::Subquery(select_stmt) = subquery.as_ref() {
                        self.extract_subquery_predicates(select_stmt, templates);
                    }
                }

                _ => {}
            }
        }

        // Also handle standalone subquery expressions
        if let Expression::Subquery(select_stmt) = expr {
            self.extract_subquery_predicates(select_stmt, templates);
        }
    }

    /// Extract predicates from a subquery
    fn extract_subquery_predicates(
        &self,
        subquery: &SelectStatement,
        templates: &mut Vec<PredicateTemplate>,
    ) {
        // Extract tables from FROM clause
        for from_clause in &subquery.from {
            Self::extract_tables_from_clause(from_clause, templates);
        }

        // Also check the WHERE clause for nested subqueries
        if let Some(where_expr) = &subquery.r#where {
            self.extract_subquery_predicates_from_expr(where_expr, templates);
        }
    }

    /// Extract subquery predicates from an expression (for nested subqueries)
    fn extract_subquery_predicates_from_expr(
        &self,
        expr: &Expression,
        templates: &mut Vec<PredicateTemplate>,
    ) {
        match expr {
            Expression::Operator(op) => {
                use Operator::*;
                match op {
                    InSubquery { subquery, .. } => {
                        if let Expression::Subquery(select_stmt) = subquery.as_ref() {
                            self.extract_subquery_predicates(select_stmt, templates);
                        }
                    }
                    Exists { subquery, .. } => {
                        if let Expression::Subquery(select_stmt) = subquery.as_ref() {
                            self.extract_subquery_predicates(select_stmt, templates);
                        }
                    }
                    And(left, right) | Or(left, right) => {
                        self.extract_subquery_predicates_from_expr(left, templates);
                        self.extract_subquery_predicates_from_expr(right, templates);
                    }
                    Not(expr) => {
                        self.extract_subquery_predicates_from_expr(expr, templates);
                    }
                    _ => {}
                }
            }
            Expression::Subquery(select_stmt) => {
                self.extract_subquery_predicates(select_stmt, templates);
            }
            _ => {}
        }
    }

    /// Helper to extract tables from a FROM clause
    fn extract_tables_from_clause(
        from_clause: &FromClause,
        templates: &mut Vec<PredicateTemplate>,
    ) {
        match from_clause {
            FromClause::Table { name, .. } => {
                // Add a full table read predicate for each table in the subquery
                // This is conservative but ensures proper locking
                templates.push(PredicateTemplate::FullTable {
                    table: name.clone(),
                });
            }
            FromClause::Subquery { .. } => {
                // Subqueries don't directly reference tables at this level
                // The tables they use internally are handled when the subquery is analyzed
            }
            FromClause::Join { left, right, .. } => {
                // Recursively extract tables from joins
                Self::extract_tables_from_clause(left, templates);
                Self::extract_tables_from_clause(right, templates);
            }
        }
    }

    /// Check if a predicate template covers a given table
    fn template_covers_table(&self, template: &PredicateTemplate, table: &str) -> bool {
        match template {
            PredicateTemplate::FullTable { table: t } => t == table,
            PredicateTemplate::PrimaryKey { table: t, .. } => t == table,
            PredicateTemplate::IndexedColumn { table: t, .. } => t == table,
            PredicateTemplate::Equality { table: t, .. } => t == table,
            PredicateTemplate::Range { table: t, .. } => t == table,
            PredicateTemplate::InList { table: t, .. } => t == table,
            PredicateTemplate::Like { table: t, .. } => t == table,
            PredicateTemplate::IsNull { table: t, .. } => t == table,
            PredicateTemplate::IsNotNull { table: t, .. } => t == table,
        }
    }

    /// Try to extract a range from an expression
    fn try_extract_range(
        &self,
        expr: &Expression,
        input: &super::analyzer::OptimizationInput,
    ) -> Option<RangeInfo> {
        if let Expression::Operator(op) = expr {
            use Operator::*;
            match op {
                GreaterThan(left, right) => {
                    if let Expression::Column(table, col) = left.as_ref() {
                        let table_name = self.resolve_table_name(table.as_deref(), col, input);
                        let value = self.expression_to_predicate_value(right);
                        return Some(RangeInfo {
                            table: table_name,
                            column: col.clone(),
                            lower: Some((value, false)),
                            upper: None,
                        });
                    }
                }
                GreaterThanOrEqual(left, right) => {
                    if let Expression::Column(table, col) = left.as_ref() {
                        let table_name = self.resolve_table_name(table.as_deref(), col, input);
                        let value = self.expression_to_predicate_value(right);
                        return Some(RangeInfo {
                            table: table_name,
                            column: col.clone(),
                            lower: Some((value, true)),
                            upper: None,
                        });
                    }
                }
                LessThan(left, right) => {
                    if let Expression::Column(table, col) = left.as_ref() {
                        let table_name = self.resolve_table_name(table.as_deref(), col, input);
                        let value = self.expression_to_predicate_value(right);
                        return Some(RangeInfo {
                            table: table_name,
                            column: col.clone(),
                            lower: None,
                            upper: Some((value, false)),
                        });
                    }
                }
                LessThanOrEqual(left, right) => {
                    if let Expression::Column(table, col) = left.as_ref() {
                        let table_name = self.resolve_table_name(table.as_deref(), col, input);
                        let value = self.expression_to_predicate_value(right);
                        return Some(RangeInfo {
                            table: table_name,
                            column: col.clone(),
                            lower: None,
                            upper: Some((value, true)),
                        });
                    }
                }
                _ => {}
            }
        }
        None
    }

    /// Build INSERT predicate templates (new architecture)
    fn build_insert_templates(
        &self,
        table: &str,
        columns: Option<&Vec<String>>,
        source: &InsertSource,
        templates: &mut Vec<PredicateTemplate>,
        input: &super::analyzer::OptimizationInput,
    ) -> Result<()> {
        if let InsertSource::Values(rows) = source
            && rows.len() == 1
            && let Some(schema) = input.schemas.get(table)
            && let Some(pk_idx) = schema.primary_key
        {
            let pk_col = &schema.columns[pk_idx];

            // Determine column order: either from explicit columns or table schema order
            let col_idx = if let Some(cols) = columns {
                // Explicit columns provided
                cols.iter().position(|c| c == &pk_col.name)
            } else {
                // No explicit columns - use table schema order
                // This assumes VALUES match the table column order
                Some(pk_idx)
            };

            if let Some(col_idx) = col_idx
                && let Some(pk_value) = rows[0].get(col_idx)
            {
                let value = self.expression_to_predicate_value(pk_value);
                templates.push(PredicateTemplate::PrimaryKey {
                    table: table.to_string(),
                    value,
                });
                return Ok(());
            }
        }

        // Fall back to full table if we couldn't extract primary key
        templates.push(PredicateTemplate::FullTable {
            table: table.to_string(),
        });
        Ok(())
    }

    /// Check if a column is indexed (new architecture)
    fn is_indexed_new(
        &self,
        table: Option<&str>,
        col: &str,
        input: &super::analyzer::OptimizationInput,
    ) -> bool {
        input
            .column_map
            .resolve(table, col)
            .map(|res| res.is_indexed)
            .unwrap_or(false)
    }

    /// Resolve table name for a column (new architecture)
    fn resolve_table_name(
        &self,
        table: Option<&str>,
        col: &str,
        input: &super::analyzer::OptimizationInput,
    ) -> String {
        if let Some(t) = table {
            t.to_string()
        } else {
            input
                .column_map
                .resolve(None, col)
                .map(|res| res.table_name.clone())
                .unwrap_or_default()
        }
    }

    /// Get schema for a column (new architecture)
    fn get_schema_for_column<'a>(
        &self,
        table: Option<&str>,
        col: &str,
        input: &'a super::analyzer::OptimizationInput,
    ) -> Option<&'a Table> {
        let table_name = if let Some(t) = table {
            t.to_string()
        } else {
            input.column_map.resolve(None, col)?.table_name.clone()
        };
        input.schemas.get(&table_name)
    }
}
