//! Optimization metadata building phase
//!
//! This module builds immutable metadata that helps the planner
//! optimize query execution without re-analyzing the query:
//! - Predicate templates for conflict detection
//! - Join structure analysis
//! - Index applicability information
//! - Expression templates (future)

use super::analyzer::{ExpressionTemplate, IndexHint, JoinHint, PredicateType};
use super::statement::{AnalyzedStatement, ExpressionId, PredicateTemplate, PredicateValue};
use crate::error::Result;
use crate::parsing::ast::{
    DmlStatement, Expression, InsertSource, Literal, Operator, SelectStatement, Statement,
};
use crate::types::schema::Table;
use crate::types::value::Value;
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

    /// Extract the primary table name from a statement
    fn extract_primary_table(&self, statement: &Statement) -> Option<String> {
        match statement {
            Statement::Dml(dml) => match dml {
                DmlStatement::Select(select) => {
                    // For SELECT, use the first table in FROM clause
                    use crate::parsing::ast::FromClause;
                    if let Some(first_from) = select.from.first() {
                        match first_from {
                            FromClause::Table { name, alias: _ } => Some(name.clone()),
                            FromClause::Join { left, .. } => {
                                if let FromClause::Table { name, alias: _ } = left.as_ref() {
                                    Some(name.clone())
                                } else {
                                    None
                                }
                            }
                        }
                    } else {
                        None
                    }
                }
                DmlStatement::Insert { table, .. } => Some(table.clone()),
                DmlStatement::Update { table, .. } => Some(table.clone()),
                DmlStatement::Delete { table, .. } => Some(table.clone()),
            },
            _ => None,
        }
    }

    /// Main entry point: build all optimization metadata
    pub fn build_metadata(
        &self,
        statement: &Statement,
        analyzed: &mut AnalyzedStatement,
    ) -> Result<()> {
        // Build predicate templates for conflict detection
        self.build_predicate_templates(statement, analyzed)?;

        // Analyze index applicability
        self.analyze_index_usage(statement, analyzed)?;

        // Future: Build expression templates
        // self.build_expression_templates(statement, analyzed)?;

        // Future: Analyze join structure
        // self.analyze_joins(statement, analyzed)?;

        Ok(())
    }

    /// Build predicate templates for efficient conflict detection
    fn build_predicate_templates(
        &self,
        statement: &Statement,
        analyzed: &mut AnalyzedStatement,
    ) -> Result<()> {
        match statement {
            Statement::Dml(dml) => match dml {
                DmlStatement::Select(select) => {
                    self.build_select_predicates(statement, select, analyzed)
                }
                DmlStatement::Insert {
                    table,
                    columns,
                    source,
                } => self.build_insert_predicates(table, columns.as_ref(), source, analyzed),
                DmlStatement::Update { table, r#where, .. } => {
                    if let Some(where_expr) = r#where {
                        self.extract_predicate_templates(where_expr, table, analyzed);
                    } else {
                        analyzed.add_predicate_template(PredicateTemplate::FullTable {
                            table: table.clone(),
                        });
                    }
                    Ok(())
                }
                DmlStatement::Delete { table, r#where } => {
                    if let Some(where_expr) = r#where {
                        self.extract_predicate_templates(where_expr, table, analyzed);
                    } else {
                        analyzed.add_predicate_template(PredicateTemplate::FullTable {
                            table: table.clone(),
                        });
                    }
                    Ok(())
                }
            },
            _ => Ok(()), // DDL doesn't need predicate templates
        }
    }

    /// Build predicate templates for SELECT
    fn build_select_predicates(
        &self,
        statement: &Statement,
        select: &SelectStatement,
        analyzed: &mut AnalyzedStatement,
    ) -> Result<()> {
        // Extract predicates from WHERE clause
        // Get primary table from the statement
        let primary_table = self.extract_primary_table(statement);
        if let Some(table) = primary_table {
            if let Some(where_expr) = &select.r#where {
                self.extract_predicate_templates(where_expr, &table, analyzed);
            } else {
                // No WHERE clause - reading entire table
                analyzed.add_predicate_template(PredicateTemplate::FullTable { table });
            }
        }
        Ok(())
    }

    /// Build predicate templates for INSERT
    fn build_insert_predicates(
        &self,
        table: &str,
        columns: Option<&Vec<String>>,
        source: &InsertSource,
        analyzed: &mut AnalyzedStatement,
    ) -> Result<()> {
        match source {
            InsertSource::Values(rows) => {
                // Try to extract primary key predicate if we're inserting a single row
                if rows.len() == 1
                    && let Some(schema) = self.schemas.get(table)
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

                    // Find the primary key value in the INSERT
                    if let Some(col_idx) = col_idx
                        && let Some(pk_value) = rows[0].get(col_idx)
                    {
                        let value = self.expression_to_predicate_value(pk_value);
                        analyzed.add_predicate_template(PredicateTemplate::PrimaryKey {
                            table: table.to_string(),
                            value,
                        });
                        return Ok(());
                    }
                }

                // Fall back to full table for multi-row inserts or when we can't find PK
                analyzed.add_predicate_template(PredicateTemplate::FullTable {
                    table: table.to_string(),
                });
            }
            InsertSource::Select(_) => {
                // INSERT ... SELECT affects the whole table
                analyzed.add_predicate_template(PredicateTemplate::FullTable {
                    table: table.to_string(),
                });
            }
            InsertSource::DefaultValues => {
                // Single row insert with defaults
                analyzed.add_predicate_template(PredicateTemplate::FullTable {
                    table: table.to_string(),
                });
            }
        }
        Ok(())
    }

    /// Extract predicate templates from a WHERE clause expression
    fn extract_predicate_templates(
        &self,
        expr: &Expression,
        table: &str,
        analyzed: &mut AnalyzedStatement,
    ) {
        match expr {
            Expression::Operator(op) => match op {
                // Equality predicates: column = value
                Operator::Equal(left, right) => {
                    if let Expression::Column(_, col_name) = &**left {
                        let col_index = self.schemas.get(table).and_then(|schema| {
                            schema.columns.iter().position(|c| &c.name == col_name)
                        });

                        if let Some(col_idx) = col_index {
                            let value_expr = self.expression_to_predicate_value(right);
                            analyzed.add_predicate_template(PredicateTemplate::Equality {
                                table: table.to_string(),
                                column_name: col_name.clone(),
                                column_index: col_idx,
                                value_expr,
                            });
                        } else {
                            analyzed.add_predicate_template(PredicateTemplate::Complex {
                                table: table.to_string(),
                                expression_id: ExpressionId::from_path(vec![]),
                            });
                        }
                    } else if let Expression::Column(_, col_name) = &**right {
                        // Handle value = column (reversed)
                        let col_index = self.schemas.get(table).and_then(|schema| {
                            schema.columns.iter().position(|c| &c.name == col_name)
                        });

                        if let Some(col_idx) = col_index {
                            let value_expr = self.expression_to_predicate_value(left);
                            analyzed.add_predicate_template(PredicateTemplate::Equality {
                                table: table.to_string(),
                                column_name: col_name.clone(),
                                column_index: col_idx,
                                value_expr,
                            });
                        } else {
                            analyzed.add_predicate_template(PredicateTemplate::Complex {
                                table: table.to_string(),
                                expression_id: ExpressionId::from_path(vec![]),
                            });
                        }
                    } else {
                        // Complex comparison
                        analyzed.add_predicate_template(PredicateTemplate::Complex {
                            table: table.to_string(),
                            expression_id: ExpressionId::from_path(vec![]),
                        });
                    }
                }

                // AND: recurse on both sides
                Operator::And(left, right) => {
                    self.extract_predicate_templates(left, table, analyzed);
                    self.extract_predicate_templates(right, table, analyzed);
                }

                // OR: complex predicate for now
                Operator::Or(_, _) => {
                    analyzed.add_predicate_template(PredicateTemplate::Complex {
                        table: table.to_string(),
                        expression_id: ExpressionId::from_path(vec![]),
                    });
                }

                // Range predicates
                Operator::GreaterThan(left, right) | Operator::GreaterThanOrEqual(left, right) => {
                    if let Expression::Column(_, col_name) = &**left {
                        let col_index = self.schemas.get(table).and_then(|schema| {
                            schema.columns.iter().position(|c| &c.name == col_name)
                        });

                        if let Some(col_idx) = col_index {
                            let inclusive = matches!(op, Operator::GreaterThanOrEqual(_, _));
                            let value_expr = self.expression_to_predicate_value(right);
                            analyzed.add_predicate_template(PredicateTemplate::Range {
                                table: table.to_string(),
                                column_name: col_name.clone(),
                                column_index: col_idx,
                                lower: Some((value_expr, inclusive)),
                                upper: None,
                            });
                        }
                    }
                }

                Operator::LessThan(left, right) | Operator::LessThanOrEqual(left, right) => {
                    if let Expression::Column(_, col_name) = &**left {
                        let col_index = self.schemas.get(table).and_then(|schema| {
                            schema.columns.iter().position(|c| &c.name == col_name)
                        });

                        if let Some(col_idx) = col_index {
                            let inclusive = matches!(op, Operator::LessThanOrEqual(_, _));
                            let value_expr = self.expression_to_predicate_value(right);
                            analyzed.add_predicate_template(PredicateTemplate::Range {
                                table: table.to_string(),
                                column_name: col_name.clone(),
                                column_index: col_idx,
                                lower: None,
                                upper: Some((value_expr, inclusive)),
                            });
                        }
                    }
                }

                // BETWEEN
                Operator::Between {
                    expr,
                    low,
                    high,
                    negated,
                } => {
                    if !*negated && let Expression::Column(_, col_name) = &**expr {
                        let col_index = self.schemas.get(table).and_then(|schema| {
                            schema.columns.iter().position(|c| &c.name == col_name)
                        });

                        if let Some(col_idx) = col_index {
                            let low_value = self.expression_to_predicate_value(low);
                            let high_value = self.expression_to_predicate_value(high);
                            analyzed.add_predicate_template(PredicateTemplate::Range {
                                table: table.to_string(),
                                column_name: col_name.clone(),
                                column_index: col_idx,
                                lower: Some((low_value, true)),
                                upper: Some((high_value, true)),
                            });
                        }
                    }
                }

                // IN list
                Operator::InList {
                    expr,
                    list,
                    negated: _,
                } => {
                    if let Expression::Column(_, col_name) = &**expr {
                        let col_index = self.schemas.get(table).and_then(|schema| {
                            schema.columns.iter().position(|c| &c.name == col_name)
                        });

                        if let Some(col_idx) = col_index {
                            let values: Vec<PredicateValue> = list
                                .iter()
                                .map(|e| self.expression_to_predicate_value(e))
                                .collect();

                            analyzed.add_predicate_template(PredicateTemplate::InList {
                                table: table.to_string(),
                                column_name: col_name.clone(),
                                column_index: col_idx,
                                values,
                            });
                        }
                    }
                }

                // Other operators are complex predicates
                _ => {
                    analyzed.add_predicate_template(PredicateTemplate::Complex {
                        table: table.to_string(),
                        expression_id: ExpressionId::from_path(vec![]),
                    });
                }
            },

            // Non-operator expressions are complex predicates
            _ => {
                analyzed.add_predicate_template(PredicateTemplate::Complex {
                    table: table.to_string(),
                    expression_id: ExpressionId::from_path(vec![]),
                });
            }
        }
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

    /// Analyze which predicates can use indexes
    fn analyze_index_usage(
        &self,
        statement: &Statement,
        analyzed: &mut AnalyzedStatement,
    ) -> Result<()> {
        // Check each predicate template to see if it can use an index
        for template in &analyzed.predicate_templates {
            match template {
                PredicateTemplate::Equality {
                    table, column_name, ..
                }
                | PredicateTemplate::Range {
                    table, column_name, ..
                } => {
                    // Check if this column has an index
                    if let Some(schema) = self.schemas.get(table) {
                        for column in &schema.columns {
                            if column.name == *column_name && column.index {
                                // Mark that this predicate can use an index
                                // This information is stored in the predicate template itself
                                // The planner will use it during optimization
                                break;
                            }
                        }
                    }
                }
                PredicateTemplate::PrimaryKey { .. } => {
                    // Primary key predicates always use the primary key index
                    // No additional analysis needed
                }
                _ => {
                    // Other predicate types don't benefit from simple indexes
                }
            }
        }

        // For SELECT statements, analyze ORDER BY for index usage
        if let Statement::Dml(DmlStatement::Select(select)) = statement
            && !select.order_by.is_empty()
        {
            // Check if ORDER BY columns have indexes
            for (expr, _) in &select.order_by {
                if let Expression::Column(_table_ref, col_name) = expr {
                    // Find the table (could be aliased)
                    let primary_table = self.extract_primary_table(statement);
                    if let Some(table) = primary_table
                        && let Some(schema) = self.schemas.get(&table)
                    {
                        for column in &schema.columns {
                            if column.name == *col_name && column.index {
                                // This ORDER BY can potentially use an index
                                // Store this information for the planner
                                break;
                            }
                        }
                    }
                }
            }
        }

        Ok(())
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
                    if let Some(where_expr) = &select.r#where {
                        self.extract_templates_new(where_expr, &mut templates, input);
                    }

                    // If no predicates were extracted (unsupported WHERE conditions),
                    // fall back to FullTable
                    if templates.is_empty() {
                        if let Some((_, table)) = input.tables.first() {
                            templates.push(PredicateTemplate::FullTable {
                                table: table.clone(),
                            });
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
            }
        }

        Ok(templates)
    }

    /// Find index opportunities for new phase architecture
    pub fn find_index_opportunities(
        &self,
        statement: &Arc<Statement>,
        input: &super::analyzer::OptimizationInput,
    ) -> Result<Vec<IndexHint>> {
        let mut hints = Vec::new();

        if let Statement::Dml(DmlStatement::Select(select)) = statement.as_ref()
            && let Some(where_expr) = &select.r#where
        {
            self.find_index_hints(where_expr, &mut hints, input);
        }

        Ok(hints)
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
        use crate::parsing::ast::{FromClause, common::JoinType as AstJoinType};

        if let FromClause::Join {
            left,
            right,
            r#type,
            predicate,
        } = from
        {
            // Recursively analyze nested joins
            self.analyze_from_joins(left, hints, input)?;
            self.analyze_from_joins(right, hints, input)?;

            // Extract table names from left and right
            let left_table = self.extract_table_from_clause(left);
            let right_table = self.extract_table_from_clause(right);

            if let (Some(left_table), Some(right_table)) = (left_table, right_table) {
                // Estimate selectivity based on join predicate
                let selectivity = if let Some(pred) = predicate {
                    self.estimate_join_selectivity(pred, &left_table, &right_table, input)
                } else {
                    1.0 // Cross join has selectivity 1.0
                };

                // Convert join type
                let join_type = match r#type {
                    AstJoinType::Inner => super::analyzer::JoinType::Inner,
                    AstJoinType::Left => super::analyzer::JoinType::Left,
                    AstJoinType::Right => super::analyzer::JoinType::Right,
                    AstJoinType::Full => super::analyzer::JoinType::Full,
                    AstJoinType::Cross => super::analyzer::JoinType::Inner,
                };

                hints.push(JoinHint {
                    left_table,
                    right_table,
                    join_type,
                    selectivity_estimate: selectivity,
                });
            }
        }

        Ok(())
    }

    /// Extract table name from FROM clause
    fn extract_table_from_clause(&self, from: &crate::parsing::ast::FromClause) -> Option<String> {
        use crate::parsing::ast::FromClause;

        match from {
            FromClause::Table { name, .. } => Some(name.clone()),
            FromClause::Join { left, .. } => self.extract_table_from_clause(left),
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

    /// Build expression templates for new phase architecture
    pub fn build_expression_templates(
        &self,
        statement: &Arc<Statement>,
        _input: &super::analyzer::OptimizationInput,
    ) -> Result<Vec<ExpressionTemplate>> {
        let mut templates = Vec::new();
        let mut expression_counts: HashMap<String, (ExpressionId, usize)> = HashMap::new();

        // First pass: count occurrences of each expression
        if let Statement::Dml(dml) = statement.as_ref() {
            match dml {
                DmlStatement::Select(select) => {
                    // Check SELECT expressions
                    for (idx, (expr, _)) in select.select.iter().enumerate() {
                        let expr_id = ExpressionId::from_path(vec![idx]);
                        self.count_expression_occurrences(expr, &expr_id, &mut expression_counts);
                    }

                    // Check WHERE clause
                    if let Some(where_expr) = &select.r#where {
                        let expr_id = ExpressionId::from_path(vec![2000]);
                        self.count_expression_occurrences(
                            where_expr,
                            &expr_id,
                            &mut expression_counts,
                        );
                    }

                    // Check GROUP BY
                    for (idx, expr) in select.group_by.iter().enumerate() {
                        let expr_id = ExpressionId::from_path(vec![3000 + idx]);
                        self.count_expression_occurrences(expr, &expr_id, &mut expression_counts);
                    }

                    // Check HAVING
                    if let Some(having) = &select.having {
                        let expr_id = ExpressionId::from_path(vec![3500]);
                        self.count_expression_occurrences(having, &expr_id, &mut expression_counts);
                    }

                    // Check ORDER BY
                    for (idx, (expr, _)) in select.order_by.iter().enumerate() {
                        let expr_id = ExpressionId::from_path(vec![4000 + idx]);
                        self.count_expression_occurrences(expr, &expr_id, &mut expression_counts);
                    }
                }
                _ => {} // Other DML types don't benefit as much from expression caching
            }
        }

        // Second pass: create templates for expressions that appear multiple times
        // and are deterministic (no side effects)
        for (template_str, (expr_id, count)) in expression_counts {
            if count > 1 {
                // Only cache expressions that appear more than once
                templates.push(ExpressionTemplate {
                    id: expr_id,
                    template: template_str,
                    is_deterministic: true, // We only track deterministic expressions
                });
            }
        }

        Ok(templates)
    }

    /// Count occurrences of expressions for caching
    fn count_expression_occurrences(
        &self,
        expr: &Expression,
        expr_id: &ExpressionId,
        counts: &mut HashMap<String, (ExpressionId, usize)>,
    ) {
        use crate::parsing::ast::Operator;

        // Check if this is a cacheable expression
        if self.is_cacheable_expression(expr) {
            let template = self.expression_to_template(expr);
            counts
                .entry(template)
                .and_modify(|(_id, count)| *count += 1)
                .or_insert((expr_id.clone(), 1));
        }

        // Recursively check sub-expressions
        match expr {
            Expression::Operator(op) => match op {
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
                    self.count_expression_occurrences(l, &expr_id.child(0), counts);
                    self.count_expression_occurrences(r, &expr_id.child(1), counts);
                }
                Operator::Not(e)
                | Operator::Negate(e)
                | Operator::Identity(e)
                | Operator::Factorial(e) => {
                    self.count_expression_occurrences(e, &expr_id.child(0), counts);
                }
                Operator::Between {
                    expr, low, high, ..
                } => {
                    self.count_expression_occurrences(expr, &expr_id.child(0), counts);
                    self.count_expression_occurrences(low, &expr_id.child(1), counts);
                    self.count_expression_occurrences(high, &expr_id.child(2), counts);
                }
                Operator::InList { expr, list, .. } => {
                    self.count_expression_occurrences(expr, &expr_id.child(0), counts);
                    for (i, item) in list.iter().enumerate() {
                        self.count_expression_occurrences(item, &expr_id.child(i + 1), counts);
                    }
                }
                Operator::Is(e, _) => {
                    self.count_expression_occurrences(e, &expr_id.child(0), counts);
                }
            },
            Expression::Function(_, args) => {
                for (i, arg) in args.iter().enumerate() {
                    self.count_expression_occurrences(arg, &expr_id.child(i), counts);
                }
            }
            _ => {} // Literals, columns don't need recursive checking
        }
    }

    /// Check if an expression is cacheable
    fn is_cacheable_expression(&self, expr: &Expression) -> bool {
        use crate::parsing::ast::Operator;

        match expr {
            // Complex expressions worth caching
            Expression::Function(name, _) => {
                // Most functions are deterministic and worth caching
                // Except for non-deterministic ones like RANDOM
                !matches!(name.to_uppercase().as_str(), "RANDOM" | "RAND" | "UUID")
            }
            Expression::Operator(op) => {
                match op {
                    // Arithmetic and complex operations worth caching
                    Operator::Multiply(_, _)
                    | Operator::Divide(_, _)
                    | Operator::Exponentiate(_, _)
                    | Operator::Factorial(_) => true,
                    // String operations can be expensive
                    Operator::Like(_, _) => true,
                    // Complex predicates
                    Operator::Between { .. } | Operator::InList { .. } => true,
                    // Simple operations not worth caching
                    _ => false,
                }
            }
            // Simple values not worth caching
            Expression::Literal(_) | Expression::Column(_, _) | Expression::Parameter(_) => false,
            _ => false,
        }
    }

    /// Convert expression to a template string for comparison
    fn expression_to_template(&self, expr: &Expression) -> String {
        // Create a normalized string representation of the expression
        // This allows us to identify identical expressions
        format!("{:?}", expr)
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
                                column_index: 0, // Will be resolved later if needed
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
                    ) {
                        if range1.table == range2.table && range1.column == range2.column {
                            // Merge the two ranges
                            let merged = PredicateTemplate::Range {
                                table: range1.table,
                                column_name: range1.column,
                                column_index: 0,
                                lower: range1.lower.or(range2.lower),
                                upper: range1.upper.or(range2.upper),
                            };
                            templates.push(merged);
                            return;
                        }
                    }

                    // Otherwise, extract separately
                    self.extract_templates_new(left, templates, input);
                    self.extract_templates_new(right, templates, input);
                }

                // Range predicates
                GreaterThan(left, right) => {
                    if let Expression::Column(table, col) = left.as_ref() {
                        let table_name = self.resolve_table_name(table.as_deref(), col, input);
                        let value = self.expression_to_predicate_value(right);
                        templates.push(PredicateTemplate::Range {
                            table: table_name,
                            column_name: col.clone(),
                            column_index: 0, // Will be resolved later if needed
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
                            column_index: 0,
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
                            column_index: 0,
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
                            column_index: 0,
                            lower: None,
                            upper: Some((value, true)), // inclusive
                        });
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
                                column_index: 0,
                                values,
                            });
                        }
                    }
                }

                _ => {}
            }
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

    /// Find index hints in WHERE clause (new architecture)
    fn find_index_hints(
        &self,
        expr: &Expression,
        hints: &mut Vec<IndexHint>,
        input: &super::analyzer::OptimizationInput,
    ) {
        if let Expression::Operator(op) = expr {
            use Operator::*;
            match op {
                Equal(left, _right) => {
                    if let Expression::Column(table, col) = left.as_ref()
                        && self.is_indexed_new(table.as_deref(), col, input)
                    {
                        hints.push(IndexHint {
                            table: self.resolve_table_name(table.as_deref(), col, input),
                            column: col.clone(),
                            predicate_type: PredicateType::Equality,
                        });
                    }
                }
                GreaterThan(left, _)
                | GreaterThanOrEqual(left, _)
                | LessThan(left, _)
                | LessThanOrEqual(left, _) => {
                    if let Expression::Column(table, col) = left.as_ref()
                        && self.is_indexed_new(table.as_deref(), col, input)
                    {
                        hints.push(IndexHint {
                            table: self.resolve_table_name(table.as_deref(), col, input),
                            column: col.clone(),
                            predicate_type: PredicateType::Range,
                        });
                    }
                }
                Like(left, right) => {
                    if let (
                        Expression::Column(table, col),
                        Expression::Literal(Literal::String(pattern)),
                    ) = (left.as_ref(), right.as_ref())
                        && !pattern.starts_with('%')
                        && self.is_indexed_new(table.as_deref(), col, input)
                    {
                        hints.push(IndexHint {
                            table: self.resolve_table_name(table.as_deref(), col, input),
                            column: col.clone(),
                            predicate_type: PredicateType::Prefix,
                        });
                    }
                }
                And(left, right) => {
                    self.find_index_hints(left, hints, input);
                    self.find_index_hints(right, hints, input);
                }
                _ => {}
            }
        }
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
