//! Expression resolution utilities
//!
//! This module provides the core expression resolution logic for the query planner,
//! including handling of columns, operators, subqueries, and special SQL constructs.
//!
//! The AnalyzedPlanContext uses semantic analysis results (especially the column resolution map)
//! for efficient O(1) column lookups and supports DuckDB-style struct field access.

use super::subquery_planner::SubqueryPlanner;
use crate::error::{Error, Result};
use crate::parsing::ast::{Expression as AstExpression, Literal, Operator};
use crate::semantic::AnalyzedStatement;
use crate::types::expression::Expression;
use crate::types::schema::Table;
use std::collections::HashMap;

/// Context that uses AnalyzedStatement for expression resolution
///
/// This context maintains table references and uses the semantic analysis results
/// to efficiently resolve column references to their positions in the query output.
pub(super) struct AnalyzedPlanContext<'a> {
    schemas: &'a HashMap<String, Table>,
    index_metadata: &'a HashMap<String, crate::types::index::IndexMetadata>,
    pub(super) analyzed: &'a AnalyzedStatement,
    tables: Vec<TableRef>,
    current_column: usize,
}

/// Reference to a table in the query
pub(super) struct TableRef {
    pub name: String,
    pub alias: Option<String>,
    pub start_column: usize,
}

/// Context for resolving ORDER BY expressions after projection
///
/// Maps column names/aliases to their position in the projection output.
/// This allows ORDER BY to reference both projected expressions and source columns
/// (following PostgreSQL's relaxed rules).
pub(super) struct ProjectionContext {
    /// Maps column names (or aliases) to their index in projection output
    column_map: HashMap<String, usize>,
    /// Original projection expressions (for structural matching)
    expressions: Vec<Expression>,
}

impl ProjectionContext {
    /// Create a ProjectionContext from projection expressions and SELECT AST
    ///
    /// For wildcard selections, we need the AnalyzedStatement to get column names.
    pub(super) fn new(
        expressions: Vec<Expression>,
        ast_select: Vec<(AstExpression, Option<String>)>,
    ) -> Self {
        let mut column_map = HashMap::new();

        for (idx, (ast_expr, alias)) in ast_select.iter().enumerate() {
            // If there's an alias, map it to this position
            if let Some(alias_name) = alias {
                column_map.insert(alias_name.clone(), idx);
            } else {
                // For unaliased expressions, try to extract the column name
                // This allows "SELECT x FROM t ORDER BY x" to work
                if let AstExpression::Column(_, col_name) = ast_expr {
                    column_map.insert(col_name.clone(), idx);
                }
            }
        }

        Self {
            column_map,
            expressions,
        }
    }

    /// Create a ProjectionContext with explicit column names (for wildcard expansion)
    pub(super) fn with_column_names(
        expressions: Vec<Expression>,
        ast_select: Vec<(AstExpression, Option<String>)>,
        column_names: Vec<String>,
    ) -> Self {
        let mut column_map = HashMap::new();

        // Map each column name to its position
        for (idx, name) in column_names.iter().enumerate() {
            column_map.insert(name.clone(), idx);
        }

        // Also handle any explicit aliases from the AST
        for (idx, (_, alias)) in ast_select.iter().enumerate() {
            if let Some(alias_name) = alias {
                column_map.insert(alias_name.clone(), idx);
            }
        }

        Self {
            column_map,
            expressions,
        }
    }

    /// Try to resolve a column name to a projection output index
    pub(super) fn resolve_column_name(&self, name: &str) -> Option<usize> {
        self.column_map.get(name).copied()
    }

    /// Try to find an expression in the projection by structural equality
    pub(super) fn find_expression(&self, expr: &Expression) -> Option<usize> {
        self.expressions.iter().position(|e| e == expr)
    }
}

impl<'a> AnalyzedPlanContext<'a> {
    /// Create a new expression resolution context
    pub(super) fn new(
        schemas: &'a HashMap<String, Table>,
        index_metadata: &'a HashMap<String, crate::types::index::IndexMetadata>,
        analyzed: &'a AnalyzedStatement,
    ) -> Self {
        Self {
            schemas,
            index_metadata,
            analyzed,
            tables: Vec::new(),
            current_column: 0,
        }
    }

    /// Add a table reference to the context
    ///
    /// This tracks which tables are available for column resolution and
    /// maintains the column offset for each table.
    pub(super) fn add_table(&mut self, name: String, alias: Option<String>) -> Result<()> {
        // Table existence already validated by semantic analyzer
        let schema = self
            .schemas
            .get(&name)
            .expect("Table should exist after semantic analysis");

        let table_ref = TableRef {
            name: name.clone(),
            alias,
            start_column: self.current_column,
        };

        self.current_column += schema.columns.len();
        self.tables.push(table_ref);

        Ok(())
    }

    /// Get reference to tables (for projection building)
    pub(super) fn get_tables(&self) -> &[TableRef] {
        &self.tables
    }

    /// Get reference to schemas (for projection building)
    pub(super) fn get_schemas(&self) -> &HashMap<String, Table> {
        self.schemas
    }

    /// Resolve expression using type annotations when possible
    ///
    /// This is the main entry point for expression resolution.
    /// It delegates to resolve_expression_with_metadata for metadata-enhanced resolution.
    pub(super) fn resolve_expression(&self, expr: &AstExpression) -> Result<Expression> {
        // Try to use metadata-enhanced resolution
        self.resolve_expression_with_metadata(expr)
    }

    /// Resolve expression with metadata support
    ///
    /// This method leverages the column resolution map from semantic analysis
    /// for efficient O(1) column lookups and parameter slot information for
    /// better type safety.
    fn resolve_expression_with_metadata(&self, expr: &AstExpression) -> Result<Expression> {
        // TODO: In a future optimization, we could use expression templates here
        // to identify expressions that should be computed once and cached
        // For now, we proceed with normal resolution

        match expr {
            AstExpression::Column(table_ref, column_name) => {
                // Use the optimized column resolution map (O(1) lookup)
                if let Some(resolution) = self
                    .analyzed
                    .column_resolution_map
                    .resolve(table_ref.as_deref(), column_name)
                {
                    return Ok(Expression::Column(resolution.offset));
                }

                // Handle struct field access (DuckDB-style resolution)
                self.resolve_column_or_struct_field(table_ref, column_name)
            }
            AstExpression::Parameter(idx) => {
                // Check if we have parameter slot information for better type safety
                if let Some(param_slot) = self.analyzed.parameter_slots.get(*idx) {
                    // We have rich parameter information available
                    // This could be used for validation at bind time
                    // For example, checking expected type vs provided type
                    if param_slot.actual_type.is_some() {
                        // Type is already known from semantic analysis
                    }
                }
                Ok(Expression::Parameter(*idx))
            }
            _ => self.resolve_expression_simple(expr),
        }
    }

    /// Simple expression resolution (temporary)
    ///
    /// Handles all expression types except those with special metadata handling
    /// (columns and parameters, which are handled in resolve_expression_with_metadata).
    pub(super) fn resolve_expression_simple(&self, expr: &AstExpression) -> Result<Expression> {
        match expr {
            AstExpression::Literal(lit) => {
                let value = match lit {
                    Literal::Null => crate::types::Value::Null,
                    Literal::Boolean(b) => crate::types::Value::boolean(*b),
                    Literal::Integer(i) => {
                        if *i >= i32::MIN as i128 && *i <= i32::MAX as i128 {
                            crate::types::Value::I32(*i as i32)
                        } else if *i >= i64::MIN as i128 && *i <= i64::MAX as i128 {
                            crate::types::Value::I64(*i as i64)
                        } else {
                            crate::types::Value::I128(*i)
                        }
                    }
                    Literal::Float(f) => crate::types::Value::F64(*f),
                    Literal::String(s) => crate::types::Value::string(s.clone()),
                    Literal::Bytea(b) => crate::types::Value::Bytea(b.clone()),
                    Literal::Date(d) => crate::types::Value::Date(*d),
                    Literal::Time(t) => crate::types::Value::Time(*t),
                    Literal::Timestamp(ts) => crate::types::Value::Timestamp(*ts),
                    Literal::Interval(i) => crate::types::Value::Interval(i.clone()),
                };
                Ok(Expression::Constant(value))
            }

            AstExpression::Column(table_ref, column_name) => {
                self.resolve_column_or_struct_field(table_ref, column_name)
            }

            AstExpression::Parameter(idx) => {
                // Parameter slots are checked in resolve_expression_with_metadata
                Ok(Expression::Parameter(*idx))
            }

            AstExpression::Function(name, args) => {
                let resolved_args = args
                    .iter()
                    .map(|a| self.resolve_expression_simple(a))
                    .collect::<Result<Vec<_>>>()?;
                Ok(Expression::Function(name.clone(), resolved_args))
            }

            AstExpression::Operator(op) => self.resolve_operator(op),

            AstExpression::All => Ok(Expression::All),
            AstExpression::QualifiedWildcard(_) => {
                // QualifiedWildcard is expanded during projection planning
                Err(Error::ExecutionError(
                    "Qualified wildcard not supported in this context".into(),
                ))
            }

            AstExpression::ArrayAccess { base, index } => {
                let base_expr = self.resolve_expression_simple(base)?;
                let index_expr = self.resolve_expression_simple(index)?;
                Ok(Expression::ArrayAccess(
                    Box::new(base_expr),
                    Box::new(index_expr),
                ))
            }

            AstExpression::FieldAccess { base, field } => {
                let base_expr = self.resolve_expression_simple(base)?;
                Ok(Expression::FieldAccess(Box::new(base_expr), field.clone()))
            }

            AstExpression::ArrayLiteral(elements) => {
                let resolved_elements = elements
                    .iter()
                    .map(|e| self.resolve_expression_simple(e))
                    .collect::<Result<Vec<_>>>()?;
                Ok(Expression::ArrayLiteral(resolved_elements))
            }

            AstExpression::MapLiteral(entries) => {
                let resolved_entries = entries
                    .iter()
                    .map(|(k, v)| {
                        Ok((
                            self.resolve_expression_simple(k)?,
                            self.resolve_expression_simple(v)?,
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(Expression::MapLiteral(resolved_entries))
            }

            AstExpression::Subquery(select) => {
                // Use SubqueryPlanner to eliminate duplication
                let subquery_plan = SubqueryPlanner::plan_with_outer_context(
                    select,
                    self.schemas,
                    self.index_metadata,
                    &self.analyzed.column_resolution_map,
                )?;

                Ok(Expression::Subquery(Box::new(subquery_plan)))
            }

            AstExpression::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                // Resolve operand (if present)
                let resolved_operand = operand
                    .as_ref()
                    .map(|op| self.resolve_expression_simple(op))
                    .transpose()?
                    .map(Box::new);

                // Resolve when/then clauses
                let resolved_when_clauses = when_clauses
                    .iter()
                    .map(|(when, then)| {
                        Ok((
                            self.resolve_expression_simple(when)?,
                            self.resolve_expression_simple(then)?,
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;

                // Resolve else clause (if present)
                let resolved_else = else_clause
                    .as_ref()
                    .map(|e| self.resolve_expression_simple(e))
                    .transpose()?
                    .map(Box::new);

                Ok(Expression::Case {
                    operand: resolved_operand,
                    when_clauses: resolved_when_clauses,
                    else_clause: resolved_else,
                })
            }
        }
    }

    /// Resolve an ORDER BY expression against projection context
    ///
    /// Follows SQL standard with PostgreSQL-style relaxed rules:
    /// 1. Try to find in projection (aliases, projected columns)
    /// 2. Fall back to source table columns (PostgreSQL extension)
    pub(super) fn resolve_order_by_expression(
        &self,
        expr: &AstExpression,
        projection_ctx: &ProjectionContext,
    ) -> Result<Expression> {
        match expr {
            // Simple column reference - try projection first, then source tables
            AstExpression::Column(None, column_name) => {
                // First try to find by name in projection (handles aliases)
                if let Some(idx) = projection_ctx.resolve_column_name(column_name) {
                    return Ok(Expression::Column(idx));
                }

                // PostgreSQL extension: allow ORDER BY on non-projected columns
                // Resolve from source tables
                self.resolve_expression(expr)
            }

            // Qualified column reference
            AstExpression::Column(Some(_table), _column_name) => {
                // Try to resolve the expression and find it in projection
                let resolved = self.resolve_expression(expr)?;
                if let Some(idx) = projection_ctx.find_expression(&resolved) {
                    return Ok(Expression::Column(idx));
                }

                // PostgreSQL extension: allow qualified columns not in SELECT
                Ok(resolved)
            }

            // For other expressions (functions, operators, etc.)
            // Try to find in projection by structural equality
            _ => {
                let resolved = self.resolve_expression(expr)?;
                if let Some(idx) = projection_ctx.find_expression(&resolved) {
                    return Ok(Expression::Column(idx));
                }

                // Expression not in projection - use the resolved expression
                // This allows complex expressions in ORDER BY (PostgreSQL extension)
                Ok(resolved)
            }
        }
    }

    /// Resolve a column reference, handling struct field access
    ///
    /// First tries the O(1) column resolution map from semantic analysis.
    /// If not found, handles DuckDB-style struct field access (e.g., struct_col.field_name).
    fn resolve_column_or_struct_field(
        &self,
        table_ref: &Option<String>,
        column_name: &str,
    ) -> Result<Expression> {
        // First try the resolution map (O(1) lookup)
        if let Some(resolution) = self
            .analyzed
            .column_resolution_map
            .resolve(table_ref.as_deref(), column_name)
        {
            return Ok(Expression::Column(resolution.offset));
        }

        // If not found, handle struct field access
        let table = if let Some(tref) = table_ref {
            self.tables
                .iter()
                .find(|t| &t.name == tref || t.alias.as_ref() == Some(tref))
        } else if self.tables.len() == 1 {
            self.tables.first()
        } else {
            return Err(Error::ColumnNotFound(column_name.to_string()));
        };

        if let Some(table) = table {
            // This shouldn't happen if semantic analysis was successful
            self.resolve_column_in_table(table, column_name)
        } else {
            // Check if it's a struct column (DuckDB-style resolution)
            let struct_col_name = table_ref.clone().unwrap_or_default();

            for table in &self.tables {
                if let Some(schema) = self.schemas.get(&table.name)
                    && let Some(col) = schema.columns.iter().find(|c| c.name == struct_col_name)
                {
                    // Check if it's a struct type
                    if let crate::types::data_type::DataType::Struct(fields) = &col.data_type {
                        // Verify the field exists
                        if fields.iter().any(|(name, _)| name == column_name) {
                            let base_expr =
                                self.resolve_column_in_table(table, &struct_col_name)?;
                            return Ok(Expression::FieldAccess(
                                Box::new(base_expr),
                                column_name.to_string(),
                            ));
                        } else {
                            return Err(Error::ExecutionError(format!(
                                "Field '{}' not found in struct '{}'",
                                column_name, struct_col_name
                            )));
                        }
                    }
                }
            }

            Err(Error::TableNotFound(table_ref.clone().unwrap_or_default()))
        }
    }

    /// Resolve a column within a specific table
    fn resolve_column_in_table(&self, table: &TableRef, column_name: &str) -> Result<Expression> {
        let schema = self.schemas.get(&table.name).expect("Table should exist");

        let col_index = schema
            .columns
            .iter()
            .position(|c| c.name == column_name)
            .ok_or_else(|| Error::ColumnNotFound(column_name.to_string()))?;

        Ok(Expression::Column(table.start_column + col_index))
    }

    /// Resolve an operator expression
    ///
    /// Recursively resolves all operands and constructs the corresponding
    /// Expression variant. Handles subqueries in IN and EXISTS operators
    /// using SubqueryPlanner to eliminate code duplication.
    fn resolve_operator(&self, op: &Operator) -> Result<Expression> {
        use Operator::*;

        Ok(match op {
            And(l, r) => Expression::And(
                Box::new(self.resolve_expression_simple(l)?),
                Box::new(self.resolve_expression_simple(r)?),
            ),
            Or(l, r) => Expression::Or(
                Box::new(self.resolve_expression_simple(l)?),
                Box::new(self.resolve_expression_simple(r)?),
            ),
            Xor(l, r) => Expression::Xor(
                Box::new(self.resolve_expression_simple(l)?),
                Box::new(self.resolve_expression_simple(r)?),
            ),
            Not(e) => Expression::Not(Box::new(self.resolve_expression_simple(e)?)),
            Equal(l, r) => Expression::Equal(
                Box::new(self.resolve_expression_simple(l)?),
                Box::new(self.resolve_expression_simple(r)?),
            ),
            NotEqual(l, r) => Expression::NotEqual(
                Box::new(self.resolve_expression_simple(l)?),
                Box::new(self.resolve_expression_simple(r)?),
            ),
            GreaterThan(l, r) => Expression::GreaterThan(
                Box::new(self.resolve_expression_simple(l)?),
                Box::new(self.resolve_expression_simple(r)?),
            ),
            GreaterThanOrEqual(l, r) => Expression::GreaterThanOrEqual(
                Box::new(self.resolve_expression_simple(l)?),
                Box::new(self.resolve_expression_simple(r)?),
            ),
            LessThan(l, r) => Expression::LessThan(
                Box::new(self.resolve_expression_simple(l)?),
                Box::new(self.resolve_expression_simple(r)?),
            ),
            LessThanOrEqual(l, r) => Expression::LessThanOrEqual(
                Box::new(self.resolve_expression_simple(l)?),
                Box::new(self.resolve_expression_simple(r)?),
            ),
            Is(e, lit) => {
                let value = match lit {
                    Literal::Null => crate::types::Value::Null,
                    _ => return Err(Error::ExecutionError("IS only supports NULL".into())),
                };
                Expression::Is(Box::new(self.resolve_expression_simple(e)?), value)
            }
            Add(l, r) => Expression::Add(
                Box::new(self.resolve_expression_simple(l)?),
                Box::new(self.resolve_expression_simple(r)?),
            ),
            Concat(l, r) => Expression::Concat(
                Box::new(self.resolve_expression_simple(l)?),
                Box::new(self.resolve_expression_simple(r)?),
            ),
            Subtract(l, r) => Expression::Subtract(
                Box::new(self.resolve_expression_simple(l)?),
                Box::new(self.resolve_expression_simple(r)?),
            ),
            Multiply(l, r) => Expression::Multiply(
                Box::new(self.resolve_expression_simple(l)?),
                Box::new(self.resolve_expression_simple(r)?),
            ),
            Divide(l, r) => Expression::Divide(
                Box::new(self.resolve_expression_simple(l)?),
                Box::new(self.resolve_expression_simple(r)?),
            ),
            Remainder(l, r) => Expression::Remainder(
                Box::new(self.resolve_expression_simple(l)?),
                Box::new(self.resolve_expression_simple(r)?),
            ),
            Exponentiate(l, r) => Expression::Exponentiate(
                Box::new(self.resolve_expression_simple(l)?),
                Box::new(self.resolve_expression_simple(r)?),
            ),
            // Unary operators
            Negate(e) => Expression::Negate(Box::new(self.resolve_expression_simple(e)?)),
            Identity(e) => Expression::Identity(Box::new(self.resolve_expression_simple(e)?)),
            Factorial(e) => Expression::Factorial(Box::new(self.resolve_expression_simple(e)?)),
            // Bitwise operators
            BitwiseAnd(l, r) => Expression::BitwiseAnd(
                Box::new(self.resolve_expression_simple(l)?),
                Box::new(self.resolve_expression_simple(r)?),
            ),
            BitwiseOr(l, r) => Expression::BitwiseOr(
                Box::new(self.resolve_expression_simple(l)?),
                Box::new(self.resolve_expression_simple(r)?),
            ),
            BitwiseXor(l, r) => Expression::BitwiseXor(
                Box::new(self.resolve_expression_simple(l)?),
                Box::new(self.resolve_expression_simple(r)?),
            ),
            BitwiseNot(e) => Expression::BitwiseNot(Box::new(self.resolve_expression_simple(e)?)),
            BitwiseShiftLeft(l, r) => Expression::BitwiseShiftLeft(
                Box::new(self.resolve_expression_simple(l)?),
                Box::new(self.resolve_expression_simple(r)?),
            ),
            BitwiseShiftRight(l, r) => Expression::BitwiseShiftRight(
                Box::new(self.resolve_expression_simple(l)?),
                Box::new(self.resolve_expression_simple(r)?),
            ),
            // String matching
            ILike {
                expr,
                pattern,
                negated,
            } => Expression::ILike(
                Box::new(self.resolve_expression_simple(expr)?),
                Box::new(self.resolve_expression_simple(pattern)?),
                *negated,
            ),
            Like {
                expr,
                pattern,
                negated,
            } => Expression::Like(
                Box::new(self.resolve_expression_simple(expr)?),
                Box::new(self.resolve_expression_simple(pattern)?),
                *negated,
            ),
            // Range operators
            Between {
                expr,
                low,
                high,
                negated,
            } => Expression::Between(
                Box::new(self.resolve_expression_simple(expr)?),
                Box::new(self.resolve_expression_simple(low)?),
                Box::new(self.resolve_expression_simple(high)?),
                *negated,
            ),
            // IN operator
            InList {
                expr,
                list,
                negated,
            } => {
                let resolved_list = list
                    .iter()
                    .map(|e| self.resolve_expression_simple(e))
                    .collect::<Result<Vec<_>>>()?;
                Expression::InList(
                    Box::new(self.resolve_expression_simple(expr)?),
                    resolved_list,
                    *negated,
                )
            }
            // IN subquery operator - use SubqueryPlanner to eliminate duplication
            InSubquery {
                expr,
                subquery,
                negated,
            } => {
                if let AstExpression::Subquery(select) = subquery.as_ref() {
                    // Use SubqueryPlanner to eliminate duplication
                    let subquery_plan = SubqueryPlanner::plan_with_outer_context(
                        select,
                        self.schemas,
                        self.index_metadata,
                        &self.analyzed.column_resolution_map,
                    )?;

                    Expression::InSubquery(
                        Box::new(self.resolve_expression_simple(expr)?),
                        Box::new(subquery_plan),
                        *negated,
                    )
                } else {
                    return Err(Error::ExecutionError(
                        "Invalid subquery in IN clause".to_string(),
                    ));
                }
            }
            // EXISTS operator - use SubqueryPlanner to eliminate duplication
            Exists { subquery, negated } => {
                if let AstExpression::Subquery(select) = subquery.as_ref() {
                    // Use SubqueryPlanner to eliminate duplication
                    let subquery_plan = SubqueryPlanner::plan_with_outer_context(
                        select,
                        self.schemas,
                        self.index_metadata,
                        &self.analyzed.column_resolution_map,
                    )?;

                    Expression::Exists(Box::new(subquery_plan), *negated)
                } else {
                    return Err(Error::ExecutionError(
                        "Invalid subquery in EXISTS clause".to_string(),
                    ));
                }
            }
        })
    }
}

/// Resolves a DEFAULT expression (which shouldn't have column references)
///
/// This function converts AST expressions used in DEFAULT clauses to
/// DefaultExpression values. DEFAULT expressions are more restricted than
/// regular expressions - they cannot reference columns or contain subqueries.
pub(super) fn resolve_default_expression(
    expr: &AstExpression,
) -> Result<crate::types::expression::DefaultExpression> {
    use crate::parsing::ast::Literal;
    use crate::types::expression::DefaultExpression;

    match expr {
        AstExpression::Literal(lit) => {
            let value = match lit {
                Literal::Null => crate::types::Value::Null,
                Literal::Boolean(b) => crate::types::Value::Bool(*b),
                Literal::Integer(n) => {
                    if *n >= i32::MIN as i128 && *n <= i32::MAX as i128 {
                        crate::types::Value::I32(*n as i32)
                    } else if *n >= i64::MIN as i128 && *n <= i64::MAX as i128 {
                        crate::types::Value::I64(*n as i64)
                    } else {
                        crate::types::Value::I128(*n)
                    }
                }
                Literal::Float(f) => crate::types::Value::F64(*f),
                Literal::String(s) => crate::types::Value::Str(s.clone()),
                Literal::Bytea(b) => crate::types::Value::Bytea(b.clone()),
                Literal::Date(d) => crate::types::Value::Date(*d),
                Literal::Time(t) => crate::types::Value::Time(*t),
                Literal::Timestamp(ts) => crate::types::Value::Timestamp(*ts),
                Literal::Interval(i) => crate::types::Value::Interval(i.clone()),
            };
            Ok(DefaultExpression::Constant(value))
        }

        AstExpression::Function(name, args) => {
            let resolved_args = args
                .iter()
                .map(resolve_default_expression)
                .collect::<Result<Vec<_>>>()?;
            Ok(DefaultExpression::Function(name.clone(), resolved_args))
        }

        AstExpression::Operator(op) => {
            use crate::parsing::Operator::*;

            Ok(match op {
                Add(l, r) => DefaultExpression::Add(
                    Box::new(resolve_default_expression(l)?),
                    Box::new(resolve_default_expression(r)?),
                ),
                Concat(l, r) => DefaultExpression::Concat(
                    Box::new(resolve_default_expression(l)?),
                    Box::new(resolve_default_expression(r)?),
                ),
                Subtract(l, r) => DefaultExpression::Subtract(
                    Box::new(resolve_default_expression(l)?),
                    Box::new(resolve_default_expression(r)?),
                ),
                Multiply(l, r) => DefaultExpression::Multiply(
                    Box::new(resolve_default_expression(l)?),
                    Box::new(resolve_default_expression(r)?),
                ),
                Divide(l, r) => DefaultExpression::Divide(
                    Box::new(resolve_default_expression(l)?),
                    Box::new(resolve_default_expression(r)?),
                ),
                Remainder(l, r) => DefaultExpression::Remainder(
                    Box::new(resolve_default_expression(l)?),
                    Box::new(resolve_default_expression(r)?),
                ),
                Exponentiate(l, r) => DefaultExpression::Exponentiate(
                    Box::new(resolve_default_expression(l)?),
                    Box::new(resolve_default_expression(r)?),
                ),
                BitwiseAnd(l, r) => DefaultExpression::BitwiseAnd(
                    Box::new(resolve_default_expression(l)?),
                    Box::new(resolve_default_expression(r)?),
                ),
                BitwiseOr(l, r) => DefaultExpression::BitwiseOr(
                    Box::new(resolve_default_expression(l)?),
                    Box::new(resolve_default_expression(r)?),
                ),
                BitwiseXor(l, r) => DefaultExpression::BitwiseXor(
                    Box::new(resolve_default_expression(l)?),
                    Box::new(resolve_default_expression(r)?),
                ),
                BitwiseShiftLeft(l, r) => DefaultExpression::BitwiseShiftLeft(
                    Box::new(resolve_default_expression(l)?),
                    Box::new(resolve_default_expression(r)?),
                ),
                BitwiseShiftRight(l, r) => DefaultExpression::BitwiseShiftRight(
                    Box::new(resolve_default_expression(l)?),
                    Box::new(resolve_default_expression(r)?),
                ),
                Negate(e) => DefaultExpression::Negate(Box::new(resolve_default_expression(e)?)),
                Identity(e) => {
                    DefaultExpression::Identity(Box::new(resolve_default_expression(e)?))
                }
                BitwiseNot(e) => {
                    DefaultExpression::BitwiseNot(Box::new(resolve_default_expression(e)?))
                }
                // Boolean operators
                And(l, r) => DefaultExpression::And(
                    Box::new(resolve_default_expression(l)?),
                    Box::new(resolve_default_expression(r)?),
                ),
                Or(l, r) => DefaultExpression::Or(
                    Box::new(resolve_default_expression(l)?),
                    Box::new(resolve_default_expression(r)?),
                ),
                Xor(l, r) => DefaultExpression::Xor(
                    Box::new(resolve_default_expression(l)?),
                    Box::new(resolve_default_expression(r)?),
                ),
                Not(e) => DefaultExpression::Not(Box::new(resolve_default_expression(e)?)),

                // Comparison operators
                Equal(l, r) => DefaultExpression::Equal(
                    Box::new(resolve_default_expression(l)?),
                    Box::new(resolve_default_expression(r)?),
                ),
                GreaterThan(l, r) => DefaultExpression::GreaterThan(
                    Box::new(resolve_default_expression(l)?),
                    Box::new(resolve_default_expression(r)?),
                ),
                LessThan(l, r) => DefaultExpression::LessThan(
                    Box::new(resolve_default_expression(l)?),
                    Box::new(resolve_default_expression(r)?),
                ),
                GreaterThanOrEqual(l, r) => DefaultExpression::GreaterThanOrEqual(
                    Box::new(resolve_default_expression(l)?),
                    Box::new(resolve_default_expression(r)?),
                ),
                LessThanOrEqual(l, r) => DefaultExpression::LessThanOrEqual(
                    Box::new(resolve_default_expression(l)?),
                    Box::new(resolve_default_expression(r)?),
                ),
                NotEqual(l, r) => DefaultExpression::NotEqual(
                    Box::new(resolve_default_expression(l)?),
                    Box::new(resolve_default_expression(r)?),
                ),

                // IS NULL
                Is(e, lit) => {
                    let value = match lit {
                        Literal::Null => crate::types::Value::Null,
                        _ => return Err(Error::ExecutionError("IS only supports NULL".into())),
                    };
                    DefaultExpression::Is(Box::new(resolve_default_expression(e)?), value)
                }

                // Pattern matching
                Like {
                    expr,
                    pattern,
                    negated,
                } => DefaultExpression::Like(
                    Box::new(resolve_default_expression(expr)?),
                    Box::new(resolve_default_expression(pattern)?),
                    *negated,
                ),
                ILike {
                    expr,
                    pattern,
                    negated,
                } => DefaultExpression::ILike(
                    Box::new(resolve_default_expression(expr)?),
                    Box::new(resolve_default_expression(pattern)?),
                    *negated,
                ),

                // Other operators
                Factorial(e) => {
                    DefaultExpression::Factorial(Box::new(resolve_default_expression(e)?))
                }

                // IN and BETWEEN
                InList {
                    expr,
                    list,
                    negated,
                } => {
                    let resolved_expr = Box::new(resolve_default_expression(expr)?);
                    let resolved_list = list
                        .iter()
                        .map(resolve_default_expression)
                        .collect::<Result<Vec<_>>>()?;
                    DefaultExpression::InList(resolved_expr, resolved_list, *negated)
                }
                Between {
                    expr,
                    low,
                    high,
                    negated,
                } => {
                    let resolved_expr = Box::new(resolve_default_expression(expr)?);
                    let resolved_low = Box::new(resolve_default_expression(low)?);
                    let resolved_high = Box::new(resolve_default_expression(high)?);
                    DefaultExpression::Between(resolved_expr, resolved_low, resolved_high, *negated)
                }

                // Subqueries are NOT allowed in DEFAULT
                InSubquery { .. } | Exists { .. } => {
                    return Err(Error::ExecutionError(
                        "Subqueries are not allowed in DEFAULT expressions".into(),
                    ));
                }
            })
        }

        AstExpression::ArrayLiteral(elements) => {
            let resolved_elements = elements
                .iter()
                .map(resolve_default_expression)
                .collect::<Result<Vec<_>>>()?;
            Ok(DefaultExpression::ArrayLiteral(resolved_elements))
        }

        AstExpression::MapLiteral(pairs) => {
            let resolved_pairs = pairs
                .iter()
                .map(|(k, v)| {
                    Ok((
                        resolve_default_expression(k)?,
                        resolve_default_expression(v)?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(DefaultExpression::MapLiteral(resolved_pairs))
        }

        _ => Err(Error::ExecutionError(
            "Expression type not supported in DEFAULT expressions".into(),
        )),
    }
}
