//! Name resolution phase of semantic analysis
//!
//! This module handles all name resolution, including:
//! - Table and alias resolution
//! - Column name resolution and ambiguity detection
//! - Function name resolution
//! - Building the column resolution map for O(1) lookups

use super::statement::{ColumnResolution, ColumnResolutionMap};
use crate::error::{Error, Result};
use crate::parsing::ast::{DmlStatement, Expression, FromClause, SelectStatement, Statement};
use crate::types::schema::Table;
use std::collections::HashMap;

/// Handles all name resolution in the semantic analysis phase
pub struct NameResolver {
    /// Available table schemas
    schemas: HashMap<String, Table>,
}

impl NameResolver {
    /// Create a new name resolver
    pub fn new(schemas: HashMap<String, Table>) -> Self {
        Self { schemas }
    }

    /// Extract tables from a statement
    pub fn extract_tables(&self, statement: &Statement) -> Result<Vec<(Option<String>, String)>> {
        let mut tables = Vec::new();

        if let Statement::Dml(dml) = statement {
            match dml {
                DmlStatement::Select(select) => {
                    for from_clause in &select.from {
                        self.extract_from_tables(from_clause, &mut tables)?;
                    }
                }
                DmlStatement::Insert { table, .. } => {
                    self.validate_table_exists(table)?;
                    tables.push((None, table.clone()));
                }
                DmlStatement::Update { table, .. } => {
                    self.validate_table_exists(table)?;
                    tables.push((None, table.clone()));
                }
                DmlStatement::Delete { table, .. } => {
                    self.validate_table_exists(table)?;
                    tables.push((None, table.clone()));
                }
            }
        }

        Ok(tables)
    }

    /// Extract tables from FROM clause
    fn extract_from_tables(
        &self,
        from: &FromClause,
        tables: &mut Vec<(Option<String>, String)>,
    ) -> Result<()> {
        match from {
            FromClause::Table { name, alias } => {
                self.validate_table_exists(name)?;
                tables.push((alias.clone(), name.clone()));
            }
            FromClause::Join { left, right, .. } => {
                self.extract_from_tables(left, tables)?;
                self.extract_from_tables(right, tables)?;
            }
        }
        Ok(())
    }

    /// Build column map from resolved tables
    pub fn build_column_map(
        &self,
        tables: &[(Option<String>, String)],
        schemas: &HashMap<String, Table>,
    ) -> Result<ColumnResolutionMap> {
        let mut resolution_map = ColumnResolutionMap::default();
        let mut column_counts: HashMap<String, usize> = HashMap::new();
        let mut global_offset = 0;

        for (table_index, (alias, table_name)) in tables.iter().enumerate() {
            if let Some(schema) = schemas.get(table_name) {
                for (column_index, column) in schema.columns.iter().enumerate() {
                    // Track column name frequency
                    *column_counts.entry(column.name.clone()).or_insert(0) += 1;

                    let resolution = ColumnResolution {
                        table_index,
                        column_index,
                        global_offset,
                        table_name: table_name.clone(),
                        data_type: column.datatype.clone(),
                        nullable: column.nullable,
                        is_indexed: column.index,
                    };

                    let table_qualifier = alias.clone().unwrap_or_else(|| table_name.clone());
                    resolution_map.add_resolution(
                        Some(table_qualifier),
                        column.name.clone(),
                        resolution,
                    );

                    global_offset += 1;
                }
            }
        }

        // Mark ambiguous columns
        for (column_name, count) in column_counts {
            if count > 1 {
                resolution_map.mark_ambiguous(column_name);
            }
        }

        Ok(resolution_map)
    }

    /// Recursively collect and validate functions
    fn collect_functions_from_statement(
        &self,
        statement: &Statement,
        functions: &mut Vec<String>,
    ) -> Result<()> {
        if let Statement::Dml(dml) = statement {
            if let DmlStatement::Select(select) = dml {
                for (expr, _) in &select.select {
                    self.collect_functions_from_expr(expr, functions)?;
                }
                if let Some(where_expr) = &select.r#where {
                    self.collect_functions_from_expr(where_expr, functions)?;
                }
                for expr in &select.group_by {
                    self.collect_functions_from_expr(expr, functions)?;
                }
                if let Some(having) = &select.having {
                    self.collect_functions_from_expr(having, functions)?;
                }
                for (expr, _) in &select.order_by {
                    self.collect_functions_from_expr(expr, functions)?;
                }
            }
        }
        Ok(())
    }

    /// Collect functions from expressions
    fn collect_functions_from_expr(
        &self,
        expr: &Expression,
        functions: &mut Vec<String>,
    ) -> Result<()> {
        match expr {
            Expression::Function(name, args) => {
                // Validate function exists
                if crate::functions::get_function(name).is_none() {
                    return Err(Error::ExecutionError(format!("Unknown function: {}", name)));
                }
                functions.push(name.clone());

                // Recurse into arguments
                for arg in args {
                    self.collect_functions_from_expr(arg, functions)?;
                }
            }
            Expression::Operator(op) => {
                self.collect_functions_from_operator(op, functions)?;
            }
            _ => {}
        }
        Ok(())
    }

    /// Helper to collect functions from operators
    fn collect_functions_from_operator(
        &self,
        op: &crate::parsing::ast::Operator,
        functions: &mut Vec<String>,
    ) -> Result<()> {
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
                self.collect_functions_from_expr(l, functions)?;
                self.collect_functions_from_expr(r, functions)?;
            }
            Operator::Not(e)
            | Operator::Negate(e)
            | Operator::Identity(e)
            | Operator::Factorial(e) => {
                self.collect_functions_from_expr(e, functions)?;
            }
            Operator::Between {
                expr, low, high, ..
            } => {
                self.collect_functions_from_expr(expr, functions)?;
                self.collect_functions_from_expr(low, functions)?;
                self.collect_functions_from_expr(high, functions)?;
            }
            Operator::InList { expr, list, .. } => {
                self.collect_functions_from_expr(expr, functions)?;
                for item in list {
                    self.collect_functions_from_expr(item, functions)?;
                }
            }
            Operator::Is(e, _) => {
                self.collect_functions_from_expr(e, functions)?;
            }
        }
        Ok(())
    }

    /// Main entry point: resolve all names in a statement
    pub fn resolve_names(&self, statement: &Statement) -> Result<()> {
        match statement {
            Statement::Dml(dml) => self.resolve_dml_names(dml),
            Statement::Ddl(_) => {
                // DDL statements have simpler name resolution
                // Table names are validated elsewhere
                Ok(())
            }
            Statement::Explain(_) => Ok(()),
        }
    }

    /// Resolve names in DML statements
    fn resolve_dml_names(&self, statement: &DmlStatement) -> Result<()> {
        match statement {
            DmlStatement::Select(select) => self.resolve_select_names(select),
            DmlStatement::Insert { table, .. } => {
                // Validate table exists
                self.validate_table_exists(table)?;
                Ok(())
            }
            DmlStatement::Update { table, .. } => {
                self.validate_table_exists(table)?;
                Ok(())
            }
            DmlStatement::Delete { table, .. } => {
                self.validate_table_exists(table)?;
                Ok(())
            }
        }
    }

    /// Resolve names in SELECT statement
    fn resolve_select_names(&self, select: &SelectStatement) -> Result<()> {
        // Resolve FROM clause tables
        for from_clause in &select.from {
            self.resolve_from_clause(from_clause)?;
        }

        // After all tables are resolved, validate column references
        // This is done during type checking phase

        Ok(())
    }

    /// Resolve FROM clause tables and aliases
    fn resolve_from_clause(&self, from: &FromClause) -> Result<()> {
        match from {
            FromClause::Table { name, alias: _ } => {
                self.validate_table_exists(name)?;
            }
            FromClause::Join { left, right, .. } => {
                // Recursively resolve both sides
                self.resolve_from_clause(left)?;
                self.resolve_from_clause(right)?;
            }
        }
        Ok(())
    }

    /// Validate that a table exists in the schema
    fn validate_table_exists(&self, table_name: &str) -> Result<()> {
        if !self.schemas.contains_key(table_name) {
            return Err(Error::TableNotFound(table_name.to_string()));
        }
        Ok(())
    }

    /// Resolve function names and validate they exist
    pub fn resolve_function_names(&self, expr: &Expression) -> Result<()> {
        match expr {
            Expression::Function(name, args) => {
                // Validate function exists
                if crate::functions::get_function(name).is_none() {
                    return Err(Error::ExecutionError(format!("Unknown function: {}", name)));
                }

                // Recursively check arguments
                for arg in args {
                    self.resolve_function_names(arg)?;
                }
                Ok(())
            }
            Expression::Operator(op) => {
                use crate::parsing::ast::Operator;
                // Recursively check operator arguments
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
                        self.resolve_function_names(l)?;
                        self.resolve_function_names(r)?;
                    }
                    Operator::Not(e)
                    | Operator::Negate(e)
                    | Operator::Identity(e)
                    | Operator::Factorial(e) => {
                        self.resolve_function_names(e)?;
                    }
                    Operator::Between {
                        expr,
                        low,
                        high,
                        negated: _,
                    } => {
                        self.resolve_function_names(expr)?;
                        self.resolve_function_names(low)?;
                        self.resolve_function_names(high)?;
                    }
                    Operator::InList {
                        expr,
                        list,
                        negated: _,
                    } => {
                        self.resolve_function_names(expr)?;
                        for item in list {
                            self.resolve_function_names(item)?;
                        }
                    }
                    Operator::Is(e, _) => {
                        self.resolve_function_names(e)?;
                    }
                }
                Ok(())
            }
            Expression::Case { .. } => {
                // Case expressions handled separately if needed
                Ok(())
            }
            Expression::ArrayAccess { base, index } => {
                self.resolve_function_names(base)?;
                self.resolve_function_names(index)
            }
            Expression::FieldAccess { base, field: _ } => self.resolve_function_names(base),
            Expression::ArrayLiteral(elements) => {
                for elem in elements {
                    self.resolve_function_names(elem)?;
                }
                Ok(())
            }
            Expression::MapLiteral(entries) => {
                for (key, value) in entries {
                    self.resolve_function_names(key)?;
                    self.resolve_function_names(value)?;
                }
                Ok(())
            }
            _ => Ok(()), // Literals, columns, parameters don't need function resolution
        }
    }

    /// Update schemas for cache invalidation
    pub fn update_schemas(&mut self, schemas: HashMap<String, Table>) {
        self.schemas = schemas;
    }
}
