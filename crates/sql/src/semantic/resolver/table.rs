//! Table name resolution

use crate::error::Result;
use crate::parsing::ast::{DmlStatement, FromClause, SelectStatement, Statement};
use crate::semantic::context::AnalysisContext;
use crate::types::schema::Table;
use std::collections::HashMap;

/// Resolver for table names
pub struct TableResolver {
    /// Available schemas
    schemas: HashMap<String, Table>,
}

impl TableResolver {
    /// Create a new table resolver
    pub fn new(schemas: HashMap<String, Table>) -> Self {
        Self { schemas }
    }

    /// Resolve tables in a statement
    pub fn resolve_statement(&mut self, statement: &Statement, context: &mut AnalysisContext) -> Result<()> {
        match statement {
            Statement::Dml(dml) => match dml {
                DmlStatement::Select(select) => {
                    self.resolve_select_tables(select, context)?;
                }
                DmlStatement::Insert { table, .. } => {
                    context.add_table(table.clone(), None)?;
                }
                DmlStatement::Update { table, .. } => {
                    context.add_table(table.clone(), None)?;
                }
                DmlStatement::Delete { table, .. } => {
                    context.add_table(table.clone(), None)?;
                }
            },
            _ => {
                // DDL statements don't need table resolution
            }
        }
        Ok(())
    }

    /// Resolve tables in a SELECT statement
    fn resolve_select_tables(&mut self, select: &SelectStatement, context: &mut AnalysisContext) -> Result<()> {
        for from_clause in &select.from {
            self.resolve_from_clause(from_clause, context)?;
        }
        Ok(())
    }

    /// Resolve a FROM clause
    fn resolve_from_clause(&mut self, from: &FromClause, context: &mut AnalysisContext) -> Result<()> {
        match from {
            FromClause::Table { name, alias } => {
                context.add_table(name.clone(), alias.clone())?;
            }
            FromClause::Join { left, right, .. } => {
                // Recursively resolve both sides
                self.resolve_from_clause(left, context)?;
                self.resolve_from_clause(right, context)?;
            }
        }
        Ok(())
    }

    /// Check if a table exists
    pub fn table_exists(&self, table_name: &str) -> bool {
        self.schemas.contains_key(table_name)
    }

    /// Get a table schema
    pub fn get_table(&self, table_name: &str) -> Option<&Table> {
        self.schemas.get(table_name)
    }
}