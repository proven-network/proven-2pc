//! Statement-level validation

use crate::error::{Error, Result};
use crate::semantic::annotated_ast::AnnotatedStatement;
use crate::semantic::context::AnalysisContext;

/// Validator for statement-level rules
pub struct StatementValidator {
    /// Whether to allow mutations
    allow_mutations: bool,
}

impl StatementValidator {
    /// Create a new statement validator
    pub fn new() -> Self {
        Self {
            allow_mutations: true,
        }
    }

    /// Validate a statement
    pub fn validate(
        &self,
        statement: &AnnotatedStatement,
        context: &mut AnalysisContext,
    ) -> Result<()> {
        match statement {
            AnnotatedStatement::Select(select) => self.validate_select(select, context),
            AnnotatedStatement::Insert(insert) => self.validate_insert(insert, context),
            AnnotatedStatement::Update(update) => self.validate_update(update, context),
            AnnotatedStatement::Delete(delete) => self.validate_delete(delete, context),
            AnnotatedStatement::CreateTable(create) => self.validate_create_table(create, context),
            AnnotatedStatement::Ddl(_) => Ok(()),
        }
    }

    /// Validate SELECT statement rules
    fn validate_select(
        &self,
        select: &super::super::annotated_ast::AnnotatedSelect,
        context: &mut AnalysisContext,
    ) -> Result<()> {
        // TODO: Implement SELECT validation rules
        // - Validate GROUP BY with non-aggregate columns
        // - Validate HAVING without GROUP BY
        // - Validate ORDER BY references
        // - Check for ambiguous column references
        Ok(())
    }

    /// Validate INSERT statement rules
    fn validate_insert(
        &self,
        insert: &super::super::annotated_ast::AnnotatedInsert,
        context: &mut AnalysisContext,
    ) -> Result<()> {
        if !self.allow_mutations {
            return Err(Error::ExecutionError("Mutations not allowed".to_string()));
        }

        context.set_mutation(true);

        // TODO: Implement INSERT validation rules
        // - Check column count matches value count
        // - Validate NOT NULL columns have values
        // - Check for duplicate column specifications
        Ok(())
    }

    /// Validate UPDATE statement rules
    fn validate_update(
        &self,
        update: &super::super::annotated_ast::AnnotatedUpdate,
        context: &mut AnalysisContext,
    ) -> Result<()> {
        if !self.allow_mutations {
            return Err(Error::ExecutionError("Mutations not allowed".to_string()));
        }

        context.set_mutation(true);

        // TODO: Implement UPDATE validation rules
        // - Check for duplicate column assignments
        // - Validate column exists in table
        Ok(())
    }

    /// Validate DELETE statement rules
    fn validate_delete(
        &self,
        delete: &super::super::annotated_ast::AnnotatedDelete,
        context: &mut AnalysisContext,
    ) -> Result<()> {
        if !self.allow_mutations {
            return Err(Error::ExecutionError("Mutations not allowed".to_string()));
        }

        context.set_mutation(true);

        // TODO: Implement DELETE validation rules
        // - Warn if no WHERE clause
        Ok(())
    }

    /// Validate CREATE TABLE statement rules
    fn validate_create_table(
        &self,
        create: &super::super::annotated_ast::AnnotatedCreateTable,
        context: &mut AnalysisContext,
    ) -> Result<()> {
        context.set_mutation(true);

        // TODO: Implement CREATE TABLE validation rules
        // - Check for duplicate column names
        // - Validate primary key columns exist
        // - Validate foreign key references
        // - Check constraint expressions
        Ok(())
    }
}
