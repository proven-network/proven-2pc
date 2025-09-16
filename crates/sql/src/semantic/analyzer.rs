//! Main semantic analyzer orchestrator

use super::annotated_ast::AnnotatedStatement;
use super::context::AnalysisContext;
use super::resolver::{ColumnResolver, ScopeManager, TableResolver};
use super::type_checker::TypeChecker;
use super::types::StatementMetadata;
use super::validator::{ConstraintValidator, ExpressionValidator, FunctionValidator, StatementValidator};
use crate::error::Result;
use crate::parsing::ast::Statement;
use crate::types::schema::Table;
use std::collections::HashMap;

/// Analyzed statement with metadata
#[derive(Debug, Clone)]
pub struct AnalyzedStatement {
    /// The annotated statement
    pub statement: AnnotatedStatement,
    /// Metadata collected during analysis
    pub metadata: StatementMetadata,
}

/// Main semantic analyzer that orchestrates all analysis phases
pub struct SemanticAnalyzer {
    /// Available schemas for validation
    schemas: HashMap<String, Table>,
    /// Type checker instance
    type_checker: TypeChecker,
    /// Table resolver
    table_resolver: TableResolver,
    /// Column resolver
    column_resolver: ColumnResolver,
    /// Scope manager
    scope_manager: ScopeManager,
    /// Expression validator
    expression_validator: ExpressionValidator,
    /// Statement validator
    statement_validator: StatementValidator,
    /// Constraint validator
    constraint_validator: ConstraintValidator,
    /// Function validator
    function_validator: FunctionValidator,
}

impl SemanticAnalyzer {
    /// Create a new semantic analyzer
    pub fn new(schemas: HashMap<String, Table>) -> Self {
        let type_checker = TypeChecker::new();
        let table_resolver = TableResolver::new(schemas.clone());
        let column_resolver = ColumnResolver::new();
        let scope_manager = ScopeManager::new();
        let expression_validator = ExpressionValidator::new();
        let statement_validator = StatementValidator::new();
        let constraint_validator = ConstraintValidator::new();
        let function_validator = FunctionValidator::new();

        Self {
            schemas,
            type_checker,
            table_resolver,
            column_resolver,
            scope_manager,
            expression_validator,
            statement_validator,
            constraint_validator,
            function_validator,
        }
    }

    /// Analyze a statement
    pub fn analyze(&mut self, statement: Statement) -> Result<AnalyzedStatement> {
        // Create analysis context
        let mut context = AnalysisContext::new(self.schemas.clone());

        // Phase 1: Name resolution
        self.resolve_names(&statement, &mut context)?;

        // Phase 2: Type checking and inference
        let annotated = self.type_check(statement, &mut context)?;

        // Phase 3: Validation
        self.validate(&annotated, &mut context)?;

        // Extract metadata
        let metadata = context.into_metadata();

        Ok(AnalyzedStatement {
            statement: annotated,
            metadata,
        })
    }

    /// Phase 1: Resolve all names in the statement
    fn resolve_names(&mut self, statement: &Statement, context: &mut AnalysisContext) -> Result<()> {
        // First resolve table references
        self.table_resolver.resolve_statement(statement, context)?;

        // Then resolve column references within the established table context
        self.column_resolver.resolve_statement(statement, context)?;

        // Manage scopes for subqueries
        self.scope_manager.analyze_statement(statement, context)?;

        Ok(())
    }

    /// Phase 2: Type check and annotate the statement
    fn type_check(&mut self, statement: Statement, context: &mut AnalysisContext) -> Result<AnnotatedStatement> {
        self.type_checker.check_statement(statement, context)
    }

    /// Phase 3: Validate constraints and semantic rules
    fn validate(&mut self, statement: &AnnotatedStatement, context: &mut AnalysisContext) -> Result<()> {
        // Validate expressions
        self.expression_validator.validate_statement(statement, context)?;

        // Validate statement-level rules
        self.statement_validator.validate(statement, context)?;

        // Validate constraints
        self.constraint_validator.validate_statement(statement, context)?;

        // Validate function calls
        self.function_validator.validate_statement(statement, context)?;

        Ok(())
    }
}