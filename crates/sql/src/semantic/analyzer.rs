//! Semantic analyzer that produces lightweight analyzed statements
//!
//! This analyzer creates AnalyzedStatements with Arc-wrapped ASTs
//! and separate type annotations, enabling efficient caching and
//! zero-copy parameter binding.

use super::context::AnalysisContext;
use super::resolver::{ColumnResolver, TableResolver};
use super::statement::{
    AccessType, AnalyzedStatement, CoercionContext, ExpressionId, ParameterSlot, SqlContext,
    StatementType, TableAccess, TypeInfo,
};
use super::type_checker::TypeChecker;
use super::validators::{
    ConstraintValidator, ExpressionValidator, FunctionValidator, StatementValidator,
};

use crate::error::{Error, Result};
use crate::parsing::ast::{DdlStatement, DmlStatement, Expression, Statement};
use crate::types::data_type::DataType;
use crate::types::schema::Table;

use std::collections::HashMap;
use std::sync::Arc;

/// Semantic analyzer with efficient AST handling
pub struct SemanticAnalyzer {
    /// Available table schemas
    schemas: HashMap<String, Table>,

    /// Component analyzers
    table_resolver: TableResolver,
    column_resolver: ColumnResolver,
    type_checker: TypeChecker,
    expression_validator: ExpressionValidator,
    statement_validator: StatementValidator,
    constraint_validator: ConstraintValidator,
    function_validator: FunctionValidator,
}

impl SemanticAnalyzer {
    /// Create a new semantic analyzer
    pub fn new(schemas: HashMap<String, Table>) -> Self {
        let table_resolver = TableResolver::new(schemas.clone());
        let column_resolver = ColumnResolver;
        let type_checker = TypeChecker::new();
        let expression_validator = ExpressionValidator::new();
        let statement_validator = StatementValidator::new();
        let constraint_validator = ConstraintValidator::new();
        let function_validator = FunctionValidator::new();

        Self {
            schemas,
            table_resolver,
            column_resolver,
            type_checker,
            expression_validator,
            statement_validator,
            constraint_validator,
            function_validator,
        }
    }

    /// Update schemas (for cache invalidation)
    pub fn update_schemas(&mut self, schemas: HashMap<String, Table>) {
        self.schemas = schemas.clone();
        self.table_resolver = TableResolver::new(schemas);
        // column_resolver is stateless, no update needed
    }

    /// Analyze a statement and produce a lightweight result
    pub fn analyze(&mut self, statement: Statement) -> Result<AnalyzedStatement> {
        // Wrap the statement in Arc for sharing
        let ast = Arc::new(statement);

        // Create the analyzed statement
        let mut analyzed = AnalyzedStatement::new(ast.clone());

        // Create analysis context
        let mut context = AnalysisContext::new(self.schemas.clone());

        // Perform analysis phases
        self.analyze_statement(&ast, &mut analyzed, &mut context)?;

        // Run validators
        self.statement_validator.validate(&analyzed, &context)?;
        self.constraint_validator
            .validate(&mut analyzed, &context)?;
        self.function_validator.validate(&mut analyzed, &context)?;

        // Sort parameter slots by index
        analyzed.sort_parameters();

        Ok(analyzed)
    }

    /// Main analysis dispatcher
    fn analyze_statement(
        &mut self,
        statement: &Statement,
        analyzed: &mut AnalyzedStatement,
        context: &mut AnalysisContext,
    ) -> Result<()> {
        // Set statement type
        analyzed.metadata.statement_type = self.get_statement_type(statement);

        match statement {
            Statement::Dml(dml) => self.analyze_dml(dml, analyzed, context)?,
            Statement::Ddl(ddl) => self.analyze_ddl(ddl, analyzed, context)?,
            _ => {} // Other statement types
        }

        // Extract metadata from context
        self.extract_metadata(analyzed, context);

        Ok(())
    }

    /// Analyze DML statements
    fn analyze_dml(
        &mut self,
        statement: &DmlStatement,
        analyzed: &mut AnalyzedStatement,
        context: &mut AnalysisContext,
    ) -> Result<()> {
        match statement {
            DmlStatement::Select(select) => {
                self.analyze_select(select, analyzed, context)?;
            }
            DmlStatement::Insert {
                table,
                columns,
                source,
            } => {
                self.analyze_insert(table, columns.as_ref(), source, analyzed, context)?;
                analyzed.metadata.is_mutation = true;
            }
            DmlStatement::Update {
                table,
                set,
                r#where,
            } => {
                self.analyze_update(table, set.clone(), r#where.as_ref(), analyzed, context)?;
                analyzed.metadata.is_mutation = true;
            }
            DmlStatement::Delete { table, r#where } => {
                self.analyze_delete(table, r#where.as_ref(), analyzed, context)?;
                analyzed.metadata.is_mutation = true;
            }
        }
        Ok(())
    }

    /// Analyze SELECT statement
    fn analyze_select(
        &mut self,
        select: &crate::parsing::ast::SelectStatement,
        analyzed: &mut AnalyzedStatement,
        context: &mut AnalysisContext,
    ) -> Result<()> {
        // Resolve tables from FROM clause
        self.table_resolver.resolve_statement(
            &Statement::Dml(DmlStatement::Select(Box::new(select.clone()))),
            context,
        )?;

        // Resolve column references
        self.column_resolver.resolve_statement(
            &Statement::Dml(DmlStatement::Select(Box::new(select.clone()))),
            context,
        )?;

        // Type check and annotate expressions
        let mut output_schema = Vec::new();

        // Process projections
        for (i, (expr, alias)) in select.select.iter().enumerate() {
            let expr_id = ExpressionId::from_path(vec![0, i]); // SELECT, projection i
            let type_info = self.analyze_expression(expr, expr_id.clone(), analyzed, context)?;

            let column_name = alias
                .as_ref()
                .map(|a| a.clone())
                .unwrap_or_else(|| format!("column_{}", i));

            output_schema.push((column_name, type_info.data_type.clone()));
        }

        analyzed.metadata.set_output_schema(output_schema);

        // Process WHERE clause
        if let Some(where_expr) = &select.r#where {
            let expr_id = ExpressionId::from_path(vec![1]); // WHERE
            self.analyze_expression(where_expr, expr_id, analyzed, context)?;
        }

        // Process GROUP BY
        for (i, group_expr) in select.group_by.iter().enumerate() {
            let expr_id = ExpressionId::from_path(vec![2, i]); // GROUP BY, expression i
            self.analyze_expression(group_expr, expr_id, analyzed, context)?;
        }

        // Process HAVING
        if let Some(having_expr) = &select.having {
            let expr_id = ExpressionId::from_path(vec![3]); // HAVING
            self.analyze_expression(having_expr, expr_id, analyzed, context)?;
        }

        // Process ORDER BY
        for (i, (order_expr, _)) in select.order_by.iter().enumerate() {
            let expr_id = ExpressionId::from_path(vec![4, i]); // ORDER BY, expression i
            self.analyze_expression(order_expr, expr_id, analyzed, context)?;
        }

        Ok(())
    }

    /// Analyze an expression and add type annotations
    fn analyze_expression(
        &mut self,
        expr: &Expression,
        expr_id: ExpressionId,
        analyzed: &mut AnalyzedStatement,
        context: &mut AnalysisContext,
    ) -> Result<TypeInfo> {
        // Check if this is a parameter
        if let Expression::Parameter(index) = expr {
            self.handle_parameter(*index, expr_id.clone(), analyzed, context)?;
        }

        // Recursively analyze sub-expressions
        match expr {
            Expression::Operator(op) => {
                self.analyze_operator(op, &expr_id, analyzed, context)?;
            }
            Expression::Function(name, args) => {
                // Validate function exists and check arg count
                if let Some(func) = crate::functions::get_function(name) {
                    let sig = func.signature();

                    // Check min args
                    if args.len() < sig.min_args {
                        return Err(Error::ExecutionError(format!(
                            "Function {} requires at least {} arguments, got {}",
                            name.to_uppercase(),
                            sig.min_args,
                            args.len()
                        )));
                    }

                    // Check max args
                    if let Some(max) = sig.max_args
                        && args.len() > max
                    {
                        return Err(Error::ExecutionError(format!(
                            "Function {} accepts at most {} arguments, got {}",
                            name.to_uppercase(),
                            max,
                            args.len()
                        )));
                    }
                } else {
                    return Err(Error::ExecutionError(format!(
                        "Unknown function: {}",
                        name.to_uppercase()
                    )));
                }

                // Store function name in context for parameters
                context.set_current_function(Some(name.clone()));

                for (i, arg) in args.iter().enumerate() {
                    let arg_id = expr_id.child(i);
                    self.analyze_expression(arg, arg_id, analyzed, context)?;
                }

                // Clear function context
                context.set_current_function(None);
            }
            _ => {}
        }

        // Type check the expression using the new API
        let type_info = self.type_checker.check_expression(
            expr,
            &expr_id,
            context,
            &mut analyzed.type_annotations,
        )?;

        // Store the annotation
        analyzed.annotate(expr_id.clone(), type_info.clone());

        // Validate the expression AFTER parameter handling
        // Skip validation for parameters as they're already handled
        if !matches!(expr, Expression::Parameter(_)) {
            self.expression_validator
                .validate(expr, &expr_id, analyzed, context)?;
        }

        Ok(type_info)
    }

    /// Analyze an operator and its operands
    fn analyze_operator(
        &mut self,
        op: &crate::parsing::ast::Operator,
        expr_id: &ExpressionId,
        analyzed: &mut AnalyzedStatement,
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
                self.analyze_expression(left, expr_id.child(0), analyzed, context)?;
                self.analyze_expression(right, expr_id.child(1), analyzed, context)?;
            }
            Operator::Not(expr)
            | Operator::Negate(expr)
            | Operator::Identity(expr)
            | Operator::Factorial(expr) => {
                self.analyze_expression(expr, expr_id.child(0), analyzed, context)?;
            }
            Operator::Is(expr, _) => {
                self.analyze_expression(expr, expr_id.child(0), analyzed, context)?;
            }
            Operator::InList { expr, list, .. } => {
                self.analyze_expression(expr, expr_id.child(0), analyzed, context)?;
                for (i, item) in list.iter().enumerate() {
                    self.analyze_expression(item, expr_id.child(i + 1), analyzed, context)?;
                }
            }
            Operator::Between {
                expr, low, high, ..
            } => {
                self.analyze_expression(expr, expr_id.child(0), analyzed, context)?;
                self.analyze_expression(low, expr_id.child(1), analyzed, context)?;
                self.analyze_expression(high, expr_id.child(2), analyzed, context)?;
            }
        }
        Ok(())
    }

    /// Handle a parameter placeholder
    fn handle_parameter(
        &mut self,
        index: usize,
        expr_id: ExpressionId,
        analyzed: &mut AnalyzedStatement,
        context: &mut AnalysisContext,
    ) -> Result<()> {
        // Determine context and acceptable types based on where the parameter appears
        let (sql_context, acceptable_types) =
            self.determine_parameter_context(&expr_id, context)?;

        let slot = ParameterSlot {
            index,
            expression_id: expr_id,
            acceptable_types,
            coercion_context: CoercionContext {
                sql_context: sql_context.clone(),
                nullable: true, // Will be refined based on context
            },
            description: format!("Parameter {} in {:?}", index, sql_context),
        };

        analyzed.add_parameter(slot);
        Ok(())
    }

    /// Determine the context and acceptable types for a parameter
    fn determine_parameter_context(
        &self,
        expr_id: &ExpressionId,
        context: &AnalysisContext,
    ) -> Result<(SqlContext, Vec<DataType>)> {
        // Determine context based on expression path
        let path = expr_id.path();

        // Check if we're in a function context
        if let Some(func_name) = context.current_function() {
            // Determine argument index from path
            let arg_index = path.last().copied().unwrap_or(0);

            let sql_context = SqlContext::FunctionArgument {
                function_name: func_name.clone(),
                arg_index,
            };

            // Note: We don't get acceptable types from function signature anymore
            // Functions use their validate() method for type checking
            let acceptable_types = vec![];

            return Ok((sql_context, acceptable_types));
        }

        if path.is_empty() {
            return Ok((SqlContext::WhereClause, vec![]));
        }

        // First element indicates the clause
        let sql_context = match path[0] {
            0 => SqlContext::SelectProjection,
            1 => SqlContext::WhereClause,
            2 => SqlContext::GroupBy,
            3 => SqlContext::HavingClause,
            4 => SqlContext::OrderBy,
            5 => SqlContext::InsertValue {
                column_index: path.get(2).copied().unwrap_or(0),
            },
            6 => SqlContext::JoinCondition,
            _ => SqlContext::WhereClause,
        };

        // TODO: Determine acceptable types based on context and surrounding operators
        let acceptable_types = vec![];

        Ok((sql_context, acceptable_types))
    }

    /// Analyze INSERT statement
    fn analyze_insert(
        &mut self,
        table: &str,
        columns: Option<&Vec<String>>,
        source: &crate::parsing::ast::InsertSource,
        analyzed: &mut AnalyzedStatement,
        context: &mut AnalysisContext,
    ) -> Result<()> {
        // Get table schema
        let schema = self
            .schemas
            .get(table)
            .ok_or_else(|| Error::TableNotFound(table.to_string()))?;

        // Determine target columns
        let target_columns = if let Some(cols) = columns {
            cols.clone()
        } else {
            schema.columns.iter().map(|c| c.name.clone()).collect()
        };

        // Add table access info
        analyzed.metadata.add_table_access(TableAccess {
            table: table.to_string(),
            alias: None,
            access_type: AccessType::Insert,
            columns_used: target_columns.clone(),
            index_opportunities: vec![],
        });

        // Analyze the insert source
        match source {
            crate::parsing::ast::InsertSource::Values(rows) => {
                for (row_idx, row) in rows.iter().enumerate() {
                    for (col_idx, expr) in row.iter().enumerate() {
                        let expr_id = ExpressionId::from_path(vec![5, row_idx, col_idx]);
                        self.analyze_expression(expr, expr_id, analyzed, context)?;
                    }
                }
            }
            crate::parsing::ast::InsertSource::Select(select) => {
                self.analyze_select(select, analyzed, context)?;
            }
            crate::parsing::ast::InsertSource::DefaultValues => {
                // No expressions to analyze
            }
        }

        Ok(())
    }

    /// Analyze UPDATE statement
    fn analyze_update(
        &mut self,
        table: &str,
        set: std::collections::BTreeMap<String, Option<Expression>>,
        where_clause: Option<&Expression>,
        analyzed: &mut AnalyzedStatement,
        context: &mut AnalysisContext,
    ) -> Result<()> {
        // Validate table exists and add to context
        context.add_table(table.to_string(), None)?;

        // Get table schema
        let _schema = self
            .schemas
            .get(table)
            .ok_or_else(|| Error::TableNotFound(table.to_string()))?;

        // Add table access info
        let columns_used: Vec<String> = set.iter().map(|(col, _)| col.clone()).collect();

        analyzed.metadata.add_table_access(TableAccess {
            table: table.to_string(),
            alias: None,
            access_type: AccessType::Update,
            columns_used,
            index_opportunities: vec![],
        });

        // Analyze SET expressions
        for (i, (_column, expr_opt)) in set.iter().enumerate() {
            if let Some(expr) = expr_opt {
                let expr_id = ExpressionId::from_path(vec![6, i]); // UPDATE SET, assignment i
                self.analyze_expression(expr, expr_id, analyzed, context)?;
            }
        }

        // Analyze WHERE clause
        if let Some(where_expr) = where_clause {
            let expr_id = ExpressionId::from_path(vec![7]); // UPDATE WHERE
            self.analyze_expression(where_expr, expr_id, analyzed, context)?;
        }

        Ok(())
    }

    /// Analyze DELETE statement
    fn analyze_delete(
        &mut self,
        table: &str,
        where_clause: Option<&Expression>,
        analyzed: &mut AnalyzedStatement,
        context: &mut AnalysisContext,
    ) -> Result<()> {
        // Validate table exists and add to context
        context.add_table(table.to_string(), None)?;

        // Add table access info
        analyzed.metadata.add_table_access(TableAccess {
            table: table.to_string(),
            alias: None,
            access_type: AccessType::Delete,
            columns_used: vec![],
            index_opportunities: vec![],
        });

        // Analyze WHERE clause
        if let Some(where_expr) = where_clause {
            let expr_id = ExpressionId::from_path(vec![8]); // DELETE WHERE
            self.analyze_expression(where_expr, expr_id, analyzed, context)?;
        }

        Ok(())
    }

    /// Analyze DDL statements
    fn analyze_ddl(
        &mut self,
        statement: &DdlStatement,
        analyzed: &mut AnalyzedStatement,
        _context: &mut AnalysisContext,
    ) -> Result<()> {
        match statement {
            DdlStatement::CreateTable { columns, .. } => {
                analyzed.metadata.statement_type = StatementType::CreateTable;
                // Validate column types
                for _column in columns {
                    // Type validation would go here
                }
            }
            DdlStatement::DropTable { .. } => {
                analyzed.metadata.statement_type = StatementType::DropTable;
            }
            DdlStatement::CreateIndex { .. } => {
                analyzed.metadata.statement_type = StatementType::CreateIndex;
            }
            DdlStatement::DropIndex { .. } => {
                analyzed.metadata.statement_type = StatementType::DropIndex;
            }
            _ => {}
        }
        Ok(())
    }

    /// Extract metadata from context
    fn extract_metadata(&self, analyzed: &mut AnalyzedStatement, context: &AnalysisContext) {
        // Get metadata from context
        let ctx_metadata = context.metadata();

        // Copy referenced tables and columns
        for table in ctx_metadata.referenced_tables.iter() {
            for col in ctx_metadata.referenced_columns.iter() {
                if col.0 == *table {
                    analyzed
                        .metadata
                        .add_column_reference(table.clone(), col.1.clone());
                }
            }
        }

        analyzed.metadata.is_deterministic = ctx_metadata.is_deterministic;
    }

    /// Get statement type
    fn get_statement_type(&self, statement: &Statement) -> StatementType {
        match statement {
            Statement::Dml(dml) => match dml {
                DmlStatement::Select(_) => StatementType::Select,
                DmlStatement::Insert { .. } => StatementType::Insert,
                DmlStatement::Update { .. } => StatementType::Update,
                DmlStatement::Delete { .. } => StatementType::Delete,
            },
            Statement::Ddl(ddl) => match ddl {
                DdlStatement::CreateTable { .. } => StatementType::CreateTable,
                DdlStatement::DropTable { .. } => StatementType::DropTable,
                DdlStatement::CreateIndex { .. } => StatementType::CreateIndex,
                DdlStatement::DropIndex { .. } => StatementType::DropIndex,
            },
            _ => StatementType::Other,
        }
    }

    /// Check if expression is an aggregate
    fn is_aggregate_expression(&self, _expr: &Expression) -> bool {
        // Implementation would check for aggregate functions
        false
    }

    /// Check if expression is deterministic
    fn is_deterministic_expression(&self, _expr: &Expression) -> bool {
        // Implementation would check for non-deterministic functions
        true
    }
}
