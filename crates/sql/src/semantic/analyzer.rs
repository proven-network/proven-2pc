//! Semantic analyzer that produces lightweight analyzed statements
//!
//! This analyzer creates AnalyzedStatements with Arc-wrapped ASTs
//! and separate type annotations, enabling efficient caching and
//! zero-copy parameter binding.

use super::context::AnalysisContext;
use super::statement::{
    AnalyzedStatement, CoercionContext, ExpressionId, ParameterSlot, SqlContext, StatementType,
    TypeInfo,
};
use super::validators::{
    ConstraintValidator, ExpressionValidator, FunctionValidator, StatementValidator, TypeValidator,
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
    type_checker: TypeValidator,
    expression_validator: ExpressionValidator,
    statement_validator: StatementValidator,
    constraint_validator: ConstraintValidator,
    function_validator: FunctionValidator,
}

impl SemanticAnalyzer {
    /// Create a new semantic analyzer
    pub fn new(schemas: HashMap<String, Table>) -> Self {
        let type_checker = TypeValidator::new();
        let expression_validator = ExpressionValidator::new();
        let statement_validator = StatementValidator::new();
        let constraint_validator = ConstraintValidator::new();
        let function_validator = FunctionValidator::new();

        Self {
            schemas,
            type_checker,
            expression_validator,
            statement_validator,
            constraint_validator,
            function_validator,
        }
    }

    /// Update schemas (for cache invalidation)
    pub fn update_schemas(&mut self, schemas: HashMap<String, Table>) {
        self.schemas = schemas;
    }

    /// Analyze a statement with known parameter types for single-pass validation
    pub fn analyze(
        &mut self,
        statement: Statement,
        param_types: Vec<DataType>,
    ) -> Result<AnalyzedStatement> {
        // Wrap the statement in Arc for sharing
        let ast = Arc::new(statement);

        // Create the analyzed statement
        let mut analyzed = AnalyzedStatement::new(ast.clone());

        // Create analysis context with parameter types
        let mut context = AnalysisContext::new(self.schemas.clone());
        context.set_parameter_types(param_types);

        // Perform analysis phases
        self.analyze_statement(&ast, &mut analyzed, &mut context)?;

        // Run validators
        self.statement_validator.validate(&analyzed, &context)?;
        self.constraint_validator
            .validate(&mut analyzed, &context)?;
        self.function_validator.validate(&mut analyzed, &context)?;

        // Validate functions with concrete parameter types if we have them
        if context.has_parameter_types() {
            self.validate_functions_with_params(&analyzed, &context)?;
        }

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
        for from_clause in &select.from {
            Self::resolve_from_clause(from_clause, context)?;
        }

        // Type check and annotate expressions
        let mut output_schema = Vec::new();

        // Process projections and check for aggregates
        for (i, (expr, alias)) in select.select.iter().enumerate() {
            let expr_id = ExpressionId::from_path(vec![0, i]); // SELECT, projection i
            let type_info = self.analyze_expression(expr, expr_id.clone(), analyzed, context)?;

            // Check if this expression contains aggregates
            if type_info.is_aggregate {
                analyzed.metadata.has_aggregates = true;
            }

            let column_name = alias.clone().unwrap_or_else(|| format!("column_{}", i));

            output_schema.push((column_name, type_info.data_type.clone()));
        }

        analyzed.metadata.set_output_schema(output_schema);

        // Extract predicates for conflict detection
        if let Some(table) = context.get_primary_table() {
            if let Some(where_expr) = &select.r#where {
                // Process WHERE clause
                let expr_id = ExpressionId::from_path(vec![1]); // WHERE
                self.analyze_expression(where_expr, expr_id, analyzed, context)?;

                // Extract predicate templates from WHERE clause
                self.extract_predicate_templates(where_expr, &table, analyzed, context);
            } else {
                // No WHERE clause - reading entire table
                analyzed.add_predicate_template(super::statement::PredicateTemplate::FullTable {
                    table: table.clone(),
                });
            }
        }

        // Process GROUP BY
        for (i, group_expr) in select.group_by.iter().enumerate() {
            let expr_id = ExpressionId::from_path(vec![2, i]); // GROUP BY, expression i
            self.analyze_expression(group_expr, expr_id, analyzed, context)?;
        }

        // Process HAVING
        if let Some(having_expr) = &select.having {
            let expr_id = ExpressionId::from_path(vec![3]); // HAVING
            let type_info = self.analyze_expression(having_expr, expr_id, analyzed, context)?;

            // HAVING often contains aggregates
            if type_info.is_aggregate {
                analyzed.metadata.has_aggregates = true;
            }
        }

        // Process ORDER BY
        for (i, (order_expr, _)) in select.order_by.iter().enumerate() {
            let expr_id = ExpressionId::from_path(vec![4, i]); // ORDER BY, expression i
            let type_info = self.analyze_expression(order_expr, expr_id, analyzed, context)?;

            // ORDER BY can contain aggregates in SELECT with GROUP BY
            if type_info.is_aggregate {
                analyzed.metadata.has_aggregates = true;
            }
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
        // If we have parameter types from context, use them
        if let Some(param_type) = context.get_parameter_type(index) {
            // We have the concrete type, no need for acceptable_types guessing
            let sql_context = self.determine_parameter_context(&expr_id, context)?;

            let slot = ParameterSlot {
                index,
                expression_id: expr_id.clone(),
                actual_type: Some(param_type.clone()),
                coercion_context: CoercionContext {
                    sql_context: sql_context.clone(),
                    nullable: true, // Will be refined based on context
                },
                description: format!("Parameter {} in {:?}", index, sql_context),
            };

            analyzed.add_parameter(slot);
        } else {
            // Fallback: determine context from position
            let sql_context = self.determine_parameter_context(&expr_id, context)?;

            let slot = ParameterSlot {
                index,
                expression_id: expr_id,
                actual_type: None,
                coercion_context: CoercionContext {
                    sql_context: sql_context.clone(),
                    nullable: true, // Will be refined based on context
                },
                description: format!("Parameter {} in {:?}", index, sql_context),
            };

            analyzed.add_parameter(slot);
        }
        Ok(())
    }

    /// Determine the context and acceptable types for a parameter
    fn determine_parameter_context(
        &self,
        expr_id: &ExpressionId,
        context: &AnalysisContext,
    ) -> Result<SqlContext> {
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

            // Functions use their validate() method for type checking
            return Ok(sql_context);
        }

        if path.is_empty() {
            return Ok(SqlContext::WhereClause);
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

        Ok(sql_context)
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
        // Validate table exists
        let schema = self
            .schemas
            .get(table)
            .ok_or_else(|| Error::TableNotFound(table.to_string()))?;

        // Determine target columns (unused but kept for future validation)
        let _target_columns = if let Some(cols) = columns {
            cols.clone()
        } else {
            schema.columns.iter().map(|c| c.name.clone()).collect()
        };

        // Analyze the insert source
        match source {
            crate::parsing::ast::InsertSource::Values(rows) => {
                // For VALUES, add target table to context for reference
                context.add_table(table.to_string(), None)?;

                for (row_idx, row) in rows.iter().enumerate() {
                    for (col_idx, expr) in row.iter().enumerate() {
                        let expr_id = ExpressionId::from_path(vec![5, row_idx, col_idx]);
                        self.analyze_expression(expr, expr_id, analyzed, context)?;
                    }
                }

                // Extract predicate template for INSERT (primary key if available)
                self.extract_insert_predicate_template(table, columns, rows, analyzed, context);
            }
            crate::parsing::ast::InsertSource::Select(select) => {
                // For INSERT ... SELECT, analyze SELECT first without target table in context
                // SELECT has its own FROM clause
                self.analyze_select(select, analyzed, context)?;

                // Note: target table is tracked via the metadata from analyzing the SELECT
            }
            crate::parsing::ast::InsertSource::DefaultValues => {
                // Add target table to context
                context.add_table(table.to_string(), None)?;
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

        // Table access tracking removed - not used downstream

        // Analyze SET expressions
        for (i, (_column, expr_opt)) in set.iter().enumerate() {
            if let Some(expr) = expr_opt {
                let expr_id = ExpressionId::from_path(vec![6, i]); // UPDATE SET, assignment i
                self.analyze_expression(expr, expr_id, analyzed, context)?;
            }
        }

        // Analyze WHERE clause and extract predicates
        if let Some(where_expr) = where_clause {
            let expr_id = ExpressionId::from_path(vec![7]); // UPDATE WHERE
            self.analyze_expression(where_expr, expr_id, analyzed, context)?;

            // Extract predicate templates from WHERE clause
            self.extract_predicate_templates(where_expr, table, analyzed, context);
        } else {
            // No WHERE clause - full table update
            analyzed.add_predicate_template(super::statement::PredicateTemplate::FullTable {
                table: table.to_string(),
            });
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

        // Table access tracking removed - not used downstream

        // Analyze WHERE clause and extract predicates
        if let Some(where_expr) = where_clause {
            let expr_id = ExpressionId::from_path(vec![8]); // DELETE WHERE
            self.analyze_expression(where_expr, expr_id, analyzed, context)?;

            // Extract predicate templates from WHERE clause
            self.extract_predicate_templates(where_expr, table, analyzed, context);
        } else {
            // No WHERE clause - full table delete
            analyzed.add_predicate_template(super::statement::PredicateTemplate::FullTable {
                table: table.to_string(),
            });
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
    }

    /// Resolve a FROM clause (from TableResolver logic)
    fn resolve_from_clause(
        from: &crate::parsing::ast::FromClause,
        context: &mut AnalysisContext,
    ) -> Result<()> {
        use crate::parsing::ast::FromClause;

        match from {
            FromClause::Table { name, alias } => {
                context.add_table(name.clone(), alias.clone())?;
            }
            FromClause::Join { left, right, .. } => {
                // Recursively resolve both sides
                Self::resolve_from_clause(left, context)?;
                Self::resolve_from_clause(right, context)?;
            }
        }
        Ok(())
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

    /// Validate functions that contain parameters now that we have concrete types
    fn validate_functions_with_params(
        &self,
        statement: &AnalyzedStatement,
        context: &AnalysisContext,
    ) -> Result<()> {
        use super::statement::SqlContext;
        use std::collections::{BTreeMap, HashMap};

        // Track all function calls that have parameters
        let mut function_calls: HashMap<(String, ExpressionId), BTreeMap<usize, DataType>> =
            HashMap::new();

        // First pass: collect parameter types for functions
        for slot in &statement.parameter_slots {
            if let SqlContext::FunctionArgument {
                ref function_name,
                arg_index,
            } = slot.coercion_context.sql_context
            {
                // Get the actual parameter type
                let param_type = if let Some(ref actual) = slot.actual_type {
                    actual.clone()
                } else if let Some(param_type) = context.get_parameter_type(slot.index) {
                    param_type.clone()
                } else {
                    continue; // Skip if we don't have type info
                };

                // Find the function's expression ID (parent of this parameter)
                let func_expr_id =
                    if let Some((_, parent_path)) = slot.expression_id.path().split_last() {
                        ExpressionId::from_path(parent_path.to_vec())
                    } else {
                        continue;
                    };

                // Store the parameter type at its argument position
                function_calls
                    .entry((function_name.clone(), func_expr_id.clone()))
                    .or_default()
                    .insert(arg_index, param_type);
            }
        }

        // Second pass: for each function with parameters, get non-parameter arg types
        for ((function_name, func_expr_id), param_arg_types) in &mut function_calls {
            // Get the function
            let func = crate::functions::get_function(function_name).ok_or_else(|| {
                Error::ExecutionError(format!("Unknown function: {}", function_name))
            })?;

            let sig = func.signature();

            // For each possible argument, check if we already have it from parameters
            for arg_idx in 0..sig.min_args {
                if let std::collections::btree_map::Entry::Vacant(e) =
                    param_arg_types.entry(arg_idx)
                {
                    // This argument is not a parameter, get its type from annotations
                    let arg_expr_id = func_expr_id.child(arg_idx);

                    if let Some(type_info) = statement.get_type(&arg_expr_id) {
                        e.insert(type_info.data_type.clone());
                    }
                }
            }
        }

        // Final pass: validate each function with complete arg list
        for ((function_name, _), arg_types_map) in function_calls {
            let func = crate::functions::get_function(&function_name).ok_or_else(|| {
                Error::ExecutionError(format!("Unknown function: {}", function_name))
            })?;

            // Build complete arg list in order
            let mut arg_types = Vec::new();
            let max_idx = arg_types_map.keys().max().copied().unwrap_or(0);

            for i in 0..=max_idx {
                if let Some(dt) = arg_types_map.get(&i) {
                    arg_types.push(dt.clone());
                } else {
                    // Missing type info - this shouldn't happen with concrete parameter types
                    return Err(Error::ExecutionError(format!(
                        "Missing type information for argument {} of function {}",
                        i, function_name
                    )));
                }
            }

            // Validate with the function's validate method
            func.validate(&arg_types)?;
        }

        Ok(())
    }

    /// Extract predicate templates from a WHERE clause expression
    fn extract_predicate_templates(
        &self,
        expr: &Expression,
        table: &str,
        analyzed: &mut AnalyzedStatement,
        context: &AnalysisContext,
    ) {
        use super::statement::PredicateTemplate;
        use crate::parsing::ast::Operator;

        match expr {
            Expression::Operator(op) => match op {
                // Equality: column = value
                Operator::Equal(left, right) => {
                    if let Expression::Column(_, col_name) = &**left {
                        // Try to find column index in schema
                        let col_index = context.schemas().get(table).and_then(|schema| {
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
                            // Column not found, use complex predicate
                            analyzed.add_predicate_template(PredicateTemplate::Complex {
                                table: table.to_string(),
                                expression_id: ExpressionId::from_path(vec![]),
                            });
                        }
                    } else if let Expression::Column(_, col_name) = &**right {
                        // Handle value = column (reversed)
                        let col_index = context.schemas().get(table).and_then(|schema| {
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
                    self.extract_predicate_templates(left, table, analyzed, context);
                    self.extract_predicate_templates(right, table, analyzed, context);
                }

                // OR: complex predicate for now
                Operator::Or(_, _) => {
                    analyzed.add_predicate_template(PredicateTemplate::Complex {
                        table: table.to_string(),
                        expression_id: ExpressionId::from_path(vec![]),
                    });
                }

                // Range predicates: column > value, column >= value, etc.
                Operator::GreaterThan(left, right) | Operator::GreaterThanOrEqual(left, right) => {
                    if let Expression::Column(_, col_name) = &**left {
                        let col_index = context.schemas().get(table).and_then(|schema| {
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
                        } else {
                            analyzed.add_predicate_template(PredicateTemplate::Complex {
                                table: table.to_string(),
                                expression_id: ExpressionId::from_path(vec![]),
                            });
                        }
                    } else {
                        analyzed.add_predicate_template(PredicateTemplate::Complex {
                            table: table.to_string(),
                            expression_id: ExpressionId::from_path(vec![]),
                        });
                    }
                }

                Operator::LessThan(left, right) | Operator::LessThanOrEqual(left, right) => {
                    if let Expression::Column(_, col_name) = &**left {
                        let col_index = context.schemas().get(table).and_then(|schema| {
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
                        } else {
                            analyzed.add_predicate_template(PredicateTemplate::Complex {
                                table: table.to_string(),
                                expression_id: ExpressionId::from_path(vec![]),
                            });
                        }
                    } else {
                        analyzed.add_predicate_template(PredicateTemplate::Complex {
                            table: table.to_string(),
                            expression_id: ExpressionId::from_path(vec![]),
                        });
                    }
                }

                // BETWEEN
                Operator::Between {
                    expr, low, high, ..
                } => {
                    if let Expression::Column(_, col_name) = &**expr {
                        let col_index = context.schemas().get(table).and_then(|schema| {
                            schema.columns.iter().position(|c| &c.name == col_name)
                        });

                        if let Some(col_idx) = col_index {
                            let lower_value = self.expression_to_predicate_value(low);
                            let upper_value = self.expression_to_predicate_value(high);
                            analyzed.add_predicate_template(PredicateTemplate::Range {
                                table: table.to_string(),
                                column_name: col_name.clone(),
                                column_index: col_idx,
                                lower: Some((lower_value, true)),
                                upper: Some((upper_value, true)),
                            });
                        } else {
                            analyzed.add_predicate_template(PredicateTemplate::Complex {
                                table: table.to_string(),
                                expression_id: ExpressionId::from_path(vec![]),
                            });
                        }
                    } else {
                        analyzed.add_predicate_template(PredicateTemplate::Complex {
                            table: table.to_string(),
                            expression_id: ExpressionId::from_path(vec![]),
                        });
                    }
                }

                // IN list
                Operator::InList { expr, list, .. } => {
                    if let Expression::Column(_, col_name) = &**expr {
                        let col_index = context.schemas().get(table).and_then(|schema| {
                            schema.columns.iter().position(|c| &c.name == col_name)
                        });

                        if let Some(col_idx) = col_index {
                            let values: Vec<super::statement::PredicateValue> = list
                                .iter()
                                .map(|e| self.expression_to_predicate_value(e))
                                .collect();
                            analyzed.add_predicate_template(PredicateTemplate::InList {
                                table: table.to_string(),
                                column_name: col_name.clone(),
                                column_index: col_idx,
                                values,
                            });
                        } else {
                            analyzed.add_predicate_template(PredicateTemplate::Complex {
                                table: table.to_string(),
                                expression_id: ExpressionId::from_path(vec![]),
                            });
                        }
                    } else {
                        analyzed.add_predicate_template(PredicateTemplate::Complex {
                            table: table.to_string(),
                            expression_id: ExpressionId::from_path(vec![]),
                        });
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

    /// Extract predicate template for INSERT statement
    fn extract_insert_predicate_template(
        &self,
        table: &str,
        columns: Option<&Vec<String>>,
        rows: &[Vec<Expression>],
        analyzed: &mut AnalyzedStatement,
        context: &AnalysisContext,
    ) {
        use super::statement::PredicateTemplate;

        // Try to extract primary key values if available
        if let Some(schema) = context.schemas().get(table) {
            if let Some(pk_idx) = schema.primary_key {
                let pk_column = &schema.columns[pk_idx];

                // Determine the index of the primary key in the INSERT
                let pk_insert_idx = if let Some(cols) = columns {
                    // Explicit columns - find the primary key position
                    cols.iter().position(|c| c == &pk_column.name)
                } else {
                    // Implicit columns - use schema order
                    Some(pk_idx)
                };

                if let Some(insert_idx) = pk_insert_idx {
                    // Extract primary key values from all rows
                    for row in rows {
                        if let Some(pk_expr) = row.get(insert_idx) {
                            let pk_value = self.expression_to_predicate_value(pk_expr);
                            analyzed.add_predicate_template(PredicateTemplate::PrimaryKey {
                                table: table.to_string(),
                                value: pk_value,
                            });
                        }
                    }
                    return;
                }
            }
        }

        // No primary key or couldn't extract - use full table
        analyzed.add_predicate_template(PredicateTemplate::FullTable {
            table: table.to_string(),
        });
    }

    /// Convert an expression to a PredicateValue
    fn expression_to_predicate_value(&self, expr: &Expression) -> super::statement::PredicateValue {
        use super::statement::PredicateValue;
        use crate::parsing::ast::Literal;
        use crate::types::value::Value;

        match expr {
            Expression::Literal(lit) => {
                let value = match lit {
                    Literal::String(s) => Value::Str(s.clone()),
                    Literal::Integer(i) => {
                        // Try to fit in i64, otherwise use i128
                        if let Ok(i64_val) = i64::try_from(*i) {
                            Value::I64(i64_val)
                        } else {
                            Value::I128(*i)
                        }
                    }
                    Literal::Float(f) => Value::F64(*f),
                    Literal::Boolean(b) => Value::Bool(*b),
                    Literal::Null => Value::Null,
                    _ => Value::Null, // For other literals we don't handle yet
                };
                PredicateValue::Constant(value)
            }
            Expression::Parameter(idx) => PredicateValue::Parameter(*idx),
            _ => {
                // For complex expressions, we'd need an expression ID
                // For now, treat as expression
                PredicateValue::Expression(ExpressionId::from_path(vec![]))
            }
        }
    }
}
