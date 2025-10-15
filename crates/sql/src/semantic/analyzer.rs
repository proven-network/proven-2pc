//! Phase-based semantic analyzer with explicit data flow
//!
//! Each phase has clear inputs and outputs, avoiding shared mutable state.

use crate::error::Result;
use crate::parsing::ast::{DmlStatement, Expression, Statement};
use crate::types::data_type::DataType;
use crate::types::schema::Table;
use std::collections::HashMap;
use std::sync::Arc;

use super::statement::{
    AnalyzedStatement, ColumnResolutionMap, ExpressionId, ParameterSlot, PredicateTemplate,
    StatementType, TypeInfo,
};

/// Output from name resolution phase
#[derive(Debug)]
pub struct ResolutionOutput {
    /// Resolved table sources
    pub table_sources: Vec<super::resolution::TableSource>,

    /// Column resolution map for O(1) lookups
    pub column_map: ColumnResolutionMap,

    /// Analyzed statements for subqueries in FROM clauses
    pub subquery_analyses:
        std::collections::HashMap<String, Arc<super::statement::AnalyzedStatement>>,
}

/// Output from type inference phase
#[derive(Debug)]
pub struct TypeOutput {
    /// Whether the statement has aggregates
    pub has_aggregates: bool,
    /// Expression type annotations
    pub expression_types: HashMap<super::statement::ExpressionId, TypeInfo>,

    /// Parameter slots with type and context information
    pub parameter_slots: Vec<ParameterSlot>,

    /// Statement type metadata
    pub statement_type: StatementType,
}

/// Output from optimization phase
#[derive(Debug, Clone)]
pub struct OptimizationOutput {
    /// Predicate templates for conflict detection
    pub predicate_templates: Vec<PredicateTemplate>,

    /// Join ordering hints
    pub join_hints: Vec<JoinHint>,
}

#[derive(Debug, Clone)]
pub struct JoinHint {
    pub left_table: String,
    pub right_table: String,
    pub selectivity_estimate: f64,
}

/// Context from outer query for correlated subqueries
#[derive(Debug, Clone)]
pub struct OuterQueryContext {
    /// Column resolution map from the outer query
    pub column_map: super::statement::ColumnResolutionMap,
}

/// Phase-based analyzer with explicit data flow
pub struct SemanticAnalyzer {
    schemas: HashMap<String, Table>,
    /// Optional outer query context for correlated subqueries
    outer_context: Option<Box<OuterQueryContext>>,
}

impl SemanticAnalyzer {
    pub fn new(schemas: HashMap<String, Table>) -> Self {
        Self {
            schemas,
            outer_context: None,
        }
    }

    /// Create a new analyzer with outer query context for correlated subqueries
    pub fn with_outer_context(
        schemas: HashMap<String, Table>,
        outer_context: OuterQueryContext,
    ) -> Self {
        Self {
            schemas,
            outer_context: Some(Box::new(outer_context)),
        }
    }

    /// Update the schemas used by the analyzer
    pub fn update_schemas(&mut self, schemas: HashMap<String, Table>) {
        self.schemas = schemas;
    }

    /// Main analysis pipeline with explicit phase outputs
    pub fn analyze(
        &self,
        statement: Statement,
        param_types: Vec<DataType>,
    ) -> Result<AnalyzedStatement> {
        let ast = Arc::new(statement);

        // Phase 1: Name Resolution
        let resolution = self.resolve_names(&ast)?;

        // Phase 2: Type Inference (uses resolution output)
        let types = self.infer_types(&ast, &resolution, &param_types)?;

        // Phase 3: Semantic Validation (uses resolution and type outputs)
        self.validate(&ast, &resolution, &types)?;

        // Phase 4: Optimization Metadata (uses all previous outputs)
        let optimization = self.build_optimization_metadata(&ast, &resolution, &types)?;

        // Build final analyzed statement
        Ok(self.build_analyzed_statement(ast, resolution, types, optimization))
    }

    /// Phase 1: Name Resolution
    fn resolve_names(&self, ast: &Arc<Statement>) -> Result<ResolutionOutput> {
        let resolver = if let Some(outer) = &self.outer_context {
            super::resolution::NameResolver::with_outer_context(
                self.schemas.clone(),
                outer.column_map.clone(),
            )
        } else {
            super::resolution::NameResolver::new(self.schemas.clone())
        };

        // Build table sources list from FROM clauses
        let table_sources = resolver.extract_table_sources(ast)?;

        // Build column resolution map and collect subquery analyses
        let (column_map, subquery_analyses) =
            resolver.build_column_map(&table_sources, &self.schemas)?;

        Ok(ResolutionOutput {
            table_sources,
            column_map,
            subquery_analyses,
        })
    }

    /// Phase 2: Type Inference
    fn infer_types(
        &self,
        ast: &Arc<Statement>,
        resolution: &ResolutionOutput,
        param_types: &[DataType],
    ) -> Result<TypeOutput> {
        let checker = super::typing::TypeChecker::new();

        // Create immutable view of resolution data
        let resolution_view = ResolutionView {
            column_map: &resolution.column_map,
        };

        // Infer types for all expressions
        let expression_types = checker.infer_all_types(ast, &resolution_view, param_types)?;

        // Build parameter slots with context information
        let parameter_slots = self.build_parameter_slots(ast, &expression_types, param_types)?;

        // Determine statement type
        let statement_type = self.get_statement_type(ast);

        // Check if statement has aggregates
        let has_aggregates = expression_types.values().any(|t| t.is_aggregate);

        Ok(TypeOutput {
            has_aggregates,
            expression_types,
            parameter_slots,
            statement_type,
        })
    }

    /// Phase 3: Semantic Validation
    fn validate(
        &self,
        ast: &Arc<Statement>,
        _resolution: &ResolutionOutput,
        types: &TypeOutput,
    ) -> Result<()> {
        let validator = super::validation::SemanticValidator::new();

        // Create immutable views of previous phase outputs
        let validation_input = ValidationInput {
            expression_types: &types.expression_types,
            schemas: &self.schemas,
        };

        // Run all validations
        let violations = validator.validate_all(ast, &validation_input)?;

        // Could collect warnings here if needed in the future
        // let warnings = validator.collect_warnings(ast, &validation_input);

        // If there are violations, return an error
        if !violations.is_empty() {
            return Err(crate::error::Error::InvalidOperation(format!(
                "Validation failed: {}",
                violations.join("; ")
            )));
        }

        Ok(())
    }

    /// Phase 4: Build Optimization Metadata
    fn build_optimization_metadata(
        &self,
        ast: &Arc<Statement>,
        resolution: &ResolutionOutput,
        _types: &TypeOutput,
    ) -> Result<OptimizationOutput> {
        let builder = super::optimization::MetadataBuilder::new(self.schemas.clone());

        // Create optimization input from previous phases
        let opt_input = OptimizationInput {
            tables: &resolution.table_sources,
            column_map: &resolution.column_map,
            schemas: &self.schemas,
        };

        // Build predicate templates
        let predicate_templates = builder.build_predicates(ast, &opt_input)?;

        // Analyze joins
        let join_hints = builder.analyze_joins(ast, &opt_input)?;

        Ok(OptimizationOutput {
            predicate_templates,
            join_hints,
        })
    }

    /// Build final analyzed statement from all phase outputs
    fn build_analyzed_statement(
        &self,
        ast: Arc<Statement>,
        resolution: ResolutionOutput,
        types: TypeOutput,
        optimization: OptimizationOutput,
    ) -> AnalyzedStatement {
        let mut analyzed = AnalyzedStatement::new(ast.clone());

        // Add resolution data
        analyzed.column_resolution_map = resolution.column_map;
        analyzed.subquery_analyses = resolution.subquery_analyses;

        // Add aggregate expressions first (while we still have types)
        for (expr_id, type_info) in &types.expression_types {
            if type_info.is_aggregate {
                analyzed.aggregate_expressions.insert(expr_id.clone());
            }
        }

        // Compute output nullability
        self.compute_output_nullability(&ast, &types, &mut analyzed)
            .unwrap_or(());

        // Add parameter slots (moving ownership)
        for slot in types.parameter_slots {
            analyzed.add_parameter(slot);
        }

        // Add statement metadata
        analyzed.metadata.statement_type = types.statement_type;
        analyzed.metadata.has_aggregates = types.has_aggregates;

        // Clone optimization data before moving
        let templates = optimization.predicate_templates.clone();
        let join_hints = optimization.join_hints.clone();

        analyzed.metadata.optimization_output = Some(Box::new(optimization));

        // Add predicate templates separately for conflict detection
        for template in templates {
            analyzed.add_predicate_template(template);
        }

        // Add join hints
        analyzed.join_hints = join_hints;

        // Sort parameters by index for consistent ordering
        analyzed.sort_parameters();

        analyzed
    }

    /// Get statement type from AST
    fn get_statement_type(&self, statement: &Statement) -> StatementType {
        match statement {
            Statement::Dml(dml) => match dml {
                crate::parsing::ast::DmlStatement::Select(_) => StatementType::Select,
                crate::parsing::ast::DmlStatement::Insert { .. } => StatementType::Insert,
                crate::parsing::ast::DmlStatement::Update { .. } => StatementType::Update,
                crate::parsing::ast::DmlStatement::Delete { .. } => StatementType::Delete,
                crate::parsing::ast::DmlStatement::Values(_) => StatementType::Select, // VALUES behaves like SELECT
            },
            Statement::Ddl(ddl) => match ddl {
                crate::parsing::ast::DdlStatement::CreateTable { .. } => StatementType::CreateTable,
                crate::parsing::ast::DdlStatement::CreateTableAsValues { .. } => {
                    StatementType::CreateTable
                }
                crate::parsing::ast::DdlStatement::CreateTableAsSelect { .. } => {
                    StatementType::CreateTable
                }
                crate::parsing::ast::DdlStatement::DropTable { .. } => StatementType::DropTable,
                crate::parsing::ast::DdlStatement::CreateIndex { .. } => StatementType::CreateIndex,
                crate::parsing::ast::DdlStatement::DropIndex { .. } => StatementType::DropIndex,
            },
            Statement::Explain(_) => StatementType::Explain,
        }
    }

    /// Build parameter slots from expressions and types
    fn build_parameter_slots(
        &self,
        ast: &Arc<Statement>,
        expression_types: &HashMap<super::statement::ExpressionId, TypeInfo>,
        param_types: &[DataType],
    ) -> Result<Vec<ParameterSlot>> {
        let mut slots = Vec::new();

        // Walk the AST to find parameters
        Self::collect_parameters(ast.as_ref(), expression_types, param_types, &mut slots)?;

        // Sort by parameter index for consistent ordering
        slots.sort_by_key(|s| s.index);

        Ok(slots)
    }

    /// Collect parameter slots from the statement
    fn collect_parameters(
        statement: &Statement,
        expression_types: &HashMap<super::statement::ExpressionId, TypeInfo>,
        param_types: &[DataType],
        slots: &mut Vec<ParameterSlot>,
    ) -> Result<()> {
        if let Statement::Dml(dml) = statement {
            match dml {
                DmlStatement::Select(select) => {
                    // Check SELECT list
                    for (idx, (expr, _)) in select.select.iter().enumerate() {
                        let expr_id = ExpressionId::from_path(vec![idx]);
                        Self::collect_params_from_expr(
                            expr,
                            &expr_id,
                            expression_types,
                            param_types,
                            slots,
                        );
                    }

                    // Check WHERE clause
                    if let Some(where_expr) = &select.r#where {
                        let expr_id = ExpressionId::from_path(vec![1000]);
                        Self::collect_params_from_expr(
                            where_expr,
                            &expr_id,
                            expression_types,
                            param_types,
                            slots,
                        );
                    }

                    // Check GROUP BY
                    for (idx, expr) in select.group_by.iter().enumerate() {
                        let expr_id = ExpressionId::from_path(vec![2000 + idx]);
                        Self::collect_params_from_expr(
                            expr,
                            &expr_id,
                            expression_types,
                            param_types,
                            slots,
                        );
                    }

                    // Check HAVING
                    if let Some(having) = &select.having {
                        let expr_id = ExpressionId::from_path(vec![3000]);
                        Self::collect_params_from_expr(
                            having,
                            &expr_id,
                            expression_types,
                            param_types,
                            slots,
                        );
                    }

                    // Check ORDER BY
                    for (idx, (expr, _)) in select.order_by.iter().enumerate() {
                        let expr_id = ExpressionId::from_path(vec![4000 + idx]);
                        Self::collect_params_from_expr(
                            expr,
                            &expr_id,
                            expression_types,
                            param_types,
                            slots,
                        );
                    }
                }
                DmlStatement::Insert { source, .. } => {
                    use crate::parsing::ast::InsertSource;
                    match source {
                        InsertSource::Values(rows) => {
                            for (row_idx, row) in rows.iter().enumerate() {
                                for (col_idx, expr) in row.iter().enumerate() {
                                    let expr_id = ExpressionId::from_path(vec![row_idx, col_idx]);
                                    Self::collect_params_from_expr(
                                        expr,
                                        &expr_id,
                                        expression_types,
                                        param_types,
                                        slots,
                                    );
                                }
                            }
                        }
                        InsertSource::Select(select) => {
                            Self::collect_parameters(
                                &Statement::Dml(DmlStatement::Select(select.clone())),
                                expression_types,
                                param_types,
                                slots,
                            )?;
                        }
                        InsertSource::DefaultValues => {}
                    }
                }
                DmlStatement::Update { set, r#where, .. } => {
                    for (idx, (_, expr_opt)) in set.iter().enumerate() {
                        if let Some(expr) = expr_opt {
                            let expr_id = ExpressionId::from_path(vec![6000 + idx]);
                            Self::collect_params_from_expr(
                                expr,
                                &expr_id,
                                expression_types,
                                param_types,
                                slots,
                            );
                        }
                    }

                    if let Some(where_expr) = r#where {
                        let expr_id = ExpressionId::from_path(vec![7000]);
                        Self::collect_params_from_expr(
                            where_expr,
                            &expr_id,
                            expression_types,
                            param_types,
                            slots,
                        );
                    }
                }
                DmlStatement::Delete { r#where, .. } => {
                    if let Some(where_expr) = r#where {
                        let expr_id = ExpressionId::from_path(vec![8000]);
                        Self::collect_params_from_expr(
                            where_expr,
                            &expr_id,
                            expression_types,
                            param_types,
                            slots,
                        );
                    }
                }
                DmlStatement::Values(values) => {
                    // Collect parameters from VALUES rows
                    for (row_idx, row) in values.rows.iter().enumerate() {
                        for (col_idx, expr) in row.iter().enumerate() {
                            let expr_id = ExpressionId::from_path(vec![9000 + row_idx, col_idx]);
                            Self::collect_params_from_expr(
                                expr,
                                &expr_id,
                                expression_types,
                                param_types,
                                slots,
                            );
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Collect parameters from an expression
    fn collect_params_from_expr(
        expr: &Expression,
        expr_id: &ExpressionId,
        expression_types: &HashMap<super::statement::ExpressionId, TypeInfo>,
        param_types: &[DataType],
        slots: &mut Vec<ParameterSlot>,
    ) {
        use crate::parsing::ast::{Expression, Operator};

        match expr {
            Expression::Parameter(idx) => {
                // Found a parameter - create a slot for it
                let data_type = param_types.get(*idx).cloned().unwrap_or(DataType::Null);

                // Try to get type info from expression types
                let _type_info = expression_types.get(expr_id);

                // Create a ParameterSlot with proper structure
                let slot = ParameterSlot {
                    index: *idx,
                    actual_type: Some(data_type),
                };

                // Only add if not already present
                if !slots.iter().any(|s| s.index == *idx) {
                    slots.push(slot);
                }
            }
            Expression::Operator(op) => {
                // Recursively check operator arguments
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
                    | Operator::Like(l, r) => {
                        Self::collect_params_from_expr(
                            l,
                            &expr_id.child(0),
                            expression_types,
                            param_types,
                            slots,
                        );
                        Self::collect_params_from_expr(
                            r,
                            &expr_id.child(1),
                            expression_types,
                            param_types,
                            slots,
                        );
                    }
                    Operator::Not(e)
                    | Operator::Negate(e)
                    | Operator::Identity(e)
                    | Operator::Factorial(e) => {
                        Self::collect_params_from_expr(
                            e,
                            &expr_id.child(0),
                            expression_types,
                            param_types,
                            slots,
                        );
                    }
                    Operator::Between {
                        expr, low, high, ..
                    } => {
                        Self::collect_params_from_expr(
                            expr,
                            &expr_id.child(0),
                            expression_types,
                            param_types,
                            slots,
                        );
                        Self::collect_params_from_expr(
                            low,
                            &expr_id.child(1),
                            expression_types,
                            param_types,
                            slots,
                        );
                        Self::collect_params_from_expr(
                            high,
                            &expr_id.child(2),
                            expression_types,
                            param_types,
                            slots,
                        );
                    }
                    Operator::InList { expr, list, .. } => {
                        Self::collect_params_from_expr(
                            expr,
                            &expr_id.child(0),
                            expression_types,
                            param_types,
                            slots,
                        );
                        for (i, item) in list.iter().enumerate() {
                            Self::collect_params_from_expr(
                                item,
                                &expr_id.child(i + 1),
                                expression_types,
                                param_types,
                                slots,
                            );
                        }
                    }
                    Operator::Is(e, _) => {
                        Self::collect_params_from_expr(
                            e,
                            &expr_id.child(0),
                            expression_types,
                            param_types,
                            slots,
                        );
                    }
                    Operator::InSubquery { expr, subquery, .. } => {
                        Self::collect_params_from_expr(
                            expr,
                            &expr_id.child(0),
                            expression_types,
                            param_types,
                            slots,
                        );
                        // Also process the subquery expression
                        Self::collect_params_from_expr(
                            subquery,
                            &expr_id.child(1),
                            expression_types,
                            param_types,
                            slots,
                        );
                    }
                    Operator::Exists { subquery, .. } => {
                        Self::collect_params_from_expr(
                            subquery,
                            &expr_id.child(0),
                            expression_types,
                            param_types,
                            slots,
                        );
                    }
                }
            }
            Expression::Function(_, args) => {
                for (i, arg) in args.iter().enumerate() {
                    Self::collect_params_from_expr(
                        arg,
                        &expr_id.child(i),
                        expression_types,
                        param_types,
                        slots,
                    );
                }
            }
            Expression::ArrayAccess { base, index } => {
                Self::collect_params_from_expr(
                    base,
                    &expr_id.child(0),
                    expression_types,
                    param_types,
                    slots,
                );
                Self::collect_params_from_expr(
                    index,
                    &expr_id.child(1),
                    expression_types,
                    param_types,
                    slots,
                );
            }
            Expression::FieldAccess { base, .. } => {
                Self::collect_params_from_expr(
                    base,
                    &expr_id.child(0),
                    expression_types,
                    param_types,
                    slots,
                );
            }
            Expression::ArrayLiteral(elements) => {
                for (i, elem) in elements.iter().enumerate() {
                    Self::collect_params_from_expr(
                        elem,
                        &expr_id.child(i),
                        expression_types,
                        param_types,
                        slots,
                    );
                }
            }
            Expression::MapLiteral(entries) => {
                for (i, (key, val)) in entries.iter().enumerate() {
                    Self::collect_params_from_expr(
                        key,
                        &expr_id.child(i * 2),
                        expression_types,
                        param_types,
                        slots,
                    );
                    Self::collect_params_from_expr(
                        val,
                        &expr_id.child(i * 2 + 1),
                        expression_types,
                        param_types,
                        slots,
                    );
                }
            }
            Expression::Subquery(_) => {
                // For now, don't collect params from subqueries
                // This would need more sophisticated handling
            }
            _ => {} // Literals, columns, etc. don't contain parameters
        }
    }

    /// Compute output nullability
    fn compute_output_nullability(
        &self,
        statement: &Arc<Statement>,
        types: &TypeOutput,
        analyzed: &mut AnalyzedStatement,
    ) -> Result<()> {
        use crate::parsing::ast::DmlStatement;

        if let Statement::Dml(DmlStatement::Select(select)) = statement.as_ref() {
            for (idx, _) in select.select.iter().enumerate() {
                let expr_id = ExpressionId::from_path(vec![idx]);
                if let Some(type_info) = types.expression_types.get(&expr_id) {
                    analyzed.output_nullability.push(type_info.nullable);
                } else {
                    analyzed.output_nullability.push(true); // Default to nullable
                }
            }
        }
        Ok(())
    }
}

/// Immutable view of resolution data for type checking
pub struct ResolutionView<'a> {
    pub column_map: &'a ColumnResolutionMap,
}

/// Immutable input for validation phase
pub struct ValidationInput<'a> {
    pub expression_types: &'a HashMap<super::statement::ExpressionId, TypeInfo>,
    pub schemas: &'a HashMap<String, Table>,
}

/// Immutable input for optimization phase
pub struct OptimizationInput<'a> {
    pub tables: &'a [super::resolution::TableSource],
    pub column_map: &'a ColumnResolutionMap,
    pub schemas: &'a HashMap<String, Table>,
}
