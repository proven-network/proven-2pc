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
    AnalyzedStatement, CoercionContext, ColumnResolutionMap, ExpressionId, ParameterSlot,
    PredicateTemplate, SqlContext, StatementType, TypeInfo,
};

/// Output from name resolution phase
#[derive(Debug)]
pub struct ResolutionOutput {
    /// Resolved table names and aliases
    pub tables: Vec<(Option<String>, String)>, // (alias, table_name)

    /// Column resolution map for O(1) lookups
    pub column_map: ColumnResolutionMap,

    /// Resolved function names (validated to exist)
    pub functions: Vec<String>,
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

    /// Inferred return types for subqueries
    pub subquery_types: HashMap<usize, Vec<DataType>>,

    /// Statement type metadata
    pub statement_type: StatementType,
}

/// Output from validation phase
#[derive(Debug)]
pub struct ValidationOutput {
    /// Validation passed
    pub valid: bool,

    /// Warnings (non-fatal issues)
    pub warnings: Vec<String>,

    /// Constraint violations detected
    pub constraint_violations: Vec<String>,
}

/// Output from optimization phase
#[derive(Debug, Clone)]
pub struct OptimizationOutput {
    /// Predicate templates for conflict detection
    pub predicate_templates: Vec<PredicateTemplate>,

    /// Index opportunities detected
    pub index_hints: Vec<IndexHint>,

    /// Join ordering hints
    pub join_hints: Vec<JoinHint>,

    /// Expression templates for pre-computation
    pub expression_templates: Vec<ExpressionTemplate>,
}

#[derive(Debug, Clone)]
pub struct IndexHint {
    pub table: String,
    pub column: String,
    pub predicate_type: PredicateType,
}

#[derive(Debug, Clone)]
pub enum PredicateType {
    Equality,
    Range,
    Prefix,
}

#[derive(Debug, Clone)]
pub struct JoinHint {
    pub left_table: String,
    pub right_table: String,
    pub join_type: JoinType,
    pub selectivity_estimate: f64,
}

#[derive(Debug, Clone)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

#[derive(Debug, Clone)]
pub struct ExpressionTemplate {
    pub id: super::statement::ExpressionId,
    pub template: String,
    pub is_deterministic: bool,
}

/// Phase-based analyzer with explicit data flow
pub struct SemanticAnalyzer {
    schemas: HashMap<String, Table>,
}

impl SemanticAnalyzer {
    pub fn new(schemas: HashMap<String, Table>) -> Self {
        Self { schemas }
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
        let validation = self.validate(&ast, &resolution, &types)?;

        // Phase 4: Optimization Metadata (uses all previous outputs)
        let optimization = self.build_optimization_metadata(&ast, &resolution, &types)?;

        // Build final analyzed statement
        Ok(self.build_analyzed_statement(ast, resolution, types, validation, optimization))
    }

    /// Phase 1: Name Resolution
    fn resolve_names(&self, ast: &Arc<Statement>) -> Result<ResolutionOutput> {
        let resolver = super::resolution::NameResolver::new(self.schemas.clone());

        // Build tables list from FROM clauses
        let tables = resolver.extract_tables(ast)?;

        // Build column resolution map
        let column_map = resolver.build_column_map(&tables, &self.schemas)?;

        // Validate function names
        let functions = resolver.validate_functions(ast)?;

        Ok(ResolutionOutput {
            tables,
            column_map,
            functions,
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
            tables: &resolution.tables,
        };

        // Infer types for all expressions
        let expression_types = checker.infer_all_types(ast, &resolution_view, param_types)?;

        // Build parameter slots with context information
        let parameter_slots = self.build_parameter_slots(ast, &expression_types, param_types)?;

        // Infer subquery return types
        let subquery_types = checker.infer_subquery_types(ast, &expression_types)?;

        // Determine statement type
        let statement_type = self.get_statement_type(ast);

        // Check if statement has aggregates
        let has_aggregates = expression_types.values().any(|t| t.is_aggregate);

        Ok(TypeOutput {
            has_aggregates,
            expression_types,
            parameter_slots,
            subquery_types,
            statement_type,
        })
    }

    /// Phase 3: Semantic Validation
    fn validate(
        &self,
        ast: &Arc<Statement>,
        resolution: &ResolutionOutput,
        types: &TypeOutput,
    ) -> Result<ValidationOutput> {
        let validator = super::validation::SemanticValidator::new();

        // Create immutable views of previous phase outputs
        let validation_input = ValidationInput {
            tables: &resolution.tables,
            column_map: &resolution.column_map,
            expression_types: &types.expression_types,
            schemas: &self.schemas,
        };

        // Run all validations
        let violations = validator.validate_all(ast, &validation_input)?;

        // Collect warnings (non-fatal)
        let warnings = validator.collect_warnings(ast, &validation_input);

        Ok(ValidationOutput {
            valid: violations.is_empty(),
            warnings,
            constraint_violations: violations,
        })
    }

    /// Phase 4: Build Optimization Metadata
    fn build_optimization_metadata(
        &self,
        ast: &Arc<Statement>,
        resolution: &ResolutionOutput,
        types: &TypeOutput,
    ) -> Result<OptimizationOutput> {
        let builder = super::optimization::MetadataBuilder::new(self.schemas.clone());

        // Create optimization input from previous phases
        let opt_input = OptimizationInput {
            tables: &resolution.tables,
            column_map: &resolution.column_map,
            expression_types: &types.expression_types,
            schemas: &self.schemas,
        };

        // Build predicate templates
        let predicate_templates = builder.build_predicates(ast, &opt_input)?;

        // Identify index opportunities
        let index_hints = builder.find_index_opportunities(ast, &opt_input)?;

        // Analyze joins
        let join_hints = builder.analyze_joins(ast, &opt_input)?;

        // Build expression templates
        let expression_templates = builder.build_expression_templates(ast, &opt_input)?;

        Ok(OptimizationOutput {
            predicate_templates,
            index_hints,
            join_hints,
            expression_templates,
        })
    }

    /// Build final analyzed statement from all phase outputs
    fn build_analyzed_statement(
        &self,
        ast: Arc<Statement>,
        resolution: ResolutionOutput,
        types: TypeOutput,
        _validation: ValidationOutput,
        optimization: OptimizationOutput,
    ) -> AnalyzedStatement {
        let mut analyzed = AnalyzedStatement::new(ast);

        // Add resolution data
        analyzed.column_resolution_map = resolution.column_map;

        // Add type annotations
        for (expr_id, type_info) in types.expression_types {
            analyzed.type_annotations.annotate(expr_id, type_info);
        }

        // Add parameter slots
        for slot in types.parameter_slots {
            analyzed.add_parameter(slot);
        }

        // Add statement metadata
        analyzed.metadata.statement_type = types.statement_type;
        analyzed.metadata.has_aggregates = types.has_aggregates;

        // Clone predicate templates for conflict detection before adding optimization output
        let templates = optimization.predicate_templates.clone();
        analyzed.metadata.optimization_output = Some(Box::new(optimization));

        // Add predicate templates separately for conflict detection
        for template in templates {
            analyzed.add_predicate_template(template);
        }

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
            },
            Statement::Ddl(ddl) => match ddl {
                crate::parsing::ast::DdlStatement::CreateTable { .. } => StatementType::CreateTable,
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
        self.collect_parameters(ast.as_ref(), expression_types, param_types, &mut slots)?;

        // Sort by parameter index for consistent ordering
        slots.sort_by_key(|s| s.index);

        Ok(slots)
    }

    /// Collect parameter slots from the statement
    fn collect_parameters(
        &self,
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
                        self.collect_params_from_expr(
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
                        self.collect_params_from_expr(
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
                        self.collect_params_from_expr(
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
                        self.collect_params_from_expr(
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
                        self.collect_params_from_expr(
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
                                    self.collect_params_from_expr(
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
                            self.collect_parameters(
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
                            self.collect_params_from_expr(
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
                        self.collect_params_from_expr(
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
                        self.collect_params_from_expr(
                            where_expr,
                            &expr_id,
                            expression_types,
                            param_types,
                            slots,
                        );
                    }
                }
            }
        }

        Ok(())
    }

    /// Collect parameters from an expression
    fn collect_params_from_expr(
        &self,
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
                    expression_id: expr_id.clone(),
                    actual_type: Some(data_type),
                    coercion_context: CoercionContext {
                        sql_context: SqlContext::WhereClause, // Default context
                        nullable: true,
                    },
                    description: format!("Parameter {}", idx),
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
                        self.collect_params_from_expr(
                            l,
                            &expr_id.child(0),
                            expression_types,
                            param_types,
                            slots,
                        );
                        self.collect_params_from_expr(
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
                        self.collect_params_from_expr(
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
                        self.collect_params_from_expr(
                            expr,
                            &expr_id.child(0),
                            expression_types,
                            param_types,
                            slots,
                        );
                        self.collect_params_from_expr(
                            low,
                            &expr_id.child(1),
                            expression_types,
                            param_types,
                            slots,
                        );
                        self.collect_params_from_expr(
                            high,
                            &expr_id.child(2),
                            expression_types,
                            param_types,
                            slots,
                        );
                    }
                    Operator::InList { expr, list, .. } => {
                        self.collect_params_from_expr(
                            expr,
                            &expr_id.child(0),
                            expression_types,
                            param_types,
                            slots,
                        );
                        for (i, item) in list.iter().enumerate() {
                            self.collect_params_from_expr(
                                item,
                                &expr_id.child(i + 1),
                                expression_types,
                                param_types,
                                slots,
                            );
                        }
                    }
                    Operator::Is(e, _) => {
                        self.collect_params_from_expr(
                            e,
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
                    self.collect_params_from_expr(
                        arg,
                        &expr_id.child(i),
                        expression_types,
                        param_types,
                        slots,
                    );
                }
            }
            Expression::ArrayAccess { base, index } => {
                self.collect_params_from_expr(
                    base,
                    &expr_id.child(0),
                    expression_types,
                    param_types,
                    slots,
                );
                self.collect_params_from_expr(
                    index,
                    &expr_id.child(1),
                    expression_types,
                    param_types,
                    slots,
                );
            }
            Expression::FieldAccess { base, .. } => {
                self.collect_params_from_expr(
                    base,
                    &expr_id.child(0),
                    expression_types,
                    param_types,
                    slots,
                );
            }
            Expression::ArrayLiteral(elements) => {
                for (i, elem) in elements.iter().enumerate() {
                    self.collect_params_from_expr(
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
                    self.collect_params_from_expr(
                        key,
                        &expr_id.child(i * 2),
                        expression_types,
                        param_types,
                        slots,
                    );
                    self.collect_params_from_expr(
                        val,
                        &expr_id.child(i * 2 + 1),
                        expression_types,
                        param_types,
                        slots,
                    );
                }
            }
            _ => {} // Literals, columns, etc. don't contain parameters
        }
    }
}

/// Immutable view of resolution data for type checking
pub struct ResolutionView<'a> {
    pub column_map: &'a ColumnResolutionMap,
    pub tables: &'a [(Option<String>, String)],
}

/// Immutable input for validation phase
pub struct ValidationInput<'a> {
    pub tables: &'a [(Option<String>, String)],
    pub column_map: &'a ColumnResolutionMap,
    pub expression_types: &'a HashMap<super::statement::ExpressionId, TypeInfo>,
    pub schemas: &'a HashMap<String, Table>,
}

/// Immutable input for optimization phase
pub struct OptimizationInput<'a> {
    pub tables: &'a [(Option<String>, String)],
    pub column_map: &'a ColumnResolutionMap,
    pub expression_types: &'a HashMap<super::statement::ExpressionId, TypeInfo>,
    pub schemas: &'a HashMap<String, Table>,
}
