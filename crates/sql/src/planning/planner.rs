//! Query planner v2 that leverages semantic analysis results
//!
//! This planner uses the column resolution map and metadata from AnalyzedStatement
//! for efficient O(1) column lookups and conflict detection.
//!
//! The planner coordinates specialized planning modules:
//! - statement_planner: Top-level DDL/DML statement routing
//! - select_planner: SELECT query planning
//! - from_planner: FROM clause and JOIN planning
//! - aggregate_planner: Aggregate function handling
//! - projection_builder: Projection/SELECT list building
//! - index_selector: Index selection for WHERE clauses
//! - expression_resolver: Expression resolution and column lookup
//! - subquery_planner: Subquery planning

use super::aggregate_planner::AggregatePlanner;
use super::expression_resolver::AnalyzedPlanContext;
use super::from_planner::FromPlanner;
use super::index_selector::IndexSelector;
use super::projection_builder::ProjectionBuilder;
use super::select_planner::SelectPlanner;
use super::statement_planner::StatementPlanner;
use crate::error::Result;
use crate::parsing::ast::common::FromClause;
use crate::parsing::ast::{Expression as AstExpression, SelectStatement, Statement};
use crate::semantic::AnalyzedStatement;
use crate::types::expression::Expression;
use crate::types::index::IndexMetadata;
use crate::types::plan::{AggregateFunc, Node, Plan};
use crate::types::schema::Table;
use std::collections::HashMap;

/// Query planner that leverages semantic analysis
pub struct Planner {
    schemas: HashMap<String, Table>,
    index_metadata: HashMap<String, IndexMetadata>,
}

impl Planner {
    /// Create a new planner
    pub fn new(
        schemas: HashMap<String, Table>,
        index_metadata: HashMap<String, IndexMetadata>,
    ) -> Self {
        Self {
            schemas,
            index_metadata,
        }
    }

    /// Update schemas (for cache invalidation)
    pub fn update_schemas(&mut self, schemas: HashMap<String, Table>) {
        self.schemas = schemas;
    }

    /// Update index metadata
    pub fn update_indexes(&mut self, indexes: HashMap<String, IndexMetadata>) {
        self.index_metadata = indexes;
    }

    /// Get reference to schemas (used by specialized planners)
    pub(super) fn get_schemas(&self) -> &HashMap<String, Table> {
        &self.schemas
    }

    /// Get reference to index metadata (used by specialized planners)
    pub(super) fn get_index_metadata(&self) -> &HashMap<String, IndexMetadata> {
        &self.index_metadata
    }

    /// Plan an analyzed statement - the main entry point
    ///
    /// Delegates to StatementPlanner for DDL/DML routing
    pub fn plan(&self, analyzed: AnalyzedStatement) -> Result<Plan> {
        let statement = analyzed.ast.clone();

        match &*statement {
            Statement::Explain(inner) => {
                // Create a new AnalyzedStatement with the inner statement
                let inner_analyzed = AnalyzedStatement {
                    ast: std::sync::Arc::new(*inner.clone()),
                    ..analyzed
                };

                // Plan the inner statement
                self.plan(inner_analyzed)
            }

            Statement::Ddl(ddl) => StatementPlanner::plan_ddl(self, ddl, &analyzed),
            Statement::Dml(dml) => StatementPlanner::plan_dml(self, dml, &analyzed),
        }
    }

    /// Plan a SELECT query
    ///
    /// Delegates to SelectPlanner for the full SELECT planning process
    pub fn plan_select(
        &self,
        select: &SelectStatement,
        analyzed: &AnalyzedStatement,
    ) -> Result<Plan> {
        SelectPlanner::plan_select(self, select, analyzed)
    }

    /// Plan FROM clause
    ///
    /// Delegates to FromPlanner for handling tables, joins, and subqueries
    pub(super) fn plan_from(
        &self,
        from: &[FromClause],
        context: &mut AnalyzedPlanContext,
    ) -> Result<Node> {
        FromPlanner::plan_from(self, from, context)
    }

    /// Plan WHERE clause with index optimization
    ///
    /// Delegates to IndexSelector for index selection and optimization
    pub(super) fn plan_where_with_index(
        &self,
        where_expr: &AstExpression,
        source: Node,
        context: &mut AnalyzedPlanContext,
    ) -> Result<Node> {
        IndexSelector::plan_where_with_index(
            where_expr,
            source,
            &self.index_metadata,
            context.analyzed,
            |expr| context.resolve_expression(expr),
        )
    }

    /// Check if an expression contains any aggregate functions
    ///
    /// Delegates to AggregatePlanner
    pub(super) fn contains_aggregate_expr(&self, expr: &AstExpression) -> bool {
        AggregatePlanner::contains_aggregate_expr(expr)
    }

    /// Extract aggregate functions from SELECT and HAVING clauses
    ///
    /// Delegates to AggregatePlanner
    pub(super) fn extract_aggregates(
        &self,
        select: &[(AstExpression, Option<String>)],
        having: Option<&AstExpression>,
        context: &mut AnalyzedPlanContext,
    ) -> Result<Vec<AggregateFunc>> {
        AggregatePlanner::extract_aggregates(select, having, |expr| {
            context.resolve_expression(expr)
        })
    }

    /// Resolve HAVING expression with aggregate-to-column mapping
    ///
    /// Delegates to AggregatePlanner
    pub(super) fn resolve_having_expression(
        &self,
        having_expr: &AstExpression,
        group_by: &[AstExpression],
        aggregates: &[AggregateFunc],
        group_by_count: usize,
        context: &mut AnalyzedPlanContext,
    ) -> Result<Expression> {
        let resolver = |expr: &AstExpression| context.resolve_expression(expr);
        AggregatePlanner::resolve_having_expression(
            having_expr,
            group_by,
            aggregates,
            group_by_count,
            &resolver,
        )
    }

    /// Plan a regular (non-aggregate) projection
    ///
    /// Delegates to ProjectionBuilder
    pub(super) fn plan_projection(
        &self,
        select: &[(AstExpression, Option<String>)],
        context: &mut AnalyzedPlanContext,
    ) -> Result<(Vec<Expression>, Vec<Option<String>>)> {
        ProjectionBuilder::plan_projection(
            select,
            context.get_tables(),
            context.get_schemas(),
            context.analyzed,
            |expr| context.resolve_expression(expr),
        )
    }

    /// Plan an aggregate projection (with GROUP BY)
    ///
    /// Delegates to ProjectionBuilder for the actual building logic
    pub(super) fn plan_aggregate_projection(
        &self,
        select: &[(AstExpression, Option<String>)],
        group_by: &[AstExpression],
        group_by_count: usize,
        context: &mut AnalyzedPlanContext,
    ) -> Result<(Vec<Expression>, Vec<Option<String>>)> {
        // First, need to get the aggregates list to resolve complex expressions
        let aggregates = self.extract_aggregates(select, None, context)?;

        ProjectionBuilder::plan_aggregate_projection(
            select,
            AggregatePlanner::is_aggregate_expr,
            AggregatePlanner::contains_aggregate_expr,
            |expr| {
                AggregatePlanner::find_aggregate_index(expr, &aggregates, group_by_count, |e| {
                    context.resolve_expression(e)
                })
            },
            |expr| AggregatePlanner::find_group_by_index(expr, group_by),
            |expr| {
                AggregatePlanner::resolve_aggregate_expression(
                    expr,
                    group_by,
                    &aggregates,
                    group_by_count,
                    &|e| context.resolve_expression(e),
                    &|e| {
                        AggregatePlanner::find_aggregate_index(
                            e,
                            &aggregates,
                            group_by_count,
                            |inner_e| context.resolve_expression(inner_e),
                        )
                    },
                )
            },
        )
    }

    /// Evaluate a constant expression to a usize value
    ///
    /// Used for LIMIT and OFFSET clauses
    pub(super) fn eval_constant(&self, expr: &AstExpression) -> Result<usize> {
        StatementPlanner::eval_constant(expr)
    }
}
