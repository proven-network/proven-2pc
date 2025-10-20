//! Subquery planning utilities
//!
//! This module provides helper functions for planning subqueries,
//! eliminating code duplication across the planner.

use super::planner::Planner;
use crate::error::Result;
use crate::parsing::ast::{DmlStatement, SelectStatement, Statement};
use crate::semantic::analyzer::{OuterQueryContext, SemanticAnalyzer};
use crate::semantic::statement::ColumnResolutionMap;
use crate::types::index::IndexMetadata;
use crate::types::plan::Plan;
use crate::types::schema::Table;
use std::collections::HashMap;

/// Helper for planning subqueries
pub struct SubqueryPlanner;

impl SubqueryPlanner {
    /// Plan a subquery with outer context for correlation
    ///
    /// This handles the common pattern of:
    /// 1. Creating a new planner
    /// 2. Setting up outer context for correlated subqueries
    /// 3. Running semantic analysis
    /// 4. Planning the SELECT
    pub fn plan_with_outer_context(
        select: &SelectStatement,
        schemas: &HashMap<String, Table>,
        index_metadata: &HashMap<String, IndexMetadata>,
        outer_column_map: &ColumnResolutionMap,
    ) -> Result<Plan> {
        let subquery_planner = Planner::new(schemas.clone(), HashMap::new());

        // Create analyzer with outer context for correlated subqueries
        let outer_context = OuterQueryContext {
            column_map: outer_column_map.clone(),
        };
        let analyzer = SemanticAnalyzer::with_outer_context(
            schemas.clone(),
            index_metadata.clone(),
            outer_context,
        );

        let subquery_stmt = Statement::Dml(DmlStatement::Select(Box::new(select.clone())));
        let subquery_analyzed = analyzer.analyze(subquery_stmt, Vec::new())?;

        subquery_planner.plan_select(select, &subquery_analyzed)
    }
}
