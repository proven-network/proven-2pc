//! Query planner v2 that leverages semantic analysis results
//!
//! This planner uses the column resolution map and metadata from AnalyzedStatement
//! for efficient O(1) column lookups and conflict detection.

use super::plan::{AggregateFunc, Direction, JoinType, Node, Plan};
use crate::error::{Error, Result};
use crate::parsing::ast::common::{Direction as AstDirection, FromClause, SubquerySource};
use crate::parsing::ast::ddl::DdlStatement;
use crate::parsing::ast::dml::{DmlStatement, ValuesStatement};
use crate::parsing::ast::{
    Column, Expression as AstExpression, InsertSource, Literal, Operator, SelectStatement,
    Statement,
};
use crate::semantic::AnalyzedStatement;
use crate::semantic::analyzer::SemanticAnalyzer;
use crate::types::DataType;
use crate::types::expression::Expression;
use crate::types::index::IndexMetadata;
use crate::types::schema::Table;
use std::collections::{BTreeMap, HashMap};

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
        self.schemas = schemas.clone();
    }

    /// Plan an analyzed statement - the main entry point
    pub fn plan(&self, analyzed: AnalyzedStatement) -> Result<Plan> {
        // Keep the analyzed statement for use throughout planning
        let statement = analyzed.ast.clone();

        match &*statement {
            Statement::Explain(_) => {
                Err(Error::ExecutionError("EXPLAIN not yet implemented".into()))
            }

            Statement::Ddl(ddl) => self.plan_ddl(ddl, &analyzed),
            Statement::Dml(dml) => self.plan_dml(dml, &analyzed),
        }
    }

    /// Plan DDL statements
    fn plan_ddl(&self, ddl: &DdlStatement, analyzed: &AnalyzedStatement) -> Result<Plan> {
        match ddl {
            DdlStatement::CreateTable {
                name,
                columns,
                foreign_keys,
                if_not_exists,
            } => self.plan_create_table(
                name.clone(),
                columns.clone(),
                foreign_keys.clone(),
                *if_not_exists,
            ),

            DdlStatement::CreateTableAsValues {
                name,
                values,
                if_not_exists,
            } => self.plan_create_table_as_values(name.clone(), values, *if_not_exists, analyzed),

            DdlStatement::DropTable {
                names,
                if_exists,
                cascade,
            } => Ok(Plan::DropTable {
                names: names.clone(),
                if_exists: *if_exists,
                cascade: *cascade,
            }),

            DdlStatement::CreateIndex {
                name,
                table,
                columns,
                unique,
                included_columns,
            } => {
                // Validate that index expressions don't contain subqueries
                for col in columns {
                    Self::validate_no_subqueries_in_expression(&col.expression)?;
                }

                // Convert AST IndexColumns to Plan IndexColumns
                let plan_columns = columns
                    .iter()
                    .map(|col| crate::planning::plan::IndexColumn {
                        expression: col.expression.clone(),
                        direction: col.direction.map(|d| match d {
                            AstDirection::Asc => crate::types::query::Direction::Ascending,
                            AstDirection::Desc => crate::types::query::Direction::Descending,
                        }),
                    })
                    .collect();

                Ok(Plan::CreateIndex {
                    name: name.clone(),
                    table: table.clone(),
                    columns: plan_columns,
                    unique: *unique,
                    included_columns: included_columns.clone(),
                })
            }

            DdlStatement::DropIndex { name, if_exists } => Ok(Plan::DropIndex {
                name: name.clone(),
                if_exists: *if_exists,
            }),
        }
    }

    /// Plan DML statements
    fn plan_dml(&self, dml: &DmlStatement, analyzed: &AnalyzedStatement) -> Result<Plan> {
        match dml {
            DmlStatement::Select(select_stmt) => self.plan_select(select_stmt, analyzed),

            DmlStatement::Insert {
                table,
                columns,
                source,
            } => self.plan_insert(table.clone(), columns.clone(), source.clone(), analyzed),

            DmlStatement::Update {
                table,
                set,
                r#where,
            } => self.plan_update(table.clone(), set.clone(), r#where.clone(), analyzed),

            DmlStatement::Delete { table, r#where } => {
                self.plan_delete(table.clone(), r#where.clone(), analyzed)
            }

            DmlStatement::Values(values_stmt) => self.plan_values(values_stmt, analyzed),
        }
    }

    /// Plan a SELECT query
    pub fn plan_select(
        &self,
        select: &SelectStatement,
        analyzed: &AnalyzedStatement,
    ) -> Result<Plan> {
        // Check if SELECT contains wildcards - we'll use this later for column names
        let has_wildcard = select.select.iter().any(|(expr, _)| {
            matches!(
                expr,
                AstExpression::All | AstExpression::QualifiedWildcard(_)
            )
        });

        // Create context that uses the analyzed statement
        let mut context = AnalyzedPlanContext::new(&self.schemas, analyzed);

        // Start with FROM clause
        let mut node = self.plan_from(&select.from, &mut context)?;

        // Apply WHERE filter
        if let Some(ref where_expr) = select.r#where {
            node = self.plan_where_with_index(where_expr, node, &mut context)?;
        }

        // Apply GROUP BY and aggregates
        // Use metadata instead of re-computing
        let has_aggregates = analyzed.metadata.has_aggregates;
        let group_by_count = select.group_by.len();

        if !select.group_by.is_empty() || has_aggregates {
            let group_exprs = select
                .group_by
                .iter()
                .map(|e| context.resolve_expression(e))
                .collect::<Result<Vec<_>>>()?;

            let aggregates = self.extract_aggregates(&select.select, &mut context)?;

            node = Node::Aggregate {
                source: Box::new(node),
                group_by: group_exprs,
                aggregates,
            };

            // Apply HAVING filter
            if let Some(ref having_expr) = select.having {
                let predicate = context.resolve_expression(having_expr)?;
                node = Node::Filter {
                    source: Box::new(node),
                    predicate,
                };
            }
        }

        // Apply ORDER BY
        if !select.order_by.is_empty() {
            let order = select
                .order_by
                .iter()
                .map(|(e, d)| {
                    let expr = context.resolve_expression(e)?;
                    let dir = match d {
                        AstDirection::Asc => Direction::Ascending,
                        AstDirection::Desc => Direction::Descending,
                    };
                    Ok((expr, dir))
                })
                .collect::<Result<Vec<_>>>()?;

            node = Node::Order {
                source: Box::new(node),
                order_by: order,
            };
        }

        // Apply projection
        let (expressions, aliases) = if has_aggregates {
            self.plan_aggregate_projection(
                &select.select,
                &select.group_by,
                group_by_count,
                &mut context,
            )?
        } else {
            self.plan_projection(&select.select, &mut context)?
        };

        node = Node::Projection {
            source: Box::new(node),
            expressions,
            aliases,
        };

        // Apply OFFSET
        if let Some(ref offset_expr) = select.offset {
            let offset = self.eval_constant(offset_expr)?;
            node = Node::Offset {
                source: Box::new(node),
                offset,
            };
        }

        // Apply LIMIT
        if let Some(ref limit_expr) = select.limit {
            let limit = self.eval_constant(limit_expr)?;
            node = Node::Limit {
                source: Box::new(node),
                limit,
            };
        }

        // Extract column names from the column resolution map only if SELECT contains wildcards
        // For other cases (aggregates, expressions), let get_column_names() handle it
        let column_names = if has_wildcard {
            Some(analyzed.column_resolution_map.get_ordered_column_names())
        } else {
            None
        };

        Ok(Plan::Query {
            root: Box::new(node),
            params: Vec::new(),
            column_names,
        })
    }

    /// Plan FROM clause
    fn plan_from(&self, from: &[FromClause], context: &mut AnalyzedPlanContext) -> Result<Node> {
        if from.is_empty() {
            return Ok(Node::Values { rows: vec![vec![]] });
        }

        let mut node = None;

        for from_item in from {
            match from_item {
                FromClause::Table { name, alias } => {
                    context.add_table(name.clone(), alias.as_ref().map(|a| a.name.clone()))?;
                    let scan = Node::Scan {
                        table: name.clone(),
                        alias: alias.as_ref().map(|a| a.name.clone()),
                    };

                    node = Some(if let Some(prev) = node {
                        Node::NestedLoopJoin {
                            left: Box::new(prev),
                            right: Box::new(scan),
                            predicate: Expression::Constant(crate::types::Value::boolean(true)),
                            join_type: JoinType::Inner,
                        }
                    } else {
                        scan
                    });
                }

                FromClause::Subquery { source, alias: _ } => {
                    // Plan the subquery based on its type
                    let subquery_node = match source {
                        SubquerySource::Select(select_stmt) => {
                            // Plan the SELECT subquery
                            let subplan = self.plan_select(select_stmt, context.analyzed)?;
                            match subplan {
                                Plan::Query { root: node, .. } => *node,
                                _ => {
                                    return Err(Error::ExecutionError(
                                        "Invalid subquery plan".into(),
                                    ));
                                }
                            }
                        }
                        SubquerySource::Values(values_stmt) => {
                            // Plan the VALUES subquery
                            self.plan_values_as_subquery(values_stmt, context)?
                        }
                    };

                    // Subqueries don't need to be added to the context as tables
                    // They produce their own column structure

                    node = Some(if let Some(prev) = node {
                        Node::NestedLoopJoin {
                            left: Box::new(prev),
                            right: Box::new(subquery_node),
                            predicate: Expression::Constant(crate::types::Value::boolean(true)),
                            join_type: JoinType::Inner,
                        }
                    } else {
                        subquery_node
                    });
                }

                FromClause::Join {
                    left,
                    right,
                    r#type,
                    predicate,
                } => {
                    let left_node = self.plan_from(&[*left.clone()], context)?;
                    let right_node = self.plan_from(&[*right.clone()], context)?;

                    // Extract table names from the nodes
                    let (left_table, right_table) =
                        self.extract_table_names(&left_node, &right_node);

                    let join_node = if let Some(pred) = predicate {
                        // Check if this is an equi-join
                        if let Some((left_col, right_col)) =
                            self.extract_equi_join_columns(pred, &left_node, &right_node, context)
                        {
                            // Check join hints for selectivity guidance
                            let use_hash_join =
                                if let (Some(lt), Some(rt)) = (&left_table, &right_table) {
                                    context
                                        .analyzed
                                        .join_hints
                                        .iter()
                                        .find(|h| {
                                            (h.left_table == *lt && h.right_table == *rt)
                                                || (h.left_table == *rt && h.right_table == *lt)
                                        })
                                        .map(|h| h.selectivity_estimate > 0.1) // Use hash join for higher selectivity
                                        .unwrap_or(true) // Default to hash join
                                } else {
                                    true // Default to hash join
                                };

                            if use_hash_join {
                                Node::HashJoin {
                                    left: Box::new(left_node),
                                    right: Box::new(right_node),
                                    left_col,
                                    right_col,
                                    join_type: Self::convert_join_type(r#type),
                                }
                            } else {
                                // Use nested loop for very selective joins
                                let join_predicate = context.resolve_expression(pred)?;
                                Node::NestedLoopJoin {
                                    left: Box::new(left_node),
                                    right: Box::new(right_node),
                                    predicate: join_predicate,
                                    join_type: Self::convert_join_type(r#type),
                                }
                            }
                        } else {
                            let join_predicate = context.resolve_expression(pred)?;
                            Node::NestedLoopJoin {
                                left: Box::new(left_node),
                                right: Box::new(right_node),
                                predicate: join_predicate,
                                join_type: Self::convert_join_type(r#type),
                            }
                        }
                    } else {
                        Node::NestedLoopJoin {
                            left: Box::new(left_node),
                            right: Box::new(right_node),
                            predicate: Expression::Constant(crate::types::Value::boolean(true)),
                            join_type: JoinType::Cross,
                        }
                    };

                    node = Some(if let Some(prev) = node {
                        Node::NestedLoopJoin {
                            left: Box::new(prev),
                            right: Box::new(join_node),
                            predicate: Expression::Constant(crate::types::Value::boolean(true)),
                            join_type: JoinType::Inner,
                        }
                    } else {
                        join_node
                    });
                }
            }
        }

        Ok(node.unwrap_or(Node::Nothing))
    }

    fn plan_insert(
        &self,
        table: String,
        columns: Option<Vec<String>>,
        source: InsertSource,
        _analyzed: &AnalyzedStatement,
    ) -> Result<Plan> {
        // Simplified for now - will be enhanced
        let schema = self
            .schemas
            .get(&table)
            .ok_or_else(|| Error::TableNotFound(table.clone()))?;

        let column_indices = if let Some(cols) = columns {
            let mut indices = Vec::new();
            for col_name in &cols {
                let idx = schema
                    .columns
                    .iter()
                    .position(|c| &c.name == col_name)
                    .ok_or_else(|| Error::ColumnNotFound(col_name.clone()))?;
                indices.push(idx);
            }
            Some(indices)
        } else {
            None
        };

        // Build source node - simplified for now
        let source_node = match source {
            InsertSource::Values(values) => {
                let context = AnalyzedPlanContext::new(&self.schemas, _analyzed);
                let rows = values
                    .into_iter()
                    .map(|row| {
                        row.into_iter()
                            .map(|e| context.resolve_expression_simple(&e))
                            .collect::<Result<Vec<_>>>()
                    })
                    .collect::<Result<Vec<_>>>()?;
                Box::new(Node::Values { rows })
            }
            InsertSource::DefaultValues => Box::new(Node::Values { rows: vec![vec![]] }),
            InsertSource::Select(select) => {
                let plan = self.plan_select(&select, _analyzed)?;
                match plan {
                    Plan::Query { root, .. } => root,
                    _ => return Err(Error::ExecutionError("Expected Query plan".into())),
                }
            }
        };

        Ok(Plan::Insert {
            table,
            columns: column_indices,
            source: source_node,
        })
    }

    fn plan_update(
        &self,
        table: String,
        set: BTreeMap<String, Option<AstExpression>>,
        r#where: Option<AstExpression>,
        analyzed: &AnalyzedStatement,
    ) -> Result<Plan> {
        let mut context = AnalyzedPlanContext::new(&self.schemas, analyzed);
        context.add_table(table.clone(), None)?;

        let mut node = Node::Scan {
            table: table.clone(),
            alias: None,
        };

        if let Some(ref where_expr) = r#where {
            let predicate = context.resolve_expression(where_expr)?;
            node = Node::Filter {
                source: Box::new(node),
                predicate,
            };
        }

        let schema = self
            .schemas
            .get(&table)
            .ok_or_else(|| Error::TableNotFound(table.clone()))?;

        let assignments = set
            .into_iter()
            .map(|(col, expr)| {
                let column = schema
                    .columns
                    .iter()
                    .position(|c| c.name == col)
                    .ok_or(Error::ColumnNotFound(col))?;

                let value = if let Some(e) = expr {
                    context.resolve_expression(&e)?
                } else {
                    Expression::Constant(crate::types::Value::Null)
                };

                Ok((column, value))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Plan::Update {
            table,
            assignments,
            source: Box::new(node),
        })
    }

    fn plan_delete(
        &self,
        table: String,
        r#where: Option<AstExpression>,
        analyzed: &AnalyzedStatement,
    ) -> Result<Plan> {
        let mut context = AnalyzedPlanContext::new(&self.schemas, analyzed);
        context.add_table(table.clone(), None)?;

        let mut node = Node::Scan {
            table: table.clone(),
            alias: None,
        };

        if let Some(ref where_expr) = r#where {
            let predicate = context.resolve_expression(where_expr)?;
            node = Node::Filter {
                source: Box::new(node),
                predicate,
            };
        }

        Ok(Plan::Delete {
            table,
            source: Box::new(node),
        })
    }

    /// Plan a VALUES statement
    fn plan_values_as_subquery(
        &self,
        values_stmt: &ValuesStatement,
        context: &mut AnalyzedPlanContext,
    ) -> Result<Node> {
        // Similar to plan_values but simpler - no ORDER BY, LIMIT, OFFSET for subqueries
        let rows = values_stmt
            .rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|e| context.resolve_expression_simple(e))
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Node::Values { rows })
    }

    fn plan_values(
        &self,
        values_stmt: &crate::parsing::ast::dml::ValuesStatement,
        analyzed: &AnalyzedStatement,
    ) -> Result<Plan> {
        let context = AnalyzedPlanContext::new(&self.schemas, analyzed);

        // Convert expression rows to planned expressions
        let rows = values_stmt
            .rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|expr| context.resolve_expression(expr))
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()?;

        // Determine number of columns for VALUES
        let num_columns = rows.first().map(|r| r.len()).unwrap_or(0);

        let mut node = Node::Values { rows: rows.clone() };

        // Apply ORDER BY if present
        if !values_stmt.order_by.is_empty() {
            let order_by = values_stmt
                .order_by
                .iter()
                .map(|(expr, dir)| {
                    // For VALUES, handle columnN references specially
                    let resolved_expr = match expr {
                        AstExpression::Column(None, name) if name.starts_with("column") => {
                            // Try to parse columnN as a column index
                            if let Ok(col_num) = name[6..].parse::<usize>() {
                                if col_num > 0 && col_num <= num_columns {
                                    // Convert to 0-based column index
                                    Expression::Column(col_num - 1)
                                } else {
                                    return Err(Error::ExecutionError(format!(
                                        "Column {} out of range for VALUES with {} columns",
                                        name, num_columns
                                    )));
                                }
                            } else {
                                // Not a valid columnN reference
                                context.resolve_expression(expr)?
                            }
                        }
                        _ => context.resolve_expression(expr)?,
                    };

                    Ok((
                        resolved_expr,
                        match dir {
                            AstDirection::Asc => Direction::Ascending,
                            AstDirection::Desc => Direction::Descending,
                        },
                    ))
                })
                .collect::<Result<Vec<_>>>()?;

            node = Node::Order {
                source: Box::new(node),
                order_by,
            };
        }

        // Apply LIMIT if present
        if let Some(limit_expr) = &values_stmt.limit {
            // For now, evaluate limit as a constant
            // TODO: Support dynamic limits
            let limit = match context.resolve_expression(limit_expr)? {
                Expression::Constant(crate::types::Value::I32(n)) if n > 0 => n as usize,
                Expression::Constant(crate::types::Value::I64(n)) if n > 0 => n as usize,
                _ => {
                    return Err(Error::ExecutionError(
                        "LIMIT must be a positive integer".into(),
                    ));
                }
            };

            node = Node::Limit {
                source: Box::new(node),
                limit,
            };
        }

        // Apply OFFSET if present
        if let Some(offset_expr) = &values_stmt.offset {
            // For now, evaluate offset as a constant
            // TODO: Support dynamic offsets
            let offset = match context.resolve_expression(offset_expr)? {
                Expression::Constant(crate::types::Value::I32(n)) if n >= 0 => n as usize,
                Expression::Constant(crate::types::Value::I64(n)) if n >= 0 => n as usize,
                _ => {
                    return Err(Error::ExecutionError(
                        "OFFSET must be a non-negative integer".into(),
                    ));
                }
            };

            node = Node::Offset {
                source: Box::new(node),
                offset,
            };
        }

        // For VALUES statements, don't pass column names - let get_column_names() handle it
        Ok(Plan::Query {
            root: Box::new(node),
            params: Vec::new(),
            column_names: None,
        })
    }

    // Helper methods...

    fn plan_create_table(
        &self,
        name: String,
        columns: Vec<Column>,
        foreign_keys: Vec<crate::parsing::ast::ddl::ForeignKeyConstraint>,
        if_not_exists: bool,
    ) -> Result<Plan> {
        // Similar to original planner for now

        let mut schema_columns = Vec::new();
        let mut primary_key_idx = None;

        for (i, col) in columns.iter().enumerate() {
            let mut schema_col =
                crate::types::schema::Column::new(col.name.clone(), col.data_type.clone());

            if col.primary_key {
                if primary_key_idx.is_some() {
                    return Err(Error::ExecutionError(
                        "Multiple primary keys not supported".into(),
                    ));
                }
                primary_key_idx = Some(i);
                schema_col = schema_col.primary_key();
            }

            if let Some(nullable) = col.nullable {
                schema_col = schema_col.nullable(nullable);
            }

            if col.unique {
                schema_col = schema_col.unique();
            }

            if col.index {
                schema_col = schema_col.with_index(true);
            }

            // Handle DEFAULT expression
            if let Some(ref default_expr) = col.default {
                // Resolve the default expression (validate it, convert to Expression)
                // Functions will be evaluated at INSERT time with transaction context
                let resolved_expr = resolve_default_expression(default_expr)?;
                schema_col = schema_col.default(resolved_expr);
            }

            schema_columns.push(schema_col);
        }

        let table =
            Table::new_with_foreign_keys(name.clone(), schema_columns, foreign_keys.clone())?;

        Ok(Plan::CreateTable {
            name,
            schema: table,
            foreign_keys,
            if_not_exists,
        })
    }

    fn plan_create_table_as_values(
        &self,
        name: String,
        values: &ValuesStatement,
        if_not_exists: bool,
        analyzed: &AnalyzedStatement,
    ) -> Result<Plan> {
        // First, plan the VALUES statement to determine types
        let values_plan = self.plan_values(values, analyzed)?;

        // Extract the data types from the VALUES plan
        let column_types = match &values_plan {
            Plan::Query { root, .. } => {
                match &**root {
                    Node::Values { rows } => {
                        // Determine types from all rows (to handle NULL properly)
                        if rows.is_empty() {
                            return Err(Error::ExecutionError("VALUES has no rows".into()));
                        }

                        let num_cols = rows[0].len();
                        let mut column_types = vec![DataType::Null; num_cols];

                        // Go through all rows to determine the actual types
                        for row in rows {
                            for (i, expr) in row.iter().enumerate() {
                                let expr_type = self.infer_expression_type(expr)?;
                                // Update column type if it's more specific than what we have
                                if column_types[i] == DataType::Null && expr_type != DataType::Null
                                {
                                    column_types[i] = expr_type;
                                }
                            }
                        }

                        // If any column is still NULL, default to Str
                        for dtype in &mut column_types {
                            if *dtype == DataType::Null {
                                *dtype = DataType::Str;
                            }
                        }

                        column_types
                    }
                    _ => return Err(Error::ExecutionError("Expected VALUES node".into())),
                }
            }
            _ => return Err(Error::ExecutionError("Expected Query plan".into())),
        };

        // Create columns with auto-generated names (column1, column2, etc.)
        let schema_columns = column_types
            .iter()
            .enumerate()
            .map(|(i, dtype)| {
                let col_name = format!("column{}", i + 1);
                crate::types::schema::Column::new(col_name, dtype.clone()).nullable(true) // Default to nullable for VALUES-created tables
            })
            .collect();

        let table = Table::new(name.clone(), schema_columns)?;

        // Create a compound plan: CREATE TABLE followed by INSERT
        Ok(Plan::CreateTableAsValues {
            name,
            schema: table,
            values_plan: Box::new(values_plan),
            if_not_exists,
        })
    }

    fn infer_expression_type(&self, expr: &Expression) -> Result<DataType> {
        use crate::types::Value;

        match expr {
            Expression::Constant(Value::Null) => Ok(DataType::Null),
            Expression::Constant(Value::Bool(_)) => Ok(DataType::Bool),
            Expression::Constant(Value::I32(_)) => Ok(DataType::I32),
            Expression::Constant(Value::I64(_)) => Ok(DataType::I64),
            Expression::Constant(Value::I128(_)) => Ok(DataType::I128),
            Expression::Constant(Value::F64(_)) => Ok(DataType::F64),
            Expression::Constant(Value::Str(_)) => Ok(DataType::Str),
            Expression::Constant(Value::Bytea(_)) => Ok(DataType::Bytea),
            // For complex expressions, default to the most general type
            Expression::Add(_, _)
            | Expression::Subtract(_, _)
            | Expression::Multiply(_, _)
            | Expression::Divide(_, _)
            | Expression::Remainder(_, _)
            | Expression::Exponentiate(_, _) => Ok(DataType::F64),
            Expression::Concat(_, _) => Ok(DataType::Str),
            Expression::And(_, _) | Expression::Or(_, _) | Expression::Not(_) => Ok(DataType::Bool),
            _ => Ok(DataType::Str), // Default to string for unknown expressions
        }
    }

    fn convert_join_type(join_type: &crate::parsing::ast::common::JoinType) -> JoinType {
        match join_type {
            crate::parsing::ast::common::JoinType::Cross => JoinType::Cross,
            crate::parsing::ast::common::JoinType::Inner => JoinType::Inner,
            crate::parsing::ast::common::JoinType::Left => JoinType::Left,
            crate::parsing::ast::common::JoinType::Right => JoinType::Right,
            crate::parsing::ast::common::JoinType::Full => JoinType::Full,
        }
    }

    fn is_aggregate_expr(&self, expr: &AstExpression) -> bool {
        match expr {
            AstExpression::Function(name, _) => {
                matches!(
                    name.to_uppercase().as_str(),
                    "COUNT"
                        | "SUM"
                        | "AVG"
                        | "MIN"
                        | "MAX"
                        | "STDEV"
                        | "VARIANCE"
                        | "COUNT_DISTINCT"
                        | "SUM_DISTINCT"
                        | "AVG_DISTINCT"
                        | "MIN_DISTINCT"
                        | "MAX_DISTINCT"
                        | "STDEV_DISTINCT"
                        | "VARIANCE_DISTINCT"
                )
            }
            _ => false,
        }
    }

    fn eval_constant(&self, expr: &AstExpression) -> Result<usize> {
        match expr {
            AstExpression::Literal(Literal::Integer(n)) if *n >= 0 && *n <= usize::MAX as i128 => {
                Ok(*n as usize)
            }
            _ => Err(Error::ExecutionError(
                "Expected non-negative integer constant".into(),
            )),
        }
    }

    fn plan_where_with_index(
        &self,
        where_expr: &AstExpression,
        source: Node,
        context: &mut AnalyzedPlanContext,
    ) -> Result<Node> {
        // Try to use indexes if source is a Scan node
        if let Node::Scan { table, alias } = &source {
            // Check predicate templates for index opportunities
            for template in &context.analyzed.predicate_templates {
                use crate::semantic::statement::PredicateTemplate;

                match template {
                    // IndexedColumn template - explicitly marked as indexed
                    PredicateTemplate::IndexedColumn {
                        table: tbl,
                        column,
                        value,
                    } if tbl == table => {
                        if let Some(index_name) = self.find_index_for_column(table, column) {
                            let value_expr = self.predicate_value_to_expression(value, context)?;
                            return Ok(Node::IndexScan {
                                table: table.clone(),
                                alias: alias.clone(),
                                index_name,
                                values: vec![value_expr],
                            });
                        }
                    }

                    // Equality template - check if column is indexed
                    PredicateTemplate::Equality {
                        table: tbl,
                        column_name,
                        value_expr: value,
                    } if tbl == table => {
                        if let Some(index_name) = self.find_index_for_column(table, column_name) {
                            let value_expr = self.predicate_value_to_expression(value, context)?;
                            return Ok(Node::IndexScan {
                                table: table.clone(),
                                alias: alias.clone(),
                                index_name,
                                values: vec![value_expr],
                            });
                        }
                    }

                    // Range template - use IndexRangeScan
                    PredicateTemplate::Range {
                        table: tbl,
                        column_name,
                        lower,
                        upper,
                    } if tbl == table => {
                        if let Some(index_name) = self.find_index_for_column(table, column_name) {
                            let start = lower
                                .as_ref()
                                .map(|(v, _)| self.predicate_value_to_expression(v, context))
                                .transpose()?;
                            let end = upper
                                .as_ref()
                                .map(|(v, _)| self.predicate_value_to_expression(v, context))
                                .transpose()?;

                            return Ok(Node::IndexRangeScan {
                                table: table.clone(),
                                alias: alias.clone(),
                                index_name,
                                start: start.map(|v| vec![v]),
                                start_inclusive: lower
                                    .as_ref()
                                    .map(|(_, inc)| *inc)
                                    .unwrap_or(true),
                                end: end.map(|v| vec![v]),
                                end_inclusive: upper.as_ref().map(|(_, inc)| *inc).unwrap_or(true),
                                reverse: false,
                            });
                        }
                    }

                    _ => continue,
                }
            }
        }

        // Fallback to Filter node
        let predicate = context.resolve_expression(where_expr)?;
        Ok(Node::Filter {
            source: Box::new(source),
            predicate,
        })
    }

    /// Convert a PredicateValue to an Expression
    fn predicate_value_to_expression(
        &self,
        value: &crate::semantic::statement::PredicateValue,
        _context: &AnalyzedPlanContext,
    ) -> Result<Expression> {
        use crate::semantic::statement::PredicateValue;

        match value {
            PredicateValue::Constant(val) => Ok(Expression::Constant(val.clone())),
            PredicateValue::Parameter(idx) => Ok(Expression::Parameter(*idx)),
            PredicateValue::Expression(_expr_id) => {
                // For complex expressions, we'd need to walk the AST
                // For now, return an error - this should be rare for index predicates
                Err(Error::ExecutionError(
                    "Complex expressions in index predicates not yet supported".into(),
                ))
            }
        }
    }

    /// Find an index that covers the given column
    fn find_index_for_column(&self, table: &str, column: &str) -> Option<String> {
        // Search through index metadata for an index on this table and column
        for (index_name, metadata) in &self.index_metadata {
            // Check if this index is for the right table
            if metadata.table.eq_ignore_ascii_case(table) {
                // Check if this index includes the column we're looking for
                if !metadata.columns.is_empty() && metadata.columns[0].eq_ignore_ascii_case(column)
                {
                    return Some(index_name.clone());
                }
            }
        }

        None
    }

    fn extract_aggregates(
        &self,
        select: &[(AstExpression, Option<String>)],
        context: &mut AnalyzedPlanContext,
    ) -> Result<Vec<AggregateFunc>> {
        let mut aggregates = Vec::new();

        // Use aggregate_expressions from metadata to quickly identify aggregates
        for (idx, (expr, _)) in select.iter().enumerate() {
            let expr_id = crate::semantic::statement::ExpressionId::from_path(vec![idx]);

            // Check if this expression is marked as an aggregate
            if context.analyzed.aggregate_expressions.contains(&expr_id)
                && let AstExpression::Function(name, args) = expr
            {
                let func_name = name.to_uppercase();

                let arg = if args.is_empty() {
                    Expression::Constant(crate::types::Value::integer(1))
                } else if args.len() == 1 && matches!(args[0], AstExpression::All) {
                    if func_name.ends_with("_DISTINCT") {
                        Expression::All
                    } else {
                        Expression::Constant(crate::types::Value::integer(1))
                    }
                } else {
                    context.resolve_expression(&args[0])?
                };

                let agg = match func_name.as_str() {
                    "COUNT" => AggregateFunc::Count(arg),
                    "COUNT_DISTINCT" => AggregateFunc::CountDistinct(arg),
                    "SUM" => AggregateFunc::Sum(arg),
                    "SUM_DISTINCT" => AggregateFunc::SumDistinct(arg),
                    "AVG" => AggregateFunc::Avg(arg),
                    "AVG_DISTINCT" => AggregateFunc::AvgDistinct(arg),
                    "MIN" => AggregateFunc::Min(arg),
                    "MIN_DISTINCT" => AggregateFunc::MinDistinct(arg),
                    "MAX" => AggregateFunc::Max(arg),
                    "MAX_DISTINCT" => AggregateFunc::MaxDistinct(arg),
                    "STDEV" => AggregateFunc::StDev(arg),
                    "STDEV_DISTINCT" => AggregateFunc::StDevDistinct(arg),
                    "VARIANCE" => AggregateFunc::Variance(arg),
                    "VARIANCE_DISTINCT" => AggregateFunc::VarianceDistinct(arg),
                    _ => continue,
                };

                aggregates.push(agg);
            }
        }

        Ok(aggregates)
    }

    fn plan_projection(
        &self,
        select: &[(AstExpression, Option<String>)],
        context: &mut AnalyzedPlanContext,
    ) -> Result<(Vec<Expression>, Vec<Option<String>>)> {
        let mut expressions = Vec::new();
        let mut aliases = Vec::new();

        for (expr, alias) in select {
            match expr {
                AstExpression::All => {
                    // Expand * to all columns
                    // For regular tables, use the old logic with table.start_column + i
                    // For subqueries (VALUES/SELECT), fall back to resolution map

                    if context.tables.is_empty() {
                        // No tables in context - this is a subquery (VALUES/SELECT)
                        // Use resolution map for both offsets and names
                        let mut all_resolutions: Vec<_> = context
                            .analyzed
                            .column_resolution_map
                            .columns
                            .values()
                            .collect();
                        all_resolutions.sort_by_key(|r| r.global_offset);
                        all_resolutions.dedup_by_key(|r| r.global_offset);

                        let column_names = context
                            .analyzed
                            .column_resolution_map
                            .get_ordered_column_names();

                        for (resolution, col_name) in
                            all_resolutions.iter().zip(column_names.iter())
                        {
                            expressions.push(Expression::Column(resolution.global_offset));
                            aliases.push(Some(col_name.clone()));
                        }
                    } else {
                        // Regular tables - use original logic for column offsets
                        // but get column names from resolution map (may be aliased)
                        let column_names = context
                            .analyzed
                            .column_resolution_map
                            .get_ordered_column_names();

                        let mut name_idx = 0;
                        for table in &context.tables {
                            if let Some(schema) = self.schemas.get(&table.name) {
                                for (i, _col) in schema.columns.iter().enumerate() {
                                    expressions.push(Expression::Column(table.start_column + i));
                                    // Use aliased name if available, otherwise use schema name
                                    if let Some(col_name) = column_names.get(name_idx) {
                                        aliases.push(Some(col_name.clone()));
                                    } else {
                                        aliases.push(Some(_col.name.clone()));
                                    }
                                    name_idx += 1;
                                }
                            }
                        }
                    }
                }
                AstExpression::QualifiedWildcard(table_alias) => {
                    // Expand table.* to all columns from that specific table
                    for table in &context.tables {
                        // Check if this is the table we're looking for (by alias or name)
                        if ((table.alias.as_deref() == Some(table_alias.as_str()))
                            && table.name == *table_alias)
                            && let Some(schema) = self.schemas.get(&table.name)
                        {
                            for (i, col) in schema.columns.iter().enumerate() {
                                expressions.push(Expression::Column(table.start_column + i));
                                aliases.push(Some(col.name.clone()));
                            }
                            break;
                        }
                    }
                }
                AstExpression::Column(_table_ref, col_name) if alias.is_none() => {
                    expressions.push(context.resolve_expression(expr)?);
                    aliases.push(Some(col_name.clone()));
                }
                _ => {
                    expressions.push(context.resolve_expression(expr)?);
                    aliases.push(alias.clone());
                }
            }
        }

        Ok((expressions, aliases))
    }

    fn plan_aggregate_projection(
        &self,
        select: &[(AstExpression, Option<String>)],
        _group_by: &[AstExpression],
        group_by_count: usize,
        _context: &mut AnalyzedPlanContext,
    ) -> Result<(Vec<Expression>, Vec<Option<String>>)> {
        let mut expressions = Vec::new();
        let mut aliases = Vec::new();
        let mut col_idx = group_by_count; // Start after GROUP BY columns

        for (expr, alias) in select {
            if self.is_aggregate_expr(expr) {
                // For aggregate functions, project the corresponding aggregate result column
                expressions.push(Expression::Column(col_idx));
                col_idx += 1;

                // Generate alias for aggregate function
                let func_alias = alias.clone().or_else(|| {
                    if let AstExpression::Function(name, args) = expr {
                        let func_name = name.to_uppercase();

                        // Handle DISTINCT functions - they come as "AVG_DISTINCT" etc
                        let (base_func, is_distinct) = if func_name.ends_with("_DISTINCT") {
                            (func_name.trim_end_matches("_DISTINCT"), true)
                        } else {
                            (func_name.as_str(), false)
                        };

                        let arg_str = if args.is_empty() || matches!(args[0], AstExpression::All) {
                            "*".to_string()
                        } else {
                            expr_to_string(&args[0])
                        };

                        if is_distinct {
                            Some(format!("{}(DISTINCT {})", base_func, arg_str))
                        } else {
                            Some(format!("{}({})", base_func, arg_str))
                        }
                    } else {
                        None
                    }
                });
                aliases.push(func_alias);
            } else {
                // For non-aggregate expressions in GROUP BY context,
                // they must be GROUP BY columns - project from position
                // For simplicity, assume it's in the first position if it's user_id
                if let AstExpression::Column(_, col_name) = expr {
                    // Find which GROUP BY position this is
                    // For now, assume it's position 0 if it matches
                    expressions.push(Expression::Column(0));
                    aliases.push(alias.clone().or(Some(col_name.clone())));
                } else {
                    expressions.push(Expression::Column(0));
                    aliases.push(alias.clone());
                }
            }
        }

        Ok((expressions, aliases))
    }

    fn extract_equi_join_columns(
        &self,
        _predicate: &AstExpression,
        _left_node: &Node,
        _right_node: &Node,
        _context: &AnalyzedPlanContext,
    ) -> Option<(usize, usize)> {
        // Simplified for now
        None
    }

    fn extract_table_names(
        &self,
        left_node: &Node,
        right_node: &Node,
    ) -> (Option<String>, Option<String>) {
        let left_table = match left_node {
            Node::Scan { table, .. } => Some(table.clone()),
            _ => None,
        };
        let right_table = match right_node {
            Node::Scan { table, .. } => Some(table.clone()),
            _ => None,
        };
        (left_table, right_table)
    }

    /// Validate that an expression doesn't contain subqueries (for index expressions)
    fn validate_no_subqueries_in_expression(expr: &AstExpression) -> Result<()> {
        use crate::parsing::ast::{Expression as AstExpression, Operator};

        match expr {
            AstExpression::Subquery(_) => {
                return Err(Error::ParseError(
                    "Subqueries are not allowed in index expressions".to_string(),
                ));
            }
            AstExpression::Operator(op) => {
                match op {
                    Operator::InSubquery { .. } | Operator::Exists { .. } => {
                        return Err(Error::ParseError(
                            "Subqueries are not allowed in index expressions".to_string(),
                        ));
                    }
                    // Check other operators recursively
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
                        Self::validate_no_subqueries_in_expression(l)?;
                        Self::validate_no_subqueries_in_expression(r)?;
                    }
                    Operator::Not(e)
                    | Operator::Negate(e)
                    | Operator::Identity(e)
                    | Operator::Factorial(e)
                    | Operator::Is(e, _) => {
                        Self::validate_no_subqueries_in_expression(e)?;
                    }
                    Operator::Between {
                        expr, low, high, ..
                    } => {
                        Self::validate_no_subqueries_in_expression(expr)?;
                        Self::validate_no_subqueries_in_expression(low)?;
                        Self::validate_no_subqueries_in_expression(high)?;
                    }
                    Operator::InList { expr, list, .. } => {
                        Self::validate_no_subqueries_in_expression(expr)?;
                        for item in list {
                            Self::validate_no_subqueries_in_expression(item)?;
                        }
                    }
                }
            }
            AstExpression::Function(_, args) => {
                for arg in args {
                    Self::validate_no_subqueries_in_expression(arg)?;
                }
            }
            AstExpression::ArrayAccess { base, index } => {
                Self::validate_no_subqueries_in_expression(base)?;
                Self::validate_no_subqueries_in_expression(index)?;
            }
            AstExpression::FieldAccess { base, field: _ } => {
                Self::validate_no_subqueries_in_expression(base)?;
            }
            AstExpression::ArrayLiteral(elements) => {
                for elem in elements {
                    Self::validate_no_subqueries_in_expression(elem)?;
                }
            }
            AstExpression::MapLiteral(pairs) => {
                for (k, v) in pairs {
                    Self::validate_no_subqueries_in_expression(k)?;
                    Self::validate_no_subqueries_in_expression(v)?;
                }
            }
            // Simple expressions are fine
            AstExpression::Literal(_)
            | AstExpression::Column(_, _)
            | AstExpression::Parameter(_)
            | AstExpression::All
            | AstExpression::QualifiedWildcard(_) => {}

            AstExpression::Case { .. } => {
                // CASE expressions are complex but shouldn't contain subqueries in index expressions
                // For now, reject them entirely in index expressions
                return Err(Error::ParseError(
                    "CASE expressions are not allowed in index expressions".to_string(),
                ));
            }
        }
        Ok(())
    }
}

/// Context that uses AnalyzedStatement for resolution
struct AnalyzedPlanContext<'a> {
    schemas: &'a HashMap<String, Table>,
    analyzed: &'a AnalyzedStatement,
    tables: Vec<TableRef>,
    current_column: usize,
}

struct TableRef {
    name: String,
    alias: Option<String>,
    start_column: usize,
}

impl<'a> AnalyzedPlanContext<'a> {
    fn new(schemas: &'a HashMap<String, Table>, analyzed: &'a AnalyzedStatement) -> Self {
        Self {
            schemas,
            analyzed,
            tables: Vec::new(),
            current_column: 0,
        }
    }

    fn add_table(&mut self, name: String, alias: Option<String>) -> Result<()> {
        // Table existence already validated by semantic analyzer
        let schema = self
            .schemas
            .get(&name)
            .expect("Table should exist after semantic analysis");

        let table_ref = TableRef {
            name: name.clone(),
            alias,
            start_column: self.current_column,
        };

        self.current_column += schema.columns.len();
        self.tables.push(table_ref);

        Ok(())
    }

    /// Resolve expression using type annotations when possible
    fn resolve_expression(&self, expr: &AstExpression) -> Result<Expression> {
        // Try to use metadata-enhanced resolution
        self.resolve_expression_with_metadata(expr)
    }

    /// Resolve expression with metadata support
    fn resolve_expression_with_metadata(&self, expr: &AstExpression) -> Result<Expression> {
        // TODO: In a future optimization, we could use expression templates here
        // to identify expressions that should be computed once and cached
        // For now, we proceed with normal resolution

        match expr {
            AstExpression::Column(table_ref, column_name) => {
                // Use the optimized column resolution map (O(1) lookup)
                if let Some(resolution) = self
                    .analyzed
                    .column_resolution_map
                    .resolve(table_ref.as_deref(), column_name)
                {
                    return Ok(Expression::Column(resolution.global_offset));
                }

                // Handle struct field access (DuckDB-style resolution)
                self.resolve_column_or_struct_field(table_ref, column_name)
            }
            AstExpression::Parameter(idx) => {
                // Check if we have parameter slot information for better type safety
                if let Some(param_slot) = self.analyzed.parameter_slots.get(*idx) {
                    // We have rich parameter information available
                    // This could be used for validation at bind time
                    // For example, checking expected type vs provided type
                    if param_slot.actual_type.is_some() {
                        // Type is already known from semantic analysis
                    }
                }
                Ok(Expression::Parameter(*idx))
            }
            _ => self.resolve_expression_simple(expr),
        }
    }

    /// Simple expression resolution (temporary)
    fn resolve_expression_simple(&self, expr: &AstExpression) -> Result<Expression> {
        match expr {
            AstExpression::Literal(lit) => {
                let value = match lit {
                    Literal::Null => crate::types::Value::Null,
                    Literal::Boolean(b) => crate::types::Value::boolean(*b),
                    Literal::Integer(i) => {
                        if *i >= i32::MIN as i128 && *i <= i32::MAX as i128 {
                            crate::types::Value::I32(*i as i32)
                        } else if *i >= i64::MIN as i128 && *i <= i64::MAX as i128 {
                            crate::types::Value::I64(*i as i64)
                        } else {
                            crate::types::Value::I128(*i)
                        }
                    }
                    Literal::Float(f) => crate::types::Value::F64(*f),
                    Literal::String(s) => crate::types::Value::string(s.clone()),
                    Literal::Bytea(b) => crate::types::Value::Bytea(b.clone()),
                    Literal::Date(d) => crate::types::Value::Date(*d),
                    Literal::Time(t) => crate::types::Value::Time(*t),
                    Literal::Timestamp(ts) => crate::types::Value::Timestamp(*ts),
                    Literal::Interval(i) => crate::types::Value::Interval(i.clone()),
                };
                Ok(Expression::Constant(value))
            }

            AstExpression::Column(table_ref, column_name) => {
                self.resolve_column_or_struct_field(table_ref, column_name)
            }

            AstExpression::Parameter(idx) => {
                // Parameter slots are checked in resolve_expression_with_metadata
                Ok(Expression::Parameter(*idx))
            }

            AstExpression::Function(name, args) => {
                let resolved_args = args
                    .iter()
                    .map(|a| self.resolve_expression_simple(a))
                    .collect::<Result<Vec<_>>>()?;
                Ok(Expression::Function(name.clone(), resolved_args))
            }

            AstExpression::Operator(op) => self.resolve_operator(op),

            AstExpression::All => Ok(Expression::All),
            AstExpression::QualifiedWildcard(_) => {
                // QualifiedWildcard is expanded during projection planning
                Err(Error::ExecutionError(
                    "Qualified wildcard not supported in this context".into(),
                ))
            }

            AstExpression::ArrayAccess { base, index } => {
                let base_expr = self.resolve_expression_simple(base)?;
                let index_expr = self.resolve_expression_simple(index)?;
                Ok(Expression::ArrayAccess(
                    Box::new(base_expr),
                    Box::new(index_expr),
                ))
            }

            AstExpression::FieldAccess { base, field } => {
                let base_expr = self.resolve_expression_simple(base)?;
                Ok(Expression::FieldAccess(Box::new(base_expr), field.clone()))
            }

            AstExpression::ArrayLiteral(elements) => {
                let resolved_elements = elements
                    .iter()
                    .map(|e| self.resolve_expression_simple(e))
                    .collect::<Result<Vec<_>>>()?;
                Ok(Expression::ArrayLiteral(resolved_elements))
            }

            AstExpression::MapLiteral(entries) => {
                let resolved_entries = entries
                    .iter()
                    .map(|(k, v)| {
                        Ok((
                            self.resolve_expression_simple(k)?,
                            self.resolve_expression_simple(v)?,
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(Expression::MapLiteral(resolved_entries))
            }

            AstExpression::Subquery(select) => {
                // Plan the subquery and store it in the expression
                let subquery_planner = Planner::new(self.schemas.clone(), HashMap::new());

                // Create a minimal analyzed statement for the subquery
                let analyzer = SemanticAnalyzer::new(self.schemas.clone());
                let subquery_stmt =
                    Statement::Dml(DmlStatement::Select(Box::new(select.as_ref().clone())));
                let subquery_analyzed = analyzer.analyze(subquery_stmt, Vec::new())?;

                let subquery_plan = subquery_planner.plan_select(select, &subquery_analyzed)?;

                Ok(Expression::Subquery(Box::new(subquery_plan)))
            }

            AstExpression::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                // Resolve operand (if present)
                let resolved_operand = operand
                    .as_ref()
                    .map(|op| self.resolve_expression_simple(op))
                    .transpose()?
                    .map(Box::new);

                // Resolve when/then clauses
                let resolved_when_clauses = when_clauses
                    .iter()
                    .map(|(when, then)| {
                        Ok((
                            self.resolve_expression_simple(when)?,
                            self.resolve_expression_simple(then)?,
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;

                // Resolve else clause (if present)
                let resolved_else = else_clause
                    .as_ref()
                    .map(|e| self.resolve_expression_simple(e))
                    .transpose()?
                    .map(Box::new);

                Ok(Expression::Case {
                    operand: resolved_operand,
                    when_clauses: resolved_when_clauses,
                    else_clause: resolved_else,
                })
            }
        }
    }

    /// Resolve a column reference, handling struct field access
    fn resolve_column_or_struct_field(
        &self,
        table_ref: &Option<String>,
        column_name: &str,
    ) -> Result<Expression> {
        // First try the resolution map (O(1) lookup)
        if let Some(resolution) = self
            .analyzed
            .column_resolution_map
            .resolve(table_ref.as_deref(), column_name)
        {
            return Ok(Expression::Column(resolution.global_offset));
        }

        // If not found, handle struct field access
        let table = if let Some(tref) = table_ref {
            self.tables
                .iter()
                .find(|t| &t.name == tref || t.alias.as_ref() == Some(tref))
        } else if self.tables.len() == 1 {
            self.tables.first()
        } else {
            return Err(Error::ColumnNotFound(column_name.to_string()));
        };

        if let Some(table) = table {
            // This shouldn't happen if semantic analysis was successful
            self.resolve_column_in_table(table, column_name)
        } else {
            // Check if it's a struct column (DuckDB-style resolution)
            let struct_col_name = table_ref.clone().unwrap_or_default();

            for table in &self.tables {
                if let Some(schema) = self.schemas.get(&table.name)
                    && let Some(col) = schema.columns.iter().find(|c| c.name == struct_col_name)
                {
                    // Check if it's a struct type
                    if let crate::types::data_type::DataType::Struct(fields) = &col.data_type {
                        // Verify the field exists
                        if fields.iter().any(|(name, _)| name == column_name) {
                            let base_expr =
                                self.resolve_column_in_table(table, &struct_col_name)?;
                            return Ok(Expression::FieldAccess(
                                Box::new(base_expr),
                                column_name.to_string(),
                            ));
                        } else {
                            return Err(Error::ExecutionError(format!(
                                "Field '{}' not found in struct '{}'",
                                column_name, struct_col_name
                            )));
                        }
                    }
                }
            }

            Err(Error::TableNotFound(table_ref.clone().unwrap_or_default()))
        }
    }

    fn resolve_column_in_table(&self, table: &TableRef, column_name: &str) -> Result<Expression> {
        let schema = self.schemas.get(&table.name).expect("Table should exist");

        let col_index = schema
            .columns
            .iter()
            .position(|c| c.name == column_name)
            .ok_or_else(|| Error::ColumnNotFound(column_name.to_string()))?;

        Ok(Expression::Column(table.start_column + col_index))
    }

    fn resolve_operator(&self, op: &Operator) -> Result<Expression> {
        use Operator::*;

        Ok(match op {
            And(l, r) => Expression::And(
                Box::new(self.resolve_expression_simple(l)?),
                Box::new(self.resolve_expression_simple(r)?),
            ),
            Or(l, r) => Expression::Or(
                Box::new(self.resolve_expression_simple(l)?),
                Box::new(self.resolve_expression_simple(r)?),
            ),
            Xor(l, r) => Expression::Xor(
                Box::new(self.resolve_expression_simple(l)?),
                Box::new(self.resolve_expression_simple(r)?),
            ),
            Not(e) => Expression::Not(Box::new(self.resolve_expression_simple(e)?)),
            Equal(l, r) => Expression::Equal(
                Box::new(self.resolve_expression_simple(l)?),
                Box::new(self.resolve_expression_simple(r)?),
            ),
            NotEqual(l, r) => Expression::NotEqual(
                Box::new(self.resolve_expression_simple(l)?),
                Box::new(self.resolve_expression_simple(r)?),
            ),
            GreaterThan(l, r) => Expression::GreaterThan(
                Box::new(self.resolve_expression_simple(l)?),
                Box::new(self.resolve_expression_simple(r)?),
            ),
            GreaterThanOrEqual(l, r) => Expression::GreaterThanOrEqual(
                Box::new(self.resolve_expression_simple(l)?),
                Box::new(self.resolve_expression_simple(r)?),
            ),
            LessThan(l, r) => Expression::LessThan(
                Box::new(self.resolve_expression_simple(l)?),
                Box::new(self.resolve_expression_simple(r)?),
            ),
            LessThanOrEqual(l, r) => Expression::LessThanOrEqual(
                Box::new(self.resolve_expression_simple(l)?),
                Box::new(self.resolve_expression_simple(r)?),
            ),
            Is(e, lit) => {
                let value = match lit {
                    Literal::Null => crate::types::Value::Null,
                    _ => return Err(Error::ExecutionError("IS only supports NULL".into())),
                };
                Expression::Is(Box::new(self.resolve_expression_simple(e)?), value)
            }
            Add(l, r) => Expression::Add(
                Box::new(self.resolve_expression_simple(l)?),
                Box::new(self.resolve_expression_simple(r)?),
            ),
            Concat(l, r) => Expression::Concat(
                Box::new(self.resolve_expression_simple(l)?),
                Box::new(self.resolve_expression_simple(r)?),
            ),
            Subtract(l, r) => Expression::Subtract(
                Box::new(self.resolve_expression_simple(l)?),
                Box::new(self.resolve_expression_simple(r)?),
            ),
            Multiply(l, r) => Expression::Multiply(
                Box::new(self.resolve_expression_simple(l)?),
                Box::new(self.resolve_expression_simple(r)?),
            ),
            Divide(l, r) => Expression::Divide(
                Box::new(self.resolve_expression_simple(l)?),
                Box::new(self.resolve_expression_simple(r)?),
            ),
            Remainder(l, r) => Expression::Remainder(
                Box::new(self.resolve_expression_simple(l)?),
                Box::new(self.resolve_expression_simple(r)?),
            ),
            Exponentiate(l, r) => Expression::Exponentiate(
                Box::new(self.resolve_expression_simple(l)?),
                Box::new(self.resolve_expression_simple(r)?),
            ),
            // Unary operators
            Negate(e) => Expression::Negate(Box::new(self.resolve_expression_simple(e)?)),
            Identity(e) => Expression::Identity(Box::new(self.resolve_expression_simple(e)?)),
            Factorial(e) => Expression::Factorial(Box::new(self.resolve_expression_simple(e)?)),
            // String matching
            ILike(l, r) => Expression::ILike(
                Box::new(self.resolve_expression_simple(l)?),
                Box::new(self.resolve_expression_simple(r)?),
            ),
            Like(l, r) => Expression::Like(
                Box::new(self.resolve_expression_simple(l)?),
                Box::new(self.resolve_expression_simple(r)?),
            ),
            // Range operators
            Between {
                expr,
                low,
                high,
                negated,
            } => Expression::Between(
                Box::new(self.resolve_expression_simple(expr)?),
                Box::new(self.resolve_expression_simple(low)?),
                Box::new(self.resolve_expression_simple(high)?),
                *negated,
            ),
            // IN operator
            InList {
                expr,
                list,
                negated,
            } => {
                let resolved_list = list
                    .iter()
                    .map(|e| self.resolve_expression_simple(e))
                    .collect::<Result<Vec<_>>>()?;
                Expression::InList(
                    Box::new(self.resolve_expression_simple(expr)?),
                    resolved_list,
                    *negated,
                )
            }
            // IN subquery operator
            InSubquery {
                expr,
                subquery,
                negated,
            } => {
                if let AstExpression::Subquery(select) = subquery.as_ref() {
                    // Plan the subquery
                    let subquery_planner = Planner::new(self.schemas.clone(), HashMap::new());

                    // Create a minimal analyzed statement for the subquery
                    let analyzer = SemanticAnalyzer::new(self.schemas.clone());
                    let subquery_stmt =
                        Statement::Dml(DmlStatement::Select(Box::new(select.as_ref().clone())));
                    let subquery_analyzed = analyzer.analyze(subquery_stmt, Vec::new())?;

                    let subquery_plan = subquery_planner.plan_select(select, &subquery_analyzed)?;

                    Expression::InSubquery(
                        Box::new(self.resolve_expression_simple(expr)?),
                        Box::new(subquery_plan),
                        *negated,
                    )
                } else {
                    return Err(Error::ExecutionError(
                        "Invalid subquery in IN clause".to_string(),
                    ));
                }
            }
            // EXISTS operator
            Exists { subquery, negated } => {
                if let AstExpression::Subquery(select) = subquery.as_ref() {
                    // Plan the subquery
                    let subquery_planner = Planner::new(self.schemas.clone(), HashMap::new());

                    // Create a minimal analyzed statement for the subquery
                    let analyzer = SemanticAnalyzer::new(self.schemas.clone());
                    let subquery_stmt =
                        Statement::Dml(DmlStatement::Select(Box::new(select.as_ref().clone())));
                    let subquery_analyzed = analyzer.analyze(subquery_stmt, Vec::new())?;

                    let subquery_plan = subquery_planner.plan_select(select, &subquery_analyzed)?;

                    Expression::Exists(Box::new(subquery_plan), *negated)
                } else {
                    return Err(Error::ExecutionError(
                        "Invalid subquery in EXISTS clause".to_string(),
                    ));
                }
            }
        })
    }
}

/// Resolves a DEFAULT expression (which shouldn't have column references)
fn resolve_default_expression(
    expr: &AstExpression,
) -> Result<crate::types::expression::DefaultExpression> {
    use crate::parsing::ast::Literal;
    use crate::types::expression::DefaultExpression;

    match expr {
        AstExpression::Literal(lit) => {
            let value = match lit {
                Literal::Null => crate::types::Value::Null,
                Literal::Boolean(b) => crate::types::Value::Bool(*b),
                Literal::Integer(n) => {
                    if *n >= i32::MIN as i128 && *n <= i32::MAX as i128 {
                        crate::types::Value::I32(*n as i32)
                    } else if *n >= i64::MIN as i128 && *n <= i64::MAX as i128 {
                        crate::types::Value::I64(*n as i64)
                    } else {
                        crate::types::Value::I128(*n)
                    }
                }
                Literal::Float(f) => crate::types::Value::F64(*f),
                Literal::String(s) => crate::types::Value::Str(s.clone()),
                Literal::Bytea(b) => crate::types::Value::Bytea(b.clone()),
                Literal::Date(d) => crate::types::Value::Date(*d),
                Literal::Time(t) => crate::types::Value::Time(*t),
                Literal::Timestamp(ts) => crate::types::Value::Timestamp(*ts),
                Literal::Interval(i) => crate::types::Value::Interval(i.clone()),
            };
            Ok(DefaultExpression::Constant(value))
        }

        AstExpression::Function(name, args) => {
            let resolved_args = args
                .iter()
                .map(resolve_default_expression)
                .collect::<Result<Vec<_>>>()?;
            Ok(DefaultExpression::Function(name.clone(), resolved_args))
        }

        AstExpression::Operator(op) => {
            use crate::parsing::Operator::*;

            Ok(match op {
                Add(l, r) => DefaultExpression::Add(
                    Box::new(resolve_default_expression(l)?),
                    Box::new(resolve_default_expression(r)?),
                ),
                Concat(l, r) => DefaultExpression::Concat(
                    Box::new(resolve_default_expression(l)?),
                    Box::new(resolve_default_expression(r)?),
                ),
                Subtract(l, r) => DefaultExpression::Subtract(
                    Box::new(resolve_default_expression(l)?),
                    Box::new(resolve_default_expression(r)?),
                ),
                Multiply(l, r) => DefaultExpression::Multiply(
                    Box::new(resolve_default_expression(l)?),
                    Box::new(resolve_default_expression(r)?),
                ),
                Divide(l, r) => DefaultExpression::Divide(
                    Box::new(resolve_default_expression(l)?),
                    Box::new(resolve_default_expression(r)?),
                ),
                Remainder(l, r) => DefaultExpression::Remainder(
                    Box::new(resolve_default_expression(l)?),
                    Box::new(resolve_default_expression(r)?),
                ),
                Exponentiate(l, r) => DefaultExpression::Exponentiate(
                    Box::new(resolve_default_expression(l)?),
                    Box::new(resolve_default_expression(r)?),
                ),
                Negate(e) => DefaultExpression::Negate(Box::new(resolve_default_expression(e)?)),
                Identity(e) => {
                    DefaultExpression::Identity(Box::new(resolve_default_expression(e)?))
                }
                // Boolean operators
                And(l, r) => DefaultExpression::And(
                    Box::new(resolve_default_expression(l)?),
                    Box::new(resolve_default_expression(r)?),
                ),
                Or(l, r) => DefaultExpression::Or(
                    Box::new(resolve_default_expression(l)?),
                    Box::new(resolve_default_expression(r)?),
                ),
                Xor(l, r) => DefaultExpression::Xor(
                    Box::new(resolve_default_expression(l)?),
                    Box::new(resolve_default_expression(r)?),
                ),
                Not(e) => DefaultExpression::Not(Box::new(resolve_default_expression(e)?)),

                // Comparison operators
                Equal(l, r) => DefaultExpression::Equal(
                    Box::new(resolve_default_expression(l)?),
                    Box::new(resolve_default_expression(r)?),
                ),
                GreaterThan(l, r) => DefaultExpression::GreaterThan(
                    Box::new(resolve_default_expression(l)?),
                    Box::new(resolve_default_expression(r)?),
                ),
                LessThan(l, r) => DefaultExpression::LessThan(
                    Box::new(resolve_default_expression(l)?),
                    Box::new(resolve_default_expression(r)?),
                ),
                GreaterThanOrEqual(l, r) => DefaultExpression::GreaterThanOrEqual(
                    Box::new(resolve_default_expression(l)?),
                    Box::new(resolve_default_expression(r)?),
                ),
                LessThanOrEqual(l, r) => DefaultExpression::LessThanOrEqual(
                    Box::new(resolve_default_expression(l)?),
                    Box::new(resolve_default_expression(r)?),
                ),
                NotEqual(l, r) => DefaultExpression::NotEqual(
                    Box::new(resolve_default_expression(l)?),
                    Box::new(resolve_default_expression(r)?),
                ),

                // IS NULL
                Is(e, lit) => {
                    let value = match lit {
                        Literal::Null => crate::types::Value::Null,
                        _ => return Err(Error::ExecutionError("IS only supports NULL".into())),
                    };
                    DefaultExpression::Is(Box::new(resolve_default_expression(e)?), value)
                }

                // Pattern matching
                Like(l, r) => DefaultExpression::Like(
                    Box::new(resolve_default_expression(l)?),
                    Box::new(resolve_default_expression(r)?),
                ),
                ILike(l, r) => DefaultExpression::ILike(
                    Box::new(resolve_default_expression(l)?),
                    Box::new(resolve_default_expression(r)?),
                ),

                // Other operators
                Factorial(e) => {
                    DefaultExpression::Factorial(Box::new(resolve_default_expression(e)?))
                }

                // IN and BETWEEN
                InList {
                    expr,
                    list,
                    negated,
                } => {
                    let resolved_expr = Box::new(resolve_default_expression(expr)?);
                    let resolved_list = list
                        .iter()
                        .map(resolve_default_expression)
                        .collect::<Result<Vec<_>>>()?;
                    DefaultExpression::InList(resolved_expr, resolved_list, *negated)
                }
                Between {
                    expr,
                    low,
                    high,
                    negated,
                } => {
                    let resolved_expr = Box::new(resolve_default_expression(expr)?);
                    let resolved_low = Box::new(resolve_default_expression(low)?);
                    let resolved_high = Box::new(resolve_default_expression(high)?);
                    DefaultExpression::Between(resolved_expr, resolved_low, resolved_high, *negated)
                }

                // Subqueries are NOT allowed in DEFAULT
                InSubquery { .. } | Exists { .. } => {
                    return Err(Error::ExecutionError(
                        "Subqueries are not allowed in DEFAULT expressions".into(),
                    ));
                }
            })
        }

        AstExpression::ArrayLiteral(elements) => {
            let resolved_elements = elements
                .iter()
                .map(resolve_default_expression)
                .collect::<Result<Vec<_>>>()?;
            Ok(DefaultExpression::ArrayLiteral(resolved_elements))
        }

        AstExpression::MapLiteral(pairs) => {
            let resolved_pairs = pairs
                .iter()
                .map(|(k, v)| {
                    Ok((
                        resolve_default_expression(k)?,
                        resolve_default_expression(v)?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(DefaultExpression::MapLiteral(resolved_pairs))
        }

        _ => Err(Error::ExecutionError(
            "Expression type not supported in DEFAULT expressions".into(),
        )),
    }
}

/// Convert an expression to a string representation for alias generation
fn expr_to_string(expr: &AstExpression) -> String {
    use crate::parsing::Operator;
    use crate::parsing::ast::Literal;

    match expr {
        AstExpression::Column(table, col) => {
            if let Some(t) = table {
                format!("{}.{}", t, col)
            } else {
                col.clone()
            }
        }
        AstExpression::Literal(Literal::Integer(i)) => i.to_string(),
        AstExpression::Literal(Literal::Float(f)) => f.to_string(),
        AstExpression::Literal(Literal::String(s)) => format!("'{}'", s),
        AstExpression::Literal(Literal::Null) => "NULL".to_string(),
        AstExpression::Literal(Literal::Boolean(b)) => {
            if *b { "TRUE" } else { "FALSE" }.to_string()
        }
        AstExpression::Literal(Literal::Date(d)) => format!("'{}'", d),
        AstExpression::Literal(Literal::Time(t)) => format!("'{}'", t),
        AstExpression::Literal(Literal::Timestamp(ts)) => format!("'{}'", ts),
        AstExpression::All => "*".to_string(),
        AstExpression::QualifiedWildcard(table) => format!("{}.*", table),
        AstExpression::Function(name, args) => {
            let arg_str = args
                .iter()
                .map(expr_to_string)
                .collect::<Vec<_>>()
                .join(", ");
            format!("{}({})", name.to_uppercase(), arg_str)
        }
        AstExpression::Operator(op) => {
            match op {
                Operator::Add(l, r) => format!("{} + {}", expr_to_string(l), expr_to_string(r)),
                Operator::Subtract(l, r) => {
                    format!("{} - {}", expr_to_string(l), expr_to_string(r))
                }
                Operator::Multiply(l, r) => {
                    format!("{} * {}", expr_to_string(l), expr_to_string(r))
                }
                Operator::Divide(l, r) => format!("{} / {}", expr_to_string(l), expr_to_string(r)),
                Operator::Remainder(l, r) => {
                    format!("{} % {}", expr_to_string(l), expr_to_string(r))
                }
                Operator::Not(e) => format!("NOT {}", expr_to_string(e)),
                Operator::Negate(e) => format!("-{}", expr_to_string(e)),
                Operator::Identity(e) => format!("+{}", expr_to_string(e)),
                _ => "expr".to_string(), // For complex operators we don't need full string representation
            }
        }
        _ => "expr".to_string(), // For other complex expressions
    }
}
