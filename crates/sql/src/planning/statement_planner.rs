//! Top-level statement planner
//!
//! This module contains the main statement planning logic that dispatches
//! different types of statements (DDL/DML) to their respective planners.
//! It serves as the coordinator for the entire planning process.

use super::expression_resolver::AnalyzedPlanContext;
use super::select_planner::SelectPlanner;
use crate::error::{Error, Result};
use crate::parsing::ast::common::Direction as AstDirection;
use crate::parsing::ast::ddl::DdlStatement;
use crate::parsing::ast::dml::{DmlStatement, ValuesStatement};
use crate::parsing::ast::{
    Column, Expression as AstExpression, InsertSource, Literal, SelectStatement,
};
use crate::semantic::AnalyzedStatement;
use crate::types::DataType;
use crate::types::expression::Expression;
use crate::types::plan::{Node, Plan};
use crate::types::schema::Table;
use std::collections::BTreeMap;

use super::expression_resolver::resolve_default_expression;

/// Top-level statement planner that coordinates all planning operations
pub(super) struct StatementPlanner;

impl StatementPlanner {
    /// Plan DDL statements
    ///
    /// Dispatches different DDL statement types to their respective planning methods:
    /// - CREATE TABLE
    /// - CREATE TABLE AS VALUES
    /// - CREATE TABLE AS SELECT
    /// - DROP TABLE
    /// - CREATE INDEX
    /// - DROP INDEX
    /// - ALTER TABLE
    ///
    /// # Arguments
    /// * `planner` - The parent planner instance
    /// * `ddl` - The DDL statement to plan
    /// * `analyzed` - The semantic analysis results
    pub(super) fn plan_ddl(
        planner: &super::planner::Planner,
        ddl: &DdlStatement,
        analyzed: &AnalyzedStatement,
    ) -> Result<Plan> {
        match ddl {
            DdlStatement::CreateTable {
                name,
                columns,
                foreign_keys,
                if_not_exists,
            } => Self::plan_create_table(
                name.clone(),
                columns.clone(),
                foreign_keys.clone(),
                *if_not_exists,
            ),

            DdlStatement::CreateTableAsValues {
                name,
                values,
                if_not_exists,
            } => Self::plan_create_table_as_values(
                planner,
                name.clone(),
                values,
                *if_not_exists,
                analyzed,
            ),

            DdlStatement::CreateTableAsSelect {
                name,
                select,
                if_not_exists,
            } => Self::plan_create_table_as_select(
                planner,
                name.clone(),
                select,
                *if_not_exists,
                analyzed,
            ),

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
                    .map(|col| crate::types::plan::IndexColumn {
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

            DdlStatement::AlterTable { name, operation } => Ok(Plan::AlterTable {
                name: name.clone(),
                operation: operation.clone(),
            }),
        }
    }

    /// Plan DML statements
    ///
    /// Dispatches different DML statement types to their respective planning methods:
    /// - SELECT - delegates to SelectPlanner
    /// - INSERT
    /// - UPDATE
    /// - DELETE
    /// - VALUES
    ///
    /// # Arguments
    /// * `planner` - The parent planner instance
    /// * `dml` - The DML statement to plan
    /// * `analyzed` - The semantic analysis results
    pub(super) fn plan_dml(
        planner: &super::planner::Planner,
        dml: &DmlStatement,
        analyzed: &AnalyzedStatement,
    ) -> Result<Plan> {
        match dml {
            DmlStatement::Select(select_stmt) => {
                SelectPlanner::plan_select(planner, select_stmt, analyzed)
            }

            DmlStatement::Insert {
                table,
                columns,
                source,
            } => Self::plan_insert(
                planner,
                table.clone(),
                columns.clone(),
                source.clone(),
                analyzed,
            ),

            DmlStatement::Update {
                table,
                set,
                r#where,
            } => Self::plan_update(
                planner,
                table.clone(),
                set.clone(),
                r#where.clone(),
                analyzed,
            ),

            DmlStatement::Delete { table, r#where } => {
                Self::plan_delete(planner, table.clone(), r#where.clone(), analyzed)
            }

            DmlStatement::Values(values_stmt) => Self::plan_values(planner, values_stmt, analyzed),
        }
    }

    /// Plan an INSERT statement
    ///
    /// Handles INSERT with different source types:
    /// - INSERT ... VALUES
    /// - INSERT ... DEFAULT VALUES
    /// - INSERT ... SELECT
    ///
    /// # Arguments
    /// * `planner` - The parent planner instance
    /// * `table` - The target table name
    /// * `columns` - Optional list of column names to insert into
    /// * `source` - The source of data (VALUES, DEFAULT VALUES, or SELECT)
    /// * `analyzed` - The semantic analysis results
    fn plan_insert(
        planner: &super::planner::Planner,
        table: String,
        columns: Option<Vec<String>>,
        source: InsertSource,
        analyzed: &AnalyzedStatement,
    ) -> Result<Plan> {
        let schema = planner
            .get_schemas()
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

        // Build source node
        let source_node = match source {
            InsertSource::Values(values) => {
                let context = AnalyzedPlanContext::new(
                    planner.get_schemas(),
                    planner.get_index_metadata(),
                    analyzed,
                );
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
                // Analyze the SELECT statement separately to detect ambiguous columns
                // and other semantic issues
                use crate::parsing::ast::Statement;
                use crate::parsing::ast::dml::DmlStatement;

                let select_stmt = Statement::Dml(DmlStatement::Select(select.clone()));
                let analyzer = crate::semantic::analyzer::SemanticAnalyzer::new(
                    planner.get_schemas().clone(),
                    planner.get_index_metadata().clone(),
                );
                let select_analyzed = analyzer.analyze(select_stmt, Vec::new())?;

                // Now plan the SELECT statement with its own analysis
                let plan = SelectPlanner::plan_select(planner, &select, &select_analyzed)?;
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

    /// Plan an UPDATE statement
    ///
    /// Creates an execution plan that:
    /// 1. Scans the target table
    /// 2. Applies WHERE filter (if present)
    /// 3. Updates specified columns
    ///
    /// # Arguments
    /// * `planner` - The parent planner instance
    /// * `table` - The target table name
    /// * `set` - Map of column names to new values
    /// * `where_clause` - Optional WHERE filter
    /// * `analyzed` - The semantic analysis results
    fn plan_update(
        planner: &super::planner::Planner,
        table: String,
        set: BTreeMap<String, Option<AstExpression>>,
        where_clause: Option<AstExpression>,
        analyzed: &AnalyzedStatement,
    ) -> Result<Plan> {
        let mut context = AnalyzedPlanContext::new(
            planner.get_schemas(),
            planner.get_index_metadata(),
            analyzed,
        );
        context.add_table(table.clone(), None)?;

        let mut node = Node::Scan {
            table: table.clone(),
            alias: None,
        };

        if let Some(ref where_expr) = where_clause {
            let predicate = context.resolve_expression(where_expr)?;
            node = Node::Filter {
                source: Box::new(node),
                predicate,
            };
        }

        let schema = planner
            .get_schemas()
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

    /// Plan a DELETE statement
    ///
    /// Creates an execution plan that:
    /// 1. Scans the target table
    /// 2. Applies WHERE filter (if present)
    /// 3. Deletes matching rows
    ///
    /// # Arguments
    /// * `planner` - The parent planner instance
    /// * `table` - The target table name
    /// * `where_clause` - Optional WHERE filter
    /// * `analyzed` - The semantic analysis results
    fn plan_delete(
        planner: &super::planner::Planner,
        table: String,
        where_clause: Option<AstExpression>,
        analyzed: &AnalyzedStatement,
    ) -> Result<Plan> {
        let mut context = AnalyzedPlanContext::new(
            planner.get_schemas(),
            planner.get_index_metadata(),
            analyzed,
        );
        context.add_table(table.clone(), None)?;

        let mut node = Node::Scan {
            table: table.clone(),
            alias: None,
        };

        if let Some(ref where_expr) = where_clause {
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
    ///
    /// Creates an execution plan for a standalone VALUES statement,
    /// including ORDER BY, LIMIT, and OFFSET if present.
    ///
    /// # Arguments
    /// * `planner` - The parent planner instance
    /// * `values_stmt` - The VALUES statement
    /// * `analyzed` - The semantic analysis results
    fn plan_values(
        planner: &super::planner::Planner,
        values_stmt: &ValuesStatement,
        analyzed: &AnalyzedStatement,
    ) -> Result<Plan> {
        let context = AnalyzedPlanContext::new(
            planner.get_schemas(),
            planner.get_index_metadata(),
            analyzed,
        );

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
                            AstDirection::Asc => crate::types::plan::Direction::Ascending,
                            AstDirection::Desc => crate::types::plan::Direction::Descending,
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

    /// Plan CREATE TABLE statement
    ///
    /// Creates a table schema from column definitions and validates constraints.
    ///
    /// # Arguments
    /// * `planner` - The parent planner instance
    /// * `name` - The table name
    /// * `columns` - Column definitions
    /// * `foreign_keys` - Foreign key constraints
    /// * `if_not_exists` - Whether to skip if table exists
    fn plan_create_table(
        name: String,
        columns: Vec<Column>,
        foreign_keys: Vec<crate::parsing::ast::ddl::ForeignKeyConstraint>,
        if_not_exists: bool,
    ) -> Result<Plan> {
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

    /// Plan CREATE TABLE AS VALUES statement
    ///
    /// Infers column types from the VALUES data and creates a table schema.
    ///
    /// # Arguments
    /// * `planner` - The parent planner instance
    /// * `name` - The table name
    /// * `values` - The VALUES statement
    /// * `if_not_exists` - Whether to skip if table exists
    /// * `analyzed` - The semantic analysis results
    fn plan_create_table_as_values(
        planner: &super::planner::Planner,
        name: String,
        values: &ValuesStatement,
        if_not_exists: bool,
        analyzed: &AnalyzedStatement,
    ) -> Result<Plan> {
        // First, plan the VALUES statement to determine types
        let values_plan = Self::plan_values(planner, values, analyzed)?;

        // Extract the data types from the VALUES plan
        let column_types = match &values_plan {
            Plan::Query { root, .. } => match &**root {
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
                            let expr_type = Self::infer_expression_type(expr)?;
                            // Update column type if it's more specific than what we have
                            if column_types[i] == DataType::Null && expr_type != DataType::Null {
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
            },
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

    /// Plan CREATE TABLE AS SELECT statement
    ///
    /// Infers column types and names from the SELECT statement.
    ///
    /// # Arguments
    /// * `planner` - The parent planner instance
    /// * `name` - The table name
    /// * `select` - The SELECT statement
    /// * `if_not_exists` - Whether to skip if table exists
    /// * `analyzed` - The semantic analysis results (for the CREATE TABLE AS, not the SELECT)
    fn plan_create_table_as_select(
        planner: &super::planner::Planner,
        name: String,
        select: &SelectStatement,
        if_not_exists: bool,
        _analyzed: &AnalyzedStatement,
    ) -> Result<Plan> {
        // Need to analyze the SELECT statement separately since the provided
        // analyzed statement is for the CREATE TABLE AS, not the SELECT
        use crate::parsing::ast::Statement;
        use crate::parsing::ast::dml::DmlStatement;

        let select_stmt = Statement::Dml(DmlStatement::Select(Box::new(select.clone())));
        let analyzer = crate::semantic::analyzer::SemanticAnalyzer::new(
            planner.get_schemas().clone(),
            planner.get_index_metadata().clone(),
        );
        let select_analyzed = analyzer.analyze(select_stmt, Vec::new())?;

        // Now plan the SELECT statement with its own analysis
        let select_plan = SelectPlanner::plan_select(planner, select, &select_analyzed)?;

        // Extract column names and types from the SELECT plan
        let (column_names, column_types) = match &select_plan {
            Plan::Query {
                column_names, root, ..
            } => {
                // Get column names - either from override or from node
                let col_names = if let Some(names) = column_names {
                    if names.is_empty() {
                        // Empty override, get names from the plan tree instead
                        root.get_column_names(planner.get_schemas())
                    } else {
                        names.clone()
                    }
                } else {
                    // No override, get names from the plan tree
                    root.get_column_names(planner.get_schemas())
                };

                // Infer types from the projection expressions
                let types = Self::infer_select_column_types(planner, root)?;
                (col_names, types)
            }
            _ => return Err(Error::ExecutionError("SELECT plan must be a Query".into())),
        };

        // Create columns using names from SELECT
        let schema_columns = column_names
            .iter()
            .zip(column_types.iter())
            .map(|(col_name, dtype)| {
                crate::types::schema::Column::new(col_name.clone(), dtype.clone()).nullable(true)
            })
            .collect();

        let table = Table::new(name.clone(), schema_columns)?;

        Ok(Plan::CreateTableAsSelect {
            name,
            schema: table,
            select_plan: Box::new(select_plan),
            if_not_exists,
        })
    }

    /// Infer column types from a SELECT's projection node
    ///
    /// Traverses the plan tree to find the projection and infers types
    /// for each projected expression.
    fn infer_select_column_types(
        planner: &super::planner::Planner,
        node: &Node,
    ) -> Result<Vec<DataType>> {
        match node {
            Node::Projection {
                expressions,
                source,
                ..
            } => {
                // For columns from the source, look up types from schemas or SeriesScan
                let types: Vec<DataType> = expressions
                    .iter()
                    .map(|expr| match expr {
                        Expression::Column(offset) => {
                            // Try to get type from the source
                            Self::infer_column_type_from_source(planner, source, *offset)
                                .unwrap_or(DataType::Str)
                        }
                        _ => Self::infer_expression_type(expr).unwrap_or(DataType::Str),
                    })
                    .collect();
                Ok(types)
            }
            // For nodes that pass through columns unchanged, recursively traverse to find the Projection
            Node::Filter { source, .. }
            | Node::Limit { source, .. }
            | Node::Offset { source, .. }
            | Node::Order { source, .. } => Self::infer_select_column_types(planner, source),
            _ => {
                // For non-projection nodes that don't pass through columns
                Err(Error::ExecutionError(
                    "Cannot infer column types from non-projection node".into(),
                ))
            }
        }
    }

    /// Infer the data type of a column from the source node
    ///
    /// Recursively traverses the plan tree to find the origin of a column
    /// and determine its type.
    fn infer_column_type_from_source(
        planner: &super::planner::Planner,
        node: &Node,
        offset: usize,
    ) -> Option<DataType> {
        match node {
            Node::SeriesScan { .. } => {
                // SERIES produces I64 column
                if offset == 0 {
                    Some(DataType::I64)
                } else {
                    None
                }
            }
            Node::Scan { table, .. } | Node::IndexScan { table, .. } => {
                // Get type from table schema
                planner
                    .get_schemas()
                    .get(table)
                    .and_then(|schema| schema.columns.get(offset).map(|col| col.data_type.clone()))
            }
            // Recursively traverse through nodes that pass columns unchanged
            Node::Filter { source, .. }
            | Node::Limit { source, .. }
            | Node::Offset { source, .. }
            | Node::Order { source, .. } => {
                Self::infer_column_type_from_source(planner, source, offset)
            }
            _ => None,
        }
    }

    /// Infer the data type of an expression
    ///
    /// Determines the result type of an expression based on its structure
    /// and the types of its sub-expressions.
    fn infer_expression_type(expr: &Expression) -> Result<DataType> {
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

    /// Evaluate a constant expression to a usize value
    ///
    /// Used for LIMIT and OFFSET clauses which must be non-negative integers.
    pub(super) fn eval_constant(expr: &AstExpression) -> Result<usize> {
        match expr {
            AstExpression::Literal(Literal::Integer(n)) if *n >= 0 && *n <= usize::MAX as i128 => {
                Ok(*n as usize)
            }
            _ => Err(Error::ExecutionError(
                "Expected non-negative integer constant".into(),
            )),
        }
    }

    /// Validate that an expression doesn't contain subqueries
    ///
    /// Used to validate index expressions, which cannot contain subqueries.
    /// Recursively checks all sub-expressions.
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
                    | Operator::BitwiseAnd(l, r)
                    | Operator::BitwiseOr(l, r)
                    | Operator::BitwiseXor(l, r)
                    | Operator::BitwiseShiftLeft(l, r)
                    | Operator::BitwiseShiftRight(l, r) => {
                        Self::validate_no_subqueries_in_expression(l)?;
                        Self::validate_no_subqueries_in_expression(r)?;
                    }
                    Operator::ILike { expr, pattern, .. }
                    | Operator::Like { expr, pattern, .. } => {
                        Self::validate_no_subqueries_in_expression(expr)?;
                        Self::validate_no_subqueries_in_expression(pattern)?;
                    }
                    Operator::Not(e)
                    | Operator::Negate(e)
                    | Operator::Identity(e)
                    | Operator::Factorial(e)
                    | Operator::BitwiseNot(e)
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

            AstExpression::Cast { expr, .. } => {
                // Validate the inner expression
                Self::validate_no_subqueries_in_expression(expr)?;
            }
        }
        Ok(())
    }
}
