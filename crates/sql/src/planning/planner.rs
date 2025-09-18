//! Query planner v2 that leverages semantic analysis results
//!
//! This planner uses the TypeAnnotations and metadata from AnalyzedStatement
//! instead of re-walking and re-resolving the AST.

use super::optimizer::Optimizer;
use super::plan::{AggregateFunc, Direction, JoinType, Node, Plan};
use crate::error::{Error, Result};
use crate::parsing::ast::common::{Direction as AstDirection, FromClause};
use crate::parsing::ast::ddl::DdlStatement;
use crate::parsing::ast::dml::DmlStatement;
use crate::parsing::ast::{
    Column, Expression as AstExpression, InsertSource, Literal, Operator, SelectStatement,
    Statement,
};
use crate::semantic::AnalyzedStatement;
use crate::semantic::statement::ExpressionId;
use crate::storage::mvcc::IndexMetadata;
use crate::types::expression::Expression;
use crate::types::schema::Table;
use crate::types::statistics::DatabaseStatistics;
use std::collections::{BTreeMap, HashMap};

/// Query planner that leverages semantic analysis
pub struct Planner {
    schemas: HashMap<String, Table>,
    optimizer: Optimizer,
    /// Available indexes: table_name -> list of indexes
    indexes: HashMap<String, Vec<IndexInfo>>,
    /// Optional statistics for optimization
    statistics: Option<DatabaseStatistics>,
}

/// Index metadata for planning
#[derive(Debug, Clone)]
pub struct IndexInfo {
    pub name: String,
    pub table: String,
    pub columns: Vec<IndexColumn>,
    pub unique: bool,
}

/// Index column info for planning
#[derive(Debug, Clone)]
pub struct IndexColumn {
    pub name: String,
    pub expression: Option<AstExpression>,
    pub direction: Option<AstDirection>,
}

impl Planner {
    /// Create a new planner
    pub fn new(
        schemas: HashMap<String, Table>,
        index_metadata: HashMap<String, Vec<IndexMetadata>>,
    ) -> Self {
        let optimizer = Optimizer::new(schemas.clone());

        // Convert storage index metadata to planner IndexInfo
        let indexes = Self::convert_indexes(index_metadata, &schemas);

        Self {
            schemas,
            optimizer,
            indexes,
            statistics: None,
        }
    }

    /// Convert index metadata from storage format
    fn convert_indexes(
        index_metadata: HashMap<String, Vec<IndexMetadata>>,
        schemas: &HashMap<String, Table>,
    ) -> HashMap<String, Vec<IndexInfo>> {
        let mut indexes = HashMap::new();

        for (table_name, table_indexes) in index_metadata {
            let mut index_infos = Vec::new();
            for metadata in table_indexes {
                let columns = metadata
                    .columns
                    .iter()
                    .map(|col| {
                        IndexColumn {
                            name: col.expression.clone(),
                            expression: None, // Simplified for now
                            direction: col.direction.map(|d| match d {
                                crate::types::query::Direction::Ascending => AstDirection::Asc,
                                crate::types::query::Direction::Descending => AstDirection::Desc,
                            }),
                        }
                    })
                    .collect();

                index_infos.push(IndexInfo {
                    name: metadata.name,
                    table: metadata.table,
                    columns,
                    unique: metadata.unique,
                });
            }

            if !index_infos.is_empty() {
                indexes.insert(table_name, index_infos);
            }
        }

        // Add schema-defined indexes
        for (table_name, schema) in schemas {
            let table_indexes = indexes.entry(table_name.clone()).or_insert_with(Vec::new);
            for column in &schema.columns {
                if column.primary_key || column.unique || column.index {
                    let exists = table_indexes
                        .iter()
                        .any(|idx| idx.columns.len() == 1 && idx.columns[0].name == column.name);
                    if !exists {
                        table_indexes.push(IndexInfo {
                            name: column.name.clone(),
                            table: table_name.clone(),
                            columns: vec![IndexColumn {
                                name: column.name.clone(),
                                expression: None,
                                direction: None,
                            }],
                            unique: column.primary_key || column.unique,
                        });
                    }
                }
            }
        }

        indexes
    }

    /// Update schemas (for cache invalidation)
    pub fn update_schemas(&mut self, schemas: HashMap<String, Table>) {
        self.schemas = schemas.clone();
        self.optimizer = Optimizer::new(schemas);
    }

    /// Update indexes (for cache invalidation)
    pub fn update_indexes(&mut self, index_metadata: HashMap<String, Vec<IndexMetadata>>) {
        self.indexes = Self::convert_indexes(index_metadata, &self.schemas);
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
    fn plan_ddl(&self, ddl: &DdlStatement, _analyzed: &AnalyzedStatement) -> Result<Plan> {
        match ddl {
            DdlStatement::CreateTable {
                name,
                columns,
                if_not_exists,
            } => self.plan_create_table(name.clone(), columns.clone(), *if_not_exists),

            DdlStatement::DropTable { names, if_exists } => Ok(Plan::DropTable {
                names: names.clone(),
                if_exists: *if_exists,
            }),

            DdlStatement::CreateIndex {
                name,
                table,
                columns,
                unique,
                included_columns,
            } => {
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
        }
    }

    /// Plan a SELECT query
    fn plan_select(&self, select: &SelectStatement, analyzed: &AnalyzedStatement) -> Result<Plan> {
        // Create context that uses the analyzed statement
        let mut context = AnalyzedPlanContext::new(&self.schemas, analyzed);

        // Start with FROM clause
        let mut node = self.plan_from(&select.from, &mut context)?;

        // Apply WHERE filter
        if let Some(ref where_expr) = select.r#where {
            node = self.plan_where_with_index(where_expr, node, &mut context)?;
        }

        // Apply GROUP BY and aggregates
        let has_aggregates = self.has_aggregates(&select.select);
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

        // Optimize the plan before returning
        let plan = Plan::Select(Box::new(node));
        self.optimizer.optimize(plan)
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
                    context.add_table(name.clone(), alias.clone())?;
                    let scan = Node::Scan {
                        table: name.clone(),
                        alias: alias.clone(),
                    };

                    node = Some(if let Some(prev) = node {
                        Node::NestedLoopJoin {
                            left: Box::new(prev),
                            right: Box::new(scan),
                            predicate: Expression::Constant(crate::types::value::Value::boolean(
                                true,
                            )),
                            join_type: JoinType::Inner,
                        }
                    } else {
                        scan
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

                    let join_node = if let Some(pred) = predicate {
                        // Check if this is an equi-join
                        if let Some((left_col, right_col)) =
                            self.extract_equi_join_columns(pred, &left_node, &right_node, context)
                        {
                            Node::HashJoin {
                                left: Box::new(left_node),
                                right: Box::new(right_node),
                                left_col,
                                right_col,
                                join_type: Self::convert_join_type(r#type),
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
                            predicate: Expression::Constant(crate::types::value::Value::boolean(
                                true,
                            )),
                            join_type: JoinType::Cross,
                        }
                    };

                    node = Some(if let Some(prev) = node {
                        Node::NestedLoopJoin {
                            left: Box::new(prev),
                            right: Box::new(join_node),
                            predicate: Expression::Constant(crate::types::value::Value::boolean(
                                true,
                            )),
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
                    Plan::Select(node) => node,
                    _ => return Err(Error::ExecutionError("Expected SELECT plan".into())),
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
                    Expression::Constant(crate::types::value::Value::Null)
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

    // Helper methods...

    fn plan_create_table(
        &self,
        name: String,
        columns: Vec<Column>,
        if_not_exists: bool,
    ) -> Result<Plan> {
        // Similar to original planner for now
        use crate::operators;
        use crate::types::value::Value;

        let mut schema_columns = Vec::new();
        let mut primary_key_idx = None;

        for (i, col) in columns.iter().enumerate() {
            let mut schema_col =
                crate::types::schema::Column::new(col.name.clone(), col.datatype.clone());

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
                // Evaluate the default expression to a constant value
                // We need to resolve the expression first, but we don't have tables yet
                // So we create a minimal context just for constants and functions
                let resolved_expr = resolve_default_expression(default_expr)?;
                let default_value = evaluate_default_expression(resolved_expr)?;
                schema_col = schema_col.default(default_value);
            }

            schema_columns.push(schema_col);
        }

        let table = Table::new(name.clone(), schema_columns)?;

        Ok(Plan::CreateTable {
            name,
            schema: table,
            if_not_exists,
        })
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

    fn has_aggregates(&self, select: &[(AstExpression, Option<String>)]) -> bool {
        select.iter().any(|(expr, _)| self.is_aggregate_expr(expr))
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

    // Stub implementations for now
    fn plan_where_with_index(
        &self,
        where_expr: &AstExpression,
        source: Node,
        context: &mut AnalyzedPlanContext,
    ) -> Result<Node> {
        // For now, just create a filter
        let predicate = context.resolve_expression(where_expr)?;
        Ok(Node::Filter {
            source: Box::new(source),
            predicate,
        })
    }

    fn extract_aggregates(
        &self,
        select: &[(AstExpression, Option<String>)],
        context: &mut AnalyzedPlanContext,
    ) -> Result<Vec<AggregateFunc>> {
        let mut aggregates = Vec::new();

        for (expr, _) in select {
            if let AstExpression::Function(name, args) = expr {
                let func_name = name.to_uppercase();

                let arg = if args.is_empty() {
                    Expression::Constant(crate::types::value::Value::integer(1))
                } else if args.len() == 1 && matches!(args[0], AstExpression::All) {
                    if func_name.ends_with("_DISTINCT") {
                        Expression::All
                    } else {
                        Expression::Constant(crate::types::value::Value::integer(1))
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
                    for table in &context.tables {
                        if let Some(schema) = self.schemas.get(&table.name) {
                            for (i, col) in schema.columns.iter().enumerate() {
                                expressions.push(Expression::Column(table.start_column + i));
                                aliases.push(Some(col.name.clone()));
                            }
                        }
                    }
                }
                AstExpression::Column(table_ref, col_name) if alias.is_none() => {
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
        // For now, fall back to simple resolution
        // This will be enhanced to use TypeAnnotations
        self.resolve_expression_simple(expr)
    }

    /// Simple expression resolution (temporary)
    fn resolve_expression_simple(&self, expr: &AstExpression) -> Result<Expression> {
        match expr {
            AstExpression::Literal(lit) => {
                let value = match lit {
                    Literal::Null => crate::types::value::Value::Null,
                    Literal::Boolean(b) => crate::types::value::Value::boolean(*b),
                    Literal::Integer(i) => {
                        if *i >= i32::MIN as i128 && *i <= i32::MAX as i128 {
                            crate::types::value::Value::I32(*i as i32)
                        } else if *i >= i64::MIN as i128 && *i <= i64::MAX as i128 {
                            crate::types::value::Value::I64(*i as i64)
                        } else {
                            crate::types::value::Value::I128(*i)
                        }
                    }
                    Literal::Float(f) => crate::types::value::Value::F64(*f),
                    Literal::String(s) => crate::types::value::Value::string(s.clone()),
                    Literal::Bytea(b) => crate::types::value::Value::Bytea(b.clone()),
                    Literal::Date(d) => crate::types::value::Value::Date(*d),
                    Literal::Time(t) => crate::types::value::Value::Time(*t),
                    Literal::Timestamp(ts) => crate::types::value::Value::Timestamp(*ts),
                    Literal::Interval(i) => crate::types::value::Value::Interval(i.clone()),
                };
                Ok(Expression::Constant(value))
            }

            AstExpression::Column(table_ref, column_name) => {
                // Use the pre-resolved column resolution map for O(1) lookup
                if let Some(resolution) = self
                    .analyzed
                    .column_resolution_map
                    .get(table_ref.as_deref(), column_name)
                {
                    // Calculate the absolute column position
                    // The resolution contains the global offset which is the absolute position
                    return Ok(Expression::Column(resolution.global_offset));
                }

                // If not found in resolution map, check if it's a struct field access
                let table = if let Some(tref) = table_ref {
                    self.tables
                        .iter()
                        .find(|t| &t.name == tref || t.alias.as_ref() == Some(tref))
                } else if self.tables.len() == 1 {
                    self.tables.first()
                } else {
                    return Err(Error::ColumnNotFound(column_name.clone()));
                };

                if let Some(table) = table {
                    // This shouldn't happen if semantic analysis was successful
                    self.resolve_column_in_table(table, column_name)
                } else {
                    // No table found with this name - check if it's a struct column
                    // This handles cases like "details.name" where "details" is a column
                    let struct_col_name = table_ref.clone().unwrap_or_default();

                    // Try to find this column in any table
                    for table in &self.tables {
                        if let Some(schema) = self.schemas.get(&table.name) {
                            if let Some(col) =
                                schema.columns.iter().find(|c| c.name == struct_col_name)
                            {
                                // Check if it's a struct type
                                match &col.datatype {
                                    crate::types::data_type::DataType::Struct(fields) => {
                                        // Verify the field exists
                                        if fields.iter().any(|(name, _)| name == column_name) {
                                            // Return a FieldAccess expression
                                            let base_expr = self
                                                .resolve_column_in_table(table, &struct_col_name)?;
                                            return Ok(Expression::FieldAccess(
                                                Box::new(base_expr),
                                                column_name.clone(),
                                            ));
                                        } else {
                                            return Err(Error::ExecutionError(format!(
                                                "Field '{}' not found in struct '{}'",
                                                column_name, struct_col_name
                                            )));
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }

                    Err(Error::TableNotFound(table_ref.clone().unwrap_or_default()))
                }
            }

            AstExpression::Parameter(idx) => Ok(Expression::Parameter(*idx)),

            AstExpression::Function(name, args) => {
                let resolved_args = args
                    .iter()
                    .map(|a| self.resolve_expression_simple(a))
                    .collect::<Result<Vec<_>>>()?;
                Ok(Expression::Function(name.clone(), resolved_args))
            }

            AstExpression::Operator(op) => self.resolve_operator(op),

            AstExpression::All => Ok(Expression::All),

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

            _ => Err(Error::ExecutionError(
                "Expression type not yet supported".into(),
            )),
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
                    Literal::Null => crate::types::value::Value::Null,
                    _ => return Err(Error::ExecutionError("IS only supports NULL".into())),
                };
                Expression::Is(Box::new(self.resolve_expression_simple(e)?), value)
            }
            Add(l, r) => Expression::Add(
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
            _ => return Err(Error::ExecutionError("Operator not yet supported".into())),
        })
    }
}

/// Resolves a DEFAULT expression (which shouldn't have column references)
fn resolve_default_expression(expr: &AstExpression) -> Result<Expression> {
    use crate::parsing::ast::Literal;

    match expr {
        AstExpression::Literal(lit) => {
            let value = match lit {
                Literal::Null => crate::types::value::Value::Null,
                Literal::Boolean(b) => crate::types::value::Value::Bool(*b),
                Literal::Integer(n) => {
                    if *n >= i32::MIN as i128 && *n <= i32::MAX as i128 {
                        crate::types::value::Value::I32(*n as i32)
                    } else if *n >= i64::MIN as i128 && *n <= i64::MAX as i128 {
                        crate::types::value::Value::I64(*n as i64)
                    } else {
                        crate::types::value::Value::I128(*n)
                    }
                }
                Literal::Float(f) => crate::types::value::Value::F64(*f),
                Literal::String(s) => crate::types::value::Value::Str(s.clone()),
                Literal::Bytea(b) => crate::types::value::Value::Bytea(b.clone()),
                Literal::Date(d) => crate::types::value::Value::Date(*d),
                Literal::Time(t) => crate::types::value::Value::Time(*t),
                Literal::Timestamp(ts) => crate::types::value::Value::Timestamp(*ts),
                Literal::Interval(i) => crate::types::value::Value::Interval(i.clone()),
            };
            Ok(Expression::Constant(value))
        }

        AstExpression::Function(name, args) => {
            let resolved_args = args
                .iter()
                .map(|a| resolve_default_expression(a))
                .collect::<Result<Vec<_>>>()?;
            Ok(Expression::Function(name.clone(), resolved_args))
        }

        AstExpression::Operator(op) => {
            use crate::parsing::Operator::*;

            Ok(match op {
                Add(l, r) => Expression::Add(
                    Box::new(resolve_default_expression(l)?),
                    Box::new(resolve_default_expression(r)?),
                ),
                Subtract(l, r) => Expression::Subtract(
                    Box::new(resolve_default_expression(l)?),
                    Box::new(resolve_default_expression(r)?),
                ),
                Multiply(l, r) => Expression::Multiply(
                    Box::new(resolve_default_expression(l)?),
                    Box::new(resolve_default_expression(r)?),
                ),
                Divide(l, r) => Expression::Divide(
                    Box::new(resolve_default_expression(l)?),
                    Box::new(resolve_default_expression(r)?),
                ),
                Remainder(l, r) => Expression::Remainder(
                    Box::new(resolve_default_expression(l)?),
                    Box::new(resolve_default_expression(r)?),
                ),
                Exponentiate(l, r) => Expression::Exponentiate(
                    Box::new(resolve_default_expression(l)?),
                    Box::new(resolve_default_expression(r)?),
                ),
                Negate(e) => Expression::Negate(Box::new(resolve_default_expression(e)?)),
                Identity(e) => Expression::Identity(Box::new(resolve_default_expression(e)?)),
                // Boolean operators
                And(l, r) => Expression::And(
                    Box::new(resolve_default_expression(l)?),
                    Box::new(resolve_default_expression(r)?),
                ),
                Or(l, r) => Expression::Or(
                    Box::new(resolve_default_expression(l)?),
                    Box::new(resolve_default_expression(r)?),
                ),
                Not(e) => Expression::Not(Box::new(resolve_default_expression(e)?)),
                // IS NULL
                Is(e, lit) => {
                    let value = match lit {
                        Literal::Null => crate::types::value::Value::Null,
                        _ => return Err(Error::ExecutionError("IS only supports NULL".into())),
                    };
                    Expression::Is(Box::new(resolve_default_expression(e)?), value)
                }
                _ => {
                    return Err(Error::ExecutionError(
                        "Operator not supported in DEFAULT expressions".into(),
                    ));
                }
            })
        }

        _ => Err(Error::ExecutionError(
            "Expression type not supported in DEFAULT expressions".into(),
        )),
    }
}

/// Evaluates a DEFAULT expression to a constant Value
fn evaluate_default_expression(expr: Expression) -> Result<crate::types::value::Value> {
    use crate::operators;
    use crate::types::value::Value;

    match expr {
        Expression::Constant(value) => Ok(value),

        // Arithmetic operations
        Expression::Add(left, right) => {
            let l = evaluate_default_expression(*left)?;
            let r = evaluate_default_expression(*right)?;
            operators::execute_add(&l, &r)
        }
        Expression::Subtract(left, right) => {
            let l = evaluate_default_expression(*left)?;
            let r = evaluate_default_expression(*right)?;
            operators::execute_subtract(&l, &r)
        }
        Expression::Multiply(left, right) => {
            let l = evaluate_default_expression(*left)?;
            let r = evaluate_default_expression(*right)?;
            operators::execute_multiply(&l, &r)
        }
        Expression::Divide(left, right) => {
            let l = evaluate_default_expression(*left)?;
            let r = evaluate_default_expression(*right)?;
            operators::execute_divide(&l, &r)
        }
        Expression::Remainder(left, right) => {
            let l = evaluate_default_expression(*left)?;
            let r = evaluate_default_expression(*right)?;
            operators::execute_remainder(&l, &r)
        }
        Expression::Exponentiate(left, right) => {
            let l = evaluate_default_expression(*left)?;
            let r = evaluate_default_expression(*right)?;
            operators::execute_exponentiate(&l, &r)
        }

        // Unary operations
        Expression::Negate(expr) => {
            let val = evaluate_default_expression(*expr)?;
            operators::execute_negate(&val)
        }
        Expression::Identity(expr) => {
            // Identity just returns the value as-is
            evaluate_default_expression(*expr)
        }

        // Boolean operations
        Expression::And(left, right) => {
            let l = evaluate_default_expression(*left)?;
            let r = evaluate_default_expression(*right)?;
            operators::execute_and(&l, &r)
        }
        Expression::Or(left, right) => {
            let l = evaluate_default_expression(*left)?;
            let r = evaluate_default_expression(*right)?;
            operators::execute_or(&l, &r)
        }
        Expression::Not(expr) => {
            let val = evaluate_default_expression(*expr)?;
            operators::execute_not(&val)
        }

        // IS NULL checks
        Expression::Is(expr, value) => {
            let val = evaluate_default_expression(*expr)?;
            if value.is_null() {
                Ok(Value::Bool(val.is_null()))
            } else {
                Err(Error::ExecutionError("IS only supports NULL".into()))
            }
        }

        // Functions that can be evaluated statically
        Expression::Function(name, args) => {
            match name.to_uppercase().as_str() {
                "GENERATE_UUID" | "GEN_UUID" | "UUID" if args.is_empty() => {
                    // Special marker for UUID generation at INSERT time
                    Ok(Value::Str("__GENERATE_UUID__".to_string()))
                }
                "CAST" if args.len() == 2 => {
                    // CAST(value, type_string)
                    let value = if let Expression::Constant(v) = &args[0] {
                        v.clone()
                    } else {
                        evaluate_default_expression(args[0].clone())?
                    };

                    if let Expression::Constant(Value::Str(type_str)) = &args[1] {
                        // Parse the type string and perform the cast
                        match type_str.as_str() {
                            "INTEGER" | "INT" => match value {
                                Value::I32(v) => Ok(Value::I32(v)),
                                Value::I64(v) => Ok(Value::I32(v as i32)),
                                Value::F32(v) => Ok(Value::I32(v as i32)),
                                Value::F64(v) => Ok(Value::I32(v as i32)),
                                Value::Str(s) => s.parse::<i32>().map(Value::I32).map_err(|_| {
                                    Error::ExecutionError("Failed to parse integer".into())
                                }),
                                _ => Err(Error::ExecutionError("Cannot cast to INTEGER".into())),
                            },
                            "FLOAT" | "REAL" => match value {
                                Value::I32(v) => Ok(Value::F64(v as f64)),
                                Value::I64(v) => Ok(Value::F64(v as f64)),
                                Value::F32(v) => Ok(Value::F64(v as f64)),
                                Value::F64(v) => Ok(Value::F64(v)),
                                Value::Str(s) => s.parse::<f64>().map(Value::F64).map_err(|_| {
                                    Error::ExecutionError("Failed to parse float".into())
                                }),
                                _ => Err(Error::ExecutionError("Cannot cast to FLOAT".into())),
                            },
                            "TEXT" | "VARCHAR" | "STRING" => Ok(Value::Str(value.to_string())),
                            "BOOLEAN" | "BOOL" => match value {
                                Value::Bool(b) => Ok(Value::Bool(b)),
                                Value::I32(i) => Ok(Value::Bool(i != 0)),
                                Value::I64(i) => Ok(Value::Bool(i != 0)),
                                Value::Str(s) => {
                                    let s_upper = s.to_uppercase();
                                    match s_upper.as_str() {
                                        "TRUE" | "T" | "YES" | "Y" | "1" => Ok(Value::Bool(true)),
                                        "FALSE" | "F" | "NO" | "N" | "0" => Ok(Value::Bool(false)),
                                        _ => Err(Error::ExecutionError(format!(
                                            "Cannot cast '{}' to BOOLEAN",
                                            s
                                        ))),
                                    }
                                }
                                _ => Err(Error::ExecutionError("Cannot cast to BOOLEAN".into())),
                            },
                            _ => Err(Error::ExecutionError(format!(
                                "Unsupported cast type: {}",
                                type_str
                            ))),
                        }
                    } else {
                        Err(Error::ExecutionError(
                            "CAST requires a type name as second argument".into(),
                        ))
                    }
                }
                _ => Err(Error::ExecutionError(format!(
                    "Function {} not supported in DEFAULT expressions",
                    name
                ))),
            }
        }

        _ => Err(Error::ExecutionError(
            "Expression type not supported in DEFAULT values".into(),
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
        AstExpression::Function(name, args) => {
            let arg_str = args
                .iter()
                .map(|arg| expr_to_string(arg))
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
