//! Query planner that converts AST to execution plan nodes
//!
//! This planner resolves columns during planning and builds an
//! optimized execution plan tree.

use super::optimizer::Optimizer;
use super::plan::{AggregateFunc, Direction, JoinType, Node, Plan};
use super::predicate::{Predicate, PredicateCondition, QueryPredicates};
use crate::error::{Error, Result};
use crate::parsing::ast;
use crate::types::expression::Expression;
use crate::types::schema::Table;
use crate::types::statistics::DatabaseStatistics;
use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;
use std::str::FromStr;

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
    pub name: String,                        // Column name for simple columns
    pub expression: Option<ast::Expression>, // Full expression if complex
    pub direction: Option<ast::Direction>,
}

/// Query planner
pub struct Planner {
    schemas: HashMap<String, Table>,
    optimizer: Optimizer,
    /// Available indexes: table_name -> list of indexes
    indexes: HashMap<String, Vec<IndexInfo>>,
    /// Optional statistics for optimization
    statistics: Option<DatabaseStatistics>,
}

impl Planner {
    /// Create a new planner
    pub fn new(
        schemas: HashMap<String, Table>,
        index_metadata: HashMap<String, Vec<crate::storage::mvcc::IndexMetadata>>,
    ) -> Self {
        let optimizer = Optimizer::new(schemas.clone());

        // Convert storage index metadata to planner IndexInfo
        let mut indexes = HashMap::new();
        for (table_name, table_indexes) in index_metadata {
            let mut index_infos = Vec::new();
            for metadata in table_indexes {
                let columns = metadata
                    .columns
                    .iter()
                    .map(|col| {
                        // Check if this is a simple column name or a complex expression
                        if !col.expression.contains('(')
                            && !col.expression.contains('+')
                            && !col.expression.contains('-')
                            && !col.expression.contains('*')
                            && !col.expression.contains('/')
                        {
                            // Simple column name
                            IndexColumn {
                                name: col.expression.clone(),
                                expression: Some(ast::Expression::Column(
                                    None,
                                    col.expression.clone(),
                                )),
                                direction: col.direction,
                            }
                        } else {
                            // Complex expression - try to parse it
                            // For debug strings like "Add(Column(None, \"id\"), Column(None, \"num\"))"
                            // we need to reconstruct the AST
                            let expr = if col.expression.starts_with("Add(") {
                                // Try to extract operands for Add expression
                                // This is a temporary hack - we should store expressions properly
                                None // For now, we can't properly reconstruct
                            } else {
                                None
                            };

                            IndexColumn {
                                name: col.expression.clone(),
                                expression: expr,
                                direction: col.direction,
                            }
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

        // Also add schema-defined indexes
        for (table_name, schema) in &schemas {
            let table_indexes = indexes.entry(table_name.clone()).or_insert_with(Vec::new);
            for column in &schema.columns {
                if column.primary_key || column.unique || column.index {
                    // Check if this index already exists (from metadata)
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

        Self {
            schemas,
            optimizer,
            indexes,
            statistics: None,
        }
    }

    /// Add a composite index to the planner's metadata
    pub fn add_index(&mut self, index: IndexInfo) {
        self.indexes
            .entry(index.table.clone())
            .or_default()
            .push(index);
    }

    /// Update statistics for query optimization
    pub fn update_statistics(&mut self, stats: DatabaseStatistics) {
        self.statistics = Some(stats);
    }

    /// Clear statistics (useful when they become stale)
    pub fn clear_statistics(&mut self) {
        self.statistics = None;
    }

    /// Plan a statement
    pub fn plan(&self, statement: ast::Statement) -> Result<Plan> {
        match statement {
            ast::Statement::Select(select_stmt) => {
                let ast::SelectStatement {
                    select,
                    from,
                    r#where,
                    group_by,
                    having,
                    order_by,
                    offset,
                    limit,
                } = *select_stmt;
                self.plan_select(
                    select, from, r#where, group_by, having, order_by, offset, limit,
                )
            }

            ast::Statement::Insert {
                table,
                columns,
                source,
            } => self.plan_insert(table, columns, source),

            ast::Statement::Update {
                table,
                set,
                r#where,
            } => self.plan_update(table, set, r#where),

            ast::Statement::Delete { table, r#where } => self.plan_delete(table, r#where),

            ast::Statement::CreateTable {
                name,
                columns,
                if_not_exists,
            } => self.plan_create_table(name, columns, if_not_exists),

            ast::Statement::DropTable { names, if_exists } => {
                Ok(Plan::DropTable { names, if_exists })
            }

            ast::Statement::CreateIndex {
                name,
                table,
                columns,
                unique,
                included_columns,
            } => {
                // Verify table exists
                if !self.schemas.contains_key(&table) {
                    return Err(Error::TableNotFound(table));
                }
                // Convert AST IndexColumns to Plan IndexColumns
                let plan_columns: Vec<crate::planning::plan::IndexColumn> = columns
                    .into_iter()
                    .map(|col| crate::planning::plan::IndexColumn {
                        expression: col.expression,
                        direction: col.direction,
                    })
                    .collect();

                Ok(Plan::CreateIndex {
                    name,
                    table,
                    columns: plan_columns,
                    unique,
                    included_columns,
                })
            }

            ast::Statement::DropIndex { name, if_exists } => {
                Ok(Plan::DropIndex { name, if_exists })
            }

            ast::Statement::Explain(_) => {
                Err(Error::ExecutionError("EXPLAIN not yet implemented".into()))
            }
        }
    }

    /// Plan a SELECT query
    #[allow(clippy::too_many_arguments)]
    fn plan_select(
        &self,
        select: Vec<(ast::Expression, Option<String>)>,
        from: Vec<ast::FromClause>,
        r#where: Option<ast::Expression>,
        group_by: Vec<ast::Expression>,
        having: Option<ast::Expression>,
        order_by: Vec<(ast::Expression, ast::Direction)>,
        offset: Option<ast::Expression>,
        limit: Option<ast::Expression>,
    ) -> Result<Plan> {
        // Build context for column resolution
        let mut context = PlanContext::new(&self.schemas);

        // Start with FROM clause
        let mut node = self.plan_from(from, &mut context)?;

        // Apply WHERE filter - check for index optimization opportunities
        if let Some(where_expr) = r#where {
            // Try to optimize single table queries with indexed columns
            node = self.plan_where_with_index(where_expr, node, &mut context)?;
        }

        // Apply GROUP BY and aggregates
        let has_aggregates = self.has_aggregates(&select);
        let group_by_count = group_by.len();

        // Clone group_by before consuming it so we can use it later
        let group_by_clone = group_by.clone();

        if !group_by.is_empty() || has_aggregates {
            let group_exprs = group_by
                .into_iter()
                .map(|e| context.resolve_expression(e))
                .collect::<Result<Vec<_>>>()?;

            let aggregates = self.extract_aggregates(&select, &mut context)?;

            node = Node::Aggregate {
                source: Box::new(node),
                group_by: group_exprs,
                aggregates,
            };

            // Apply HAVING filter
            if let Some(having_expr) = having {
                let predicate = context.resolve_expression(having_expr)?;
                node = Node::Filter {
                    source: Box::new(node),
                    predicate,
                };
            }
        }

        // Apply ORDER BY first (before projection, like toydb)
        // Check if we can use an index to avoid sorting
        if !order_by.is_empty() {
            let mut can_use_index = false;
            let mut matched_index: Option<&IndexInfo> = None;

            // Check if ORDER BY matches an existing index
            if let Node::Scan {
                ref table,
                ref alias,
            } = node
                && let Some(table_indexes) = self.indexes.get(table)
            {
                for index in table_indexes {
                    // Check if ORDER BY columns match index columns (in order)
                    if self.order_by_matches_index(&order_by, &index.columns, table, &context) {
                        // Transform node to use IndexRangeScan with the matching index
                        matched_index = Some(index);
                        can_use_index = true;
                        break;
                    }
                }

                // Transform to IndexRangeScan if we found a matching index
                if let Some(index) = matched_index {
                    // Determine if we need to reverse based on ORDER BY vs index direction
                    let reverse = if !order_by.is_empty() && !index.columns.is_empty() {
                        // Get the direction from ORDER BY and the index
                        let order_dir = order_by[0].1;
                        let index_dir = index.columns[0]
                            .direction
                            .unwrap_or(ast::Direction::Ascending);
                        // Reverse if they don't match
                        order_dir != index_dir
                    } else {
                        false
                    };

                    // Use IndexRangeScan to scan all rows but in index order
                    node = Node::IndexRangeScan {
                        table: table.clone(),
                        alias: alias.clone(),
                        index_name: index.name.clone(),
                        start: None, // No start bound - scan from beginning
                        start_inclusive: true,
                        end: None, // No end bound - scan to end
                        end_inclusive: true,
                        reverse,
                    };
                }
            }

            // Only add Order node if we can't use an index
            if !can_use_index {
                let order = order_by
                    .into_iter()
                    .map(|(e, d)| {
                        let expr = context.resolve_expression(e)?;
                        let dir = match d {
                            ast::Direction::Ascending => Direction::Ascending,
                            ast::Direction::Descending => Direction::Descending,
                        };
                        Ok((expr, dir))
                    })
                    .collect::<Result<Vec<_>>>()?;

                node = Node::Order {
                    source: Box::new(node),
                    order_by: order,
                };
            }
        }

        // Apply projection after ORDER BY
        // Special handling when we have aggregates - the projection needs to work with aggregate results
        let (expressions, aliases) = if has_aggregates {
            let mut expressions = Vec::new();
            let mut aliases = Vec::new();
            let mut col_idx = group_by_count; // Start after GROUP BY columns

            // Use the cloned GROUP BY expressions for matching
            let group_by_exprs = group_by_clone;

            for (expr, alias) in select {
                if self.is_aggregate_expr(&expr) {
                    // For aggregate functions, project the corresponding aggregate result column
                    expressions.push(Expression::Column(col_idx));
                    col_idx += 1;

                    // Generate alias for aggregate function
                    let func_alias = alias.or_else(|| self.generate_aggregate_alias(&expr));
                    aliases.push(func_alias);
                } else {
                    // Check if this expression matches a GROUP BY expression
                    let mut found_group_by = false;
                    for (i, group_expr) in group_by_exprs.iter().enumerate() {
                        if Self::expressions_equal(&expr, group_expr) {
                            // This is a GROUP BY column - project from the GROUP BY output
                            expressions.push(Expression::Column(i));

                            // Use the column name as alias if no explicit alias is provided
                            let column_alias = alias.clone().or_else(|| {
                                // Extract column name from the expression
                                match &expr {
                                    ast::Expression::Column(_, col_name) => Some(col_name.clone()),
                                    _ => None,
                                }
                            });
                            aliases.push(column_alias);
                            found_group_by = true;
                            break;
                        }
                    }

                    if !found_group_by {
                        // Not a GROUP BY expression - this shouldn't happen in valid SQL
                        // but we'll handle it by resolving the expression
                        expressions.push(context.resolve_expression(expr)?);
                        aliases.push(alias);
                    }
                }
            }
            (expressions, aliases)
        } else {
            self.plan_projection(select, &mut context)?
        };

        node = Node::Projection {
            source: Box::new(node),
            expressions,
            aliases,
        };

        // Apply OFFSET
        if let Some(offset_expr) = offset {
            let offset = self.eval_constant(offset_expr)?;
            node = Node::Offset {
                source: Box::new(node),
                offset,
            };
        }

        // Apply LIMIT
        if let Some(limit_expr) = limit {
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
    fn plan_from(&self, from: Vec<ast::FromClause>, context: &mut PlanContext) -> Result<Node> {
        if from.is_empty() {
            // For SELECT without FROM, return a single empty row
            // This allows expressions like SELECT 1, SELECT NULL IS NULL, etc.
            return Ok(Node::Values { rows: vec![vec![]] });
        }

        let mut node = None;

        for from_item in from {
            match from_item {
                ast::FromClause::Table { name, alias } => {
                    // Register table in context
                    context.add_table(name.clone(), alias.clone())?;

                    let scan = Node::Scan { table: name, alias };

                    node = Some(if let Some(prev) = node {
                        // Cross join with previous tables
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

                ast::FromClause::Join {
                    left,
                    right,
                    r#type,
                    predicate,
                } => {
                    // Recursively plan left and right sides
                    let left_node = self.plan_from(vec![*left], context)?;
                    let right_node = self.plan_from(vec![*right], context)?;

                    // Determine join algorithm based on predicate
                    let join_node = if let Some(ref pred) = predicate {
                        // Check if this is an equi-join (equality condition)
                        if let Some((left_col, right_col)) =
                            self.extract_equi_join_columns(pred, &left_node, &right_node, context)
                        {
                            // Use hash join for equi-joins - generally more efficient
                            Node::HashJoin {
                                left: Box::new(left_node),
                                right: Box::new(right_node),
                                left_col,
                                right_col,
                                join_type: r#type,
                            }
                        } else {
                            // Use nested loop join for complex predicates
                            let join_predicate = context.resolve_expression(pred.clone())?;
                            Node::NestedLoopJoin {
                                left: Box::new(left_node),
                                right: Box::new(right_node),
                                predicate: join_predicate,
                                join_type: r#type,
                            }
                        }
                    } else {
                        // Cross join (no predicate)
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
                        // Chain multiple joins
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

    /// Plan INSERT statement
    fn plan_insert(
        &self,
        table: String,
        columns: Option<Vec<String>>,
        source: ast::InsertSource,
    ) -> Result<Plan> {
        let schema = self
            .schemas
            .get(&table)
            .ok_or_else(|| Error::TableNotFound(table.clone()))?;

        // Map column names to indices
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

        // Build source node based on InsertSource
        let source_node = match source {
            ast::InsertSource::Values(values) => {
                // Build VALUES node with resolved expressions
                let context = PlanContext::new(&self.schemas);
                let rows = values
                    .into_iter()
                    .map(|row| {
                        row.into_iter()
                            .map(|e| context.resolve_expression(e))
                            .collect::<Result<Vec<_>>>()
                    })
                    .collect::<Result<Vec<_>>>()?;
                Box::new(Node::Values { rows })
            }
            ast::InsertSource::DefaultValues => {
                // Create an empty row - the executor will fill in default values
                // Note: column_indices should be None when using DEFAULT VALUES
                if column_indices.is_some() {
                    return Err(Error::ExecutionError(
                        "Cannot specify columns with DEFAULT VALUES".into(),
                    ));
                }
                Box::new(Node::Values { rows: vec![vec![]] })
            }
            ast::InsertSource::Select(select) => {
                // Plan the SELECT statement as the source
                let plan = self.plan_select(
                    select.select,
                    select.from,
                    select.r#where,
                    select.group_by,
                    select.having,
                    select.order_by,
                    select.offset,
                    select.limit,
                )?;
                // Extract the node from the Plan::Select
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

    /// Plan UPDATE statement
    fn plan_update(
        &self,
        table: String,
        set: BTreeMap<String, Option<ast::Expression>>,
        r#where: Option<ast::Expression>,
    ) -> Result<Plan> {
        let mut context = PlanContext::new(&self.schemas);
        context.add_table(table.clone(), None)?;

        // Build scan node
        let mut node = Node::Scan {
            table: table.clone(),
            alias: None,
        };

        // Apply WHERE filter
        if let Some(where_expr) = r#where {
            let predicate = context.resolve_expression(where_expr)?;
            node = Node::Filter {
                source: Box::new(node),
                predicate,
            };
        }

        // Resolve assignments
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
                    context.resolve_expression(e)?
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

    /// Plan DELETE statement
    fn plan_delete(&self, table: String, r#where: Option<ast::Expression>) -> Result<Plan> {
        let mut context = PlanContext::new(&self.schemas);
        context.add_table(table.clone(), None)?;

        // Build scan node
        let mut node = Node::Scan {
            table: table.clone(),
            alias: None,
        };

        // Apply WHERE filter
        if let Some(where_expr) = r#where {
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

    /// Plan CREATE TABLE
    fn plan_create_table(
        &self,
        name: String,
        columns: Vec<ast::Column>,
        if_not_exists: bool,
    ) -> Result<Plan> {
        // Convert AST columns to schema
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
                // Create a temporary context for evaluating the DEFAULT expression
                let context = PlanContext::new(&self.schemas);
                let resolved_expr = context.resolve_expression(default_expr.clone())?;

                // Evaluate the expression to a constant value
                let default_value = evaluate_default_expression(resolved_expr)?;
                schema_col = schema_col.default(default_value);
            }

            schema_columns.push(schema_col);
        }

        let table = Table::new(name.clone(), schema_columns)?;

        // Collect columns that need indexes (for future use)
        let _indexed_columns: Vec<String> = columns
            .iter()
            .filter(|col| col.index)
            .map(|col| col.name.clone())
            .collect();

        Ok(Plan::CreateTable {
            name,
            schema: table,
            if_not_exists,
            // TODO: Pass indexed_columns to executor somehow
        })
    }

    /// Plan projection (SELECT expressions)
    fn plan_projection(
        &self,
        select: Vec<(ast::Expression, Option<String>)>,
        context: &mut PlanContext,
    ) -> Result<(Vec<Expression>, Vec<Option<String>>)> {
        let mut expressions = Vec::new();
        let mut aliases = Vec::new();

        for (expr, alias) in select {
            match expr {
                ast::Expression::All => {
                    // Expand * to all columns
                    for table in &context.tables {
                        if let Some(schema) = self.schemas.get(&table.name) {
                            for (i, col) in schema.columns.iter().enumerate() {
                                expressions.push(Expression::Column(table.start_column + i));
                                // Use the actual column name as alias for SELECT *
                                aliases.push(Some(col.name.clone()));
                            }
                        }
                    }
                }
                ast::Expression::Column(ref table_ref, ref col_name) if alias.is_none() => {
                    // No alias provided - use the column name
                    expressions.push(context.resolve_expression(ast::Expression::Column(
                        table_ref.clone(),
                        col_name.clone(),
                    ))?);
                    aliases.push(Some(col_name.clone()));
                }
                _ => {
                    expressions.push(context.resolve_expression(expr)?);
                    aliases.push(alias);
                }
            }
        }

        Ok((expressions, aliases))
    }

    /// Check if SELECT has aggregate functions
    fn has_aggregates(&self, select: &[(ast::Expression, Option<String>)]) -> bool {
        select.iter().any(|(expr, _)| self.is_aggregate_expr(expr))
    }

    /// Check if expression is an aggregate
    fn is_aggregate_expr(&self, expr: &ast::Expression) -> bool {
        match expr {
            ast::Expression::Function(name, _) => {
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

    /// Extract aggregate functions
    fn extract_aggregates(
        &self,
        select: &[(ast::Expression, Option<String>)],
        context: &mut PlanContext,
    ) -> Result<Vec<AggregateFunc>> {
        let mut aggregates = Vec::new();

        for (expr, _) in select {
            if let ast::Expression::Function(name, args) = expr {
                let func_name = name.to_uppercase();

                // Get the argument expression
                let arg = if args.is_empty() {
                    Expression::Constant(crate::types::value::Value::integer(1)) // For COUNT(*)
                } else if args.len() == 1 && matches!(args[0], ast::Expression::All) {
                    // Handle COUNT(*) and COUNT(DISTINCT *)
                    // For DISTINCT *, we need to signal to count distinct rows
                    if func_name.ends_with("_DISTINCT") {
                        Expression::All // Special marker for COUNT(DISTINCT *)
                    } else {
                        Expression::Constant(crate::types::value::Value::integer(1))
                    }
                } else {
                    context.resolve_expression(args[0].clone())?
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

    /// Evaluate a constant expression
    fn eval_constant(&self, expr: ast::Expression) -> Result<usize> {
        match expr {
            ast::Expression::Literal(ast::Literal::Integer(n))
                if n >= 0 && n <= usize::MAX as i128 =>
            {
                Ok(n as usize)
            }
            _ => Err(Error::ExecutionError(
                "Expected non-negative integer constant".into(),
            )),
        }
    }

    /// Plan WHERE clause with index optimization
    fn plan_where_with_index(
        &self,
        where_expr: ast::Expression,
        source: Node,
        context: &mut PlanContext,
    ) -> Result<Node> {
        // Check if this is a simple equality on an indexed column
        if let Node::Scan {
            ref table,
            ref alias,
        } = source
            && let Some(index_scan) = self.try_create_index_scan(&where_expr, table, alias, context)
        {
            return Ok(index_scan);
        }

        // Fall back to regular filter
        let predicate = context.resolve_expression(where_expr)?;
        Ok(Node::Filter {
            source: Box::new(source),
            predicate,
        })
    }

    /// Extract equality conditions from a WHERE expression (handles AND chains)
    fn extract_equality_conditions(
        expr: &ast::Expression,
        table_name: &str,
        alias: &Option<String>,
    ) -> Vec<(String, ast::Expression)> {
        let mut conditions = Vec::new();

        match expr {
            // Handle AND expressions recursively
            ast::Expression::Operator(ast::Operator::And(left, right)) => {
                conditions.extend(Self::extract_equality_conditions(left, table_name, alias));
                conditions.extend(Self::extract_equality_conditions(right, table_name, alias));
            }
            // Handle equality conditions
            ast::Expression::Operator(ast::Operator::Equal(left, right)) => {
                // Check if left is a column from our table
                if let ast::Expression::Column(ref table_ref, ref column_name) = **left {
                    if table_ref.is_none()
                        || table_ref.as_ref().map(|s| s.as_str()) == Some(table_name)
                        || table_ref == alias
                    {
                        conditions.push((column_name.clone(), (**right).clone()));
                    }
                }
                // Also check if right is a column (value = column)
                else if let ast::Expression::Column(ref table_ref, ref column_name) = **right
                    && (table_ref.is_none()
                        || table_ref.as_ref().map(|s| s.as_str()) == Some(table_name)
                        || table_ref == alias)
                {
                    conditions.push((column_name.clone(), (**left).clone()));
                }
            }
            _ => {}
        }

        conditions
    }

    /// Estimate the selectivity of an index for given conditions
    fn estimate_index_selectivity(
        &self,
        index_name: &str,
        table_name: &str,
        conditions: &[(String, ast::Expression)],
    ) -> f64 {
        // Get table statistics if available
        if let Some(db_stats) = &self.statistics
            && let Some(table_stats) = db_stats.tables.get(table_name)
        {
            let mut combined_selectivity = 1.0;

            // Use column-level statistics for more accurate selectivity
            for (col_name, expr) in conditions {
                if let Some(col_stats) = table_stats.columns.get(col_name) {
                    // Check if the value is in most common values
                    if let ast::Expression::Literal(ast::Literal::Integer(i)) = expr {
                        let value = if *i >= i32::MIN as i128 && *i <= i32::MAX as i128 {
                            crate::types::value::Value::I32(*i as i32)
                        } else if *i >= i64::MIN as i128 && *i <= i64::MAX as i128 {
                            crate::types::value::Value::I64(*i as i64)
                        } else {
                            crate::types::value::Value::I128(*i)
                        };

                        // Check most common values first
                        let mut found_selectivity = None;
                        for (common_val, freq) in &col_stats.most_common_values {
                            if common_val == &value {
                                found_selectivity =
                                    Some(*freq as f64 / table_stats.row_count.max(1) as f64);
                                break;
                            }
                        }

                        if let Some(sel) = found_selectivity {
                            combined_selectivity *= sel;
                        } else if col_stats.distinct_count > 0 {
                            // Use 1/distinct_count for non-common values
                            combined_selectivity *= 1.0 / col_stats.distinct_count as f64;
                        } else {
                            combined_selectivity *= 0.1; // Default
                        }
                    } else if col_stats.distinct_count > 0 {
                        // For non-literal expressions, use 1/distinct_count
                        combined_selectivity *= 1.0 / col_stats.distinct_count as f64;
                    } else {
                        combined_selectivity *= 0.1; // Default
                    }
                } else if let Some(index_stats) = table_stats.indexes.get(index_name) {
                    // Fall back to index statistics if column stats not available
                    if let Some(col_idx) = index_stats.columns.iter().position(|c| c == col_name) {
                        if col_idx < index_stats.selectivity.len() {
                            combined_selectivity *= index_stats.selectivity[col_idx];
                        } else {
                            combined_selectivity *= 0.1;
                        }
                    } else {
                        combined_selectivity *= 0.1;
                    }
                } else {
                    combined_selectivity *= 0.1; // Default
                }
            }

            return combined_selectivity;
        }

        // Default: assume 10% selectivity per condition if no stats available
        0.1_f64.powf(conditions.len() as f64)
    }

    /// Try to create an IndexScan or IndexRangeScan node if WHERE clause matches an indexed column
    fn try_create_index_scan(
        &self,
        where_expr: &ast::Expression,
        table_name: &str,
        alias: &Option<String>,
        context: &PlanContext,
    ) -> Option<Node> {
        // First check for range queries (e.g., col > X AND col < Y)
        if let Some(range_scan) = self.try_create_range_scan(where_expr, table_name, alias, context)
        {
            return Some(range_scan);
        }

        // Extract all equality conditions from the WHERE clause
        let conditions = Self::extract_equality_conditions(where_expr, table_name, alias);

        // Try to find the best matching index using statistics
        if let Some(table_indexes) = self.indexes.get(table_name) {
            let mut best_index = None;
            let mut best_selectivity = 1.0;
            let mut best_matched_values = Vec::new();

            // Evaluate each index
            for index in table_indexes {
                // Check if we have values for all or a prefix of the index columns
                let mut matched_values = Vec::new();
                let mut matched_conditions = Vec::new();

                for index_col in &index.columns {
                    if let Some((col, expr)) =
                        conditions.iter().find(|(col, _)| **col == index_col.name)
                    {
                        // Try to resolve the expression
                        if let Ok(resolved_expr) = context.resolve_expression(expr.clone()) {
                            // Don't use index for NULL equality comparisons
                            // (NULL = NULL is unknown in SQL, not true)
                            if matches!(resolved_expr, Expression::Constant(ref v) if v.is_null()) {
                                break;
                            }
                            matched_values.push(resolved_expr);
                            matched_conditions.push((col.clone(), expr.clone()));
                        } else {
                            break;
                        }
                    } else {
                        // No condition for this column, can only use as prefix
                        break;
                    }
                }

                // If we matched at least one column, evaluate this index
                if !matched_values.is_empty() {
                    let selectivity = self.estimate_index_selectivity(
                        &index.name,
                        table_name,
                        &matched_conditions,
                    );

                    // Choose this index if it's more selective
                    if selectivity < best_selectivity {
                        best_selectivity = selectivity;
                        best_index = Some(index);
                        best_matched_values = matched_values;
                    }
                }
            }

            // Use the best index if selectivity is good enough (< 30%)
            if let Some(index) = best_index
                && best_selectivity < 0.3
            {
                return Some(Node::IndexScan {
                    table: table_name.to_string(),
                    alias: alias.clone(),
                    index_name: index.name.clone(),
                    values: best_matched_values,
                });
            }
        }

        // Fall back to checking for simple single-column equality
        if let ast::Expression::Operator(ast::Operator::Equal(left, right)) = where_expr {
            // Check if left side is a column
            if let ast::Expression::Column(ref table_ref, ref column_name) = **left {
                // Verify this column belongs to our table
                if table_ref.is_none()
                    || table_ref.as_ref().map(|s| s.as_str()) == Some(table_name)
                    || table_ref == alias
                {
                    // Check if this column has an index
                    if let Some(schema) = self.schemas.get(table_name)
                        && let Some((_col_idx, column)) = schema.get_column(column_name)
                    {
                        // Check if column is indexed (primary key, unique, or has index)
                        if column.primary_key || column.unique || column.index {
                            // Create IndexScan node using the column name as index name
                            // (for single-column indexes, the index name is the column name)
                            return Some(Node::IndexScan {
                                table: table_name.to_string(),
                                alias: alias.clone(),
                                index_name: column_name.to_string(),
                                values: vec![context.resolve_expression((**right).clone()).ok()?],
                            });
                        }
                    }
                }
            }
            // Also check if right side is a column (value = column)
            else if let ast::Expression::Column(ref table_ref, ref column_name) = **right
                && (table_ref.is_none()
                    || table_ref.as_ref().map(|s| s.as_str()) == Some(table_name)
                    || table_ref == alias)
                && let Some(schema) = self.schemas.get(table_name)
                && let Some((_col_idx, column)) = schema.get_column(column_name)
                && (column.primary_key || column.unique || column.index)
            {
                return Some(Node::IndexScan {
                    table: table_name.to_string(),
                    alias: alias.clone(),
                    index_name: column_name.to_string(),
                    values: vec![context.resolve_expression((**left).clone()).ok()?],
                });
            }
        }

        None
    }

    /// Check if ORDER BY matches an index (for optimization)
    fn order_by_matches_index(
        &self,
        order_by: &[(ast::Expression, ast::Direction)],
        index_columns: &[IndexColumn],
        _table_name: &str,
        _context: &PlanContext,
    ) -> bool {
        // ORDER BY must have same or fewer columns than index
        if order_by.len() > index_columns.len() {
            return false;
        }

        // Check each ORDER BY column matches the corresponding index column
        for (i, (expr, _dir)) in order_by.iter().enumerate() {
            let index_col = &index_columns[i];

            // We don't need to check directions here - we can use an index
            // for both ASC and DESC by setting the reverse flag appropriately

            // Check if expressions match
            if let Some(ref index_expr) = index_col.expression {
                // Index has a complex expression, check if it matches
                if expr != index_expr {
                    return false;
                }
            } else {
                // Index has a simple column, check if ORDER BY is the same column
                if let ast::Expression::Column(None, col_name) = expr {
                    if col_name != &index_col.name {
                        return false;
                    }
                } else {
                    // ORDER BY has expression but index has simple column
                    return false;
                }
            }
        }

        true
    }

    /// Try to create an IndexRangeScan node for range queries
    fn try_create_range_scan(
        &self,
        where_expr: &ast::Expression,
        table_name: &str,
        alias: &Option<String>,
        context: &PlanContext,
    ) -> Option<Node> {
        // Check for single comparison operators (>, <, >=, <=)
        if let ast::Expression::Operator(op) = where_expr {
            match op {
                ast::Operator::GreaterThan(left, right)
                | ast::Operator::GreaterThanOrEqual(left, right)
                | ast::Operator::LessThan(left, right)
                | ast::Operator::LessThanOrEqual(left, right) => {
                    // Check if one side is an indexed column
                    if let ast::Expression::Column(ref table_ref, ref column_name) = **left
                        && (table_ref.is_none()
                            || table_ref.as_ref().map(|s| s.as_str()) == Some(table_name)
                            || table_ref == alias)
                        && let Some(schema) = self.schemas.get(table_name)
                        && let Some((_, column)) = schema.get_column(column_name)
                        && (column.primary_key || column.unique || column.index)
                    {
                        // Create range scan based on operator
                        let value = context.resolve_expression((**right).clone()).ok()?;
                        return Some(match op {
                            ast::Operator::GreaterThan(..) => Node::IndexRangeScan {
                                table: table_name.to_string(),
                                alias: alias.clone(),
                                index_name: column_name.to_string(),
                                start: Some(vec![value]),
                                start_inclusive: false,
                                end: None,
                                end_inclusive: false,
                                reverse: false,
                            },
                            ast::Operator::GreaterThanOrEqual(..) => Node::IndexRangeScan {
                                table: table_name.to_string(),
                                alias: alias.clone(),
                                index_name: column_name.to_string(),
                                start: Some(vec![value]),
                                start_inclusive: true,
                                end: None,
                                end_inclusive: false,
                                reverse: false,
                            },
                            ast::Operator::LessThan(..) => Node::IndexRangeScan {
                                table: table_name.to_string(),
                                alias: alias.clone(),
                                index_name: column_name.to_string(),
                                start: None,
                                start_inclusive: false,
                                end: Some(vec![value]),
                                end_inclusive: false,
                                reverse: false,
                            },
                            ast::Operator::LessThanOrEqual(..) => Node::IndexRangeScan {
                                table: table_name.to_string(),
                                alias: alias.clone(),
                                index_name: column_name.to_string(),
                                start: None,
                                start_inclusive: false,
                                end: Some(vec![value]),
                                end_inclusive: true,
                                reverse: false,
                            },
                            _ => unreachable!(),
                        });
                    }
                }
                _ => {}
            }
        }

        None
    }

    /// Extract equi-join columns from a join predicate (e.g., t1.id = t2.id)
    fn extract_equi_join_columns(
        &self,
        predicate: &ast::Expression,
        left_node: &Node,
        right_node: &Node,
        _context: &PlanContext,
    ) -> Option<(usize, usize)> {
        // Look for equality operator
        if let ast::Expression::Operator(ast::Operator::Equal(left_expr, right_expr)) = predicate {
            // Get column names from both nodes
            let left_columns = left_node.get_column_names(&self.schemas);
            let right_columns = right_node.get_column_names(&self.schemas);

            // Try to extract column references
            let left_col = self.extract_column_name(left_expr);
            let right_col = self.extract_column_name(right_expr);

            if let (Some(left_col_name), Some(right_col_name)) = (left_col, right_col) {
                // Find column indices
                let left_idx = left_columns.iter().position(|name| {
                    name == &left_col_name || name.ends_with(&format!(".{}", left_col_name))
                });
                let right_idx = right_columns.iter().position(|name| {
                    name == &right_col_name || name.ends_with(&format!(".{}", right_col_name))
                });

                if let (Some(left_idx), Some(right_idx)) = (left_idx, right_idx) {
                    return Some((left_idx, right_idx));
                }

                // Try swapping - maybe left column is in right node and vice versa
                let left_in_right = right_columns.iter().position(|name| {
                    name == &left_col_name || name.ends_with(&format!(".{}", left_col_name))
                });
                let right_in_left = left_columns.iter().position(|name| {
                    name == &right_col_name || name.ends_with(&format!(".{}", right_col_name))
                });

                if let (Some(right_in_left_idx), Some(left_in_right_idx)) =
                    (right_in_left, left_in_right)
                {
                    return Some((right_in_left_idx, left_in_right_idx));
                }
            }
        }

        None
    }

    /// Extract column name from an expression
    fn extract_column_name(&self, expr: &ast::Expression) -> Option<String> {
        match expr {
            ast::Expression::Column(table_ref, column_name) => {
                if let Some(table) = table_ref {
                    Some(format!("{}.{}", table, column_name))
                } else {
                    Some(column_name.clone())
                }
            }
            _ => None,
        }
    }
}

/// Context for resolving columns during planning
struct PlanContext<'a> {
    schemas: &'a HashMap<String, Table>,
    tables: Vec<TableRef>,
    current_column: usize,
}

struct TableRef {
    name: String,
    alias: Option<String>,
    start_column: usize,
}

impl<'a> PlanContext<'a> {
    fn new(schemas: &'a HashMap<String, Table>) -> Self {
        Self {
            schemas,
            tables: Vec::new(),
            current_column: 0,
        }
    }

    fn add_table(&mut self, name: String, alias: Option<String>) -> Result<()> {
        let schema = self
            .schemas
            .get(&name)
            .ok_or_else(|| Error::TableNotFound(name.clone()))?;

        let table_ref = TableRef {
            name: name.clone(),
            alias,
            start_column: self.current_column,
        };

        self.current_column += schema.columns.len();
        self.tables.push(table_ref);

        Ok(())
    }

    fn resolve_expression(&self, expr: ast::Expression) -> Result<Expression> {
        match expr {
            ast::Expression::Literal(lit) => {
                let value = match lit {
                    ast::Literal::Null => crate::types::value::Value::Null,
                    ast::Literal::Boolean(b) => crate::types::value::Value::boolean(b),
                    ast::Literal::Integer(i) => {
                        // Try to fit the integer in the smallest type possible
                        // Type coercion will handle conversions during execution if needed
                        if i >= i32::MIN as i128 && i <= i32::MAX as i128 {
                            crate::types::value::Value::I32(i as i32)
                        } else if i >= i64::MIN as i128 && i <= i64::MAX as i128 {
                            crate::types::value::Value::I64(i as i64)
                        } else {
                            crate::types::value::Value::I128(i)
                        }
                    }
                    ast::Literal::Float(f) => {
                        // Handle special float values that can't be represented as Decimal
                        if f.is_nan() || f.is_infinite() {
                            // Keep as F64 for special values (will be coerced to F32 if needed)
                            crate::types::value::Value::F64(f)
                        } else {
                            // Check if this value can be represented as a Decimal
                            // Decimal has a max value of about 7.9e28, much smaller than u128::MAX (3.4e38)
                            // For very large values (like u128 values > i128::MAX), keep as F64
                            if f.abs() > 7.9e28 {
                                // Value too large for Decimal, keep as F64
                                // This is used for large unsigned integer literals
                                crate::types::value::Value::F64(f)
                            } else {
                                // Try to parse as a decimal string to avoid float precision issues
                                // If the float is a simple value like 25.12, format with limited precision
                                let decimal = if f.fract() != 0.0 && f.abs() < 1e10 {
                                    // Format with up to 10 decimal places, then parse
                                    let s = format!("{:.10}", f);
                                    // Trim trailing zeros and parse
                                    let trimmed = s.trim_end_matches('0').trim_end_matches('.');
                                    rust_decimal::Decimal::from_str(trimmed).unwrap_or_else(|_| {
                                        rust_decimal::Decimal::from_f64_retain(f).unwrap_or_else(
                                            || {
                                                // If that fails, convert to string and back for better precision
                                                rust_decimal::Decimal::from_str(&f.to_string())
                                                    .unwrap_or_default()
                                            },
                                        )
                                    })
                                } else {
                                    // For integer-like values or large values
                                    rust_decimal::Decimal::from_f64_retain(f).unwrap_or_else(|| {
                                        // If that fails, convert to string and back for better precision
                                        rust_decimal::Decimal::from_str(&f.to_string())
                                            .unwrap_or_default()
                                    })
                                };
                                crate::types::value::Value::Decimal(decimal)
                            }
                        }
                    }
                    ast::Literal::String(s) => crate::types::value::Value::string(s),
                    ast::Literal::Bytea(b) => crate::types::value::Value::Bytea(b),
                    ast::Literal::Date(d) => crate::types::value::Value::Date(d),
                    ast::Literal::Time(t) => crate::types::value::Value::Time(t),
                    ast::Literal::Timestamp(ts) => crate::types::value::Value::Timestamp(ts),
                    ast::Literal::Interval(i) => crate::types::value::Value::Interval(i),
                };
                Ok(Expression::Constant(value))
            }

            ast::Expression::Column(table_ref, column_name) => {
                // Find the table
                let table = if let Some(ref tref) = table_ref {
                    self.tables
                        .iter()
                        .find(|t| &t.name == tref || t.alias.as_ref() == Some(tref))
                } else if self.tables.len() == 1 {
                    // No table specified and only one table - use it
                    self.tables.first()
                } else {
                    // Try to find column in any table
                    for table in &self.tables {
                        if let Some(schema) = self.schemas.get(&table.name)
                            && schema.columns.iter().any(|c| c.name == column_name)
                        {
                            return self.resolve_column_in_table(table, &column_name);
                        }
                    }
                    return Err(Error::ColumnNotFound(column_name));
                }
                .ok_or_else(|| Error::TableNotFound(table_ref.unwrap_or_default()))?;

                self.resolve_column_in_table(table, &column_name)
            }

            ast::Expression::Function(name, args) => {
                let resolved_args = args
                    .into_iter()
                    .map(|a| self.resolve_expression(a))
                    .collect::<Result<Vec<_>>>()?;
                Ok(Expression::Function(name, resolved_args))
            }

            ast::Expression::Operator(op) => self.resolve_operator(op),

            ast::Expression::Parameter(idx) => {
                // Keep parameters as-is for prepared statements
                Ok(Expression::Parameter(idx))
            }

            ast::Expression::All => {
                Err(Error::ExecutionError("* not valid in this context".into()))
            }

            ast::Expression::Case {
                operand: _,
                when_clauses: _,
                else_clause: _,
            } => {
                // CASE expressions need to be resolved to internal Expression type
                // For now, return an error as we need to implement this in the Expression type first
                Err(Error::ExecutionError(
                    "CASE expressions not yet implemented in planner".into(),
                ))
            }
        }
    }

    fn resolve_column_in_table(&self, table: &TableRef, column_name: &str) -> Result<Expression> {
        let schema = self
            .schemas
            .get(&table.name)
            .ok_or_else(|| Error::TableNotFound(table.name.clone()))?;

        let col_index = schema
            .columns
            .iter()
            .position(|c| c.name == column_name)
            .ok_or_else(|| Error::ColumnNotFound(column_name.to_string()))?;

        Ok(Expression::Column(table.start_column + col_index))
    }

    fn resolve_operator(&self, op: ast::Operator) -> Result<Expression> {
        use ast::Operator::*;

        Ok(match op {
            And(l, r) => Expression::And(
                Box::new(self.resolve_expression(*l)?),
                Box::new(self.resolve_expression(*r)?),
            ),
            Or(l, r) => Expression::Or(
                Box::new(self.resolve_expression(*l)?),
                Box::new(self.resolve_expression(*r)?),
            ),
            Not(e) => Expression::Not(Box::new(self.resolve_expression(*e)?)),
            Equal(l, r) => Expression::Equal(
                Box::new(self.resolve_expression(*l)?),
                Box::new(self.resolve_expression(*r)?),
            ),
            NotEqual(l, r) => Expression::NotEqual(
                Box::new(self.resolve_expression(*l)?),
                Box::new(self.resolve_expression(*r)?),
            ),
            GreaterThan(l, r) => Expression::GreaterThan(
                Box::new(self.resolve_expression(*l)?),
                Box::new(self.resolve_expression(*r)?),
            ),
            GreaterThanOrEqual(l, r) => Expression::GreaterThanOrEqual(
                Box::new(self.resolve_expression(*l)?),
                Box::new(self.resolve_expression(*r)?),
            ),
            LessThan(l, r) => Expression::LessThan(
                Box::new(self.resolve_expression(*l)?),
                Box::new(self.resolve_expression(*r)?),
            ),
            LessThanOrEqual(l, r) => Expression::LessThanOrEqual(
                Box::new(self.resolve_expression(*l)?),
                Box::new(self.resolve_expression(*r)?),
            ),
            Is(e, lit) => {
                let value = match lit {
                    ast::Literal::Null => crate::types::value::Value::Null,
                    _ => return Err(Error::ExecutionError("IS only supports NULL".into())),
                };
                Expression::Is(Box::new(self.resolve_expression(*e)?), value)
            }
            Add(l, r) => Expression::Add(
                Box::new(self.resolve_expression(*l)?),
                Box::new(self.resolve_expression(*r)?),
            ),
            Subtract(l, r) => Expression::Subtract(
                Box::new(self.resolve_expression(*l)?),
                Box::new(self.resolve_expression(*r)?),
            ),
            Multiply(l, r) => Expression::Multiply(
                Box::new(self.resolve_expression(*l)?),
                Box::new(self.resolve_expression(*r)?),
            ),
            Divide(l, r) => Expression::Divide(
                Box::new(self.resolve_expression(*l)?),
                Box::new(self.resolve_expression(*r)?),
            ),
            Remainder(l, r) => Expression::Remainder(
                Box::new(self.resolve_expression(*l)?),
                Box::new(self.resolve_expression(*r)?),
            ),
            Exponentiate(l, r) => Expression::Exponentiate(
                Box::new(self.resolve_expression(*l)?),
                Box::new(self.resolve_expression(*r)?),
            ),
            Factorial(e) => Expression::Factorial(Box::new(self.resolve_expression(*e)?)),
            Identity(e) => Expression::Identity(Box::new(self.resolve_expression(*e)?)),
            Negate(e) => Expression::Negate(Box::new(self.resolve_expression(*e)?)),
            Like(l, r) => Expression::Like(
                Box::new(self.resolve_expression(*l)?),
                Box::new(self.resolve_expression(*r)?),
            ),
            InList {
                expr,
                list,
                negated,
            } => {
                let resolved_expr = Box::new(self.resolve_expression(*expr)?);
                let resolved_list = list
                    .into_iter()
                    .map(|e| self.resolve_expression(e))
                    .collect::<Result<Vec<_>>>()?;
                Expression::InList(resolved_expr, resolved_list, negated)
            }
            Between {
                expr,
                low,
                high,
                negated,
            } => Expression::Between(
                Box::new(self.resolve_expression(*expr)?),
                Box::new(self.resolve_expression(*low)?),
                Box::new(self.resolve_expression(*high)?),
                negated,
            ),
        })
    }
}

/// Evaluates a DEFAULT expression to a constant Value
fn evaluate_default_expression(expr: Expression) -> Result<crate::types::value::Value> {
    use crate::types::evaluator;
    use crate::types::value::Value;

    match expr {
        Expression::Constant(value) => Ok(value),

        // Arithmetic operations
        Expression::Add(left, right) => {
            let l = evaluate_default_expression(*left)?;
            let r = evaluate_default_expression(*right)?;
            evaluator::add(&l, &r)
        }
        Expression::Subtract(left, right) => {
            let l = evaluate_default_expression(*left)?;
            let r = evaluate_default_expression(*right)?;
            evaluator::subtract(&l, &r)
        }
        Expression::Multiply(left, right) => {
            let l = evaluate_default_expression(*left)?;
            let r = evaluate_default_expression(*right)?;
            evaluator::multiply(&l, &r)
        }
        Expression::Divide(left, right) => {
            let l = evaluate_default_expression(*left)?;
            let r = evaluate_default_expression(*right)?;
            evaluator::divide(&l, &r)
        }
        Expression::Remainder(left, right) => {
            let l = evaluate_default_expression(*left)?;
            let r = evaluate_default_expression(*right)?;
            // Use simple modulo for now
            match (&l, &r) {
                (Value::I32(a), Value::I32(b)) if *b != 0 => Ok(Value::I32(a % b)),
                (Value::I64(a), Value::I64(b)) if *b != 0 => Ok(Value::I64(a % b)),
                _ => Err(Error::ExecutionError(
                    "Modulo not supported for these types".into(),
                )),
            }
        }

        // Unary operations
        Expression::Negate(expr) => {
            let val = evaluate_default_expression(*expr)?;
            match val {
                Value::I8(v) => Ok(Value::I8(-v)),
                Value::I16(v) => Ok(Value::I16(-v)),
                Value::I32(v) => Ok(Value::I32(-v)),
                Value::I64(v) => Ok(Value::I64(-v)),
                Value::I128(v) => Ok(Value::I128(-v)),
                Value::F32(v) => Ok(Value::F32(-v)),
                Value::F64(v) => Ok(Value::F64(-v)),
                _ => Err(Error::ExecutionError("Cannot negate this type".into())),
            }
        }
        Expression::Identity(expr) => {
            // Identity just returns the value as-is
            evaluate_default_expression(*expr)
        }

        // Boolean operations
        Expression::And(left, right) => {
            let l = evaluate_default_expression(*left)?;
            let r = evaluate_default_expression(*right)?;
            match (&l, &r) {
                (Value::Bool(a), Value::Bool(b)) => Ok(Value::Bool(*a && *b)),
                _ => Err(Error::ExecutionError(
                    "AND requires boolean operands".into(),
                )),
            }
        }
        Expression::Or(left, right) => {
            let l = evaluate_default_expression(*left)?;
            let r = evaluate_default_expression(*right)?;
            match (&l, &r) {
                (Value::Bool(a), Value::Bool(b)) => Ok(Value::Bool(*a || *b)),
                _ => Err(Error::ExecutionError("OR requires boolean operands".into())),
            }
        }
        Expression::Not(expr) => {
            let val = evaluate_default_expression(*expr)?;
            match val {
                Value::Bool(b) => Ok(Value::Bool(!b)),
                _ => Err(Error::ExecutionError("NOT requires boolean operand".into())),
            }
        }

        // Comparison operations
        Expression::Equal(left, right) => {
            let l = evaluate_default_expression(*left)?;
            let r = evaluate_default_expression(*right)?;
            Ok(Value::Bool(l == r))
        }
        Expression::NotEqual(left, right) => {
            let l = evaluate_default_expression(*left)?;
            let r = evaluate_default_expression(*right)?;
            Ok(Value::Bool(l != r))
        }
        Expression::GreaterThan(left, right) => {
            let l = evaluate_default_expression(*left)?;
            let r = evaluate_default_expression(*right)?;
            Ok(Value::Bool(l > r))
        }
        Expression::GreaterThanOrEqual(left, right) => {
            let l = evaluate_default_expression(*left)?;
            let r = evaluate_default_expression(*right)?;
            Ok(Value::Bool(l >= r))
        }
        Expression::LessThan(left, right) => {
            let l = evaluate_default_expression(*left)?;
            let r = evaluate_default_expression(*right)?;
            Ok(Value::Bool(l < r))
        }
        Expression::LessThanOrEqual(left, right) => {
            let l = evaluate_default_expression(*left)?;
            let r = evaluate_default_expression(*right)?;
            Ok(Value::Bool(l <= r))
        }

        // IS checks (for NULL and NAN)
        Expression::Is(expr, check_val) => {
            let val = evaluate_default_expression(*expr)?;
            match check_val {
                Value::Null => Ok(Value::Bool(val == Value::Null)),
                _ => Err(Error::ExecutionError(
                    "IS only supports NULL checks in DEFAULT".into(),
                )),
            }
        }

        // Functions
        Expression::Function(name, args) => {
            match name.to_uppercase().as_str() {
                "GENERATE_UUID" | "GEN_UUID" | "UUID" if args.is_empty() => {
                    // Special marker for UUID generation at INSERT time
                    Ok(Value::Str("__GENERATE_UUID__".to_string()))
                }
                "CAST" if args.len() == 2 => {
                    // Evaluate CAST at CREATE TABLE time
                    let val = evaluate_default_expression(args[0].clone())?;
                    if let Expression::Constant(Value::Str(type_name)) = &args[1] {
                        crate::types::functions::cast_value(&val, type_name)
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

impl Planner {
    /// Extract predicates from a plan for conflict detection
    pub fn extract_predicates(&self, plan: &Plan) -> QueryPredicates {
        match plan {
            Plan::Select(node) => self.extract_node_predicates(node),

            Plan::Insert {
                table,
                source,
                columns,
            } => {
                // Get source predicates to find what values we're inserting
                let source_predicates = self.extract_node_predicates(source);

                // Try to extract primary key values from the source
                let insert_predicates = if let Some(schema) = self.schemas.get(table) {
                    if let Some(pk_idx) = schema.primary_key {
                        // We have a primary key, try to extract values
                        let mut pk_predicates = Vec::new();

                        // Check if source is VALUES with concrete rows
                        if let Node::Values { rows } = &**source {
                            for row in rows {
                                // Determine which expression contains the primary key
                                let pk_expr = if let Some(column_mapping) = columns {
                                    // Columns specified - find which maps to primary key
                                    column_mapping
                                        .iter()
                                        .position(|&idx| idx == pk_idx)
                                        .and_then(|pos| row.get(pos))
                                } else {
                                    // No column mapping - direct positional
                                    row.get(pk_idx)
                                };

                                // Extract primary key value if it's a constant
                                if let Some(expr) = pk_expr {
                                    if let Expression::Constant(value) = expr {
                                        pk_predicates.push(Predicate::primary_key(
                                            table.clone(),
                                            value.clone(),
                                        ));
                                    } else {
                                        // Non-literal PK (expression, function, etc.)
                                        // Fall back to full table lock
                                        pk_predicates.clear();
                                        pk_predicates.push(Predicate::full_table(table.clone()));
                                        break;
                                    }
                                } else {
                                    // Couldn't find PK value
                                    pk_predicates.clear();
                                    pk_predicates.push(Predicate::full_table(table.clone()));
                                    break;
                                }
                            }
                        } else {
                            // Source is not VALUES (e.g., INSERT SELECT)
                            pk_predicates.push(Predicate::full_table(table.clone()));
                        }

                        pk_predicates
                    } else {
                        // No primary key defined - use full table
                        vec![Predicate::full_table(table.clone())]
                    }
                } else {
                    // Table not found in schema
                    vec![Predicate::full_table(table.clone())]
                };

                QueryPredicates {
                    reads: source_predicates.reads,
                    writes: vec![],
                    inserts: insert_predicates,
                }
            }

            Plan::Update { table, source, .. } => {
                // UPDATE reads via source, then writes the same predicates
                let source_predicates = self.extract_node_predicates(source);

                // Find predicates for the table being updated
                let table_predicates: Vec<Predicate> = source_predicates
                    .reads
                    .iter()
                    .filter(|p| p.table == *table)
                    .cloned()
                    .collect();

                QueryPredicates {
                    reads: source_predicates.reads,
                    writes: table_predicates,
                    inserts: vec![],
                }
            }

            Plan::Delete { table, source } => {
                // DELETE reads via source, then writes (deletes) the same predicates
                let source_predicates = self.extract_node_predicates(source);

                // Find predicates for the table being deleted from
                let table_predicates: Vec<Predicate> = source_predicates
                    .reads
                    .iter()
                    .filter(|p| p.table == *table)
                    .cloned()
                    .collect();

                QueryPredicates {
                    reads: source_predicates.reads,
                    writes: table_predicates,
                    inserts: vec![],
                }
            }

            Plan::CreateTable { .. } | Plan::CreateIndex { .. } => {
                // DDL doesn't have predicates in our model
                QueryPredicates::default()
            }

            Plan::DropTable { names, .. } => {
                // Dropping tables is like writing to all of them
                QueryPredicates {
                    reads: vec![],
                    writes: names
                        .iter()
                        .map(|name| Predicate::full_table(name.clone()))
                        .collect(),
                    inserts: vec![],
                }
            }

            Plan::DropIndex { .. } => {
                // Index operations don't have predicates
                QueryPredicates::default()
            }
        }
    }

    /// Extract predicates from a node
    fn extract_node_predicates(&self, node: &Node) -> QueryPredicates {
        match node {
            Node::Scan { table, .. } => {
                // Full table scan - will be refined by Filter if present
                QueryPredicates {
                    reads: vec![Predicate::full_table(table.clone())],
                    writes: vec![],
                    inserts: vec![],
                }
            }

            Node::IndexScan { table, values, .. } => {
                // Index scan with specific values - assume primary key lookup
                let predicate = match values.first() {
                    Some(Expression::Constant(val)) => {
                        Predicate::primary_key(table.clone(), val.clone())
                    }
                    Some(_) | None => Predicate::full_table(table.clone()),
                };

                QueryPredicates {
                    reads: vec![predicate],
                    writes: vec![],
                    inserts: vec![],
                }
            }

            Node::IndexRangeScan {
                table, start, end, ..
            } => {
                // Range scan
                let start_bound = start
                    .as_ref()
                    .and_then(|exprs| exprs.first())
                    .and_then(|expr| {
                        if let Expression::Constant(val) = expr {
                            Some(Bound::Included(val.clone()))
                        } else {
                            None
                        }
                    })
                    .unwrap_or(Bound::Unbounded);

                let end_bound = end
                    .as_ref()
                    .and_then(|exprs| exprs.first())
                    .and_then(|expr| {
                        if let Expression::Constant(val) = expr {
                            Some(Bound::Included(val.clone()))
                        } else {
                            None
                        }
                    })
                    .unwrap_or(Bound::Unbounded);

                let predicate = Predicate {
                    table: table.clone(),
                    condition: PredicateCondition::Range {
                        column: "id".to_string(), // Assume primary key for now
                        start: start_bound,
                        end: end_bound,
                    },
                };

                QueryPredicates {
                    reads: vec![predicate],
                    writes: vec![],
                    inserts: vec![],
                }
            }

            Node::Values { .. } => {
                // VALUES clause doesn't read anything
                QueryPredicates::default()
            }

            Node::Filter { source, predicate } => {
                // Get the table name from the source
                let table_name = Self::get_table_from_node(source);

                // If we can determine the table, use filter predicates
                if let Some(table) = table_name {
                    let filter_predicate =
                        self.filter_to_predicate(&table, &Some(predicate.clone()));
                    QueryPredicates {
                        reads: vec![filter_predicate],
                        writes: vec![],
                        inserts: vec![],
                    }
                } else {
                    // Fall back to source predicates
                    self.extract_node_predicates(source)
                }
            }

            Node::Projection { source, .. }
            | Node::Order { source, .. }
            | Node::Limit { source, .. }
            | Node::Offset { source, .. } => {
                // Pass-through nodes
                self.extract_node_predicates(source)
            }

            Node::Aggregate { source, .. } => {
                // Aggregation reads all the source data
                self.extract_node_predicates(source)
            }

            Node::HashJoin { left, right, .. } | Node::NestedLoopJoin { left, right, .. } => {
                // Combine predicates from both sides
                let left_predicates = self.extract_node_predicates(left);
                let right_predicates = self.extract_node_predicates(right);

                QueryPredicates {
                    reads: [left_predicates.reads, right_predicates.reads].concat(),
                    writes: [left_predicates.writes, right_predicates.writes].concat(),
                    inserts: [left_predicates.inserts, right_predicates.inserts].concat(),
                }
            }

            Node::Nothing => {
                // Nothing produces no data, no predicates
                QueryPredicates::default()
            }
        }
    }

    /// Get the table name from a node if it's a scan
    fn get_table_from_node(node: &Node) -> Option<String> {
        match node {
            Node::Scan { table, .. }
            | Node::IndexScan { table, .. }
            | Node::IndexRangeScan { table, .. } => Some(table.clone()),
            Node::Filter { source, .. }
            | Node::Projection { source, .. }
            | Node::Order { source, .. }
            | Node::Limit { source, .. }
            | Node::Offset { source, .. }
            | Node::Aggregate { source, .. } => Self::get_table_from_node(source),
            _ => None,
        }
    }

    /// Convert a filter expression to a predicate
    fn filter_to_predicate(&self, table: &str, filter: &Option<Expression>) -> Predicate {
        let condition = match filter {
            None => PredicateCondition::All,

            Some(Expression::Equal(left, right)) => {
                // Check if this is a primary key or column comparison
                if let (Expression::Column(idx), Expression::Constant(val)) = (&**left, &**right) {
                    if *idx == 0 {
                        // Assume column 0 is primary key
                        PredicateCondition::PrimaryKey(val.clone())
                    } else {
                        // Get column name from schema
                        let column_name = self
                            .schemas
                            .get(table)
                            .and_then(|schema| schema.columns.get(*idx))
                            .map(|col| col.name.clone())
                            .unwrap_or_else(|| format!("col{}", idx));

                        PredicateCondition::Equals {
                            column: column_name,
                            value: val.clone(),
                        }
                    }
                } else {
                    // Complex expression, fall back to generic
                    PredicateCondition::Expression(filter.as_ref().unwrap().clone())
                }
            }

            Some(Expression::GreaterThan(left, right))
            | Some(Expression::GreaterThanOrEqual(left, right)) => {
                if let (Expression::Column(idx), Expression::Constant(val)) = (&**left, &**right) {
                    let column_name = self
                        .schemas
                        .get(table)
                        .and_then(|schema| schema.columns.get(*idx))
                        .map(|col| col.name.clone())
                        .unwrap_or_else(|| format!("col{}", idx));

                    PredicateCondition::Range {
                        column: column_name,
                        start: Bound::Included(val.clone()),
                        end: Bound::Unbounded,
                    }
                } else {
                    PredicateCondition::Expression(filter.as_ref().unwrap().clone())
                }
            }

            Some(Expression::LessThan(left, right))
            | Some(Expression::LessThanOrEqual(left, right)) => {
                if let (Expression::Column(idx), Expression::Constant(val)) = (&**left, &**right) {
                    let column_name = self
                        .schemas
                        .get(table)
                        .and_then(|schema| schema.columns.get(*idx))
                        .map(|col| col.name.clone())
                        .unwrap_or_else(|| format!("col{}", idx));

                    PredicateCondition::Range {
                        column: column_name,
                        start: Bound::Unbounded,
                        end: Bound::Included(val.clone()),
                    }
                } else {
                    PredicateCondition::Expression(filter.as_ref().unwrap().clone())
                }
            }

            Some(Expression::And(left, right)) => {
                let left_pred = self.filter_to_predicate(table, &Some((**left).clone()));
                let right_pred = self.filter_to_predicate(table, &Some((**right).clone()));

                PredicateCondition::And(vec![left_pred.condition, right_pred.condition])
            }

            Some(Expression::Or(left, right)) => {
                let left_pred = self.filter_to_predicate(table, &Some((**left).clone()));
                let right_pred = self.filter_to_predicate(table, &Some((**right).clone()));

                PredicateCondition::Or(vec![left_pred.condition, right_pred.condition])
            }

            Some(expr) => {
                // For any other expression, use generic expression predicate
                PredicateCondition::Expression(expr.clone())
            }
        };

        Predicate {
            table: table.to_string(),
            condition,
        }
    }

    /// Convert an expression to a string for column naming
    fn expr_to_string(expr: &ast::Expression) -> String {
        match expr {
            ast::Expression::Column(_, col) => col.clone(),
            ast::Expression::Literal(ast::Literal::Integer(i)) => i.to_string(),
            ast::Expression::Literal(ast::Literal::Float(f)) => f.to_string(),
            _ => "?".to_string(),
        }
    }

    /// Generate a proper alias for an aggregate function expression
    fn generate_aggregate_alias(&self, expr: &ast::Expression) -> Option<String> {
        match expr {
            ast::Expression::Function(name, args) => {
                let func_name = name.to_uppercase();

                // Handle DISTINCT functions
                let (base_func, is_distinct) = if func_name.ends_with("_DISTINCT") {
                    (func_name.trim_end_matches("_DISTINCT"), true)
                } else {
                    (func_name.as_str(), false)
                };

                // Build the argument list representation
                let arg_str = if args.is_empty() {
                    String::new()
                } else if args.len() == 1 {
                    match &args[0] {
                        ast::Expression::All => "*".to_string(),
                        ast::Expression::Column(table, col) => {
                            if let Some(t) = table {
                                format!("{}.{}", t, col)
                            } else {
                                col.clone()
                            }
                        }
                        ast::Expression::Literal(ast::Literal::Null) => "NULL".to_string(),
                        ast::Expression::Literal(ast::Literal::Integer(i)) => i.to_string(),
                        ast::Expression::Literal(ast::Literal::Float(f)) => f.to_string(),
                        ast::Expression::Literal(ast::Literal::String(s)) => format!("'{}'", s),
                        ast::Expression::Function(fname, fargs) => {
                            // For functions, show the function name and arguments
                            let farg_str = fargs
                                .iter()
                                .map(|arg| match arg {
                                    ast::Expression::Column(_, col) => col.clone(),
                                    ast::Expression::Literal(ast::Literal::Integer(i)) => {
                                        i.to_string()
                                    }
                                    _ => "?".to_string(),
                                })
                                .collect::<Vec<_>>()
                                .join(", ");
                            format!("{}({})", fname.to_uppercase(), farg_str)
                        }
                        ast::Expression::Operator(op) => {
                            // For operators, try to reconstruct the expression
                            use crate::parsing::ast::Operator;
                            match op {
                                Operator::Add(l, r) => format!(
                                    "{} + {}",
                                    Self::expr_to_string(l),
                                    Self::expr_to_string(r)
                                ),
                                Operator::Subtract(l, r) => format!(
                                    "{} - {}",
                                    Self::expr_to_string(l),
                                    Self::expr_to_string(r)
                                ),
                                Operator::Multiply(l, r) => format!(
                                    "{} * {}",
                                    Self::expr_to_string(l),
                                    Self::expr_to_string(r)
                                ),
                                Operator::Divide(l, r) => format!(
                                    "{} / {}",
                                    Self::expr_to_string(l),
                                    Self::expr_to_string(r)
                                ),
                                _ => "expr".to_string(),
                            }
                        }
                        _ => "expr".to_string(), // For other complex expressions
                    }
                } else {
                    // Multiple arguments - join them
                    args.iter()
                        .map(|arg| match arg {
                            ast::Expression::Column(_, col) => col.clone(),
                            _ => "expr".to_string(),
                        })
                        .collect::<Vec<_>>()
                        .join(", ")
                };

                if is_distinct {
                    Some(format!("{}(DISTINCT {})", base_func, arg_str))
                } else {
                    Some(format!("{}({})", base_func, arg_str))
                }
            }
            _ => None,
        }
    }

    /// Check if two AST expressions are equal
    fn expressions_equal(expr1: &ast::Expression, expr2: &ast::Expression) -> bool {
        match (expr1, expr2) {
            // Column references must match exactly
            (ast::Expression::Column(t1, c1), ast::Expression::Column(t2, c2)) => {
                t1 == t2 && c1 == c2
            }
            // Literals must match
            (ast::Expression::Literal(l1), ast::Expression::Literal(l2)) => l1 == l2,
            // All expressions match
            (ast::Expression::All, ast::Expression::All) => true,
            // Parameters must have the same index
            (ast::Expression::Parameter(i1), ast::Expression::Parameter(i2)) => i1 == i2,
            // Functions must have the same name and arguments
            (ast::Expression::Function(n1, args1), ast::Expression::Function(n2, args2)) => {
                n1 == n2
                    && args1.len() == args2.len()
                    && args1
                        .iter()
                        .zip(args2.iter())
                        .all(|(a1, a2)| Self::expressions_equal(a1, a2))
            }
            // Operators must match and have equal operands
            (ast::Expression::Operator(op1), ast::Expression::Operator(op2)) => {
                use crate::parsing::Operator;
                match (op1, op2) {
                    (Operator::Add(l1, r1), Operator::Add(l2, r2))
                    | (Operator::Subtract(l1, r1), Operator::Subtract(l2, r2))
                    | (Operator::Multiply(l1, r1), Operator::Multiply(l2, r2))
                    | (Operator::Divide(l1, r1), Operator::Divide(l2, r2))
                    | (Operator::Remainder(l1, r1), Operator::Remainder(l2, r2))
                    | (Operator::And(l1, r1), Operator::And(l2, r2))
                    | (Operator::Or(l1, r1), Operator::Or(l2, r2))
                    | (Operator::Equal(l1, r1), Operator::Equal(l2, r2))
                    | (Operator::NotEqual(l1, r1), Operator::NotEqual(l2, r2))
                    | (Operator::LessThan(l1, r1), Operator::LessThan(l2, r2))
                    | (Operator::LessThanOrEqual(l1, r1), Operator::LessThanOrEqual(l2, r2))
                    | (Operator::GreaterThan(l1, r1), Operator::GreaterThan(l2, r2))
                    | (
                        Operator::GreaterThanOrEqual(l1, r1),
                        Operator::GreaterThanOrEqual(l2, r2),
                    )
                    | (Operator::Like(l1, r1), Operator::Like(l2, r2))
                    | (Operator::Exponentiate(l1, r1), Operator::Exponentiate(l2, r2)) => {
                        Self::expressions_equal(l1, l2) && Self::expressions_equal(r1, r2)
                    }
                    (Operator::Not(e1), Operator::Not(e2))
                    | (Operator::Negate(e1), Operator::Negate(e2))
                    | (Operator::Identity(e1), Operator::Identity(e2))
                    | (Operator::Factorial(e1), Operator::Factorial(e2)) => {
                        Self::expressions_equal(e1, e2)
                    }
                    (Operator::Is(e1, t1), Operator::Is(e2, t2)) => {
                        t1 == t2 && Self::expressions_equal(e1, e2)
                    }
                    _ => false,
                }
            }
            // Case expressions
            (
                ast::Expression::Case {
                    operand: o1,
                    when_clauses: w1,
                    else_clause: e1,
                },
                ast::Expression::Case {
                    operand: o2,
                    when_clauses: w2,
                    else_clause: e2,
                },
            ) => {
                // Check operands match (if present)
                match (o1, o2) {
                    (Some(op1), Some(op2)) if !Self::expressions_equal(op1, op2) => return false,
                    (None, None) => {}
                    _ => return false,
                }
                // Check when clauses
                if w1.len() != w2.len() {
                    return false;
                }
                for ((cond1, res1), (cond2, res2)) in w1.iter().zip(w2.iter()) {
                    if !Self::expressions_equal(cond1, cond2)
                        || !Self::expressions_equal(res1, res2)
                    {
                        return false;
                    }
                }
                // Check else clause
                match (e1, e2) {
                    (Some(else1), Some(else2)) => Self::expressions_equal(else1, else2),
                    (None, None) => true,
                    _ => false,
                }
            }
            // Different expression types don't match
            _ => false,
        }
    }
}
