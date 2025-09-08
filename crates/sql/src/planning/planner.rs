//! Query planner that converts AST to execution plan nodes
//!
//! This planner resolves columns during planning and builds an
//! optimized execution plan tree.

use super::optimizer::Optimizer;
use super::plan::{AggregateFunc, Direction, JoinType, Node, Plan};
use crate::error::{Error, Result};
use crate::parsing::ast;
use crate::types::expression::Expression;
use crate::types::schema::Table;
use crate::types::statistics::DatabaseStatistics;
use std::collections::{BTreeMap, HashMap};

/// Index metadata for planning
#[derive(Debug, Clone)]
pub struct IndexInfo {
    pub name: String,
    pub table: String,
    pub columns: Vec<String>,
    pub unique: bool,
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
    pub fn new(schemas: HashMap<String, Table>) -> Self {
        let optimizer = Optimizer::new(schemas.clone());

        // Initialize indexes from schema columns
        let mut indexes = HashMap::new();
        for (table_name, schema) in &schemas {
            let mut table_indexes = Vec::new();
            for column in &schema.columns {
                if column.primary_key || column.unique || column.index {
                    table_indexes.push(IndexInfo {
                        name: column.name.clone(),
                        table: table_name.clone(),
                        columns: vec![column.name.clone()],
                        unique: column.primary_key || column.unique,
                    });
                }
            }
            if !table_indexes.is_empty() {
                indexes.insert(table_name.clone(), table_indexes);
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
                values,
            } => self.plan_insert(table, columns, values),

            ast::Statement::Update {
                table,
                set,
                r#where,
            } => self.plan_update(table, set, r#where),

            ast::Statement::Delete { table, r#where } => self.plan_delete(table, r#where),

            ast::Statement::CreateTable { name, columns } => self.plan_create_table(name, columns),

            ast::Statement::DropTable { name, if_exists } => {
                Ok(Plan::DropTable { name, if_exists })
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
                // Verify all columns exist
                let schema = &self.schemas[&table];
                for column in &columns {
                    if !schema.columns.iter().any(|c| &c.name == column) {
                        return Err(Error::ColumnNotFound(column.clone()));
                    }
                }
                Ok(Plan::CreateIndex {
                    name,
                    table,
                    columns,
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

            // Check if ORDER BY matches an existing index
            if let Node::Scan { ref table, .. } = node
                && let Some(table_indexes) = self.indexes.get(table)
            {
                for index in table_indexes {
                    // Check if ORDER BY columns match index columns (in order)
                    if self.order_by_matches_index(&order_by, &index.columns, table, &context) {
                        // TODO: Transform node to use IndexScan with the matching index
                        // For now, we'll mark that we could use it
                        can_use_index = true;
                        break;
                    }
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

            for (expr, alias) in select {
                if self.is_aggregate_expr(&expr) {
                    // For aggregate functions, project the corresponding aggregate result column
                    expressions.push(Expression::Column(col_idx));
                    col_idx += 1;

                    // Generate alias for aggregate function
                    let func_alias = alias.or_else(|| {
                        if let ast::Expression::Function(name, _) = &expr {
                            Some(name.to_uppercase())
                        } else {
                            None
                        }
                    });
                    aliases.push(func_alias);
                } else {
                    // For GROUP BY expressions, project from the beginning columns
                    expressions.push(context.resolve_expression(expr)?);
                    aliases.push(alias);
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
            return Ok(Node::Nothing);
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
                            predicate: Expression::Constant(crate::types::value::Value::Boolean(
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
                            predicate: Expression::Constant(crate::types::value::Value::Boolean(
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
                            predicate: Expression::Constant(crate::types::value::Value::Boolean(
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
        values: Vec<Vec<ast::Expression>>,
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

        Ok(Plan::Insert {
            table,
            columns: column_indices,
            source: Box::new(Node::Values { rows }),
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
    fn plan_create_table(&self, name: String, columns: Vec<ast::Column>) -> Result<Plan> {
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
                    "COUNT" | "SUM" | "AVG" | "MIN" | "MAX"
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
                    Expression::Constant(crate::types::value::Value::Integer(1)) // For COUNT(*)
                } else if args.len() == 1 && matches!(args[0], ast::Expression::All) {
                    // Handle COUNT(*) - the * is parsed as Expression::All
                    Expression::Constant(crate::types::value::Value::Integer(1))
                } else {
                    context.resolve_expression(args[0].clone())?
                };

                let agg = match func_name.as_str() {
                    "COUNT" => AggregateFunc::Count(arg),
                    "SUM" => AggregateFunc::Sum(arg),
                    "AVG" => AggregateFunc::Avg(arg),
                    "MIN" => AggregateFunc::Min(arg),
                    "MAX" => AggregateFunc::Max(arg),
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
            ast::Expression::Literal(ast::Literal::Integer(n)) if n >= 0 => Ok(n as usize),
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
                        let value = crate::types::value::Value::Integer(*i);

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
                    if let Some((col, expr)) = conditions.iter().find(|(col, _)| col == index_col) {
                        // Try to resolve the expression
                        if let Ok(value) = context.resolve_expression(expr.clone()) {
                            matched_values.push(value);
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
        index_columns: &[String],
        _table_name: &str,
        _context: &PlanContext,
    ) -> bool {
        // ORDER BY must have same or fewer columns than index
        if order_by.len() > index_columns.len() {
            return false;
        }

        // Check each ORDER BY column matches the corresponding index column
        for (i, (expr, dir)) in order_by.iter().enumerate() {
            // For now, only support ASC order (most indexes are ASC by default)
            if !matches!(dir, ast::Direction::Ascending) {
                return false;
            }

            // Check if expression is a simple column reference
            if let ast::Expression::Column(None, col_name) = expr {
                // Check if it matches the index column at this position
                if &index_columns[i] != col_name {
                    return false;
                }
            } else {
                // Not a simple column reference
                return false;
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
                            },
                            ast::Operator::GreaterThanOrEqual(..) => Node::IndexRangeScan {
                                table: table_name.to_string(),
                                alias: alias.clone(),
                                index_name: column_name.to_string(),
                                start: Some(vec![value]),
                                start_inclusive: true,
                                end: None,
                                end_inclusive: false,
                            },
                            ast::Operator::LessThan(..) => Node::IndexRangeScan {
                                table: table_name.to_string(),
                                alias: alias.clone(),
                                index_name: column_name.to_string(),
                                start: None,
                                start_inclusive: false,
                                end: Some(vec![value]),
                                end_inclusive: false,
                            },
                            ast::Operator::LessThanOrEqual(..) => Node::IndexRangeScan {
                                table: table_name.to_string(),
                                alias: alias.clone(),
                                index_name: column_name.to_string(),
                                start: None,
                                start_inclusive: false,
                                end: Some(vec![value]),
                                end_inclusive: true,
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
                    ast::Literal::Boolean(b) => crate::types::value::Value::Boolean(b),
                    ast::Literal::Integer(i) => crate::types::value::Value::Integer(i),
                    ast::Literal::Float(f) => crate::types::value::Value::Decimal(
                        rust_decimal::Decimal::from_f64_retain(f)
                            .ok_or_else(|| Error::InvalidValue("Invalid decimal".into()))?,
                    ),
                    ast::Literal::String(s) => crate::types::value::Value::String(s),
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

            ast::Expression::All => {
                Err(Error::ExecutionError("* not valid in this context".into()))
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
        })
    }
}
