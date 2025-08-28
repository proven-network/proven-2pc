//! Query planner that converts AST to execution plan nodes
//!
//! This planner resolves columns during planning and builds an
//! optimized execution plan tree.

use super::optimizer::Optimizer;
use super::plan::{AggregateFunc, Direction, JoinType, Node, Plan};
use crate::error::{Error, Result};
use crate::parser::ast;
use crate::types::expression::Expression;
use crate::types::schema::Table;
use std::collections::{BTreeMap, HashMap};

/// Query planner
pub struct Planner {
    schemas: HashMap<String, Table>,
    optimizer: Optimizer,
}

impl Planner {
    /// Create a new planner
    pub fn new(schemas: HashMap<String, Table>) -> Self {
        let optimizer = Optimizer::new(schemas.clone());
        Self { schemas, optimizer }
    }

    /// Plan a statement
    pub fn plan(&self, statement: ast::Statement) -> Result<Plan> {
        match statement {
            ast::Statement::Select {
                select,
                from,
                r#where,
                group_by,
                having,
                order_by,
                offset,
                limit,
            } => self.plan_select(
                select, from, r#where, group_by, having, order_by, offset, limit,
            ),

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
                column,
                unique,
            } => {
                // Verify table exists
                if !self.schemas.contains_key(&table) {
                    return Err(Error::TableNotFound(table));
                }
                // Verify column exists
                let schema = &self.schemas[&table];
                if !schema.columns.iter().any(|c| c.name == column) {
                    return Err(Error::ColumnNotFound(column));
                }
                Ok(Plan::CreateIndex {
                    name,
                    table,
                    column,
                    unique,
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

        // Apply WHERE filter
        if let Some(where_expr) = r#where {
            let predicate = context.resolve_expression(where_expr)?;
            node = Node::Filter {
                source: Box::new(node),
                predicate,
            };
        }

        // Apply GROUP BY and aggregates
        if !group_by.is_empty() || self.has_aggregates(&select) {
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
        if !order_by.is_empty() {
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

        // Apply projection after ORDER BY
        let (expressions, aliases) = self.plan_projection(select, &mut context)?;
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

                ast::FromClause::Join { .. } => {
                    return Err(Error::ExecutionError(
                        "Joins not yet fully implemented".into(),
                    ));
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
                    .ok_or_else(|| Error::ColumnNotFound(col))?;

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
                        if let Some(schema) = self.schemas.get(&table.name) {
                            if schema.columns.iter().any(|c| c.name == column_name) {
                                return self.resolve_column_in_table(table, &column_name);
                            }
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
