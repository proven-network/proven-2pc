//! SQL query executor with PCC integration
//!
//! Executes SQL statements using the query plan, acquiring locks through
//! the storage layer's lock manager and returning results with Arc-wrapped
//! rows for efficient streaming.

use crate::error::{Error, Result};
use crate::lock::LockKey;
use crate::sql::expression::Expression as EvalExpression;
use crate::sql::parser::ast;
use crate::sql::planner::{LockRequirement, QueryPlan};
use crate::sql::schema::Table;
use crate::transaction::Transaction;
use crate::transaction_id::TransactionContext;
use crate::types::{DataType, Row, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

/// Result of executing a SQL statement
#[derive(Debug)]
pub enum ExecutionResult {
    /// SELECT query results with Arc-wrapped rows for streaming
    Select {
        columns: Vec<String>,
        rows: Vec<Arc<Row>>,
    },
    /// Number of rows affected by INSERT/UPDATE/DELETE
    Modified(usize),
    /// DDL operation completed
    DDL(String),
    /// Transaction control operation
    Transaction(String),
    /// EXPLAIN output
    Explain(String),
}

/// SQL query executor
pub struct Executor {
    /// Registered table schemas
    schemas: Arc<HashMap<String, Table>>,
    /// Track row ID mappings for primary keys (table -> pk_value_string -> row_id)
    /// We use string representation since Value doesn't implement Hash
    pk_index: HashMap<String, HashMap<String, u64>>,
}

impl Executor {
    /// Create a new executor
    pub fn new() -> Self {
        Self {
            schemas: Arc::new(HashMap::new()),
            pk_index: HashMap::new(),
        }
    }

    /// Update schemas (called when tables are created/dropped)
    pub fn update_schemas(&mut self, schemas: HashMap<String, Table>) {
        self.schemas = Arc::new(schemas);
    }
    
    /// Get a reference to the schemas for planning
    pub fn schemas(&self) -> Arc<HashMap<String, Table>> {
        self.schemas.clone()
    }

    /// Execute a query plan within a transaction
    pub fn execute(&mut self, plan: QueryPlan, tx: &mut Transaction) -> Result<ExecutionResult> {
        // Get transaction context from the transaction
        let context = &tx.context;

        // First, acquire all necessary locks according to the plan
        self.acquire_locks(&plan.locks, tx)?;

        // Then execute the statement
        match plan.statement {
            ast::Statement::Select {
                select,
                from,
                r#where,
                group_by,
                having,
                order_by,
                offset,
                limit,
            } => self.execute_select(
                select, from, r#where, group_by, having, order_by, offset, limit, tx, context,
            ),

            ast::Statement::Insert {
                table,
                columns,
                values,
            } => self.execute_insert(table, columns, values, tx, context),

            ast::Statement::Update {
                table,
                set,
                r#where,
            } => self.execute_update(table, set, r#where, tx, context),

            ast::Statement::Delete { table, r#where } => {
                self.execute_delete(table, r#where, tx, context)
            }

            ast::Statement::CreateTable { name, columns } => {
                self.execute_create_table(name, columns, tx)
            }

            ast::Statement::DropTable { name, if_exists } => {
                self.execute_drop_table(name, if_exists, tx)
            }

            ast::Statement::Begin { read_only } => Ok(ExecutionResult::Transaction(
                if read_only {
                    "BEGIN READ ONLY"
                } else {
                    "BEGIN"
                }
                .to_string(),
            )),

            ast::Statement::Commit => Ok(ExecutionResult::Transaction("COMMIT".to_string())),

            ast::Statement::Rollback => Ok(ExecutionResult::Transaction("ROLLBACK".to_string())),

            ast::Statement::Explain(stmt) => self.execute_explain(*stmt),
        }
    }

    /// Acquire locks based on the query plan
    fn acquire_locks(&mut self, requirements: &[LockRequirement], tx: &Transaction) -> Result<()> {
        for req in requirements {
            // Build lock key based on requirement
            let lock_key = if let Some(ref _keys) = req.keys {
                // Row-level lock if we have specific keys
                // For now, use table lock as fallback
                LockKey::Table {
                    table: req.table.clone(),
                }
            } else {
                // Table-level lock
                LockKey::Table {
                    table: req.table.clone(),
                }
            };

            tx.acquire_lock(lock_key, req.mode)?;
        }
        Ok(())
    }

    /// Execute SELECT statement
    fn execute_select(
        &mut self,
        select: Vec<(ast::Expression, Option<String>)>,
        from: Vec<ast::FromClause>,
        where_clause: Option<ast::Expression>,
        _group_by: Vec<ast::Expression>,
        _having: Option<ast::Expression>,
        order_by: Vec<(ast::Expression, ast::Direction)>,
        offset: Option<ast::Expression>,
        limit: Option<ast::Expression>,
        tx: &Transaction,
        context: &TransactionContext,
    ) -> Result<ExecutionResult> {
        // Extract table name from FROM clause for column resolution
        let table_name = if from.len() == 1 {
            match &from[0] {
                ast::FromClause::Table { name, .. } => Some(name.clone()),
                _ => None,
            }
        } else {
            None
        };
        
        // Get source rows from FROM clause
        let mut source_rows = self.evaluate_from(from, tx)?;

        // Apply WHERE filter
        if let Some(where_expr) = where_clause {
            source_rows = self.filter_rows(source_rows, where_expr, table_name.as_deref(), context)?;
        }

        // TODO: Implement GROUP BY and HAVING

        // Evaluate SELECT expressions
        let (columns, rows) = self.project_rows(source_rows, select, context)?;

        // Apply ORDER BY
        let rows = if !order_by.is_empty() {
            self.sort_rows(rows, order_by, context)?
        } else {
            rows
        };

        // Apply OFFSET and LIMIT
        let rows = self.apply_offset_limit(rows, offset, limit, context)?;

        Ok(ExecutionResult::Select { columns, rows })
    }

    /// Evaluate FROM clause to get source rows
    fn evaluate_from(
        &mut self,
        from: Vec<ast::FromClause>,
        tx: &Transaction,
    ) -> Result<Vec<Arc<Row>>> {
        let mut result = Vec::new();

        for from_item in from {
            match from_item {
                ast::FromClause::Table { name, .. } => {
                    // Scan the entire table
                    let rows = tx.scan(&name, None, None)?;

                    // Convert to Arc<Row> for efficient sharing
                    // rows are (row_id, Vec<Value>) tuples
                    for (row_id, values) in rows {
                        // Store row_id mapping if we have a primary key
                        if let Some(schema) = self.schemas.get(&name) {
                            if let Some(pk_value) = values.get(schema.primary_key) {
                                self.pk_index
                                    .entry(name.clone())
                                    .or_insert_with(HashMap::new)
                                    .insert(pk_value.to_string(), row_id);
                            }
                        }
                        result.push(Arc::new(values));
                    }
                }

                ast::FromClause::Join { .. } => {
                    // TODO: Implement joins
                    return Err(Error::ExecutionError("Joins not yet implemented".into()));
                }
            }
        }

        Ok(result)
    }

    /// Filter rows based on WHERE clause
    fn filter_rows(
        &self,
        rows: Vec<Arc<Row>>,
        where_expr: ast::Expression,
        table_name: Option<&str>,
        context: &TransactionContext,
    ) -> Result<Vec<Arc<Row>>> {
        let eval_expr = self.ast_to_eval_expression_with_schema(where_expr, table_name)?;
        let mut result = Vec::new();

        for row in rows {
            let value = eval_expr.evaluate(Some(&row), context)?;
            match value {
                Value::Boolean(true) => result.push(row),
                Value::Boolean(false) => {}
                Value::Null => {} // NULL is treated as false in WHERE
                _ => {
                    return Err(Error::TypeMismatch {
                        expected: "boolean".into(),
                        found: value.data_type().to_string(),
                    });
                }
            }
        }

        Ok(result)
    }

    /// Project rows based on SELECT expressions
    fn project_rows(
        &self,
        rows: Vec<Arc<Row>>,
        select: Vec<(ast::Expression, Option<String>)>,
        context: &TransactionContext,
    ) -> Result<(Vec<String>, Vec<Arc<Row>>)> {
        let mut columns = Vec::new();
        let mut result_rows = Vec::new();

        // Build column names
        for (i, (expr, alias)) in select.iter().enumerate() {
            let name = if let Some(alias) = alias {
                alias.clone()
            } else {
                match expr {
                    ast::Expression::Column(_, name) => name.clone(),
                    _ => format!("column_{}", i),
                }
            };
            columns.push(name);
        }

        // Project each row
        for row in rows {
            let mut new_row = Vec::new();

            for (expr, _) in &select {
                match expr {
                    ast::Expression::All => {
                        // SELECT * - include all columns
                        new_row.extend(row.iter().cloned());
                    }
                    _ => {
                        let eval_expr = self.ast_to_eval_expression(expr.clone())?;
                        let value = eval_expr.evaluate(Some(&row), context)?;
                        new_row.push(value);
                    }
                }
            }

            result_rows.push(Arc::new(new_row));
        }

        Ok((columns, result_rows))
    }

    /// Sort rows based on ORDER BY
    fn sort_rows(
        &self,
        mut rows: Vec<Arc<Row>>,
        order_by: Vec<(ast::Expression, ast::Direction)>,
        context: &TransactionContext,
    ) -> Result<Vec<Arc<Row>>> {
        // Convert ORDER BY expressions to evaluators
        let order_exprs: Result<Vec<_>> = order_by
            .into_iter()
            .map(|(expr, dir)| Ok((self.ast_to_eval_expression(expr)?, dir)))
            .collect();
        let order_exprs = order_exprs?;

        // Sort rows
        rows.sort_by(|a, b| {
            for (expr, direction) in &order_exprs {
                let val_a = expr.evaluate(Some(a), context).unwrap_or(Value::Null);
                let val_b = expr.evaluate(Some(b), context).unwrap_or(Value::Null);

                let ordering = val_a
                    .partial_cmp(&val_b)
                    .unwrap_or(std::cmp::Ordering::Equal);

                let ordering = match direction {
                    ast::Direction::Ascending => ordering,
                    ast::Direction::Descending => ordering.reverse(),
                };

                if ordering != std::cmp::Ordering::Equal {
                    return ordering;
                }
            }
            std::cmp::Ordering::Equal
        });

        Ok(rows)
    }

    /// Apply OFFSET and LIMIT
    fn apply_offset_limit(
        &self,
        rows: Vec<Arc<Row>>,
        offset: Option<ast::Expression>,
        limit: Option<ast::Expression>,
        context: &TransactionContext,
    ) -> Result<Vec<Arc<Row>>> {
        let offset_val = if let Some(expr) = offset {
            let eval_expr = self.ast_to_eval_expression(expr)?;
            match eval_expr.evaluate(None, context)? {
                Value::Integer(n) if n >= 0 => n as usize,
                _ => {
                    return Err(Error::ExecutionError(
                        "OFFSET must be non-negative integer".into(),
                    ));
                }
            }
        } else {
            0
        };

        let limit_val = if let Some(expr) = limit {
            let eval_expr = self.ast_to_eval_expression(expr)?;
            match eval_expr.evaluate(None, context)? {
                Value::Integer(n) if n >= 0 => Some(n as usize),
                _ => {
                    return Err(Error::ExecutionError(
                        "LIMIT must be non-negative integer".into(),
                    ));
                }
            }
        } else {
            None
        };

        let result: Vec<Arc<Row>> = rows
            .into_iter()
            .skip(offset_val)
            .take(limit_val.unwrap_or(usize::MAX))
            .collect();

        Ok(result)
    }

    /// Execute INSERT statement
    fn execute_insert(
        &mut self,
        table: String,
        columns: Option<Vec<String>>,
        values: Vec<Vec<ast::Expression>>,
        tx: &Transaction,
        context: &TransactionContext,
    ) -> Result<ExecutionResult> {
        let schema = self
            .schemas
            .get(&table)
            .ok_or_else(|| Error::TableNotFound(table.clone()))?;

        let mut count = 0;

        for value_row in values {
            // Build row from values - evaluate expressions to get Values
            let mut row: Vec<Value> = Vec::new();

            // Map column names to indices if provided
            let column_indices = if let Some(ref cols) = columns {
                // Build a mapping of column name to schema index
                let mut indices = vec![None; schema.columns.len()];
                for (i, col_name) in cols.iter().enumerate() {
                    if let Some(schema_idx) =
                        schema.columns.iter().position(|c| &c.name == col_name)
                    {
                        indices[schema_idx] = Some(i);
                    }
                }
                Some(indices)
            } else {
                None
            };

            // Build the row with proper column ordering
            if let Some(ref indices) = column_indices {
                // Specific columns provided - need to map correctly
                for (schema_idx, schema_col) in schema.columns.iter().enumerate() {
                    if let Some(value_idx) = indices[schema_idx] {
                        if let Some(expr) = value_row.get(value_idx) {
                            let eval_expr = self.ast_to_eval_expression(expr.clone())?;
                            let value = eval_expr.evaluate(None, context)?;

                            // Type check
                            if !self.is_value_compatible(&value, &schema_col.datatype)
                                && !value.is_null()
                            {
                                return Err(Error::TypeMismatch {
                                    expected: schema_col.datatype.to_string(),
                                    found: value.data_type().to_string(),
                                });
                            }

                            row.push(value);
                        } else {
                            // Use default or NULL
                            row.push(if schema_col.nullable {
                                Value::Null
                            } else {
                                return Err(Error::NullConstraintViolation(
                                    schema_col.name.clone(),
                                ));
                            });
                        }
                    } else {
                        // Column not specified - use default or NULL
                        row.push(if schema_col.nullable {
                            Value::Null
                        } else {
                            return Err(Error::NullConstraintViolation(schema_col.name.clone()));
                        });
                    }
                }
            } else {
                // All columns provided in order
                for (i, expr) in value_row.iter().enumerate() {
                    let eval_expr = self.ast_to_eval_expression(expr.clone())?;
                    let value = eval_expr.evaluate(None, context)?;

                    if let Some(schema_col) = schema.columns.get(i) {
                        // Type check
                        if !self.is_value_compatible(&value, &schema_col.datatype)
                            && !value.is_null()
                        {
                            return Err(Error::TypeMismatch {
                                expected: schema_col.datatype.to_string(),
                                found: value.data_type().to_string(),
                            });
                        }
                    }

                    row.push(value);
                }
            }

            // Validate row against schema
            schema.validate_row(&row)?;

            // Insert the row using Transaction's typed API
            let row_id = tx.insert(&table, row.clone())?;

            // Store primary key mapping
            if let Some(pk_value) = row.get(schema.primary_key) {
                self.pk_index
                    .entry(table.clone())
                    .or_insert_with(HashMap::new)
                    .insert(pk_value.to_string(), row_id);
            }

            count += 1;
        }

        Ok(ExecutionResult::Modified(count))
    }

    /// Execute UPDATE statement
    fn execute_update(
        &mut self,
        table: String,
        set: BTreeMap<String, Option<ast::Expression>>,
        where_clause: Option<ast::Expression>,
        tx: &Transaction,
        context: &TransactionContext,
    ) -> Result<ExecutionResult> {
        let schema = self
            .schemas
            .get(&table)
            .ok_or_else(|| Error::TableNotFound(table.clone()))?;

        // Get all rows from the table
        let rows = tx.scan(&table, None, None)?;

        let mut count = 0;

        for (row_id, mut values) in rows {
            // Check WHERE clause
            if let Some(ref where_expr) = where_clause {
                let eval_expr = self.ast_to_eval_expression(where_expr.clone())?;
                let matches = eval_expr.evaluate(Some(&values), context)?;

                match matches {
                    Value::Boolean(true) => {}
                    _ => continue, // Skip this row
                }
            }

            // Apply updates
            for (col_name, expr) in &set {
                if let Some(col_idx) = schema.columns.iter().position(|c| &c.name == col_name) {
                    let new_value = if let Some(expr) = expr {
                        let eval_expr = self.ast_to_eval_expression(expr.clone())?;
                        eval_expr.evaluate(Some(&values), context)?
                    } else {
                        Value::Null
                    };

                    // Type check
                    if !self.is_value_compatible(&new_value, &schema.columns[col_idx].datatype)
                        && !new_value.is_null()
                    {
                        return Err(Error::TypeMismatch {
                            expected: schema.columns[col_idx].datatype.to_string(),
                            found: new_value.data_type().to_string(),
                        });
                    }

                    if col_idx < values.len() {
                        values[col_idx] = new_value;
                    }
                }
            }

            // Validate updated row
            schema.validate_row(&values)?;

            // Write back using Transaction's typed API
            tx.write(&table, row_id, values)?;
            count += 1;
        }

        Ok(ExecutionResult::Modified(count))
    }

    /// Execute DELETE statement
    fn execute_delete(
        &mut self,
        table: String,
        where_clause: Option<ast::Expression>,
        tx: &Transaction,
        context: &TransactionContext,
    ) -> Result<ExecutionResult> {
        // Get all rows from the table
        let rows = tx.scan(&table, None, None)?;

        let mut count = 0;
        let mut rows_to_delete = Vec::new();

        for (row_id, values) in rows {
            // Check WHERE clause
            if let Some(ref where_expr) = where_clause {
                let eval_expr = self.ast_to_eval_expression(where_expr.clone())?;
                let matches = eval_expr.evaluate(Some(&values), context)?;

                match matches {
                    Value::Boolean(true) => {}
                    _ => continue, // Skip this row
                }
            }

            rows_to_delete.push(row_id);
            count += 1;
        }

        // Delete the rows using Transaction's typed API
        for row_id in rows_to_delete {
            tx.delete(&table, row_id)?;
        }

        Ok(ExecutionResult::Modified(count))
    }

    /// Execute CREATE TABLE statement
    fn execute_create_table(
        &mut self,
        name: String,
        columns: Vec<ast::Column>,
        _tx: &Transaction,
    ) -> Result<ExecutionResult> {
        // Check if table already exists
        if self.schemas.contains_key(&name) {
            return Err(Error::ExecutionError(format!(
                "Table '{}' already exists",
                name
            )));
        }

        // Convert AST columns to schema columns
        let mut schema_columns = Vec::new();
        let mut primary_key_idx = None;

        for (i, col) in columns.iter().enumerate() {
            let mut schema_col =
                crate::sql::schema::Column::new(col.name.clone(), col.datatype.clone());

            if col.primary_key {
                if primary_key_idx.is_some() {
                    return Err(Error::ExecutionError(
                        "Multiple primary keys not supported".into(),
                    ));
                }
                primary_key_idx = Some(i);
                schema_col = schema_col.primary_key();
            }

            if let Some(is_nullable) = col.nullable {
                schema_col = schema_col.nullable(is_nullable);
            }

            if col.unique {
                schema_col = schema_col.unique();
            }

            schema_columns.push(schema_col);
        }

        // Create table schema
        let _table = Table::new(name.clone(), schema_columns)?;

        // TODO: Store schema persistently
        // For now, we would need to update the schemas through update_schemas

        Ok(ExecutionResult::DDL(format!("Table '{}' created", name)))
    }

    /// Execute DROP TABLE statement
    fn execute_drop_table(
        &mut self,
        name: String,
        if_exists: bool,
        tx: &Transaction,
    ) -> Result<ExecutionResult> {
        // Check if table exists
        if !self.schemas.contains_key(&name) {
            if if_exists {
                return Ok(ExecutionResult::DDL(format!(
                    "Table '{}' does not exist",
                    name
                )));
            } else {
                return Err(Error::TableNotFound(name));
            }
        }

        // Delete all rows in the table
        let rows = tx.scan(&name, None, None)?;

        for (row_id, _) in rows {
            tx.delete(&name, row_id)?;
        }

        // TODO: Remove schema persistently
        // For now, would need to update schemas through update_schemas

        Ok(ExecutionResult::DDL(format!("Table '{}' dropped", name)))
    }

    /// Execute EXPLAIN statement
    fn execute_explain(&self, _statement: ast::Statement) -> Result<ExecutionResult> {
        // TODO: Implement proper EXPLAIN output
        Ok(ExecutionResult::Explain(
            "Query plan not yet implemented".to_string(),
        ))
    }

    /// Check if a value is compatible with a data type
    fn is_value_compatible(&self, value: &Value, datatype: &DataType) -> bool {
        match (value, datatype) {
            (Value::Null, _) => true, // NULL is compatible with any type
            (Value::Boolean(_), DataType::Boolean) => true,
            (Value::Integer(_), DataType::Integer) => true,
            (Value::Decimal(_), DataType::Decimal(_, _)) => true,
            (Value::String(_), DataType::String) => true,
            (Value::Blob(_), DataType::Blob) => true,
            (Value::Timestamp(_), DataType::Timestamp) => true,
            (Value::Uuid(_), DataType::Uuid) => true,
            // Allow some implicit conversions
            (Value::Integer(_), DataType::Decimal(_, _)) => true, // Integer can be decimal
            _ => false,
        }
    }

    /// Convert AST expression to evaluation expression  
    fn ast_to_eval_expression(&self, expr: ast::Expression) -> Result<EvalExpression> {
        self.ast_to_eval_expression_with_schema(expr, None)
    }
    
    /// Convert AST expression to evaluation expression with schema context
    fn ast_to_eval_expression_with_schema(&self, expr: ast::Expression, table_name: Option<&str>) -> Result<EvalExpression> {
        match expr {
            ast::Expression::Literal(lit) => {
                let value = match lit {
                    ast::Literal::Null => Value::Null,
                    ast::Literal::Boolean(b) => Value::Boolean(b),
                    ast::Literal::Integer(i) => Value::Integer(i),
                    ast::Literal::Float(f) => Value::Decimal(
                        rust_decimal::Decimal::from_f64_retain(f)
                            .ok_or_else(|| Error::InvalidValue("Invalid decimal".into()))?,
                    ),
                    ast::Literal::String(s) => Value::String(s),
                };
                Ok(EvalExpression::Constant(value))
            }

            ast::Expression::Column(_table_ref, column_name) => {
                // Try to resolve column name to index if we have the schema
                if let Some(tbl) = table_name {
                    if let Some(schema) = self.schemas.get(tbl) {
                        // Find column index
                        for (i, col) in schema.columns.iter().enumerate() {
                            if col.name == column_name {
                                return Ok(EvalExpression::Column(i));
                            }
                        }
                        return Err(Error::ColumnNotFound(column_name));
                    }
                }
                // If no schema context, try to find in any table (for simple queries)
                // This is a fallback - ideally we'd always have the table context
                for schema in self.schemas.values() {
                    for (i, col) in schema.columns.iter().enumerate() {
                        if col.name == column_name {
                            return Ok(EvalExpression::Column(i));
                        }
                    }
                }
                Err(Error::ColumnNotFound(column_name))
            }

            ast::Expression::Function(name, args) => {
                let eval_args: Result<Vec<_>> = args
                    .into_iter()
                    .map(|arg| self.ast_to_eval_expression_with_schema(arg, table_name))
                    .collect();
                Ok(EvalExpression::Function(name, eval_args?))
            }

            ast::Expression::Operator(op) => self.ast_operator_to_eval_expression_with_schema(op, table_name),

            ast::Expression::All => {
                // This shouldn't appear in WHERE/expressions
                Err(Error::ExecutionError("* not valid in this context".into()))
            }
        }
    }

    /// Convert AST operator to evaluation expression
    fn ast_operator_to_eval_expression(&self, op: ast::Operator) -> Result<EvalExpression> {
        self.ast_operator_to_eval_expression_with_schema(op, None)
    }
    
    /// Convert AST operator to evaluation expression with schema context
    fn ast_operator_to_eval_expression_with_schema(&self, op: ast::Operator, table_name: Option<&str>) -> Result<EvalExpression> {
        use ast::Operator::*;

        Ok(match op {
            And(l, r) => EvalExpression::And(
                Box::new(self.ast_to_eval_expression_with_schema(*l, table_name)?),
                Box::new(self.ast_to_eval_expression_with_schema(*r, table_name)?),
            ),
            Or(l, r) => EvalExpression::Or(
                Box::new(self.ast_to_eval_expression_with_schema(*l, table_name)?),
                Box::new(self.ast_to_eval_expression_with_schema(*r, table_name)?),
            ),
            Not(e) => EvalExpression::Not(Box::new(self.ast_to_eval_expression_with_schema(*e, table_name)?)),
            Equal(l, r) => EvalExpression::Equal(
                Box::new(self.ast_to_eval_expression_with_schema(*l, table_name)?),
                Box::new(self.ast_to_eval_expression_with_schema(*r, table_name)?),
            ),
            NotEqual(l, r) => EvalExpression::NotEqual(
                Box::new(self.ast_to_eval_expression_with_schema(*l, table_name)?),
                Box::new(self.ast_to_eval_expression_with_schema(*r, table_name)?),
            ),
            GreaterThan(l, r) => EvalExpression::GreaterThan(
                Box::new(self.ast_to_eval_expression_with_schema(*l, table_name)?),
                Box::new(self.ast_to_eval_expression_with_schema(*r, table_name)?),
            ),
            GreaterThanOrEqual(l, r) => EvalExpression::GreaterThanOrEqual(
                Box::new(self.ast_to_eval_expression_with_schema(*l, table_name)?),
                Box::new(self.ast_to_eval_expression_with_schema(*r, table_name)?),
            ),
            LessThan(l, r) => EvalExpression::LessThan(
                Box::new(self.ast_to_eval_expression_with_schema(*l, table_name)?),
                Box::new(self.ast_to_eval_expression_with_schema(*r, table_name)?),
            ),
            LessThanOrEqual(l, r) => EvalExpression::LessThanOrEqual(
                Box::new(self.ast_to_eval_expression_with_schema(*l, table_name)?),
                Box::new(self.ast_to_eval_expression_with_schema(*r, table_name)?),
            ),
            Is(e, lit) => {
                let value = match lit {
                    ast::Literal::Null => Value::Null,
                    _ => return Err(Error::ExecutionError("IS only supports NULL".into())),
                };
                EvalExpression::Is(Box::new(self.ast_to_eval_expression_with_schema(*e, table_name)?), value)
            }
            Add(l, r) => EvalExpression::Add(
                Box::new(self.ast_to_eval_expression_with_schema(*l, table_name)?),
                Box::new(self.ast_to_eval_expression_with_schema(*r, table_name)?),
            ),
            Subtract(l, r) => EvalExpression::Subtract(
                Box::new(self.ast_to_eval_expression_with_schema(*l, table_name)?),
                Box::new(self.ast_to_eval_expression_with_schema(*r, table_name)?),
            ),
            Multiply(l, r) => EvalExpression::Multiply(
                Box::new(self.ast_to_eval_expression_with_schema(*l, table_name)?),
                Box::new(self.ast_to_eval_expression_with_schema(*r, table_name)?),
            ),
            Divide(l, r) => EvalExpression::Divide(
                Box::new(self.ast_to_eval_expression_with_schema(*l, table_name)?),
                Box::new(self.ast_to_eval_expression_with_schema(*r, table_name)?),
            ),
            Remainder(l, r) => EvalExpression::Remainder(
                Box::new(self.ast_to_eval_expression_with_schema(*l, table_name)?),
                Box::new(self.ast_to_eval_expression_with_schema(*r, table_name)?),
            ),
            Exponentiate(l, r) => EvalExpression::Exponentiate(
                Box::new(self.ast_to_eval_expression_with_schema(*l, table_name)?),
                Box::new(self.ast_to_eval_expression_with_schema(*r, table_name)?),
            ),
            Factorial(e) => EvalExpression::Factorial(Box::new(self.ast_to_eval_expression_with_schema(*e, table_name)?)),
            Identity(e) => EvalExpression::Identity(Box::new(self.ast_to_eval_expression_with_schema(*e, table_name)?)),
            Negate(e) => EvalExpression::Negate(Box::new(self.ast_to_eval_expression_with_schema(*e, table_name)?)),
            Like(l, r) => EvalExpression::Like(
                Box::new(self.ast_to_eval_expression_with_schema(*l, table_name)?),
                Box::new(self.ast_to_eval_expression_with_schema(*r, table_name)?),
            ),
        })
    }
}

#[cfg(test)]
#[path = "integration_tests.rs"]
mod tests;
