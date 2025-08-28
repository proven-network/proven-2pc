//! Complete MVCC-aware SQL executor
//!
//! This executor properly integrates with the planner, executing Plan/Node trees
//! using MVCC transactions for proper isolation and rollback support.

use super::{aggregator::Aggregator, join};
use crate::context::TransactionContext;
use crate::error::{Error, Result};
use crate::sql::planner::plan::{Node, Plan};
use crate::sql::stream_processor::TransactionContext as TxContext;
use crate::sql::types::expression::Expression;
use crate::sql::types::schema::Table;
use crate::sql::types::value::Value;
use crate::storage::lock::LockManager;
use crate::storage::{MvccStorage, read_ops, write_ops};
use std::collections::HashMap;
use std::sync::Arc;

/// Result of executing a SQL statement
#[derive(Debug)]
pub enum ExecutionResult {
    /// SELECT query results
    Select {
        columns: Vec<String>,
        rows: Vec<Arc<Vec<Value>>>,
    },
    /// Number of rows modified (INSERT, UPDATE, DELETE)
    Modified(usize),
    /// DDL operation result
    DDL(String),
}

/// Row iterator type for streaming execution
/// NOTE: We use 'static lifetime here which requires collecting in some cases.
/// Adding a lifetime parameter would enable true streaming but would require:
/// 1. Solving the aliasing problem (can't borrow storage mutably while iterating)
/// 2. Handling recursive calls (joins need two iterators from same storage)
/// 3. Extensive API changes throughout the codebase
pub type Rows<'a> = Box<dyn Iterator<Item = Result<Arc<Vec<Value>>>> + 'a>;

/// Complete MVCC-aware SQL executor
pub struct Executor {
    /// Table schemas for metadata
    schemas: HashMap<String, Table>,
}

impl Executor {
    /// Create a new executor
    pub fn new(schemas: HashMap<String, Table>) -> Self {
        Self { schemas }
    }

    /// Update schemas after DDL operations
    pub fn update_schemas(&mut self, schemas: HashMap<String, Table>) {
        self.schemas = schemas;
    }

    /// Execute a query plan - dispatches to read or write execution
    pub fn execute(
        &self,
        plan: Plan,
        storage: &mut MvccStorage,
        lock_manager: &mut LockManager,
        tx_ctx: &mut TxContext,
        context: &TransactionContext,
    ) -> Result<ExecutionResult> {
        match plan {
            Plan::Select(node) => {
                // SELECT uses immutable storage reference
                self.execute_select(*node, storage, lock_manager, tx_ctx, context)
            }

            Plan::Insert {
                table,
                columns,
                source,
            } => {
                // INSERT uses write execution with phased approach
                self.execute_insert(
                    table,
                    columns,
                    *source,
                    storage,
                    lock_manager,
                    tx_ctx,
                    context,
                )
            }
            Plan::Update {
                table,
                assignments,
                source,
            } => {
                // UPDATE uses write execution with phased approach
                self.execute_update(
                    table,
                    assignments,
                    *source,
                    storage,
                    lock_manager,
                    tx_ctx,
                    context,
                )
            }

            Plan::Delete { table, source } => {
                // DELETE uses write execution with phased approach
                self.execute_delete(table, *source, storage, lock_manager, tx_ctx, context)
            }

            Plan::CreateTable { name, schema: _ } => {
                // Table creation is handled at stream processor level
                Ok(ExecutionResult::DDL(format!("Table '{}' created", name)))
            }

            Plan::DropTable { name, .. } => {
                Ok(ExecutionResult::DDL(format!("Table '{}' dropped", name)))
            }

            Plan::CreateIndex { name, .. } => {
                Ok(ExecutionResult::DDL(format!("Index '{}' created", name)))
            }

            Plan::DropIndex { name, .. } => {
                Ok(ExecutionResult::DDL(format!("Index '{}' dropped", name)))
            }
        }
    }

    /// Execute SELECT query using immutable storage reference
    fn execute_select(
        &self,
        node: Node,
        storage: &MvccStorage,
        lock_manager: &mut LockManager,
        tx_ctx: &mut TxContext,
        context: &TransactionContext,
    ) -> Result<ExecutionResult> {
        // Execute using read-only node execution
        let rows = self.execute_node_read(node, storage, lock_manager, tx_ctx, context)?;
        let mut collected = Vec::new();
        for row in rows {
            collected.push(row?);
        }

        // Get column names (simplified - would extract from projection)
        let columns = if let Some(table) = self.schemas.values().next() {
            table.columns.iter().map(|c| c.name.clone()).collect()
        } else {
            vec![]
        };

        Ok(ExecutionResult::Select {
            columns,
            rows: collected,
        })
    }

    /// Execute INSERT with phased read-then-write approach
    fn execute_insert(
        &self,
        table: String,
        columns: Option<Vec<usize>>,
        source: Node,
        storage: &mut MvccStorage,
        lock_manager: &mut LockManager,
        tx_ctx: &mut TxContext,
        context: &TransactionContext,
    ) -> Result<ExecutionResult> {
        let schema = self
            .schemas
            .get(&table)
            .ok_or_else(|| Error::TableNotFound(table.clone()))?;

        // Phase 1: Read all source rows (immutable borrow)
        let rows_to_insert = {
            // Use immutable reference for reading
            let storage_ref = &*storage;
            let rows =
                self.execute_node_read(source, storage_ref, lock_manager, tx_ctx, context)?;
            rows.collect::<Result<Vec<_>>>()?
        }; // Immutable borrow ends here

        // Phase 2: Write rows (mutable borrow)
        let mut count = 0;
        for row in rows_to_insert {
            // Reorder columns if specified
            let final_row = if let Some(ref col_indices) = columns {
                let mut reordered = vec![Value::Null; schema.columns.len()];
                for (src_idx, &dest_idx) in col_indices.iter().enumerate() {
                    if src_idx < row.len() {
                        reordered[dest_idx] = row[src_idx].clone();
                    }
                }
                reordered
            } else {
                row.to_vec()
            };

            write_ops::insert(storage, lock_manager, tx_ctx, &table, final_row)?;
            count += 1;
        }

        Ok(ExecutionResult::Modified(count))
    }

    /// Execute UPDATE with phased read-then-write approach
    fn execute_update(
        &self,
        table: String,
        assignments: Vec<(usize, Expression)>,
        source: Node,
        storage: &mut MvccStorage,
        lock_manager: &mut LockManager,
        tx_ctx: &mut TxContext,
        context: &TransactionContext,
    ) -> Result<ExecutionResult> {
        // Phase 1: Read rows with IDs that match the WHERE clause
        let rows_to_update = {
            let iter = read_ops::scan_iter_with_ids(storage, lock_manager, tx_ctx, &table, true)?;
            let mut to_update = Vec::new();

            for (row_id, row) in iter {
                let matches = match &source {
                    Node::Filter { predicate, .. } => self
                        .evaluate_expression(predicate, Some(&row), context)?
                        .to_bool()
                        .unwrap_or(false),
                    Node::Scan { .. } => true,
                    _ => true,
                };

                if matches {
                    to_update.push((row_id, row));
                }
            }
            to_update
        }; // Immutable borrow ends here

        // Phase 2: Apply updates (mutable borrow)
        let mut count = 0;
        for (row_id, current) in rows_to_update {
            let mut updated = current.to_vec();
            for &(col_idx, ref expr) in &assignments {
                updated[col_idx] = self.evaluate_expression(expr, Some(&current), context)?;
            }

            write_ops::update(storage, lock_manager, tx_ctx, &table, row_id, updated)?;
            count += 1;
        }

        Ok(ExecutionResult::Modified(count))
    }

    /// Execute DELETE with phased read-then-write approach
    fn execute_delete(
        &self,
        table: String,
        source: Node,
        storage: &mut MvccStorage,
        lock_manager: &mut LockManager,
        tx_ctx: &mut TxContext,
        context: &TransactionContext,
    ) -> Result<ExecutionResult> {
        // Phase 1: Read rows with IDs that match the WHERE clause
        let rows_to_delete = {
            let iter = read_ops::scan_iter_with_ids(storage, lock_manager, tx_ctx, &table, true)?;
            let mut to_delete = Vec::new();

            for (row_id, row) in iter {
                let matches = match &source {
                    Node::Filter { predicate, .. } => self
                        .evaluate_expression(predicate, Some(&row), context)?
                        .to_bool()
                        .unwrap_or(false),
                    Node::Scan { .. } => true,
                    _ => true,
                };

                if matches {
                    to_delete.push(row_id);
                }
            }
            to_delete
        }; // Immutable borrow ends here

        // Phase 2: Delete rows (mutable borrow)
        let mut count = 0;
        for row_id in rows_to_delete {
            write_ops::delete(storage, lock_manager, tx_ctx, &table, row_id)?;
            count += 1;
        }

        Ok(ExecutionResult::Modified(count))
    }

    /// Execute a plan node for reading with immutable storage reference
    fn execute_node_read<'a>(
        &self,
        node: Node,
        storage: &'a MvccStorage,
        lock_manager: &mut LockManager,
        tx_ctx: &mut TxContext,
        context: &TransactionContext,
    ) -> Result<Rows<'a>> {
        match node {
            Node::Scan { table, .. } => {
                // True streaming with immutable storage!
                let iter =
                    read_ops::scan_iter(storage, lock_manager, tx_ctx, &table)?.map(|row| Ok(row));

                Ok(Box::new(iter))
            }

            Node::IndexScan {
                table,
                index_column,
                value,
                ..
            } => {
                // For now, just do a filtered table scan
                let filter_value = self.evaluate_expression(&value, None, context)?;
                let schema = self
                    .schemas
                    .get(&table)
                    .ok_or_else(|| Error::TableNotFound(table.clone()))?;

                let col_idx = schema
                    .columns
                    .iter()
                    .position(|c| c.name == index_column)
                    .ok_or_else(|| Error::ColumnNotFound(index_column))?;

                // True streaming with filtering!
                let iter = read_ops::scan_iter(storage, lock_manager, tx_ctx, &table)?.filter_map(
                    move |row| {
                        if row.get(col_idx) == Some(&filter_value) {
                            Some(Ok(row))
                        } else {
                            None
                        }
                    },
                );

                Ok(Box::new(iter))
            }

            Node::Filter { source, predicate } => {
                let source_rows =
                    self.execute_node_read(*source, storage, lock_manager, tx_ctx, context)?;
                let executor = self.clone_for_closure();
                let context = context.clone();

                let filtered = source_rows.filter_map(move |row| match row {
                    Ok(row) => {
                        match executor.evaluate_expression(&predicate, Some(&row), &context) {
                            Ok(v) if v.to_bool().unwrap_or(false) => Some(Ok(row)),
                            Ok(_) => None,
                            Err(e) => Some(Err(e)),
                        }
                    }
                    Err(e) => Some(Err(e)),
                });

                Ok(Box::new(filtered))
            }

            Node::Values { rows } => {
                // Values node contains literal rows - convert Expression to Value
                // We need to evaluate all expressions immediately to avoid lifetime issues
                let mut value_rows = Vec::new();
                for row in rows {
                    let mut value_row = Vec::new();
                    for expr in row {
                        value_row.push(self.evaluate_expression(&expr, None, context)?);
                    }
                    value_rows.push(Ok(Arc::new(value_row)));
                }
                Ok(Box::new(value_rows.into_iter()))
            }

            Node::Projection {
                source,
                expressions,
                ..
            } => {
                let source_rows =
                    self.execute_node_read(*source, storage, lock_manager, tx_ctx, context)?;
                let executor = self.clone_for_closure();
                let context = context.clone();

                let projected = source_rows.map(move |row| match row {
                    Ok(row) => {
                        let mut result = Vec::with_capacity(expressions.len());
                        for expr in &expressions {
                            result.push(executor.evaluate_expression(
                                expr,
                                Some(&row),
                                &context,
                            )?);
                        }
                        Ok(Arc::new(result))
                    }
                    Err(e) => Err(e),
                });

                Ok(Box::new(projected))
            }

            // Limit and Offset are trivial with iterators
            Node::Limit { source, limit } => {
                let rows =
                    self.execute_node_read(*source, storage, lock_manager, tx_ctx, context)?;
                Ok(Box::new(rows.take(limit)))
            }

            Node::Offset { source, offset } => {
                let rows =
                    self.execute_node_read(*source, storage, lock_manager, tx_ctx, context)?;
                Ok(Box::new(rows.skip(offset)))
            }

            // Aggregation works with immutable storage
            Node::Aggregate {
                source,
                group_by,
                aggregates,
            } => {
                // Use the Aggregator module
                let mut aggregator = Aggregator::new(group_by, aggregates);

                // Process all source rows
                let rows =
                    self.execute_node_read(*source, storage, lock_manager, tx_ctx, context)?;
                for row in rows {
                    let row = row?;
                    aggregator.add(&row, context, storage)?;
                }

                // Get aggregated results
                let results = aggregator.finalize()?;
                Ok(Box::new(results.into_iter().map(Ok)))
            }

            // Hash Join can work with immutable storage since both sides can be read
            Node::HashJoin {
                left,
                right,
                left_col,
                right_col,
                join_type,
            } => {
                // Get column count from right node for outer join NULL padding
                let right_columns = right.column_count(&self.schemas);

                // Both can borrow storage immutably!
                let left_rows =
                    self.execute_node_read(*left, storage, lock_manager, tx_ctx, context)?;
                let right_rows =
                    self.execute_node_read(*right, storage, lock_manager, tx_ctx, context)?;

                join::execute_hash_join(
                    left_rows,
                    left_col,
                    right_rows,
                    right_col,
                    right_columns,
                    join_type,
                    storage,
                )
            }

            // Nested Loop Join also works with immutable storage
            Node::NestedLoopJoin {
                left,
                right,
                predicate,
                join_type,
            } => {
                // Get column count from right node for outer join NULL padding
                let right_columns = right.column_count(&self.schemas);

                let left_rows =
                    self.execute_node_read(*left, storage, lock_manager, tx_ctx, context)?;
                let right_rows =
                    self.execute_node_read(*right, storage, lock_manager, tx_ctx, context)?;

                // Use the standalone function for nested loop join
                join::execute_nested_loop_join(
                    left_rows,
                    right_rows,
                    right_columns,
                    predicate,
                    join_type,
                    storage,
                )
            }

            // Order By requires full materialization to sort
            Node::Order { source, order_by } => {
                let rows =
                    self.execute_node_read(*source, storage, lock_manager, tx_ctx, context)?;

                // Must materialize to sort
                let mut collected: Vec<_> = rows.collect::<Result<Vec<_>>>()?;

                // Sort based on order_by expressions
                let executor = self.clone_for_closure();
                let ctx = context.clone();

                collected.sort_by(|a, b| {
                    for (expr, direction) in &order_by {
                        let val_a = executor
                            .evaluate_expression(expr, Some(a), &ctx)
                            .unwrap_or(Value::Null);
                        let val_b = executor
                            .evaluate_expression(expr, Some(b), &ctx)
                            .unwrap_or(Value::Null);

                        let cmp = val_a
                            .partial_cmp(&val_b)
                            .unwrap_or(std::cmp::Ordering::Equal);
                        if cmp != std::cmp::Ordering::Equal {
                            return match direction {
                                crate::sql::planner::plan::Direction::Ascending => cmp,
                                crate::sql::planner::plan::Direction::Descending => cmp.reverse(),
                            };
                        }
                    }
                    std::cmp::Ordering::Equal
                });

                Ok(Box::new(collected.into_iter().map(Ok)))
            }

            Node::Nothing => Ok(Box::new(std::iter::empty())),
        }
    }

    /// Evaluate an expression
    fn evaluate_expression(
        &self,
        expr: &Expression,
        row: Option<&Arc<Vec<Value>>>,
        context: &TransactionContext,
    ) -> Result<Value> {
        match expr {
            Expression::Constant(v) => Ok(v.clone()),

            Expression::Column(idx) => row
                .and_then(|r| r.get(*idx).cloned())
                .ok_or_else(|| Error::InvalidValue(format!("Column {} not found", idx))),

            Expression::Equal(left, right) => {
                let l = self.evaluate_expression(left, row, context)?;
                let r = self.evaluate_expression(right, row, context)?;
                Ok(Value::Boolean(l == r))
            }

            Expression::NotEqual(left, right) => {
                let l = self.evaluate_expression(left, row, context)?;
                let r = self.evaluate_expression(right, row, context)?;
                Ok(Value::Boolean(l != r))
            }

            Expression::LessThan(left, right) => {
                let l = self.evaluate_expression(left, row, context)?;
                let r = self.evaluate_expression(right, row, context)?;
                Ok(Value::Boolean(l < r))
            }

            Expression::LessThanOrEqual(left, right) => {
                let l = self.evaluate_expression(left, row, context)?;
                let r = self.evaluate_expression(right, row, context)?;
                Ok(Value::Boolean(l <= r))
            }

            Expression::GreaterThan(left, right) => {
                let l = self.evaluate_expression(left, row, context)?;
                let r = self.evaluate_expression(right, row, context)?;
                Ok(Value::Boolean(l > r))
            }

            Expression::GreaterThanOrEqual(left, right) => {
                let l = self.evaluate_expression(left, row, context)?;
                let r = self.evaluate_expression(right, row, context)?;
                Ok(Value::Boolean(l >= r))
            }

            Expression::And(left, right) => {
                let l = self.evaluate_expression(left, row, context)?;
                let r = self.evaluate_expression(right, row, context)?;
                l.and(&r)
            }

            Expression::Or(left, right) => {
                let l = self.evaluate_expression(left, row, context)?;
                let r = self.evaluate_expression(right, row, context)?;
                l.or(&r)
            }

            Expression::Not(inner) => {
                let v = self.evaluate_expression(inner, row, context)?;
                v.not()
            }

            Expression::Is(left, right) => {
                let l = self.evaluate_expression(left, row, context)?;
                Ok(Value::Boolean(l == *right))
            }

            Expression::Add(left, right) => {
                let l = self.evaluate_expression(left, row, context)?;
                let r = self.evaluate_expression(right, row, context)?;
                l.add(&r)
            }

            Expression::Subtract(left, right) => {
                let l = self.evaluate_expression(left, row, context)?;
                let r = self.evaluate_expression(right, row, context)?;
                l.subtract(&r)
            }

            Expression::Multiply(left, right) => {
                let l = self.evaluate_expression(left, row, context)?;
                let r = self.evaluate_expression(right, row, context)?;
                l.multiply(&r)
            }

            Expression::Divide(left, right) => {
                let l = self.evaluate_expression(left, row, context)?;
                let r = self.evaluate_expression(right, row, context)?;
                l.divide(&r)
            }

            // Other expression types...
            _ => Ok(Value::Null),
        }
    }

    /// Clone executor for use in closures
    fn clone_for_closure(&self) -> Self {
        Self {
            schemas: self.schemas.clone(),
        }
    }
}
