//! Complete MVCC-aware SQL executor
//!
//! This executor properly integrates with the planner, executing Plan/Node trees
//! using MVCC transactions for proper isolation and rollback support.

use super::{aggregator::Aggregator, join};
use crate::error::{Error, Result};
use crate::planner::plan::{Node, Plan};
use crate::storage::lock::LockManager;
use crate::storage::{MvccStorage, read_ops, write_ops};
use crate::stream::TransactionContext;
use crate::types::expression::Expression;
use crate::types::query::Rows;
use crate::types::schema::Table;
use crate::types::value::Value;
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
        tx_ctx: &mut TransactionContext,
    ) -> Result<ExecutionResult> {
        match plan {
            Plan::Select(node) => {
                // SELECT uses immutable storage reference
                self.execute_select(*node, storage, lock_manager, tx_ctx)
            }

            Plan::Insert {
                table,
                columns,
                source,
            } => {
                // INSERT uses write execution with phased approach
                self.execute_insert(table, columns, *source, storage, lock_manager, tx_ctx)
            }
            Plan::Update {
                table,
                assignments,
                source,
            } => {
                // UPDATE uses write execution with phased approach
                self.execute_update(table, assignments, *source, storage, lock_manager, tx_ctx)
            }

            Plan::Delete { table, source } => {
                // DELETE uses write execution with phased approach
                self.execute_delete(table, *source, storage, lock_manager, tx_ctx)
            }

            Plan::CreateTable { name, schema: _ } => {
                // Table creation is handled at stream processor level
                Ok(ExecutionResult::DDL(format!("Table '{}' created", name)))
            }

            Plan::DropTable { name, .. } => {
                Ok(ExecutionResult::DDL(format!("Table '{}' dropped", name)))
            }

            Plan::CreateIndex {
                name,
                table,
                column,
                unique,
            } => {
                // Actually create the index in storage
                storage.create_index(name.clone(), &table, column.clone(), unique)?;
                Ok(ExecutionResult::DDL(format!(
                    "Index '{}' created on {}.{}",
                    name, table, column
                )))
            }

            Plan::DropIndex { name, .. } => {
                // Drop the index using its name
                storage.drop_index(&name)?;
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
        tx_ctx: &mut TransactionContext,
    ) -> Result<ExecutionResult> {
        // Get column names from the node tree
        let columns = node.get_column_names(&self.schemas);

        // Execute using read-only node execution
        let rows = self.execute_node_read(node, storage, lock_manager, tx_ctx)?;
        let mut collected = Vec::new();
        for row in rows {
            collected.push(row?);
        }

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
        tx_ctx: &mut TransactionContext,
    ) -> Result<ExecutionResult> {
        let schema = self
            .schemas
            .get(&table)
            .ok_or_else(|| Error::TableNotFound(table.clone()))?;

        // Phase 1: Read all source rows (immutable borrow)
        let rows_to_insert = {
            // Use immutable reference for reading
            let storage_ref = &*storage;
            let rows = self.execute_node_read(source, storage_ref, lock_manager, tx_ctx)?;
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
        tx_ctx: &mut TransactionContext,
    ) -> Result<ExecutionResult> {
        // Phase 1: Read rows with IDs that match the WHERE clause
        let rows_to_update = {
            let iter = read_ops::scan_iter_with_ids(storage, lock_manager, tx_ctx, &table, true)?;
            let mut to_update = Vec::new();

            for (row_id, row) in iter {
                let matches = match &source {
                    Node::Filter { predicate, .. } => self
                        .evaluate_expression(predicate, Some(&row), &tx_ctx)?
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
                updated[col_idx] = self.evaluate_expression(expr, Some(&current), &tx_ctx)?;
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
        tx_ctx: &mut TransactionContext,
    ) -> Result<ExecutionResult> {
        // Phase 1: Read rows with IDs that match the WHERE clause
        let rows_to_delete = {
            let iter = read_ops::scan_iter_with_ids(storage, lock_manager, tx_ctx, &table, true)?;
            let mut to_delete = Vec::new();

            for (row_id, row) in iter {
                let matches = match &source {
                    Node::Filter { predicate, .. } => self
                        .evaluate_expression(predicate, Some(&row), &tx_ctx)?
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
        tx_ctx: &mut TransactionContext,
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
                // Evaluate the lookup value
                let filter_value = self.evaluate_expression(&value, None, &tx_ctx)?;

                // Try to use index lookup first
                if let Some(versioned_table) = storage.tables.get(&table) {
                    // Check if this column has an index
                    if versioned_table.indexes.contains_key(&index_column) {
                        // Use index lookup for O(log n) performance
                        let rows = versioned_table.index_lookup(
                            &index_column,
                            &filter_value,
                            tx_ctx.id,
                            tx_ctx.timestamp,
                        );

                        // Convert to iterator format
                        let iter = rows.into_iter().map(|row| Ok(Arc::new(row.values.clone())));
                        return Ok(Box::new(iter));
                    }
                }

                // Fall back to filtered table scan if no index exists
                let schema = self
                    .schemas
                    .get(&table)
                    .ok_or_else(|| Error::TableNotFound(table.clone()))?;

                let col_idx = schema
                    .columns
                    .iter()
                    .position(|c| c.name == index_column)
                    .ok_or_else(|| Error::ColumnNotFound(index_column))?;

                // Filtered scan as fallback
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

            Node::IndexRangeScan {
                table,
                index_column,
                start,
                start_inclusive,
                end,
                end_inclusive,
                ..
            } => {
                // Evaluate range bounds
                let start_value = start
                    .as_ref()
                    .map(|e| self.evaluate_expression(e, None, &tx_ctx))
                    .transpose()?;
                let end_value = end
                    .as_ref()
                    .map(|e| self.evaluate_expression(e, None, &tx_ctx))
                    .transpose()?;

                // Try to use index range lookup
                if let Some(versioned_table) = storage.tables.get(&table) {
                    if versioned_table.indexes.contains_key(&index_column) {
                        // Use index range lookup for O(log n) performance
                        let rows = versioned_table.index_range_lookup(
                            &index_column,
                            start_value.as_ref(),
                            start_inclusive,
                            end_value.as_ref(),
                            end_inclusive,
                            tx_ctx.id,
                            tx_ctx.timestamp,
                        );

                        // Convert to iterator format
                        let iter = rows.into_iter().map(|row| Ok(Arc::new(row.values.clone())));
                        return Ok(Box::new(iter));
                    }
                }

                // Fall back to filtered table scan if no index exists
                let schema = self
                    .schemas
                    .get(&table)
                    .ok_or_else(|| Error::TableNotFound(table.clone()))?;
                let col_idx = schema
                    .columns
                    .iter()
                    .position(|c| c.name == index_column)
                    .ok_or_else(|| Error::ColumnNotFound(index_column))?;

                // Build filter predicate for range
                let iter = read_ops::scan_iter(storage, lock_manager, tx_ctx, &table)?.filter_map(
                    move |row| {
                        let value = row.get(col_idx)?;

                        // Check start bound
                        if let Some(ref start_val) = start_value {
                            let cmp = value.partial_cmp(start_val)?;
                            if start_inclusive {
                                if cmp == std::cmp::Ordering::Less {
                                    return None;
                                }
                            } else if cmp != std::cmp::Ordering::Greater {
                                return None;
                            }
                        }

                        // Check end bound
                        if let Some(ref end_val) = end_value {
                            let cmp = value.partial_cmp(end_val)?;
                            if end_inclusive {
                                if cmp == std::cmp::Ordering::Greater {
                                    return None;
                                }
                            } else if cmp != std::cmp::Ordering::Less {
                                return None;
                            }
                        }

                        Some(Ok(row))
                    },
                );

                Ok(Box::new(iter))
            }

            Node::Filter { source, predicate } => {
                let source_rows = self.execute_node_read(*source, storage, lock_manager, tx_ctx)?;
                let executor = self.clone_for_closure();

                // Clone transaction context for use in closure
                let tx_ctx_clone = tx_ctx.clone();

                let filtered = source_rows.filter_map(move |row| match row {
                    Ok(row) => {
                        match executor.evaluate_expression(&predicate, Some(&row), &tx_ctx_clone) {
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
                // Values node contains literal rows - convert Expression to Value lazily
                let executor = self.clone_for_closure();
                let tx_ctx_clone = tx_ctx.clone();

                let values_iter = rows.into_iter().map(move |row| {
                    let mut value_row = Vec::new();
                    for expr in row {
                        value_row.push(executor.evaluate_expression(&expr, None, &tx_ctx_clone)?);
                    }
                    Ok(Arc::new(value_row))
                });

                Ok(Box::new(values_iter))
            }

            Node::Projection {
                source,
                expressions,
                ..
            } => {
                let source_rows = self.execute_node_read(*source, storage, lock_manager, tx_ctx)?;
                let executor = self.clone_for_closure();

                // Clone transaction context for use in closure
                let tx_ctx_clone = tx_ctx.clone();

                let projected = source_rows.map(move |row| match row {
                    Ok(row) => {
                        let mut result = Vec::with_capacity(expressions.len());
                        for expr in &expressions {
                            result.push(executor.evaluate_expression(
                                expr,
                                Some(&row),
                                &tx_ctx_clone,
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
                let rows = self.execute_node_read(*source, storage, lock_manager, tx_ctx)?;
                Ok(Box::new(rows.take(limit)))
            }

            Node::Offset { source, offset } => {
                let rows = self.execute_node_read(*source, storage, lock_manager, tx_ctx)?;
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
                let rows = self.execute_node_read(*source, storage, lock_manager, tx_ctx)?;
                for row in rows {
                    let row = row?;
                    // Use the transaction context for the aggregator
                    aggregator.add(&row, tx_ctx, &storage)?;
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
                let left_rows = self.execute_node_read(*left, storage, lock_manager, tx_ctx)?;
                let right_rows = self.execute_node_read(*right, storage, lock_manager, tx_ctx)?;

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

                let left_rows = self.execute_node_read(*left, storage, lock_manager, tx_ctx)?;
                let right_rows = self.execute_node_read(*right, storage, lock_manager, tx_ctx)?;

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
                let rows = self.execute_node_read(*source, storage, lock_manager, tx_ctx)?;

                // Must materialize to sort
                let mut collected: Vec<_> = rows.collect::<Result<Vec<_>>>()?;

                // Sort based on order_by expressions
                let executor = self.clone_for_closure();

                collected.sort_by(|a, b| {
                    for (expr, direction) in &order_by {
                        let val_a = executor
                            .evaluate_expression(expr, Some(a), tx_ctx)
                            .unwrap_or(Value::Null);
                        let val_b = executor
                            .evaluate_expression(expr, Some(b), tx_ctx)
                            .unwrap_or(Value::Null);

                        let cmp = val_a
                            .partial_cmp(&val_b)
                            .unwrap_or(std::cmp::Ordering::Equal);
                        if cmp != std::cmp::Ordering::Equal {
                            return match direction {
                                crate::planner::plan::Direction::Ascending => cmp,
                                crate::planner::plan::Direction::Descending => cmp.reverse(),
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
