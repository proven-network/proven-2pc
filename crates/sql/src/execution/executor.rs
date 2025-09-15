//! Complete MVCC-aware SQL executor
//!
//! This executor properly integrates with the planner, executing Plan/Node trees
//! using MVCC transactions for proper isolation and rollback support.

use super::{aggregator::Aggregator, join};
use crate::error::{Error, Result};
use crate::planning::plan::{Node, Plan};
use crate::storage::{MvccStorage, read_ops, write_ops};
use crate::stream::TransactionContext;
use crate::types::DataType;
use crate::types::evaluator;
use crate::types::expression::Expression;
use crate::types::query::Rows;
use crate::types::value::Value;
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
///
/// The executor is now stateless - it gets all necessary information
/// from storage or as parameters. This makes it simpler and eliminates
/// synchronization issues.
#[derive(Default)]
pub struct Executor;

impl Executor {
    /// Create a new executor (stateless)
    pub fn new() -> Self {
        Self
    }

    /// Execute a query plan - dispatches to read or write execution
    pub fn execute(
        &self,
        plan: Plan,
        storage: &mut MvccStorage,
        tx_ctx: &mut TransactionContext,
    ) -> Result<ExecutionResult> {
        match plan {
            Plan::Select(node) => {
                // SELECT uses immutable storage reference
                self.execute_select(*node, storage, tx_ctx)
            }

            Plan::Insert {
                table,
                columns,
                source,
            } => {
                // INSERT uses write execution with phased approach
                self.execute_insert(table, columns, *source, storage, tx_ctx)
            }
            Plan::Update {
                table,
                assignments,
                source,
            } => {
                // UPDATE uses write execution with phased approach
                self.execute_update(table, assignments, *source, storage, tx_ctx)
            }

            Plan::Delete { table, source } => {
                // DELETE uses write execution with phased approach
                self.execute_delete(table, *source, storage, tx_ctx)
            }

            // DDL operations are delegated to storage
            Plan::CreateTable { .. }
            | Plan::DropTable { .. }
            | Plan::CreateIndex { .. }
            | Plan::DropIndex { .. } => {
                let result_msg = storage.execute_ddl(&plan, tx_ctx.id)?;
                Ok(ExecutionResult::DDL(result_msg))
            }
        }
    }

    /// Execute SELECT query using immutable storage reference
    fn execute_select(
        &self,
        node: Node,
        storage: &MvccStorage,
        tx_ctx: &mut TransactionContext,
    ) -> Result<ExecutionResult> {
        // Get schemas from storage
        let schemas = storage.get_schemas();

        // Get column names from the node tree
        let columns = node.get_column_names(&schemas);

        // Execute using read-only node execution
        let rows = self.execute_node_read(node, storage, tx_ctx)?;
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
        tx_ctx: &mut TransactionContext,
    ) -> Result<ExecutionResult> {
        // Get schema from storage
        let schemas = storage.get_schemas();
        let schema = schemas
            .get(&table)
            .ok_or_else(|| Error::TableNotFound(table.clone()))?;

        // Phase 1: Read all source rows (immutable borrow)
        let rows_to_insert = {
            // Use immutable reference for reading
            let storage_ref = &*storage;
            let rows = self.execute_node_read(source, storage_ref, tx_ctx)?;
            rows.collect::<Result<Vec<_>>>()?
        }; // Immutable borrow ends here

        // Phase 2: Write rows (mutable borrow)
        let mut count = 0;
        for row in rows_to_insert {
            // Reorder columns if specified
            let final_row = if let Some(ref col_indices) = columns {
                // Build the row with DEFAULT values for missing columns
                let mut reordered = Vec::with_capacity(schema.columns.len());
                for (idx, col) in schema.columns.iter().enumerate() {
                    if let Some(src_pos) = col_indices.iter().position(|&i| i == idx) {
                        // Column was specified in INSERT
                        if src_pos < row.len() {
                            reordered.push(row[src_pos].clone());
                        } else {
                            reordered.push(Value::Null);
                        }
                    } else if let Some(ref default) = col.default {
                        // Use DEFAULT value
                        let value = match default {
                            Value::Str(s) if s == "__GENERATE_UUID__" => {
                                // Generate a deterministic UUID based on transaction context
                                Value::Uuid(tx_ctx.deterministic_uuid())
                            }
                            _ => default.clone(),
                        };
                        reordered.push(value);
                    } else if col.nullable {
                        // No DEFAULT and nullable - use NULL
                        reordered.push(Value::Null);
                    } else {
                        // No DEFAULT and NOT NULL - error
                        return Err(Error::NullConstraintViolation(col.name.clone()));
                    }
                }
                reordered
            } else if row.is_empty() {
                // INSERT DEFAULT VALUES - fill all columns with defaults
                let mut default_row = Vec::with_capacity(schema.columns.len());
                for col in &schema.columns {
                    if let Some(ref default) = col.default {
                        let value = match default {
                            Value::Str(s) if s == "__GENERATE_UUID__" => {
                                // Generate a deterministic UUID based on transaction context
                                Value::Uuid(tx_ctx.deterministic_uuid())
                            }
                            _ => default.clone(),
                        };
                        default_row.push(value);
                    } else if col.nullable {
                        default_row.push(Value::Null);
                    } else {
                        return Err(Error::NullConstraintViolation(col.name.clone()));
                    }
                }
                default_row
            } else {
                row.to_vec()
            };

            // Apply type coercion to match schema
            let coerced_row = crate::types::coercion::coerce_row(final_row, schema)?;

            write_ops::insert(storage, tx_ctx, &table, coerced_row)?;
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
        tx_ctx: &mut TransactionContext,
    ) -> Result<ExecutionResult> {
        // Phase 1: Read rows with IDs that match the WHERE clause
        let rows_to_update = {
            let iter = read_ops::scan_iter_with_ids(storage, tx_ctx, &table)?;
            let mut to_update = Vec::new();

            for (row_id, row) in iter {
                let matches = match &source {
                    Node::Filter { predicate, .. } => self
                        .evaluate_expression(predicate, Some(&row), tx_ctx)?
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
        // Get table schema for type coercion
        let schema = storage
            .tables
            .get(&table)
            .ok_or_else(|| Error::TableNotFound(table.clone()))?
            .schema
            .clone();

        let mut count = 0;
        for (row_id, current) in rows_to_update {
            let mut updated = current.to_vec();
            for &(col_idx, ref expr) in &assignments {
                updated[col_idx] = self.evaluate_expression(expr, Some(&current), tx_ctx)?;
            }

            // Apply type coercion to match schema
            let coerced_row = crate::types::coercion::coerce_row(updated, &schema)?;

            write_ops::update(storage, tx_ctx, &table, row_id, coerced_row)?;
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
        tx_ctx: &mut TransactionContext,
    ) -> Result<ExecutionResult> {
        // Phase 1: Read rows with IDs that match the WHERE clause
        let rows_to_delete = {
            let iter = read_ops::scan_iter_with_ids(storage, tx_ctx, &table)?;
            let mut to_delete = Vec::new();

            for (row_id, row) in iter {
                let matches = match &source {
                    Node::Filter { predicate, .. } => self
                        .evaluate_expression(predicate, Some(&row), tx_ctx)?
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
            write_ops::delete(storage, tx_ctx, &table, row_id)?;
            count += 1;
        }

        Ok(ExecutionResult::Modified(count))
    }

    /// Execute a plan node for reading with immutable storage reference
    fn execute_node_read<'a>(
        &self,
        node: Node,
        storage: &'a MvccStorage,
        tx_ctx: &mut TransactionContext,
    ) -> Result<Rows<'a>> {
        match node {
            Node::Scan { table, .. } => {
                // True streaming with immutable storage!
                let iter = read_ops::scan_iter(storage, tx_ctx, &table)?.map(Ok);

                Ok(Box::new(iter))
            }

            Node::IndexScan {
                table,
                index_name,
                values,
                ..
            } => {
                // Evaluate the lookup values
                let mut filter_values = Vec::new();
                for value_expr in &values {
                    filter_values.push(self.evaluate_expression(value_expr, None, tx_ctx)?);
                }

                // Try to use index lookup first
                if let Some(versioned_table) = storage.tables.get(&table) {
                    // Check if this index exists
                    if versioned_table.index_columns.contains_key(&index_name) {
                        // Use index lookup for O(log n) performance
                        let rows = versioned_table.index_lookup(
                            &index_name,
                            filter_values.clone(),
                            tx_ctx.id,
                            tx_ctx.timestamp,
                        );

                        // Convert to iterator format
                        let iter = rows.into_iter().map(|row| Ok(Arc::new(row.values.clone())));
                        return Ok(Box::new(iter));
                    }
                }

                // Fall back to filtered table scan if no index exists
                // We need to check if we can resolve the index_name to columns
                let schemas = storage.get_schemas();
                let schema = schemas
                    .get(&table)
                    .ok_or_else(|| Error::TableNotFound(table.clone()))?;

                // Check if we can get column info from the versioned table's index_columns
                let column_names = if let Some(versioned_table) = storage.tables.get(&table) {
                    versioned_table.index_columns.get(&index_name).cloned()
                } else {
                    // Fallback: assume index_name is a column name for backward compatibility
                    if schema.columns.iter().any(|c| c.name == index_name) {
                        Some(vec![index_name.clone()])
                    } else {
                        None
                    }
                };

                if let Some(columns) = column_names {
                    // Get column indices
                    let mut col_indices = Vec::new();
                    for col_name in &columns {
                        if let Some(idx) = schema.columns.iter().position(|c| &c.name == col_name) {
                            col_indices.push(idx);
                        } else {
                            // Column not found, can't filter
                            let iter = read_ops::scan_iter(storage, tx_ctx, &table)?.map(Ok);
                            return Ok(Box::new(iter));
                        }
                    }

                    // Filter by checking all column values match
                    if col_indices.len() == filter_values.len() {
                        let iter =
                            read_ops::scan_iter(storage, tx_ctx, &table)?.filter_map(move |row| {
                                // Check if all indexed columns match the filter values
                                for (idx, expected_val) in col_indices.iter().zip(&filter_values) {
                                    if row.get(*idx) != Some(expected_val) {
                                        return None;
                                    }
                                }
                                Some(Ok(row))
                            });
                        return Ok(Box::new(iter));
                    }
                }

                // Can't determine columns, fall back to full scan
                let iter = read_ops::scan_iter(storage, tx_ctx, &table)?.map(Ok);
                Ok(Box::new(iter))
            }

            Node::IndexRangeScan {
                table,
                index_name,
                start,
                start_inclusive,
                end,
                end_inclusive,
                reverse,
                ..
            } => {
                // Evaluate range bounds
                let start_values = start
                    .as_ref()
                    .map(|exprs| {
                        exprs
                            .iter()
                            .map(|e| self.evaluate_expression(e, None, tx_ctx))
                            .collect::<Result<Vec<_>>>()
                    })
                    .transpose()?;
                let end_values = end
                    .as_ref()
                    .map(|exprs| {
                        exprs
                            .iter()
                            .map(|e| self.evaluate_expression(e, None, tx_ctx))
                            .collect::<Result<Vec<_>>>()
                    })
                    .transpose()?;

                // Try to use index range lookup
                if let Some(versioned_table) = storage.tables.get(&table)
                    && versioned_table.index_columns.contains_key(&index_name)
                {
                    // Use index range lookup for O(log n) performance
                    let rows = versioned_table.index_range_lookup(
                        &index_name,
                        start_values,
                        start_inclusive,
                        end_values,
                        end_inclusive,
                        reverse,
                        tx_ctx.id,
                        tx_ctx.timestamp,
                    );

                    // Convert to iterator format
                    let iter = rows.into_iter().map(|row| Ok(Arc::new(row.values.clone())));
                    return Ok(Box::new(iter));
                }

                // Fall back to filtered range scan if no index exists
                let schemas = storage.get_schemas();
                let schema = schemas
                    .get(&table)
                    .ok_or_else(|| Error::TableNotFound(table.clone()))?;

                // Get column info from the versioned table's index_columns
                let column_names = if let Some(versioned_table) = storage.tables.get(&table) {
                    versioned_table.index_columns.get(&index_name).cloned()
                } else {
                    // Fallback: assume index_name is a column name
                    if schema.columns.iter().any(|c| c.name == index_name) {
                        Some(vec![index_name.clone()])
                    } else {
                        None
                    }
                };

                if let Some(columns) = column_names {
                    // Get column indices
                    let mut col_indices = Vec::new();
                    for col_name in &columns {
                        if let Some(idx) = schema.columns.iter().position(|c| &c.name == col_name) {
                            col_indices.push(idx);
                        } else {
                            // Column not found, can't filter
                            let iter = read_ops::scan_iter(storage, tx_ctx, &table)?.map(Ok);
                            return Ok(Box::new(iter));
                        }
                    }

                    // Perform range filtering
                    let start_vals = start_values.clone();
                    let end_vals = end_values.clone();
                    let start_incl = start_inclusive;
                    let end_incl = end_inclusive;

                    let iter =
                        read_ops::scan_iter(storage, tx_ctx, &table)?.filter_map(move |row| {
                            // Extract values for the indexed columns
                            let mut row_values = Vec::new();
                            for &idx in &col_indices {
                                if let Some(val) = row.get(idx) {
                                    row_values.push(val.clone());
                                } else {
                                    return None; // NULL in key, skip
                                }
                            }

                            // Check start bound
                            if let Some(ref start) = start_vals {
                                match evaluator::compare_composite(&row_values, start).ok() {
                                    Some(std::cmp::Ordering::Less) => return None,
                                    Some(std::cmp::Ordering::Equal) if !start_incl => return None,
                                    _ => {}
                                }
                            }

                            // Check end bound
                            if let Some(ref end) = end_vals {
                                match evaluator::compare_composite(&row_values, end).ok() {
                                    Some(std::cmp::Ordering::Greater) => return None,
                                    Some(std::cmp::Ordering::Equal) if !end_incl => return None,
                                    _ => {}
                                }
                            }

                            Some(Ok(row))
                        });
                    return Ok(Box::new(iter));
                }

                // Can't determine columns, fall back to full scan
                let iter = read_ops::scan_iter(storage, tx_ctx, &table)?.map(Ok);
                Ok(Box::new(iter))
            }

            Node::Filter { source, predicate } => {
                let source_rows = self.execute_node_read(*source, storage, tx_ctx)?;

                // Clone transaction context for use in closure
                let tx_ctx_clone = tx_ctx.clone();

                let filtered = source_rows.filter_map(move |row| match row {
                    Ok(row) => {
                        // Create executor instance in closure (it's stateless)
                        let executor = Executor::new();
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
                let tx_ctx_clone = tx_ctx.clone();

                let values_iter = rows.into_iter().map(move |row| {
                    let mut value_row = Vec::new();
                    let executor = Executor::new();
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
                let source_rows = self.execute_node_read(*source, storage, tx_ctx)?;

                // Clone transaction context for use in closure
                let tx_ctx_clone = tx_ctx.clone();

                let projected = source_rows.map(move |row| match row {
                    Ok(row) => {
                        let mut result = Vec::with_capacity(expressions.len());
                        let executor = Executor::new();
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
                let rows = self.execute_node_read(*source, storage, tx_ctx)?;
                Ok(Box::new(rows.take(limit)))
            }

            Node::Offset { source, offset } => {
                let rows = self.execute_node_read(*source, storage, tx_ctx)?;
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
                let rows = self.execute_node_read(*source, storage, tx_ctx)?;
                for row in rows {
                    let row = row?;
                    // Use the transaction context for the aggregator
                    aggregator.add(&row, tx_ctx, storage)?;
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
                // Get column counts from both nodes for outer join NULL padding
                let schemas = storage.get_schemas();
                let left_columns = left.column_count(&schemas);
                let right_columns = right.column_count(&schemas);

                // Both can borrow storage immutably!
                let left_rows = self.execute_node_read(*left, storage, tx_ctx)?;
                let right_rows = self.execute_node_read(*right, storage, tx_ctx)?;

                join::execute_hash_join(
                    left_rows,
                    left_col,
                    right_rows,
                    right_col,
                    left_columns,
                    right_columns,
                    join_type,
                )
            }

            // Nested Loop Join also works with immutable storage
            Node::NestedLoopJoin {
                left,
                right,
                predicate,
                join_type,
            } => {
                // Get column counts from both nodes for outer join NULL padding
                let schemas = storage.get_schemas();
                let left_columns = left.column_count(&schemas);
                let right_columns = right.column_count(&schemas);

                let left_rows = self.execute_node_read(*left, storage, tx_ctx)?;
                let right_rows = self.execute_node_read(*right, storage, tx_ctx)?;

                // Use the standalone function for nested loop join
                join::execute_nested_loop_join(
                    left_rows,
                    right_rows,
                    left_columns,
                    right_columns,
                    predicate,
                    join_type,
                    storage,
                )
            }

            // Order By requires full materialization to sort
            Node::Order { source, order_by } => {
                let rows = self.execute_node_read(*source, storage, tx_ctx)?;

                // Must materialize to sort
                let mut collected: Vec<_> = rows.collect::<Result<Vec<_>>>()?;

                // Sort based on order_by expressions
                collected.sort_by(|a, b| {
                    for (expr, direction) in &order_by {
                        let val_a = self
                            .evaluate_expression(expr, Some(a), tx_ctx)
                            .unwrap_or(Value::Null);
                        let val_b = self
                            .evaluate_expression(expr, Some(b), tx_ctx)
                            .unwrap_or(Value::Null);

                        let cmp = val_a
                            .partial_cmp(&val_b)
                            .unwrap_or(std::cmp::Ordering::Equal);
                        if cmp != std::cmp::Ordering::Equal {
                            return match direction {
                                crate::planning::plan::Direction::Ascending => cmp,
                                crate::planning::plan::Direction::Descending => cmp.reverse(),
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
        _context: &TransactionContext,
    ) -> Result<Value> {
        Self::evaluate_expression_static(expr, row)
    }

    /// Static helper for evaluating expressions
    fn evaluate_expression_static(
        expr: &Expression,
        row: Option<&Arc<Vec<Value>>>,
    ) -> Result<Value> {
        match expr {
            Expression::Constant(v) => Ok(v.clone()),

            Expression::Column(idx) => row
                .and_then(|r| r.get(*idx).cloned())
                .ok_or_else(|| Error::InvalidValue(format!("Column {} not found", idx))),

            Expression::Equal(left, right) => {
                let l = Self::evaluate_expression_static(left, row)?;
                let mut r = Self::evaluate_expression_static(right, row)?;

                // Special case: if comparing collections with JSON strings, parse the strings
                // Handle Map comparison
                if matches!(l, Value::Map(_)) && matches!(r, Value::Str(_)) {
                    if let Value::Str(s) = &r
                        && s.starts_with('{')
                        && s.ends_with('}')
                    {
                        // Try to parse as JSON map
                        if let Ok(parsed) = Value::parse_json_object(s)
                            && matches!(parsed, Value::Map(_))
                        {
                            r = parsed;
                        }
                    }
                } else if matches!(r, Value::Map(_)) && matches!(l, Value::Str(_)) {
                    // Handle the reverse case as well
                    if let Value::Str(s) = &l
                        && s.starts_with('{')
                        && s.ends_with('}')
                        && let Ok(parsed) = Value::parse_json_object(s)
                        && matches!(parsed, Value::Map(_))
                    {
                        let l_parsed = parsed;
                        let r_val = r;
                        return Self::evaluate_expression_static(
                            &Expression::Equal(
                                Box::new(Expression::Constant(l_parsed)),
                                Box::new(Expression::Constant(r_val)),
                            ),
                            row,
                        );
                    }
                }
                // Handle Array/List comparison
                else if (matches!(l, Value::Array(_)) || matches!(l, Value::List(_)))
                    && matches!(r, Value::Str(_))
                {
                    if let Value::Str(s) = &r
                        && s.starts_with('[')
                        && s.ends_with(']')
                    {
                        // Try to parse as JSON array
                        if let Ok(parsed) = Value::parse_json_array(s) {
                            r = parsed;
                        }
                    }
                } else if (matches!(r, Value::Array(_)) || matches!(r, Value::List(_)))
                    && matches!(l, Value::Str(_))
                    && let Value::Str(s) = &l
                    && s.starts_with('[')
                    && s.ends_with(']')
                    && let Ok(parsed) = Value::parse_json_array(s)
                {
                    let l_parsed = parsed;
                    let r_val = r;
                    return Self::evaluate_expression_static(
                        &Expression::Equal(
                            Box::new(Expression::Constant(l_parsed)),
                            Box::new(Expression::Constant(r_val)),
                        ),
                        row,
                    );
                }
                // Handle Struct comparison with JSON string
                else if matches!(l, Value::Struct(_)) && matches!(r, Value::Str(_)) {
                    if let Value::Str(s) = &r
                        && s.starts_with('{')
                        && s.ends_with('}')
                    {
                        // Try to parse as JSON object and coerce to struct
                        if let Ok(parsed) = Value::parse_json_object(s) {
                            // Get the struct's type for coercion
                            if let Value::Struct(fields) = &l {
                                // Build the struct's schema
                                let schema: Vec<(String, DataType)> = fields
                                    .iter()
                                    .map(|(name, val)| (name.clone(), val.data_type()))
                                    .collect();
                                // Try to coerce the parsed map to struct
                                if let Ok(coerced) = crate::types::coercion::coerce_value(
                                    parsed,
                                    &DataType::Struct(schema),
                                ) {
                                    r = coerced;
                                }
                            }
                        }
                    }
                } else if matches!(r, Value::Struct(_)) && matches!(l, Value::Str(_)) {
                    // Handle the reverse case
                    if let Value::Str(s) = &l
                        && s.starts_with('{')
                        && s.ends_with('}')
                        && let Ok(parsed) = Value::parse_json_object(s)
                        && let Value::Struct(fields) = &r
                    {
                        let schema: Vec<(String, DataType)> = fields
                            .iter()
                            .map(|(name, val)| (name.clone(), val.data_type()))
                            .collect();
                        if let Ok(coerced) =
                            crate::types::coercion::coerce_value(parsed, &DataType::Struct(schema))
                        {
                            let l_parsed = coerced;
                            let r_val = r;
                            return Self::evaluate_expression_static(
                                &Expression::Equal(
                                    Box::new(Expression::Constant(l_parsed)),
                                    Box::new(Expression::Constant(r_val)),
                                ),
                                row,
                            );
                        }
                    }
                }
                // SQL semantics: any comparison with NULL returns NULL
                if l.is_null() || r.is_null() {
                    return Ok(Value::Null);
                }
                // IEEE 754 semantics: NaN is never equal to anything, including itself
                // Check for any float NaN values
                let has_nan = match (&l, &r) {
                    (Value::F32(a), _) if a.is_nan() => true,
                    (_, Value::F32(b)) if b.is_nan() => true,
                    (Value::F64(a), _) if a.is_nan() => true,
                    (_, Value::F64(b)) if b.is_nan() => true,
                    _ => false,
                };
                if has_nan {
                    return Ok(Value::boolean(false));
                }
                // Use evaluator::compare for type-aware comparison
                match evaluator::compare(&l, &r) {
                    Ok(std::cmp::Ordering::Equal) => Ok(Value::boolean(true)),
                    Ok(_ordering) => Ok(Value::boolean(false)),
                    Err(_e) => Ok(Value::boolean(false)), // Type mismatch means not equal
                }
            }

            Expression::NotEqual(left, right) => {
                let l = Self::evaluate_expression_static(left, row)?;
                let r = Self::evaluate_expression_static(right, row)?;
                // SQL semantics: any comparison with NULL returns NULL
                if l.is_null() || r.is_null() {
                    return Ok(Value::Null);
                }
                // IEEE 754 semantics: NaN is never equal to anything, so always not equal
                // Check for any float NaN values
                let has_nan = match (&l, &r) {
                    (Value::F32(a), _) if a.is_nan() => true,
                    (_, Value::F32(b)) if b.is_nan() => true,
                    (Value::F64(a), _) if a.is_nan() => true,
                    (_, Value::F64(b)) if b.is_nan() => true,
                    _ => false,
                };
                if has_nan {
                    return Ok(Value::boolean(true));
                }
                // Use evaluator::compare for type-aware comparison
                match evaluator::compare(&l, &r) {
                    Ok(std::cmp::Ordering::Equal) => Ok(Value::boolean(false)),
                    Ok(_) => Ok(Value::boolean(true)),
                    Err(_) => Ok(Value::boolean(true)), // Type mismatch means not equal
                }
            }

            Expression::LessThan(left, right) => {
                let l = Self::evaluate_expression_static(left, row)?;
                let r = Self::evaluate_expression_static(right, row)?;
                // SQL semantics: any comparison with NULL returns NULL
                if l.is_null() || r.is_null() {
                    return Ok(Value::Null);
                }
                // IEEE 754 semantics: comparisons with NaN return false
                let has_nan = match (&l, &r) {
                    (Value::F32(a), _) if a.is_nan() => true,
                    (_, Value::F32(b)) if b.is_nan() => true,
                    (Value::F64(a), _) if a.is_nan() => true,
                    (_, Value::F64(b)) if b.is_nan() => true,
                    _ => false,
                };
                if has_nan {
                    return Ok(Value::boolean(false));
                }
                // Use evaluator::compare for type-aware comparison
                match evaluator::compare(&l, &r) {
                    Ok(std::cmp::Ordering::Less) => Ok(Value::boolean(true)),
                    Ok(_) => Ok(Value::boolean(false)),
                    Err(_) => Ok(Value::boolean(false)),
                }
            }

            Expression::LessThanOrEqual(left, right) => {
                let l = Self::evaluate_expression_static(left, row)?;
                let r = Self::evaluate_expression_static(right, row)?;
                // SQL semantics: any comparison with NULL returns NULL
                if l.is_null() || r.is_null() {
                    return Ok(Value::Null);
                }
                // IEEE 754 semantics: comparisons with NaN return false
                let has_nan = match (&l, &r) {
                    (Value::F32(a), _) if a.is_nan() => true,
                    (_, Value::F32(b)) if b.is_nan() => true,
                    (Value::F64(a), _) if a.is_nan() => true,
                    (_, Value::F64(b)) if b.is_nan() => true,
                    _ => false,
                };
                if has_nan {
                    return Ok(Value::boolean(false));
                }
                // Use evaluator::compare for type-aware comparison
                match evaluator::compare(&l, &r) {
                    Ok(std::cmp::Ordering::Less | std::cmp::Ordering::Equal) => {
                        Ok(Value::boolean(true))
                    }
                    Ok(_) => Ok(Value::boolean(false)),
                    Err(_) => Ok(Value::boolean(false)),
                }
            }

            Expression::GreaterThan(left, right) => {
                let l = Self::evaluate_expression_static(left, row)?;
                let r = Self::evaluate_expression_static(right, row)?;
                // SQL semantics: any comparison with NULL returns NULL
                if l.is_null() || r.is_null() {
                    return Ok(Value::Null);
                }
                // IEEE 754 semantics: comparisons with NaN return false
                let has_nan = match (&l, &r) {
                    (Value::F32(a), _) if a.is_nan() => true,
                    (_, Value::F32(b)) if b.is_nan() => true,
                    (Value::F64(a), _) if a.is_nan() => true,
                    (_, Value::F64(b)) if b.is_nan() => true,
                    _ => false,
                };
                if has_nan {
                    return Ok(Value::boolean(false));
                }
                // Use evaluator::compare for type-aware comparison
                match evaluator::compare(&l, &r) {
                    Ok(std::cmp::Ordering::Greater) => Ok(Value::boolean(true)),
                    Ok(_) => Ok(Value::boolean(false)),
                    Err(_) => Ok(Value::boolean(false)),
                }
            }

            Expression::GreaterThanOrEqual(left, right) => {
                let l = Self::evaluate_expression_static(left, row)?;
                let r = Self::evaluate_expression_static(right, row)?;
                // SQL semantics: any comparison with NULL returns NULL
                if l.is_null() || r.is_null() {
                    return Ok(Value::Null);
                }
                // IEEE 754 semantics: comparisons with NaN return false
                let has_nan = match (&l, &r) {
                    (Value::F32(a), _) if a.is_nan() => true,
                    (_, Value::F32(b)) if b.is_nan() => true,
                    (Value::F64(a), _) if a.is_nan() => true,
                    (_, Value::F64(b)) if b.is_nan() => true,
                    _ => false,
                };
                if has_nan {
                    return Ok(Value::boolean(false));
                }
                // Use evaluator::compare for type-aware comparison
                match evaluator::compare(&l, &r) {
                    Ok(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal) => {
                        Ok(Value::boolean(true))
                    }
                    Ok(_) => Ok(Value::boolean(false)),
                    Err(_) => Ok(Value::boolean(false)),
                }
            }

            Expression::And(left, right) => {
                let l = Self::evaluate_expression_static(left, row)?;
                let r = Self::evaluate_expression_static(right, row)?;
                evaluator::and(&l, &r)
            }

            Expression::Or(left, right) => {
                let l = Self::evaluate_expression_static(left, row)?;
                let r = Self::evaluate_expression_static(right, row)?;
                evaluator::or(&l, &r)
            }

            Expression::Not(inner) => {
                let v = Self::evaluate_expression_static(inner, row)?;
                evaluator::not(&v)
            }

            Expression::Is(left, right) => {
                let l = Self::evaluate_expression_static(left, row)?;
                Ok(Value::boolean(l == *right))
            }

            Expression::Add(left, right) => {
                let l = Self::evaluate_expression_static(left, row)?;
                let r = Self::evaluate_expression_static(right, row)?;
                evaluator::add(&l, &r)
            }

            Expression::Subtract(left, right) => {
                let l = Self::evaluate_expression_static(left, row)?;
                let r = Self::evaluate_expression_static(right, row)?;
                evaluator::subtract(&l, &r)
            }

            Expression::Multiply(left, right) => {
                let l = Self::evaluate_expression_static(left, row)?;
                let r = Self::evaluate_expression_static(right, row)?;
                evaluator::multiply(&l, &r)
            }

            Expression::Divide(left, right) => {
                let l = Self::evaluate_expression_static(left, row)?;
                let r = Self::evaluate_expression_static(right, row)?;
                evaluator::divide(&l, &r)
            }

            Expression::Remainder(left, right) => {
                let l = Self::evaluate_expression_static(left, row)?;
                let r = Self::evaluate_expression_static(right, row)?;
                evaluator::remainder(&l, &r)
            }

            Expression::Negate(expr) => {
                let value = Self::evaluate_expression_static(expr, row)?;
                match value {
                    Value::Null => Ok(Value::Null),
                    Value::I8(i) => Ok(Value::I8(-i)),
                    Value::I16(i) => Ok(Value::I16(-i)),
                    Value::I32(i) => Ok(Value::I32(-i)),
                    Value::I64(i) => Ok(Value::I64(-i)),
                    Value::I128(i) => Ok(Value::I128(-i)),
                    // Unsigned types need to be converted to signed equivalents
                    // Note: This will fail during coercion if the result doesn't fit
                    Value::U8(u) => {
                        if u <= i8::MAX as u8 {
                            Ok(Value::I8(-(u as i8)))
                        } else {
                            // Value too large to negate as i8, try i16
                            Ok(Value::I16(-(u as i16)))
                        }
                    }
                    Value::U16(u) => {
                        if u <= i16::MAX as u16 {
                            Ok(Value::I16(-(u as i16)))
                        } else {
                            // Value too large to negate as i16, try i32
                            Ok(Value::I32(-(u as i32)))
                        }
                    }
                    Value::U32(u) => {
                        if u <= i32::MAX as u32 {
                            Ok(Value::I32(-(u as i32)))
                        } else {
                            // Value too large to negate as i32, try i64
                            Ok(Value::I64(-(u as i64)))
                        }
                    }
                    Value::U64(u) => {
                        if u <= i64::MAX as u64 {
                            Ok(Value::I64(-(u as i64)))
                        } else {
                            // Value too large to negate as i64, try i128
                            Ok(Value::I128(-(u as i128)))
                        }
                    }
                    Value::U128(u) => {
                        if u <= i128::MAX as u128 {
                            Ok(Value::I128(-(u as i128)))
                        } else {
                            // Value too large to negate - this will error
                            Err(Error::InvalidValue(format!(
                                "Cannot negate {}: value too large",
                                u
                            )))
                        }
                    }
                    Value::F32(f) => Ok(Value::F32(-f)),
                    Value::F64(f) => Ok(Value::F64(-f)),
                    Value::Decimal(d) => Ok(Value::Decimal(-d)),
                    _ => Err(Error::TypeMismatch {
                        expected: "numeric".into(),
                        found: format!("{:?}", value),
                    }),
                }
            }

            Expression::Identity(expr) => {
                // Unary plus - just returns the value as-is
                Self::evaluate_expression_static(expr, row)
            }

            Expression::Function(name, args) => {
                // Evaluate function arguments
                let arg_values: Result<Vec<_>> = args
                    .iter()
                    .map(|arg| Self::evaluate_expression_static(arg, row))
                    .collect();

                // Create a dummy transaction context for function evaluation
                // In a real implementation, this should come from the query context
                use proven_hlc::{HlcTimestamp, NodeId};
                let timestamp = HlcTimestamp::new(1, 0, NodeId::new(0));
                let context = crate::stream::transaction::TransactionContext::new(timestamp);

                crate::types::functions::evaluate_function(name, &arg_values?, &context)
            }

            Expression::InList(expr, list, negated) => {
                let value = Self::evaluate_expression_static(expr, row)?;

                // If value is NULL, return NULL
                if value == Value::Null {
                    return Ok(Value::Null);
                }

                let mut found = false;
                let mut has_null = false;

                for item in list {
                    let item_value = Self::evaluate_expression_static(item, row)?;
                    if item_value == Value::Null {
                        has_null = true;
                    } else {
                        // Use evaluator::compare for type-aware comparison
                        // Two values are equal if compare returns Ordering::Equal
                        if let Ok(std::cmp::Ordering::Equal) =
                            evaluator::compare(&value, &item_value)
                        {
                            found = true;
                            break;
                        }
                    }
                }

                // SQL three-valued logic:
                // - If found, return !negated
                // - If not found and list has NULL, return NULL
                // - Otherwise return negated
                if found {
                    Ok(Value::boolean(!negated))
                } else if has_null {
                    Ok(Value::Null)
                } else {
                    Ok(Value::boolean(*negated))
                }
            }

            Expression::Between(expr, low, high, negated) => {
                let value = Self::evaluate_expression_static(expr, row)?;
                let low_value = Self::evaluate_expression_static(low, row)?;
                let high_value = Self::evaluate_expression_static(high, row)?;

                // If any value is NULL, return NULL
                if value == Value::Null || low_value == Value::Null || high_value == Value::Null {
                    return Ok(Value::Null);
                }

                // Check if value is between low and high (inclusive)
                // Use evaluator::compare for proper type handling
                let low_cmp =
                    evaluator::compare(&value, &low_value).unwrap_or(std::cmp::Ordering::Less);
                let high_cmp =
                    evaluator::compare(&value, &high_value).unwrap_or(std::cmp::Ordering::Greater);

                let in_range =
                    low_cmp != std::cmp::Ordering::Less && high_cmp != std::cmp::Ordering::Greater;

                if *negated {
                    Ok(Value::boolean(!in_range))
                } else {
                    Ok(Value::boolean(in_range))
                }
            }

            // Collection access expressions
            Expression::ArrayAccess(base, index) => {
                let collection = Self::evaluate_expression_static(base, row)?;
                let idx = Self::evaluate_expression_static(index, row)?;

                match (collection, idx) {
                    (Value::Array(arr), Value::I32(i)) if i >= 0 => {
                        Ok(arr.get(i as usize).cloned().unwrap_or(Value::Null))
                    }
                    (Value::Array(arr), Value::I64(i)) if i >= 0 => {
                        Ok(arr.get(i as usize).cloned().unwrap_or(Value::Null))
                    }
                    (Value::List(list), Value::I32(i)) if i >= 0 => {
                        Ok(list.get(i as usize).cloned().unwrap_or(Value::Null))
                    }
                    (Value::List(list), Value::I64(i)) if i >= 0 => {
                        Ok(list.get(i as usize).cloned().unwrap_or(Value::Null))
                    }
                    (Value::Map(map), Value::Str(key)) => {
                        Ok(map.get(&key).cloned().unwrap_or(Value::Null))
                    }
                    _ => Ok(Value::Null),
                }
            }

            Expression::FieldAccess(base, field) => {
                let struct_val = Self::evaluate_expression_static(base, row)?;
                match struct_val {
                    Value::Struct(fields) => Ok(fields
                        .iter()
                        .find(|(name, _)| name == field)
                        .map(|(_, val)| val.clone())
                        .unwrap_or(Value::Null)),
                    _ => Ok(Value::Null),
                }
            }

            Expression::ArrayLiteral(elements) => {
                let values: Result<Vec<_>> = elements
                    .iter()
                    .map(|e| Self::evaluate_expression_static(e, row))
                    .collect();
                Ok(Value::List(values?))
            }

            Expression::MapLiteral(pairs) => {
                let mut map = std::collections::HashMap::new();
                for (k, v) in pairs {
                    let key = Self::evaluate_expression_static(k, row)?;
                    let value = Self::evaluate_expression_static(v, row)?;
                    if let Value::Str(key_str) = key {
                        map.insert(key_str, value);
                    }
                }
                Ok(Value::Map(map))
            }

            // Other expression types...
            _ => Ok(Value::Null),
        }
    }
}
