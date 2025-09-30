//! Complete MVCC-aware SQL executor
//!
//! This executor properly integrates with the planner, executing Plan/Node trees
//! using MVCC transactions for proper isolation and rollback support.

use super::{aggregator::Aggregator, expression, join};
use crate::error::{Error, Result};
use crate::operators;
use crate::planning::plan::{Node, Plan};
use crate::storage::Storage;
use crate::stream::TransactionContext;
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
    Ddl(String),
}

/// Execute a query plan with parameters
pub fn execute_with_params(
    plan: Plan,
    storage: &mut Storage,
    tx_ctx: &mut TransactionContext,
    params: Option<&Vec<Value>>,
) -> Result<ExecutionResult> {
    match plan {
        Plan::Query { root, params: _ } => {
            // Query uses immutable storage reference (handles SELECT and VALUES)
            super::select::execute_select(*root, storage, tx_ctx, params)
        }

        Plan::Select(node) => {
            // SELECT uses immutable storage reference (legacy)
            super::select::execute_select(*node, storage, tx_ctx, params)
        }

        Plan::Insert {
            table,
            columns,
            source,
        } => {
            // INSERT uses write execution with phased approach
            super::insert::execute_insert(table, columns, *source, storage, tx_ctx, params)
        }
        Plan::Update {
            table,
            assignments,
            source,
        } => {
            // UPDATE uses write execution with phased approach
            super::update::execute_update(table, assignments, *source, storage, tx_ctx, params)
        }

        Plan::Delete { table, source } => {
            // DELETE uses write execution with phased approach
            super::delete::execute_delete(table, *source, storage, tx_ctx, params)
        }

        // DDL operations are delegated to storage
        Plan::CreateTable { .. }
        | Plan::DropTable { .. }
        | Plan::CreateIndex { .. }
        | Plan::DropIndex { .. } => {
            let result_msg = storage.execute_ddl(&plan, tx_ctx.id)?;
            Ok(ExecutionResult::Ddl(result_msg))
        }

        Plan::CreateTableAsValues {
            name,
            schema,
            values_plan,
            if_not_exists,
        } => {
            // First execute CREATE TABLE
            let create_plan = Plan::CreateTable {
                name: name.clone(),
                schema: schema.clone(),
                foreign_keys: Vec::new(), // TODO: Handle foreign keys for CREATE TABLE AS VALUES
                if_not_exists,
            };
            let create_msg = storage.execute_ddl(&create_plan, tx_ctx.id)?;

            // Then execute the VALUES insertion
            // Convert VALUES plan to INSERT
            if let Plan::Query { root, params: _ } = values_plan.as_ref() {
                // Execute the insert directly with the VALUES source
                let insert_result = super::insert::execute_insert(
                    name.clone(),
                    None,
                    *root.clone(),
                    storage,
                    tx_ctx,
                    params,
                )?;

                // Return success message with row count
                if let ExecutionResult::Modified(count) = insert_result {
                    Ok(ExecutionResult::Ddl(format!(
                        "{} with {} rows inserted",
                        create_msg, count
                    )))
                } else {
                    Ok(ExecutionResult::Ddl(create_msg))
                }
            } else {
                Ok(ExecutionResult::Ddl(create_msg))
            }
        }
    }
}

/// Execute a plan node for reading with immutable storage reference
pub fn execute_node_read<'a>(
    node: Node,
    storage: &'a Storage,
    tx_ctx: &mut TransactionContext,
    params: Option<&Vec<Value>>,
) -> Result<Rows<'a>> {
    match node {
        Node::Scan { table, .. } => {
            // True streaming with immutable storage!
            let iter = storage
                .iter(tx_ctx.id, &table)?
                .map(|result| result.map(|row| row.values.clone().into()));

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
                filter_values.push(expression::evaluate(value_expr, None, tx_ctx, params)?);
            }

            // Try to use index lookup first
            if storage.has_index(&table, &index_name) {
                // Type-coerce filter values based on column schema
                let schemas = storage.get_schemas();
                let mut coerced_values = filter_values.clone();
                if let Some(schema) = schemas.get(&table)
                    && let Some(column) = schema.columns.iter().find(|c| c.name == *index_name)
                {
                    for (i, value) in filter_values.iter().enumerate() {
                        if i < coerced_values.len() {
                            coerced_values[i] =
                                crate::coercion::coerce_value(value.clone(), &column.data_type)?;
                        }
                    }
                }

                // Use index lookup for O(log n) performance
                let rows = storage.index_lookup_rows(&index_name, coerced_values, tx_ctx.id)?;

                // Convert to iterator format
                let iter = rows.into_iter().map(|row| Ok(row.values.clone().into()));
                return Ok(Box::new(iter));
            }

            // Fall back to filtered table scan if no index exists
            // We need to check if we can resolve the index_name to columns
            let schemas = storage.get_schemas();
            let schema = schemas
                .get(&table)
                .ok_or_else(|| Error::TableNotFound(table.clone()))?;

            // Check if we can get column info from the index metadata
            let column_names = storage.get_index_columns(&table, &index_name).or_else(|| {
                // Fallback: assume index_name is a column name for backward compatibility
                if schema.columns.iter().any(|c| c.name == index_name) {
                    Some(vec![index_name.clone()])
                } else {
                    None
                }
            });

            if let Some(columns) = column_names {
                // Get column indices
                let mut col_indices = Vec::new();
                for col_name in &columns {
                    if let Some(idx) = schema.columns.iter().position(|c| &c.name == col_name) {
                        col_indices.push(idx);
                    } else {
                        // Column not found, can't filter
                        let iter = storage
                            .iter(tx_ctx.id, &table)?
                            .map(|result| result.map(|row| row.values.clone().into()));
                        return Ok(Box::new(iter));
                    }
                }

                // Filter by checking all column values match
                if col_indices.len() == filter_values.len() {
                    let iter = storage.iter(tx_ctx.id, &table)?.filter_map(move |result| {
                        match result {
                            Ok(row) => {
                                let values: Arc<Vec<Value>> = row.values.clone().into();
                                // Check if all indexed columns match the filter values
                                for (idx, expected_val) in col_indices.iter().zip(&filter_values) {
                                    if values.get(*idx) != Some(expected_val) {
                                        return None;
                                    }
                                }
                                Some(Ok(values))
                            }
                            Err(e) => Some(Err(e)),
                        }
                    });
                    return Ok(Box::new(iter));
                }
            }

            // Can't determine columns, fall back to full scan
            let iter = storage
                .iter(tx_ctx.id, &table)?
                .map(|result| result.map(|row| row.values.clone().into()));
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
                        .map(|e| expression::evaluate(e, None, tx_ctx, params))
                        .collect::<Result<Vec<_>>>()
                })
                .transpose()?;
            let end_values = end
                .as_ref()
                .map(|exprs| {
                    exprs
                        .iter()
                        .map(|e| expression::evaluate(e, None, tx_ctx, params))
                        .collect::<Result<Vec<_>>>()
                })
                .transpose()?;

            // Try to use index range lookup
            if storage.has_index(&table, &index_name) {
                // Use index range lookup for O(log n) performance
                let rows = storage.index_range_lookup_rows(
                    &index_name,
                    start_values,
                    end_values,
                    reverse,
                    tx_ctx.id,
                )?;

                // Convert to iterator format
                let iter = rows.into_iter().map(|row| Ok(row.values.clone().into()));
                return Ok(Box::new(iter));
            }

            // Fall back to filtered range scan if no index exists
            let schemas = storage.get_schemas();
            let schema = schemas
                .get(&table)
                .ok_or_else(|| Error::TableNotFound(table.clone()))?;

            // Get column info from the index metadata
            let column_names = storage.get_index_columns(&table, &index_name).or_else(|| {
                // Fallback: assume index_name is a column name
                if schema.columns.iter().any(|c| c.name == index_name) {
                    Some(vec![index_name.clone()])
                } else {
                    None
                }
            });

            if let Some(columns) = column_names {
                // Get column indices
                let mut col_indices = Vec::new();
                for col_name in &columns {
                    if let Some(idx) = schema.columns.iter().position(|c| &c.name == col_name) {
                        col_indices.push(idx);
                    } else {
                        // Column not found, can't filter
                        let iter = storage
                            .iter(tx_ctx.id, &table)?
                            .map(|result| result.map(|row| row.values.clone().into()));
                        return Ok(Box::new(iter));
                    }
                }

                // Perform range filtering
                let start_vals = start_values.clone();
                let end_vals = end_values.clone();
                let start_incl = start_inclusive;
                let end_incl = end_inclusive;

                let iter = storage.iter(tx_ctx.id, &table)?.filter_map(move |result| {
                    match result {
                        Ok(row) => {
                            let values: Arc<Vec<Value>> = row.values.clone().into();
                            // Extract values for the indexed columns
                            let mut row_values = Vec::new();
                            for &idx in &col_indices {
                                if let Some(val) = values.get(idx) {
                                    row_values.push(val.clone());
                                } else {
                                    return None; // NULL in key, skip
                                }
                            }

                            // Check start bound
                            if let Some(ref start) = start_vals {
                                match operators::compare_composite(&row_values, start).ok() {
                                    Some(std::cmp::Ordering::Less) => return None,
                                    Some(std::cmp::Ordering::Equal) if !start_incl => return None,
                                    _ => {}
                                }
                            }

                            // Check end bound
                            if let Some(ref end) = end_vals {
                                match operators::compare_composite(&row_values, end).ok() {
                                    Some(std::cmp::Ordering::Greater) => return None,
                                    Some(std::cmp::Ordering::Equal) if !end_incl => return None,
                                    _ => {}
                                }
                            }

                            Some(Ok(values))
                        }
                        Err(e) => Some(Err(e)),
                    }
                });
                return Ok(Box::new(iter));
            }

            // Can't determine columns, fall back to full scan
            let iter = storage
                .iter(tx_ctx.id, &table)?
                .map(|result| result.map(|row| row.values.clone().into()));
            Ok(Box::new(iter))
        }

        Node::Filter { source, predicate } => {
            let source_rows = execute_node_read(*source, storage, tx_ctx, params)?;

            // Clone transaction context and params for use in closure
            let tx_ctx_clone = tx_ctx.clone();
            let params_clone = params.cloned();
            // We need storage for subquery evaluation - capture a raw pointer
            // SAFETY: We know storage lives for the entire query execution
            let storage_ptr = storage as *const Storage;

            let filtered = source_rows.filter_map(move |row| match row {
                Ok(row) => {
                    // SAFETY: storage outlives the iterator since it's borrowed for 'a
                    let storage_ref = unsafe { &*storage_ptr };
                    match expression::evaluate_with_arc_and_storage(
                        &predicate,
                        Some(&row),
                        &tx_ctx_clone,
                        params_clone.as_ref(),
                        Some(storage_ref),
                    ) {
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
            let params_clone = params.cloned();

            let values_iter = rows.into_iter().map(move |row| {
                let mut value_row = Vec::new();
                for expr in row {
                    value_row.push(expression::evaluate(
                        &expr,
                        None,
                        &tx_ctx_clone,
                        params_clone.as_ref(),
                    )?);
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
            let source_rows = execute_node_read(*source, storage, tx_ctx, params)?;

            // Clone transaction context and params for use in closure
            let tx_ctx_clone = tx_ctx.clone();
            let params_clone = params.cloned();

            let projected = source_rows.map(move |row| match row {
                Ok(row) => {
                    let mut result = Vec::with_capacity(expressions.len());
                    for expr in &expressions {
                        result.push(expression::evaluate_with_arc(
                            expr,
                            Some(&row),
                            &tx_ctx_clone,
                            params_clone.as_ref(),
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
            let rows = execute_node_read(*source, storage, tx_ctx, params)?;
            Ok(Box::new(rows.take(limit)))
        }

        Node::Offset { source, offset } => {
            let rows = execute_node_read(*source, storage, tx_ctx, params)?;
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
            let rows = execute_node_read(*source, storage, tx_ctx, params)?;
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
            let left_rows = execute_node_read(*left, storage, tx_ctx, params)?;
            let right_rows = execute_node_read(*right, storage, tx_ctx, params)?;

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

            let left_rows = execute_node_read(*left, storage, tx_ctx, params)?;
            let right_rows = execute_node_read(*right, storage, tx_ctx, params)?;

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
            let rows = execute_node_read(*source, storage, tx_ctx, params)?;

            // Must materialize to sort
            let mut collected: Vec<_> = rows.collect::<Result<Vec<_>>>()?;

            // Sort based on order_by expressions
            collected.sort_by(|a, b| {
                for (expr, direction) in &order_by {
                    let val_a = expression::evaluate_with_arc(expr, Some(a), tx_ctx, params)
                        .unwrap_or(Value::Null);
                    let val_b = expression::evaluate_with_arc(expr, Some(b), tx_ctx, params)
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
