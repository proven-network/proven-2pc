//! DELETE statement execution
//!
//! This module handles the execution of DELETE statements with proper
//! MVCC transaction support.

use crate::error::{Error, Result};
use crate::execution::{ExecutionResult, expression};
use crate::parsing::ast::ddl::ReferentialAction;
use crate::planning::plan::Node;
use crate::storage::Storage;
use crate::stream::TransactionContext;
use crate::types::value::{Row, Value};

/// Cascading operation to perform
#[derive(Debug, Clone)]
enum CascadeOp {
    Delete {
        table: String,
        row_id: u64,
    },
    SetNull {
        table: String,
        row_id: u64,
        col_idx: usize,
    },
    SetDefault {
        table: String,
        row_id: u64,
        col_idx: usize,
        default: Value,
    },
}

/// Helper function to update a single column value
fn update_column_value(
    storage: &mut Storage,
    tx_ctx: &mut TransactionContext,
    table: &str,
    row_id: u64,
    col_idx: usize,
    new_value: Value,
) -> Result<()> {
    // Get the current row
    let current_row = {
        let iter = storage.iter_with_ids(tx_ctx.id, table)?;
        let mut found_row = None;
        for result in iter {
            let (rid, row) = result?;
            if rid == row_id {
                found_row = Some(row.values.clone());
                break;
            }
        }
        found_row.ok_or_else(|| {
            Error::ExecutionError(format!("Row {} not found in table {}", row_id, table))
        })?
    };

    // Update the specific column
    let mut updated_row = current_row;
    updated_row[col_idx] = new_value;

    // Write the updated row
    storage.update(tx_ctx.id, table, row_id, updated_row)?;
    Ok(())
}

/// Check if deleting a row would violate foreign key constraints and collect cascade operations
fn check_and_collect_cascade_ops(
    _row_id: u64,
    row: &Row,
    table_name: &str,
    storage: &mut Storage,
    tx_ctx: &mut TransactionContext,
) -> Result<Vec<CascadeOp>> {
    let mut cascade_ops = Vec::new();
    let schemas = storage.get_schemas();
    let schema = schemas
        .get(table_name)
        .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;

    // If this table doesn't have a primary key, no other table can reference it
    let pk_idx = match schema.primary_key {
        Some(idx) => idx,
        None => return Ok(cascade_ops),
    };

    let pk_value = &row[pk_idx];

    // Check all tables that might have foreign keys pointing to this table
    for (other_table_name, other_schema) in schemas.iter() {
        for fk in &other_schema.foreign_keys {
            if fk.referenced_table == table_name {
                // Find which column in the referencing table has the foreign key
                let fk_col_idx = other_schema
                    .get_column(&fk.columns[0])
                    .ok_or_else(|| Error::ColumnNotFound(fk.columns[0].clone()))?
                    .0;

                // Find all rows in the referencing table that point to this row
                let ref_iter = storage.iter_with_ids(tx_ctx.id, other_table_name)?;
                for result in ref_iter {
                    let (ref_row_id, ref_row) = result?;
                    if &ref_row.values[fk_col_idx] == pk_value
                        && !ref_row.values[fk_col_idx].is_null()
                    {
                        // Found a referencing row - apply the referential action
                        match fk.on_delete {
                            ReferentialAction::Restrict | ReferentialAction::NoAction => {
                                return Err(Error::ForeignKeyViolation(format!(
                                    "Cannot delete row from '{}': referenced by foreign key in '{}'",
                                    table_name, other_table_name
                                )));
                            }
                            ReferentialAction::Cascade => {
                                // Cascade the delete
                                cascade_ops.push(CascadeOp::Delete {
                                    table: other_table_name.clone(),
                                    row_id: ref_row_id,
                                });
                            }
                            ReferentialAction::SetNull => {
                                // Set the foreign key column to NULL
                                cascade_ops.push(CascadeOp::SetNull {
                                    table: other_table_name.clone(),
                                    row_id: ref_row_id,
                                    col_idx: fk_col_idx,
                                });
                            }
                            ReferentialAction::SetDefault => {
                                // Set the foreign key column to its default value
                                let default_value = other_schema.columns[fk_col_idx]
                                    .default
                                    .clone()
                                    .unwrap_or(Value::Null);
                                cascade_ops.push(CascadeOp::SetDefault {
                                    table: other_table_name.clone(),
                                    row_id: ref_row_id,
                                    col_idx: fk_col_idx,
                                    default: default_value,
                                });
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(cascade_ops)
}

/// Execute DELETE with phased read-then-write approach
pub fn execute_delete(
    table: String,
    source: Node,
    storage: &mut Storage,
    tx_ctx: &mut TransactionContext,
    params: Option<&Vec<Value>>,
) -> Result<ExecutionResult> {
    // Phase 1: Read rows with IDs that match the WHERE clause
    let rows_to_delete = {
        let iter = storage.iter_with_ids(tx_ctx.id, &table)?;
        let mut to_delete = Vec::new();

        for result in iter {
            let (row_id, row) = result?;
            let row_arc = std::sync::Arc::new(row.values.clone());
            let matches = match &source {
                Node::Filter { predicate, .. } => expression::evaluate_with_storage(
                    predicate,
                    Some(&row_arc),
                    tx_ctx,
                    params,
                    Some(storage),
                )?
                .to_bool()
                .unwrap_or(false),
                Node::Scan { .. } => true,
                _ => true,
            };

            if matches {
                to_delete.push((row_id, row_arc));
            }
        }
        to_delete
    }; // Immutable borrow ends here

    // Phase 2: Collect cascade operations and delete rows
    let mut all_cascade_ops = Vec::new();
    let mut count = 0;

    // First, collect all cascade operations for rows to be deleted
    for (row_id, row) in &rows_to_delete {
        let cascade_ops = check_and_collect_cascade_ops(*row_id, row, &table, storage, tx_ctx)?;
        all_cascade_ops.extend(cascade_ops);
    }

    // Phase 3: Execute cascade operations first (before deleting the parent rows)
    for op in all_cascade_ops {
        match op {
            CascadeOp::Delete {
                table: cascade_table,
                row_id: cascade_row_id,
            } => {
                // Recursively handle cascading deletes
                // Get the row data first for potential further cascades
                let cascade_row = {
                    let iter = storage.iter_with_ids(tx_ctx.id, &cascade_table)?;
                    let mut found_row = None;
                    for result in iter {
                        let (rid, r) = result?;
                        if rid == cascade_row_id {
                            found_row = Some(r.values.clone());
                            break;
                        }
                    }
                    found_row
                };

                if let Some(row_to_cascade) = cascade_row {
                    // Check for further cascades
                    let further_cascades = check_and_collect_cascade_ops(
                        cascade_row_id,
                        &row_to_cascade,
                        &cascade_table,
                        storage,
                        tx_ctx,
                    )?;
                    // Apply further cascades recursively
                    for further_op in further_cascades {
                        match further_op {
                            CascadeOp::Delete {
                                table: t,
                                row_id: r,
                            } => {
                                storage.delete(tx_ctx.id, &t, r)?;
                            }
                            CascadeOp::SetNull {
                                table: t,
                                row_id: r,
                                col_idx,
                            } => {
                                update_column_value(storage, tx_ctx, &t, r, col_idx, Value::Null)?;
                            }
                            CascadeOp::SetDefault {
                                table: t,
                                row_id: r,
                                col_idx,
                                default,
                            } => {
                                update_column_value(storage, tx_ctx, &t, r, col_idx, default)?;
                            }
                        }
                    }
                    // Now delete the cascaded row
                    storage.delete(tx_ctx.id, &cascade_table, cascade_row_id)?;
                }
            }
            CascadeOp::SetNull {
                table: cascade_table,
                row_id: cascade_row_id,
                col_idx,
            } => {
                update_column_value(
                    storage,
                    tx_ctx,
                    &cascade_table,
                    cascade_row_id,
                    col_idx,
                    Value::Null,
                )?;
            }
            CascadeOp::SetDefault {
                table: cascade_table,
                row_id: cascade_row_id,
                col_idx,
                default,
            } => {
                update_column_value(
                    storage,
                    tx_ctx,
                    &cascade_table,
                    cascade_row_id,
                    col_idx,
                    default,
                )?;
            }
        }
    }

    // Phase 4: Delete the original rows
    for (row_id, _) in rows_to_delete {
        storage.delete(tx_ctx.id, &table, row_id)?;
        count += 1;
    }

    Ok(ExecutionResult::Modified(count))
}
