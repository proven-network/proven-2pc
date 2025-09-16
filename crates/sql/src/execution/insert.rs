//! INSERT statement execution
//!
//! This module handles the execution of INSERT statements with proper
//! MVCC transaction support and type coercion.

use crate::error::{Error, Result};
use crate::execution::ExecutionResult;
use crate::planning::plan::Node;
use crate::storage::{MvccStorage, write_ops};
use crate::stream::TransactionContext;
use crate::types::value::Value;

/// Execute INSERT with phased read-then-write approach
pub fn execute_insert(
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
        let rows = super::executor::execute_node_read(source, storage_ref, tx_ctx)?;
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
        let coerced_row = crate::semantic::coercion::coerce_row(final_row, schema)?;

        write_ops::insert(storage, tx_ctx, &table, coerced_row)?;
        count += 1;
    }

    Ok(ExecutionResult::Modified(count))
}
