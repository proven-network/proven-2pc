//! INSERT statement execution
//!
//! This module handles the execution of INSERT statements with proper
//! MVCC transaction support and type coercion.

use crate::error::{Error, Result};
use crate::execution::ExecutionResult;
use crate::planning::plan::Node;
use crate::storage::{MvccStorage, read_ops, write_ops};
use crate::stream::TransactionContext;
use crate::types::schema::Table;
use crate::types::value::{Row, Value};

/// Validate foreign key constraints for a row before insertion
fn validate_foreign_keys(
    row: &Row,
    schema: &Table,
    storage: &MvccStorage,
    tx_ctx: &mut TransactionContext,
) -> Result<()> {
    // Check each foreign key constraint
    let schemas = storage.get_schemas();
    for fk in &schema.foreign_keys {
        // Get the referenced table schema
        let ref_schema = schemas.get(&fk.referenced_table).ok_or_else(|| {
            Error::ForeignKeyViolation(format!(
                "Referenced table '{}' does not exist",
                fk.referenced_table
            ))
        })?;

        // Build the foreign key values from the row
        let mut fk_values = Vec::new();
        for col_name in &fk.columns {
            let (col_idx, _) = schema
                .get_column(col_name)
                .ok_or_else(|| Error::ColumnNotFound(col_name.clone()))?;
            fk_values.push(&row[col_idx]);
        }

        // If all foreign key values are NULL, skip the check
        if fk_values.iter().all(|v| v.is_null()) {
            continue;
        }

        // Find the referenced column indices
        let mut ref_indices = Vec::new();
        for ref_col in &fk.referenced_columns {
            let (idx, _) = ref_schema
                .get_column(ref_col)
                .ok_or_else(|| Error::ColumnNotFound(ref_col.clone()))?;
            ref_indices.push(idx);
        }

        // Scan the referenced table to check if the value exists
        let mut found = false;
        let ref_iter = read_ops::scan_iter(storage, tx_ctx, &fk.referenced_table)?;

        for ref_row in ref_iter {
            let ref_row = ref_row.as_ref();

            // Check if all referenced columns match
            let mut all_match = true;
            for (i, &ref_idx) in ref_indices.iter().enumerate() {
                if &ref_row[ref_idx] != fk_values[i] {
                    all_match = false;
                    break;
                }
            }

            if all_match {
                found = true;
                break;
            }
        }

        if !found {
            return Err(Error::ForeignKeyViolation(format!(
                "Foreign key constraint violation: referenced value {:?} not found in {}.{:?}",
                fk_values
                    .iter()
                    .map(|v| format!("{}", v))
                    .collect::<Vec<_>>(),
                fk.referenced_table,
                fk.referenced_columns
            )));
        }
    }

    Ok(())
}

/// Execute INSERT with phased read-then-write approach
pub fn execute_insert(
    table: String,
    columns: Option<Vec<usize>>,
    source: Node,
    storage: &mut MvccStorage,
    tx_ctx: &mut TransactionContext,
    params: Option<&Vec<Value>>,
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
        let rows = super::executor::execute_node_read(source, storage_ref, tx_ctx, params)?;
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
        let coerced_row = crate::coercion::coerce_row(final_row, schema)?;

        // Validate foreign key constraints before insertion
        validate_foreign_keys(&coerced_row, schema, storage, tx_ctx)?;

        write_ops::insert(storage, tx_ctx, &table, coerced_row)?;
        count += 1;
    }

    Ok(ExecutionResult::Modified(count))
}
