//! INSERT statement execution
//!
//! This module handles the execution of INSERT statements with proper
//! MVCC transaction support and type coercion.

use crate::error::{Error, Result};
use crate::execution::{ExecutionResult, helpers};
use crate::planning::plan::Node;
use crate::storage::SqlStorage;
use crate::types::Value;
use crate::types::context::ExecutionContext;
use crate::types::expression::{DefaultExpression, Expression};
use crate::types::schema::Table;

/// Evaluate a DEFAULT expression at INSERT time with transaction context
fn evaluate_default(
    expr: &DefaultExpression,
    tx_ctx: &ExecutionContext,
    storage: &SqlStorage,
) -> Result<Value> {
    // Convert DefaultExpression to Expression and evaluate using the general evaluator
    let full_expr: Expression = expr.clone().into();
    crate::execution::expression::evaluate_with_storage(
        &full_expr,
        None,
        tx_ctx,
        None,
        Some(storage),
    )
}

/// Validate foreign key constraints for a row before insertion
fn validate_foreign_keys(
    row: &[Value],
    schema: &Table,
    storage: &SqlStorage,
    tx_ctx: &ExecutionContext,
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
        let ref_iter = storage.scan_table(&fk.referenced_table, tx_ctx.txn_id)?;

        for ref_row_result in ref_iter {
            let (_, ref_values) = ref_row_result?;

            // Check if all referenced columns match
            let mut all_match = true;
            for (i, &ref_idx) in ref_indices.iter().enumerate() {
                if &ref_values[ref_idx] != fk_values[i] {
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
    storage: &mut SqlStorage,
    batch: &mut fjall::Batch,
    tx_ctx: &mut ExecutionContext,
    params: Option<&Vec<Value>>,
) -> Result<ExecutionResult> {
    // Get schema from storage
    let schemas = storage.get_schemas();
    let schema = schemas
        .get(&table)
        .ok_or_else(|| Error::TableNotFound(table.clone()))?;

    // Phase 1: Read all source rows (immutable borrow)
    let rows_to_insert = {
        let rows = super::executor::execute_node_read(source, storage, tx_ctx, params)?;
        rows.collect::<Result<Vec<_>>>()?
    }; // Immutable borrow ends here

    // Phase 2: Use batch insert for atomic operation
    if rows_to_insert.is_empty() {
        return Ok(ExecutionResult::Modified(0));
    }

    // Prepare all rows for batch insert
    let mut final_rows = Vec::new();
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
                } else if let Some(ref default_expr) = col.default {
                    // Evaluate DEFAULT expression with transaction context
                    let value = evaluate_default(default_expr, tx_ctx, storage)?;
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
                if let Some(ref default_expr) = col.default {
                    // Evaluate DEFAULT expression with transaction context
                    let value = evaluate_default(default_expr, tx_ctx, storage)?;
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

        // Validate row against schema constraints (NULL, type checks)
        schema.validate_row(&coerced_row)?;

        // Validate foreign key constraints before insertion
        validate_foreign_keys(&coerced_row, schema, storage, tx_ctx)?;

        final_rows.push(coerced_row);
    }

    // Phase 3: Generate row IDs
    let mut row_ids = Vec::with_capacity(final_rows.len());
    for _ in &final_rows {
        row_ids.push(storage.generate_row_id(&table)?);
    }

    // Phase 4: Check unique constraints
    let unique_indexes = storage.get_unique_indexes(&table);

    // First, check for duplicates within the batch itself
    use std::collections::HashMap;
    for index in &unique_indexes {
        let mut seen_values: HashMap<Vec<Value>, usize> = HashMap::new();
        for (i, row) in final_rows.iter().enumerate() {
            let index_values = helpers::extract_index_values(row, index, schema, tx_ctx)?;

            // Skip NULL values in unique constraint checks (SQL standard)
            if index_values.iter().any(|v| v.is_null()) {
                continue;
            }

            if let Some(first_idx) = seen_values.get(&index_values) {
                return Err(Error::UniqueConstraintViolation(format!(
                    "Duplicate value for unique index '{}' in batch (rows {} and {}): {:?}",
                    index.name, first_idx, i, index_values
                )));
            }
            seen_values.insert(index_values, i);
        }
    }

    // Then, check against existing data in storage
    for (row, _) in final_rows.iter().zip(&row_ids) {
        for index in &unique_indexes {
            let index_values = helpers::extract_index_values(row, index, schema, tx_ctx)?;

            // Skip NULL values in unique constraint checks (SQL standard)
            if index_values.iter().any(|v| v.is_null()) {
                continue;
            }

            if storage.check_unique_violation(
                &index.name,
                index_values.clone(),
                None,
                tx_ctx.txn_id,
            )? {
                return Err(Error::UniqueConstraintViolation(format!(
                    "Duplicate value for unique index '{}': {:?}",
                    index.name, index_values
                )));
            }
        }
    }

    // Phase 5: Write data + indexes to batch (engine will commit with predicates)
    let all_indexes: Vec<_> = storage
        .get_table_indexes(&table)
        .into_iter()
        .cloned()
        .collect();

    for (row, row_id) in final_rows.iter().zip(&row_ids) {
        // Insert row
        storage.write_row(batch, &table, *row_id, row, tx_ctx.txn_id, tx_ctx.log_index)?;

        // Update all indexes for this table
        for index in &all_indexes {
            let index_values = helpers::extract_index_values(row, index, schema, tx_ctx)?;
            storage.insert_index_entry(
                batch,
                &index.name,
                index_values,
                *row_id,
                tx_ctx.txn_id,
                tx_ctx.log_index,
            )?;
        }
    }

    // Engine will add predicates and commit batch
    Ok(ExecutionResult::Modified(row_ids.len()))
}
