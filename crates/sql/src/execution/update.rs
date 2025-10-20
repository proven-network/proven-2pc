//! UPDATE statement execution
//!
//! This module handles the execution of UPDATE statements with proper
//! MVCC transaction support and type coercion.

use crate::error::{Error, Result};
use crate::execution::{ExecutionResult, expression, helpers};
use crate::storage::SqlStorage;
use crate::types::context::ExecutionContext;
use crate::types::expression::Expression;
use crate::types::plan::Node;
use crate::types::schema::Table;
use crate::types::{Value, ValueExt};

/// Validate foreign key constraints for an updated row
fn validate_foreign_keys_on_update(
    row: &[Value],
    _old_row: &[Value],
    schema: &Table,
    storage: &SqlStorage,
    tx_ctx: &ExecutionContext,
) -> Result<()> {
    // Check each foreign key constraint on this table
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

/// Execute UPDATE with phased read-then-write approach
pub fn execute_update(
    table: String,
    assignments: Vec<(usize, Expression)>,
    source: Node,
    storage: &mut SqlStorage,
    batch: &mut fjall::Batch,
    tx_ctx: &mut ExecutionContext,
    params: Option<&Vec<Value>>,
) -> Result<ExecutionResult> {
    // Get schema
    let schemas = storage.get_schemas();
    let schema = schemas
        .get(&table)
        .ok_or_else(|| Error::TableNotFound(table.clone()))?
        .clone();

    // Phase 1: Scan to find matching rows
    let matching_rows: Vec<(u64, Vec<Value>)> = {
        let mut matches = Vec::new();
        for result in storage.scan_table(&table, tx_ctx.txn_id)? {
            let (row_id, values) = result?;
            let row_arc = std::sync::Arc::new(values.clone());

            let is_match = match &source {
                Node::Filter { predicate, .. } => expression::evaluate_with_arc_and_storage(
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

            if is_match {
                matches.push((row_id, values));
            }
        }
        matches
    };

    // Phase 2: Compute updated values and check unique constraints
    let mut updates = Vec::new();
    for (row_id, old_values) in matching_rows {
        let current_arc = std::sync::Arc::new(old_values.clone());
        let mut new_values = old_values.clone();

        for &(col_idx, ref expr) in &assignments {
            new_values[col_idx] = expression::evaluate_with_arc_and_storage(
                expr,
                Some(&current_arc),
                tx_ctx,
                params,
                Some(storage),
            )?;
        }

        // Apply type coercion
        let coerced_row = crate::coercion::coerce_row(new_values, &schema)?;

        // Validate row against schema constraints (NULL, type checks)
        schema.validate_row(&coerced_row)?;

        // Validate foreign keys (simplified - no cascades for now)
        validate_foreign_keys_on_update(&coerced_row, &old_values, &schema, storage, tx_ctx)?;

        updates.push((row_id, old_values, coerced_row));
    }

    // Phase 2.5: Check unique constraints - both within batch and against storage
    let unique_indexes = storage.get_unique_indexes(&table);

    // First, check for duplicates within the batch itself
    use std::collections::HashMap;
    for index in &unique_indexes {
        let mut seen_values: HashMap<Vec<Value>, u64> = HashMap::new();
        for (row_id, _, new_values) in &updates {
            let index_values = helpers::extract_index_values(new_values, index, &schema, tx_ctx)?;

            // Skip NULL values in unique constraint checks (SQL standard)
            if index_values.iter().any(|v| v.is_null()) {
                continue;
            }

            if let Some(first_row_id) = seen_values.get(&index_values) {
                return Err(Error::UniqueConstraintViolation(format!(
                    "Duplicate value for unique index '{}' in batch (rows {} and {}): {:?}",
                    index.name, first_row_id, row_id, index_values
                )));
            }
            seen_values.insert(index_values, *row_id);
        }
    }

    // Then, check each update against existing data (excluding its own row_id)
    for (row_id, _, new_values) in &updates {
        for index in &unique_indexes {
            let new_index_values =
                helpers::extract_index_values(new_values, index, &schema, tx_ctx)?;

            // Skip NULL values in unique constraint checks (SQL standard)
            if new_index_values.iter().any(|v| v.is_null()) {
                continue;
            }

            if storage.check_unique_violation(
                &index.name,
                new_index_values.clone(),
                Some(*row_id), // Exclude current row from check
                tx_ctx.txn_id,
            )? {
                return Err(Error::UniqueConstraintViolation(format!(
                    "Duplicate value for unique index '{}': {:?}",
                    index.name, new_index_values
                )));
            }
        }
    }

    // Phase 3: Write updates to batch (engine will commit with predicates)
    let all_indexes: Vec<_> = storage
        .get_table_indexes(&table)
        .into_iter()
        .cloned()
        .collect();

    for (row_id, old_values, new_values) in &updates {
        // Update row
        storage.write_row(
            batch,
            &table,
            *row_id,
            new_values,
            tx_ctx.txn_id,
            tx_ctx.log_index,
        )?;

        // Update indexes if values changed
        for index in &all_indexes {
            let old_index_values =
                helpers::extract_index_values(old_values, index, &schema, tx_ctx)?;
            let new_index_values =
                helpers::extract_index_values(new_values, index, &schema, tx_ctx)?;

            if old_index_values != new_index_values {
                storage.update_index_entries(
                    batch,
                    &index.name,
                    old_index_values,
                    new_index_values,
                    *row_id,
                    tx_ctx.txn_id,
                    tx_ctx.log_index,
                )?;
            }
        }
    }

    // Engine will add predicates and commit batch
    Ok(ExecutionResult::Modified(updates.len()))
}
