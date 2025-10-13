//! DELETE statement execution
//!
//! This module handles the execution of DELETE statements with proper
//! MVCC transaction support.

use crate::error::{Error, Result};
use crate::execution::{ExecutionResult, expression, helpers};
use crate::planning::plan::Node;
use crate::storage::SqlStorage;
use crate::types::context::ExecutionContext;
use crate::types::{Value, ValueExt};

/// Execute DELETE with phased read-then-write approach
pub fn execute_delete(
    table: String,
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

    // Phase 2: Handle foreign key constraints for rows being deleted
    handle_foreign_key_on_delete(&table, &matching_rows, &schema, storage, batch, tx_ctx)?;

    // Phase 3: Write deletes to batch (engine will commit with predicates)
    let all_indexes: Vec<_> = storage
        .get_table_indexes(&table)
        .into_iter()
        .cloned()
        .collect();

    for (row_id, values) in &matching_rows {
        // Delete row
        storage.delete_row(batch, &table, *row_id, tx_ctx.txn_id, tx_ctx.log_index)?;

        // Delete from all indexes
        for index in &all_indexes {
            let index_values = helpers::extract_index_values(values, index, &schema)?;
            storage.delete_index_entry(
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
    Ok(ExecutionResult::Modified(matching_rows.len()))
}

/// Handle foreign key constraints when deleting rows from a table
fn handle_foreign_key_on_delete(
    table: &str,
    rows_being_deleted: &[(u64, Vec<Value>)],
    schema: &crate::types::schema::Table,
    storage: &mut SqlStorage,
    batch: &mut fjall::Batch,
    tx_ctx: &mut ExecutionContext,
) -> Result<()> {
    use crate::parsing::ast::ddl::ReferentialAction;

    // Find all tables that have foreign keys referencing this table
    let all_schemas = storage.get_schemas();
    let mut referencing_tables = Vec::new();

    for (other_table_name, other_schema) in all_schemas.iter() {
        for fk in &other_schema.foreign_keys {
            if fk.referenced_table == table {
                referencing_tables.push((
                    other_table_name.clone(),
                    other_schema.clone(),
                    fk.clone(),
                ));
            }
        }
    }

    // If no tables reference this one, we're done
    if referencing_tables.is_empty() {
        return Ok(());
    }

    // For each row being deleted
    for (_row_id, row_values) in rows_being_deleted {
        // Get the primary key value(s) of the row being deleted
        let pk_idx = schema.primary_key.ok_or_else(|| {
            Error::Other(
                "Cannot delete from table without primary key when foreign keys reference it"
                    .to_string(),
            )
        })?;
        let pk_value = &row_values[pk_idx];

        // Check each referencing table
        for (ref_table_name, ref_schema, fk) in &referencing_tables {
            // Find referencing rows in this table
            let mut referencing_rows = Vec::new();
            for result in storage.scan_table(ref_table_name, tx_ctx.txn_id)? {
                let (ref_row_id, ref_row_values) = result?;

                // Check if this row references the row being deleted
                let fk_column_idx = ref_schema
                    .columns
                    .iter()
                    .position(|c| c.name == fk.columns[0])
                    .ok_or_else(|| Error::ColumnNotFound(fk.columns[0].clone()))?;

                let fk_value = &ref_row_values[fk_column_idx];

                // Skip if FK is NULL (NULL values don't participate in FK constraints)
                if fk_value.is_null() {
                    continue;
                }

                // Check if this row references the row being deleted
                if fk_value == pk_value {
                    referencing_rows.push((ref_row_id, ref_row_values));
                }
            }

            // Handle based on ON DELETE action
            match fk.on_delete {
                ReferentialAction::NoAction | ReferentialAction::Restrict => {
                    // Reject the deletion if there are referencing rows
                    if !referencing_rows.is_empty() {
                        return Err(Error::ForeignKeyViolation(format!(
                            "Cannot delete from table '{}': {} row(s) in '{}' reference this row",
                            table,
                            referencing_rows.len(),
                            ref_table_name
                        )));
                    }
                }
                ReferentialAction::Cascade => {
                    // Delete all referencing rows
                    let ref_indexes: Vec<_> = storage
                        .get_table_indexes(ref_table_name)
                        .into_iter()
                        .cloned()
                        .collect();

                    for (ref_row_id, ref_row_values) in &referencing_rows {
                        // Delete the referencing row
                        storage.delete_row(
                            batch,
                            ref_table_name,
                            *ref_row_id,
                            tx_ctx.txn_id,
                            tx_ctx.log_index,
                        )?;

                        // Delete from indexes
                        for index in &ref_indexes {
                            let index_values =
                                helpers::extract_index_values(ref_row_values, index, ref_schema)?;
                            storage.delete_index_entry(
                                batch,
                                &index.name,
                                index_values,
                                *ref_row_id,
                                tx_ctx.txn_id,
                                tx_ctx.log_index,
                            )?;
                        }
                    }
                }
                ReferentialAction::SetNull => {
                    // Set foreign key columns to NULL in referencing rows
                    let ref_indexes: Vec<_> = storage
                        .get_table_indexes(ref_table_name)
                        .into_iter()
                        .cloned()
                        .collect();

                    for (ref_row_id, ref_row_values) in &referencing_rows {
                        let mut new_values = ref_row_values.clone();

                        // Set FK column to NULL
                        let fk_column_idx = ref_schema
                            .columns
                            .iter()
                            .position(|c| c.name == fk.columns[0])
                            .ok_or_else(|| Error::ColumnNotFound(fk.columns[0].clone()))?;

                        new_values[fk_column_idx] = Value::Null;

                        // Validate the updated row
                        ref_schema.validate_row(&new_values)?;

                        // Update the row
                        storage.write_row(
                            batch,
                            ref_table_name,
                            *ref_row_id,
                            &new_values,
                            tx_ctx.txn_id,
                            tx_ctx.log_index,
                        )?;

                        // Update indexes
                        for index in &ref_indexes {
                            let old_index_values =
                                helpers::extract_index_values(ref_row_values, index, ref_schema)?;
                            let new_index_values =
                                helpers::extract_index_values(&new_values, index, ref_schema)?;

                            if old_index_values != new_index_values {
                                storage.update_index_entries(
                                    batch,
                                    &index.name,
                                    old_index_values,
                                    new_index_values,
                                    *ref_row_id,
                                    tx_ctx.txn_id,
                                    tx_ctx.log_index,
                                )?;
                            }
                        }
                    }
                }
                ReferentialAction::SetDefault => {
                    // Set foreign key columns to DEFAULT in referencing rows
                    let ref_indexes: Vec<_> = storage
                        .get_table_indexes(ref_table_name)
                        .into_iter()
                        .cloned()
                        .collect();

                    for (ref_row_id, ref_row_values) in &referencing_rows {
                        let mut new_values = ref_row_values.clone();

                        // Set FK column to DEFAULT
                        let fk_column_idx = ref_schema
                            .columns
                            .iter()
                            .position(|c| c.name == fk.columns[0])
                            .ok_or_else(|| Error::ColumnNotFound(fk.columns[0].clone()))?;

                        let fk_column = &ref_schema.columns[fk_column_idx];
                        let default_value = if let Some(ref default_expr) = fk_column.default {
                            // Evaluate DEFAULT expression
                            let full_expr: crate::types::expression::Expression =
                                default_expr.clone().into();
                            expression::evaluate_with_storage(
                                &full_expr,
                                None,
                                tx_ctx,
                                None,
                                Some(storage),
                            )?
                        } else {
                            return Err(Error::Other(format!(
                                "Column '{}' has no DEFAULT value for ON DELETE SET DEFAULT",
                                fk_column.name
                            )));
                        };

                        new_values[fk_column_idx] = default_value;

                        // Validate the updated row
                        ref_schema.validate_row(&new_values)?;

                        // Update the row
                        storage.write_row(
                            batch,
                            ref_table_name,
                            *ref_row_id,
                            &new_values,
                            tx_ctx.txn_id,
                            tx_ctx.log_index,
                        )?;

                        // Update indexes
                        for index in &ref_indexes {
                            let old_index_values =
                                helpers::extract_index_values(ref_row_values, index, ref_schema)?;
                            let new_index_values =
                                helpers::extract_index_values(&new_values, index, ref_schema)?;

                            if old_index_values != new_index_values {
                                storage.update_index_entries(
                                    batch,
                                    &index.name,
                                    old_index_values,
                                    new_index_values,
                                    *ref_row_id,
                                    tx_ctx.txn_id,
                                    tx_ctx.log_index,
                                )?;
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(())
}
