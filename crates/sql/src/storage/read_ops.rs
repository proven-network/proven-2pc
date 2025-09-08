//! Read-only storage operations using immutable references
//!
//! This module provides read operations that only require immutable references
//! to storage, enabling true zero-copy streaming iterators.

use crate::error::{Error, Result};
use crate::storage::mvcc::{MvccRowIterator, MvccRowWithIdIterator, MvccStorage};
use crate::stream::{TransactionContext, TransactionState};
use crate::types::value::Value;
use std::sync::Arc;

/// Scan a table returning a true streaming iterator
pub fn scan_iter<'a>(
    storage: &'a MvccStorage,
    tx_ctx: &mut TransactionContext,
    table: &str,
) -> Result<MvccRowIterator<'a>> {
    // Check transaction is active
    if tx_ctx.state != TransactionState::Active {
        return Err(Error::TransactionNotActive(tx_ctx.id));
    }

    // Get the table reference
    let table_ref = storage
        .tables
        .get(table)
        .ok_or_else(|| Error::TableNotFound(table.to_string()))?;

    // Return the MVCC iterator directly - true zero-copy streaming!
    Ok(table_ref.iter(tx_ctx.id, tx_ctx.timestamp))
}

/// Scan with row IDs for UPDATE/DELETE operations
pub fn scan_iter_with_ids<'a>(
    storage: &'a MvccStorage,
    tx_ctx: &mut TransactionContext,
    table: &str,
) -> Result<MvccRowWithIdIterator<'a>> {
    // Check transaction is active
    if tx_ctx.state != TransactionState::Active {
        return Err(Error::TransactionNotActive(tx_ctx.id));
    }

    // Get the table reference
    let table_ref = storage
        .tables
        .get(table)
        .ok_or_else(|| Error::TableNotFound(table.to_string()))?;

    // Return the MVCC iterator with IDs
    Ok(table_ref.iter_with_ids(tx_ctx.id, tx_ctx.timestamp))
}

/// Read a specific row by ID
pub fn read_row(
    storage: &MvccStorage,
    tx_ctx: &mut TransactionContext,
    table: &str,
    row_id: u64,
) -> Result<Option<Arc<Vec<Value>>>> {
    // Check transaction is active
    if tx_ctx.state != TransactionState::Active {
        return Err(Error::TransactionNotActive(tx_ctx.id));
    }

    // Get the table reference
    let table_ref = storage
        .tables
        .get(table)
        .ok_or_else(|| Error::TableNotFound(table.to_string()))?;

    // Read the row using MVCC visibility
    Ok(table_ref
        .read(tx_ctx.id, tx_ctx.timestamp, row_id)
        .map(|row| Arc::new(row.values.clone())))
}

/// Check if a table exists (no locking needed for metadata)
pub fn table_exists(storage: &MvccStorage, table: &str) -> bool {
    storage.tables.contains_key(table)
}

/// Get table schema (no locking needed for metadata)
pub fn get_table_schema<'a>(
    storage: &'a MvccStorage,
    table: &str,
) -> Result<&'a crate::types::schema::Table> {
    storage
        .tables
        .get(table)
        .map(|t| &t.schema)
        .ok_or_else(|| Error::TableNotFound(table.to_string()))
}
