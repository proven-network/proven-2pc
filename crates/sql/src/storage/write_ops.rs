//! Write operations requiring mutable storage references
//!
//! This module provides write operations that require mutable references
//! to storage and lock manager. These operations modify data.

use crate::error::{Error, Result};
use crate::storage::mvcc::MvccStorage;
use crate::stream::{TransactionContext, TransactionState};
use crate::types::value::Value;

/// Insert a row into storage
pub fn insert(
    storage: &mut MvccStorage,
    tx_ctx: &mut TransactionContext,
    table: &str,
    values: Vec<Value>,
) -> Result<u64> {
    // Check transaction is active
    if tx_ctx.state != TransactionState::Active {
        return Err(Error::TransactionNotActive(tx_ctx.id));
    }

    // Insert into storage
    let table_mut = storage
        .tables
        .get_mut(table)
        .ok_or_else(|| Error::TableNotFound(table.to_string()))?;
    let row_id = table_mut.insert(tx_ctx.id, tx_ctx.timestamp, values)?;

    Ok(row_id)
}

/// Update a row
pub fn update(
    storage: &mut MvccStorage,
    tx_ctx: &mut TransactionContext,
    table: &str,
    row_id: u64,
    values: Vec<Value>,
) -> Result<()> {
    // Check transaction is active
    if tx_ctx.state != TransactionState::Active {
        return Err(Error::TransactionNotActive(tx_ctx.id));
    }

    // Update in storage
    let table_mut = storage
        .tables
        .get_mut(table)
        .ok_or_else(|| Error::TableNotFound(table.to_string()))?;
    table_mut.update(tx_ctx.id, tx_ctx.timestamp, row_id, values)?;

    Ok(())
}

/// Delete a row
pub fn delete(
    storage: &mut MvccStorage,
    tx_ctx: &mut TransactionContext,
    table: &str,
    row_id: u64,
) -> Result<()> {
    // Check transaction is active
    if tx_ctx.state != TransactionState::Active {
        return Err(Error::TransactionNotActive(tx_ctx.id));
    }

    // Delete in storage
    let table_mut = storage
        .tables
        .get_mut(table)
        .ok_or_else(|| Error::TableNotFound(table.to_string()))?;
    table_mut.delete(tx_ctx.id, tx_ctx.timestamp, row_id)?;

    Ok(())
}

/// Create a new table
pub fn create_table(
    storage: &mut MvccStorage,
    tx_ctx: &mut TransactionContext,
    name: String,
    schema: crate::types::schema::Table,
) -> Result<()> {
    // Check transaction is active
    if tx_ctx.state != TransactionState::Active {
        return Err(Error::TransactionNotActive(tx_ctx.id));
    }

    // Create the table
    storage.create_table(name.clone(), schema)?;

    Ok(())
}

/// Drop a table
pub fn drop_table(
    storage: &mut MvccStorage,
    tx_ctx: &mut TransactionContext,
    name: &str,
) -> Result<()> {
    // Check transaction is active
    if tx_ctx.state != TransactionState::Active {
        return Err(Error::TransactionNotActive(tx_ctx.id));
    }

    // Drop the table
    storage.drop_table(name)?;

    Ok(())
}
