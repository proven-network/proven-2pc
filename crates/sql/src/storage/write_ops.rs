//! Write operations requiring mutable storage references
//!
//! This module provides write operations that require mutable references
//! to storage and lock manager. These operations modify data.

use crate::error::{Error, Result};
use crate::storage::lock::{LockAttemptResult, LockKey, LockManager, LockMode};
use crate::storage::mvcc::MvccStorage;
use crate::stream::{AccessLogEntry, TransactionContext, TransactionState};
use crate::types::value::Value;

/// Insert a row into storage with proper locking
pub fn insert(
    storage: &mut MvccStorage,
    lock_manager: &mut LockManager,
    tx_ctx: &mut TransactionContext,
    table: &str,
    values: Vec<Value>,
) -> Result<u64> {
    // Check transaction is active
    if tx_ctx.state != TransactionState::Active {
        return Err(Error::TransactionNotActive(tx_ctx.id));
    }

    // First, prepare the lock key (we need to insert first to get row_id)
    // Insert into storage optimistically
    let table_mut = storage
        .tables
        .get_mut(table)
        .ok_or_else(|| Error::TableNotFound(table.to_string()))?;
    let row_id = table_mut.insert(tx_ctx.id, tx_ctx.timestamp, values)?;

    // Now check if we can acquire the lock
    let lock_key = LockKey::Row {
        table: table.to_string(),
        row_id,
    };

    // Use the new pure API: check first, then grant
    match lock_manager.check(tx_ctx.id, &lock_key, LockMode::Exclusive) {
        LockAttemptResult::WouldGrant => {
            // Grant the lock
            lock_manager.grant(tx_ctx.id, lock_key.clone(), LockMode::Exclusive)?;
            tx_ctx.locks_held.push(lock_key);
        }
        LockAttemptResult::Conflict { holder, mode } => {
            // Rollback the insert since we couldn't get the lock
            table_mut.remove_transaction_versions(tx_ctx.id);
            return Err(Error::LockConflict { holder, mode });
        }
    }

    // Log access
    tx_ctx.access_log.push(AccessLogEntry {
        operation: "INSERT".to_string(),
        table: table.to_string(),
        keys: vec![row_id],
        lock_mode: LockMode::Exclusive,
    });

    Ok(row_id)
}

/// Update a row with proper locking
pub fn update(
    storage: &mut MvccStorage,
    lock_manager: &mut LockManager,
    tx_ctx: &mut TransactionContext,
    table: &str,
    row_id: u64,
    values: Vec<Value>,
) -> Result<()> {
    // Check transaction is active
    if tx_ctx.state != TransactionState::Active {
        return Err(Error::TransactionNotActive(tx_ctx.id));
    }

    // Check if we can acquire exclusive lock
    let lock_key = LockKey::Row {
        table: table.to_string(),
        row_id,
    };

    // Use the new pure API: check first, then grant
    match lock_manager.check(tx_ctx.id, &lock_key, LockMode::Exclusive) {
        LockAttemptResult::WouldGrant => {
            // Grant the lock
            lock_manager.grant(tx_ctx.id, lock_key.clone(), LockMode::Exclusive)?;
            tx_ctx.locks_held.push(lock_key);
        }
        LockAttemptResult::Conflict { holder, mode } => {
            return Err(Error::LockConflict { holder, mode });
        }
    }

    // Update in storage
    let table_mut = storage
        .tables
        .get_mut(table)
        .ok_or_else(|| Error::TableNotFound(table.to_string()))?;
    table_mut.update(tx_ctx.id, tx_ctx.timestamp, row_id, values)?;

    // Log access
    tx_ctx.access_log.push(AccessLogEntry {
        operation: "UPDATE".to_string(),
        table: table.to_string(),
        keys: vec![row_id],
        lock_mode: LockMode::Exclusive,
    });

    Ok(())
}

/// Delete a row with proper locking
pub fn delete(
    storage: &mut MvccStorage,
    lock_manager: &mut LockManager,
    tx_ctx: &mut TransactionContext,
    table: &str,
    row_id: u64,
) -> Result<()> {
    // Check transaction is active
    if tx_ctx.state != TransactionState::Active {
        return Err(Error::TransactionNotActive(tx_ctx.id));
    }

    // Check if we can acquire exclusive lock
    let lock_key = LockKey::Row {
        table: table.to_string(),
        row_id,
    };

    // Use the new pure API: check first, then grant
    match lock_manager.check(tx_ctx.id, &lock_key, LockMode::Exclusive) {
        LockAttemptResult::WouldGrant => {
            // Grant the lock
            lock_manager.grant(tx_ctx.id, lock_key.clone(), LockMode::Exclusive)?;
            tx_ctx.locks_held.push(lock_key);
        }
        LockAttemptResult::Conflict { holder, mode } => {
            return Err(Error::LockConflict { holder, mode });
        }
    }

    // Delete in storage
    let table_mut = storage
        .tables
        .get_mut(table)
        .ok_or_else(|| Error::TableNotFound(table.to_string()))?;
    table_mut.delete(tx_ctx.id, tx_ctx.timestamp, row_id)?;

    // Log access
    tx_ctx.access_log.push(AccessLogEntry {
        operation: "DELETE".to_string(),
        table: table.to_string(),
        keys: vec![row_id],
        lock_mode: LockMode::Exclusive,
    });

    Ok(())
}

/// Create a new table
pub fn create_table(
    storage: &mut MvccStorage,
    lock_manager: &mut LockManager,
    tx_ctx: &mut TransactionContext,
    name: String,
    schema: crate::types::schema::Table,
) -> Result<()> {
    // Check transaction is active
    if tx_ctx.state != TransactionState::Active {
        return Err(Error::TransactionNotActive(tx_ctx.id));
    }

    // Acquire schema lock for DDL operations
    let lock_key = LockKey::Schema;

    // Use the new pure API: check first, then grant
    match lock_manager.check(tx_ctx.id, &lock_key, LockMode::Exclusive) {
        LockAttemptResult::WouldGrant => {
            // Grant the lock
            lock_manager.grant(tx_ctx.id, lock_key.clone(), LockMode::Exclusive)?;
            tx_ctx.locks_held.push(lock_key);
        }
        LockAttemptResult::Conflict { holder, mode } => {
            return Err(Error::LockConflict { holder, mode });
        }
    }

    // Create the table
    storage.create_table(name.clone(), schema)?;

    // Log access
    tx_ctx.access_log.push(AccessLogEntry {
        operation: "CREATE_TABLE".to_string(),
        table: name,
        keys: vec![],
        lock_mode: LockMode::Exclusive,
    });

    Ok(())
}

/// Drop a table
pub fn drop_table(
    storage: &mut MvccStorage,
    lock_manager: &mut LockManager,
    tx_ctx: &mut TransactionContext,
    name: &str,
) -> Result<()> {
    // Check transaction is active
    if tx_ctx.state != TransactionState::Active {
        return Err(Error::TransactionNotActive(tx_ctx.id));
    }

    // Acquire schema lock for DDL operations
    let lock_key = LockKey::Schema;

    // Use the new pure API: check first, then grant
    match lock_manager.check(tx_ctx.id, &lock_key, LockMode::Exclusive) {
        LockAttemptResult::WouldGrant => {
            // Grant the lock
            lock_manager.grant(tx_ctx.id, lock_key.clone(), LockMode::Exclusive)?;
            tx_ctx.locks_held.push(lock_key);
        }
        LockAttemptResult::Conflict { holder, mode } => {
            return Err(Error::LockConflict { holder, mode });
        }
    }

    // Drop the table
    storage.drop_table(name)?;

    // Log access
    tx_ctx.access_log.push(AccessLogEntry {
        operation: "DROP_TABLE".to_string(),
        table: name.to_string(),
        keys: vec![],
        lock_mode: LockMode::Exclusive,
    });

    Ok(())
}
