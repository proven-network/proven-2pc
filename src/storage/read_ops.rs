//! Read-only storage operations using immutable references
//!
//! This module provides read operations that only require immutable references
//! to storage, enabling true zero-copy streaming iterators.

use crate::error::{Error, Result};
use crate::sql::stream_processor::{AccessLogEntry, TransactionContext, TransactionState};
use crate::sql::types::value::Value;
use crate::storage::lock::{LockKey, LockManager, LockMode, LockResult};
use crate::storage::mvcc::{MvccRowIterator, MvccRowWithIdIterator, MvccStorage};
use std::sync::Arc;

/// Scan a table returning a true streaming iterator
pub fn scan_iter<'a>(
    storage: &'a MvccStorage,
    lock_manager: &mut LockManager,
    tx_ctx: &mut TransactionContext,
    table: &str,
) -> Result<MvccRowIterator<'a>> {
    // Check transaction is active
    if tx_ctx.state != TransactionState::Active {
        return Err(Error::TransactionNotActive(tx_ctx.id));
    }

    // Acquire intent-shared lock on table
    let lock_key = LockKey::Table {
        table: table.to_string(),
    };

    match lock_manager.try_acquire(tx_ctx.id, lock_key.clone(), LockMode::IntentShared)? {
        LockResult::Granted => {
            tx_ctx.locks_held.push(lock_key);
        }
        LockResult::Conflict { holder, mode } => {
            return Err(Error::LockConflict { holder, mode });
        }
    }

    // Get the table reference
    let table_ref = storage
        .tables
        .get(table)
        .ok_or_else(|| Error::TableNotFound(table.to_string()))?;

    // Log access
    tx_ctx.access_log.push(AccessLogEntry {
        operation: "SCAN".to_string(),
        table: table.to_string(),
        keys: vec![],
        lock_mode: LockMode::IntentShared,
    });

    // Return the MVCC iterator directly - true zero-copy streaming!
    Ok(table_ref.iter(tx_ctx.id, tx_ctx.timestamp))
}

/// Scan with row IDs for UPDATE/DELETE operations
pub fn scan_iter_with_ids<'a>(
    storage: &'a MvccStorage,
    lock_manager: &mut LockManager,
    tx_ctx: &mut TransactionContext,
    table: &str,
    for_update: bool,
) -> Result<MvccRowWithIdIterator<'a>> {
    // Check transaction is active
    if tx_ctx.state != TransactionState::Active {
        return Err(Error::TransactionNotActive(tx_ctx.id));
    }

    // Use intent-exclusive for UPDATE/DELETE to signal write intent
    let lock_mode = if for_update {
        LockMode::IntentExclusive
    } else {
        LockMode::IntentShared
    };

    let lock_key = LockKey::Table {
        table: table.to_string(),
    };

    match lock_manager.try_acquire(tx_ctx.id, lock_key.clone(), lock_mode)? {
        LockResult::Granted => {
            tx_ctx.locks_held.push(lock_key);
        }
        LockResult::Conflict { holder, mode } => {
            return Err(Error::LockConflict { holder, mode });
        }
    }

    // Get the table reference
    let table_ref = storage
        .tables
        .get(table)
        .ok_or_else(|| Error::TableNotFound(table.to_string()))?;

    // Log access
    tx_ctx.access_log.push(AccessLogEntry {
        operation: if for_update {
            "SCAN_FOR_UPDATE"
        } else {
            "SCAN_WITH_IDS"
        }
        .to_string(),
        table: table.to_string(),
        keys: vec![],
        lock_mode,
    });

    // Return the MVCC iterator with IDs
    Ok(table_ref.iter_with_ids(tx_ctx.id, tx_ctx.timestamp))
}

/// Read a specific row by ID
pub fn read_row(
    storage: &MvccStorage,
    lock_manager: &mut LockManager,
    tx_ctx: &mut TransactionContext,
    table: &str,
    row_id: u64,
) -> Result<Option<Arc<Vec<Value>>>> {
    // Check transaction is active
    if tx_ctx.state != TransactionState::Active {
        return Err(Error::TransactionNotActive(tx_ctx.id));
    }

    // Acquire shared lock on the specific row
    let lock_key = LockKey::Row {
        table: table.to_string(),
        row_id,
    };

    match lock_manager.try_acquire(tx_ctx.id, lock_key.clone(), LockMode::Shared)? {
        LockResult::Granted => {
            tx_ctx.locks_held.push(lock_key);
        }
        LockResult::Conflict { holder, mode } => {
            return Err(Error::LockConflict { holder, mode });
        }
    }

    // Get the table reference
    let table_ref = storage
        .tables
        .get(table)
        .ok_or_else(|| Error::TableNotFound(table.to_string()))?;

    // Log access
    tx_ctx.access_log.push(AccessLogEntry {
        operation: "READ".to_string(),
        table: table.to_string(),
        keys: vec![row_id],
        lock_mode: LockMode::Shared,
    });

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
) -> Result<&'a crate::sql::types::schema::Table> {
    storage
        .tables
        .get(table)
        .map(|t| &t.schema)
        .ok_or_else(|| Error::TableNotFound(table.to_string()))
}
