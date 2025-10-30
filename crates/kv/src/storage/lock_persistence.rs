//! Lock persistence for crash recovery
//!
//! Similar to SQL's predicate persistence, we persist locks so they can be
//! restored after a crash and properly released on commit/abort.
//!
//! IMPORTANT: Persisted locks are ONLY read during recovery (startup scan).
//! During normal operation, all lock operations use the in-memory LockManager.

use super::lock::LockMode;
use proven_common::TransactionId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Persisted lock information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedLock {
    pub key: String,
    pub mode: LockMode,
}

/// Lock state for a transaction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionLocks {
    pub txn_id: TransactionId,
    pub locks: Vec<PersistedLock>,
}

impl TransactionLocks {
    pub fn new(txn_id: TransactionId) -> Self {
        Self {
            txn_id,
            locks: Vec::new(),
        }
    }

    pub fn add_lock(&mut self, key: String, mode: LockMode) {
        self.locks.push(PersistedLock { key, mode });
    }

    pub fn to_hashmap(&self) -> HashMap<String, LockMode> {
        self.locks
            .iter()
            .map(|lock| (lock.key.clone(), lock.mode))
            .collect()
    }
}

/// Encode transaction locks for persistence
pub fn encode_transaction_locks(locks: &TransactionLocks) -> Result<Vec<u8>, String> {
    use std::io::Write;
    let mut buf = Vec::new();

    // Encode TransactionId (16 bytes for UUIDv7)
    buf.write_all(&locks.txn_id.to_bytes())
        .map_err(|e| e.to_string())?;

    // Encode number of locks
    buf.write_all(&(locks.locks.len() as u32).to_be_bytes())
        .map_err(|e| e.to_string())?;

    // Encode each lock
    for lock in &locks.locks {
        // Encode key
        let key_bytes = lock.key.as_bytes();
        buf.write_all(&(key_bytes.len() as u32).to_be_bytes())
            .map_err(|e| e.to_string())?;
        buf.write_all(key_bytes).map_err(|e| e.to_string())?;

        // Encode mode (1 byte: 1=Shared, 2=Exclusive)
        buf.push(match lock.mode {
            LockMode::Shared => 1,
            LockMode::Exclusive => 2,
        });
    }

    Ok(buf)
}

/// Decode transaction locks from persistence
pub fn decode_transaction_locks(bytes: &[u8]) -> Result<TransactionLocks, String> {
    use std::io::{Cursor, Read};

    let mut cursor = Cursor::new(bytes);

    // Decode TransactionId (16 bytes for UUIDv7)
    let mut ts_bytes = [0u8; 16];
    cursor
        .read_exact(&mut ts_bytes)
        .map_err(|e| e.to_string())?;
    let txn_id = TransactionId::from_bytes(ts_bytes);

    // Decode number of locks
    let mut len_bytes = [0u8; 4];
    cursor
        .read_exact(&mut len_bytes)
        .map_err(|e| e.to_string())?;
    let num_locks = u32::from_be_bytes(len_bytes) as usize;

    let mut locks = Vec::with_capacity(num_locks);

    // Decode each lock
    for _ in 0..num_locks {
        // Decode key
        cursor
            .read_exact(&mut len_bytes)
            .map_err(|e| e.to_string())?;
        let key_len = u32::from_be_bytes(len_bytes) as usize;

        let mut key_bytes = vec![0u8; key_len];
        cursor
            .read_exact(&mut key_bytes)
            .map_err(|e| e.to_string())?;
        let key = String::from_utf8(key_bytes).map_err(|e| format!("Invalid UTF-8: {}", e))?;

        // Decode mode
        let mut mode_byte = [0u8; 1];
        cursor
            .read_exact(&mut mode_byte)
            .map_err(|e| e.to_string())?;
        let mode = match mode_byte[0] {
            1 => LockMode::Shared,
            2 => LockMode::Exclusive,
            _ => return Err(format!("Unknown lock mode: {}", mode_byte[0])),
        };

        locks.push(PersistedLock { key, mode });
    }

    Ok(TransactionLocks { txn_id, locks })
}
