//! Lock persistence for crash recovery
//!
//! Similar to KV's lock persistence, we persist locks so they can be
//! restored after a crash and properly released on commit/abort.
//!
//! IMPORTANT: Persisted locks are ONLY read during recovery (startup scan).
//! During normal operation, all lock operations use the in-memory LockManager.

use super::lock::LockMode;
use proven_hlc::HlcTimestamp;
use serde::{Deserialize, Serialize};

/// Lock state for a queue transaction
///
/// Unlike KV which has multiple key-level locks, queue has a single queue-level lock
/// per transaction, so this is simpler.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueTransactionLock {
    pub txn_id: HlcTimestamp,
    pub mode: LockMode,
}

impl QueueTransactionLock {
    pub fn new(txn_id: HlcTimestamp, mode: LockMode) -> Self {
        Self { txn_id, mode }
    }
}

/// Encode transaction lock for persistence
pub fn encode_transaction_lock(lock: &QueueTransactionLock) -> Result<Vec<u8>, String> {
    let mut buf = Vec::new();

    // Encode HlcTimestamp (20 bytes)
    buf.extend_from_slice(&lock.txn_id.to_lexicographic_bytes());

    // Encode mode (1 byte: 1=Shared, 2=Append, 3=Exclusive)
    buf.push(match lock.mode {
        LockMode::Shared => 1,
        LockMode::Append => 2,
        LockMode::Exclusive => 3,
    });

    Ok(buf)
}

/// Decode transaction lock from persistence
pub fn decode_transaction_lock(bytes: &[u8]) -> Result<QueueTransactionLock, String> {
    if bytes.len() != 21 {
        return Err(format!(
            "Invalid lock bytes length: {} (expected 21)",
            bytes.len()
        ));
    }

    // Decode HlcTimestamp (20 bytes)
    let mut ts_bytes = [0u8; 20];
    ts_bytes.copy_from_slice(&bytes[0..20]);
    let txn_id = HlcTimestamp::from_lexicographic_bytes(&ts_bytes).map_err(|e| e.to_string())?;

    // Decode mode
    let mode = match bytes[20] {
        1 => LockMode::Shared,
        2 => LockMode::Append,
        3 => LockMode::Exclusive,
        _ => return Err(format!("Unknown lock mode: {}", bytes[20])),
    };

    Ok(QueueTransactionLock { txn_id, mode })
}

#[cfg(test)]
mod tests {
    use super::*;
    use proven_hlc::NodeId;

    fn create_timestamp(seconds: u64) -> HlcTimestamp {
        HlcTimestamp::new(seconds, 0, NodeId::new(1))
    }

    #[test]
    fn test_encode_decode_lock() {
        let lock = QueueTransactionLock::new(create_timestamp(100), LockMode::Exclusive);

        let encoded = encode_transaction_lock(&lock).unwrap();
        let decoded = decode_transaction_lock(&encoded).unwrap();

        assert_eq!(lock.txn_id, decoded.txn_id);
        assert_eq!(lock.mode, decoded.mode);
    }

    #[test]
    fn test_encode_decode_all_modes() {
        let modes = vec![LockMode::Shared, LockMode::Append, LockMode::Exclusive];

        for mode in modes {
            let lock = QueueTransactionLock::new(create_timestamp(100), mode);
            let encoded = encode_transaction_lock(&lock).unwrap();
            let decoded = decode_transaction_lock(&encoded).unwrap();

            assert_eq!(lock.mode, decoded.mode);
        }
    }
}
