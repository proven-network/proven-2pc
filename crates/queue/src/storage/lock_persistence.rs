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

/// Encode transaction lock for persistence (using bincode for efficiency)
pub fn encode_transaction_lock(lock: &QueueTransactionLock) -> Result<Vec<u8>, String> {
    bincode::serialize(lock).map_err(|e| e.to_string())
}

/// Decode transaction lock from persistence
pub fn decode_transaction_lock(bytes: &[u8]) -> Result<QueueTransactionLock, String> {
    bincode::deserialize(bytes).map_err(|e| e.to_string())
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
