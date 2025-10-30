use crate::error::{Error, Result};
use crate::semantic::predicate::Predicate;
use fjall;
use proven_common::TransactionId;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// Stores predicates in a dedicated partition for crash recovery.
///
/// Uses dedicated partition (not metadata partition) to avoid prefix overhead.
/// Key format: {txn_id}{r|w}{seq} (20 + 1 + 8 bytes = 29 bytes)
pub struct PredicateStore {
    /// Dedicated partition for predicates (no prefix needed!)
    predicate_partition: fjall::PartitionHandle,
    /// Sequence for unique keys within a transaction
    next_seq: AtomicU64,
}

impl PredicateStore {
    /// Create a new PredicateStore with dedicated partition
    pub fn new(keyspace: &fjall::Keyspace) -> Result<Self> {
        let predicate_partition = keyspace
            .open_partition("predicates", fjall::PartitionCreateOptions::default())
            .map_err(|e| Error::Other(format!("Failed to open predicates partition: {}", e)))?;

        Ok(Self {
            predicate_partition,
            next_seq: AtomicU64::new(0),
        })
    }

    /// Encode key: {txn_id}{r|w}{seq}
    /// No prefix needed - dedicated partition provides isolation
    fn encode_key(txn_id: TransactionId, is_write: bool, seq: u64) -> Vec<u8> {
        let mut key = Vec::new();
        key.extend_from_slice(&txn_id.to_bytes());
        key.push(if is_write { b'w' } else { b'r' });
        key.extend_from_slice(&seq.to_be_bytes());
        key
    }

    /// Add predicate to batch (atomic with data operations)
    pub fn add_to_batch(
        &self,
        batch: &mut fjall::Batch,
        txn_id: TransactionId,
        predicate: &Predicate,
        is_write: bool,
    ) -> Result<()> {
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
        let key = Self::encode_key(txn_id, is_write, seq);

        let stored = StoredPredicate {
            predicate: predicate.clone(),
            is_write,
        };
        let mut value = Vec::new();
        ciborium::into_writer(&stored, &mut value)
            .map_err(|e| Error::Serialization(e.to_string()))?;

        batch.insert(&self.predicate_partition, key, value);
        Ok(())
    }

    /// Remove all predicates for a transaction (on commit/abort)
    pub fn remove_all(&self, batch: &mut fjall::Batch, txn_id: TransactionId) -> Result<()> {
        let prefix = txn_id.to_bytes().to_vec();
        for result in self.predicate_partition.prefix(prefix) {
            let (key, _) =
                result.map_err(|e| Error::Other(format!("Failed to iterate predicates: {}", e)))?;
            batch.remove(self.predicate_partition.clone(), key);
        }
        Ok(())
    }

    /// Remove read predicates only (on prepare)
    pub fn remove_reads(&self, batch: &mut fjall::Batch, txn_id: TransactionId) -> Result<()> {
        let mut prefix = txn_id.to_bytes().to_vec();
        prefix.push(b'r');
        for result in self.predicate_partition.prefix(prefix) {
            let (key, _) =
                result.map_err(|e| Error::Other(format!("Failed to iterate predicates: {}", e)))?;
            batch.remove(self.predicate_partition.clone(), key);
        }
        Ok(())
    }

    /// Get all active transactions (for recovery)
    pub fn get_all_active_transactions(&self) -> Result<HashMap<TransactionId, Vec<Predicate>>> {
        let mut transactions = HashMap::new();

        // Scan all keys in predicate partition
        for result in self.predicate_partition.iter() {
            let (key, value) =
                result.map_err(|e| Error::Other(format!("Failed to iterate predicates: {}", e)))?;
            if let Some((txn_id, predicate)) = self.parse_key(&key, &value)? {
                transactions
                    .entry(txn_id)
                    .or_insert_with(Vec::new)
                    .push(predicate);
            }
        }

        Ok(transactions)
    }

    /// Parse key and value to extract transaction ID and predicate
    fn parse_key(&self, key: &[u8], value: &[u8]) -> Result<Option<(TransactionId, Predicate)>> {
        const TXN_ID_SIZE: usize = 16;

        if key.len() < TXN_ID_SIZE + 1 {
            return Ok(None);
        }

        let txn_id_bytes: [u8; 16] = key[..TXN_ID_SIZE]
            .try_into()
            .map_err(|_| Error::Other("Invalid txn_id size".to_string()))?;
        let txn_id = TransactionId::from_bytes(txn_id_bytes);

        let stored: StoredPredicate =
            ciborium::from_reader(value).map_err(|e| Error::Serialization(e.to_string()))?;

        Ok(Some((txn_id, stored.predicate)))
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct StoredPredicate {
    predicate: Predicate,
    is_write: bool,
}
