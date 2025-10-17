use crate::error::{Error, Result};
use crate::types::context::PendingDdl;
use fjall;
use proven_hlc::HlcTimestamp;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// Stores pending DDL operations in a dedicated partition for crash recovery.
///
/// Similar to PredicateStore, this allows us to recover in-flight DDL operations
/// after a crash and properly roll them back if the transaction aborts.
///
/// Key format: {txn_id}{seq} (20 + 8 bytes = 28 bytes)
pub struct DdlStore {
    /// Dedicated partition for DDL tracking
    ddl_partition: fjall::PartitionHandle,
    /// Sequence for unique keys within a transaction
    next_seq: AtomicU64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredDdl {
    ddl: PendingDdl,
}

impl DdlStore {
    /// Create a new DdlStore with dedicated partition
    pub fn new(keyspace: &fjall::Keyspace) -> Result<Self> {
        let ddl_partition = keyspace
            .open_partition("pending_ddls", fjall::PartitionCreateOptions::default())
            .map_err(|e| Error::Other(format!("Failed to open pending_ddls partition: {}", e)))?;

        Ok(Self {
            ddl_partition,
            next_seq: AtomicU64::new(0),
        })
    }

    /// Encode key: {txn_id}{seq}
    fn encode_key(txn_id: HlcTimestamp, seq: u64) -> Vec<u8> {
        let mut key = Vec::new();
        key.extend_from_slice(&txn_id.to_lexicographic_bytes());
        key.extend_from_slice(&seq.to_be_bytes());
        key
    }

    /// Add pending DDL to batch (atomic with data operations)
    pub fn add_to_batch(
        &self,
        batch: &mut fjall::Batch,
        txn_id: HlcTimestamp,
        ddl: &PendingDdl,
    ) -> Result<()> {
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
        let key = Self::encode_key(txn_id, seq);

        let stored = StoredDdl { ddl: ddl.clone() };
        let mut value = Vec::new();
        ciborium::into_writer(&stored, &mut value)
            .map_err(|e| Error::Serialization(e.to_string()))?;

        batch.insert(&self.ddl_partition, key, value);
        Ok(())
    }

    /// Remove all pending DDLs for a transaction (on commit/abort)
    pub fn remove_all(&self, batch: &mut fjall::Batch, txn_id: HlcTimestamp) -> Result<()> {
        let prefix = txn_id.to_lexicographic_bytes().to_vec();
        for result in self.ddl_partition.prefix(prefix) {
            let (key, _) = result
                .map_err(|e| Error::Other(format!("Failed to iterate pending DDLs: {}", e)))?;
            batch.remove(self.ddl_partition.clone(), key);
        }
        Ok(())
    }

    /// Get all active transactions with pending DDLs (for recovery)
    pub fn get_all_active_ddls(&self) -> Result<HashMap<HlcTimestamp, Vec<PendingDdl>>> {
        let mut transactions = HashMap::new();

        // Scan all keys in DDL partition
        for result in self.ddl_partition.iter() {
            let (key, value) = result
                .map_err(|e| Error::Other(format!("Failed to iterate pending DDLs: {}", e)))?;
            if let Some((txn_id, ddl)) = self.parse_key(&key, &value)? {
                transactions
                    .entry(txn_id)
                    .or_insert_with(Vec::new)
                    .push(ddl);
            }
        }

        Ok(transactions)
    }

    /// Parse a key-value pair into transaction ID and DDL
    fn parse_key(&self, key: &[u8], value: &[u8]) -> Result<Option<(HlcTimestamp, PendingDdl)>> {
        // Key format: {txn_id:20}{seq:8}
        if key.len() < 20 {
            return Ok(None);
        }

        let txn_id_bytes: [u8; 20] = key[0..20]
            .try_into()
            .map_err(|_| Error::Other("Invalid txn_id in DDL key".to_string()))?;
        let txn_id = HlcTimestamp::from_lexicographic_bytes(&txn_id_bytes)
            .map_err(|e| Error::Other(format!("Failed to parse txn_id: {}", e)))?;

        let stored: StoredDdl = ciborium::from_reader(value)
            .map_err(|e| Error::Serialization(format!("Failed to deserialize DDL: {}", e)))?;

        Ok(Some((txn_id, stored.ddl)))
    }
}
