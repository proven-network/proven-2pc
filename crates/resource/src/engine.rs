//! Resource engine implementation with MVCC storage and persistent reservations

use crate::storage::entity::{ResourceDelta, ResourceEntity, ResourceKey, ResourceValue};
use crate::storage::lock_persistence::{
    TransactionReservations, decode_transaction_reservations, encode_transaction_reservations,
};
use crate::storage::{ReservationManager, ReservationType, ResourceMetadata};
use crate::types::{Amount, ResourceOperation, ResourceResponse};
use proven_hlc::HlcTimestamp;
use proven_mvcc::{MvccStorage, StorageConfig};
use proven_stream::engine::BlockingInfo;
use proven_stream::{OperationResult, RetryOn, TransactionEngine};

/// Resource engine for processing resource operations with persistent storage
pub struct ResourceTransactionEngine {
    /// MVCC storage for versioned data (persistent via proven-mvcc)
    storage: MvccStorage<ResourceEntity>,

    /// Reservation manager for pessimistic concurrency control
    reservations: ReservationManager,
}

impl ResourceTransactionEngine {
    /// Create a new resource engine with default storage
    pub fn new() -> Self {
        let config = StorageConfig::default();
        Self::with_config(config)
    }

    /// Create a new resource engine with custom config
    pub fn with_config(config: StorageConfig) -> Self {
        let storage =
            MvccStorage::<ResourceEntity>::new(config).expect("Failed to create MVCC storage");

        let mut engine = Self {
            storage,
            reservations: ReservationManager::new(),
        };

        // Recover reservations from persisted state (crash recovery)
        engine.recover_reservations_from_storage();

        engine
    }

    /// Recover reservations from persisted state (crash recovery)
    fn recover_reservations_from_storage(&mut self) {
        let metadata = self.storage.metadata_partition();

        // Scan for all persisted reservations
        for (_key_bytes, value_bytes) in metadata.prefix("_locks_").flatten() {
            // Decode the transaction reservations
            if let Ok(tx_res) = decode_transaction_reservations(&value_bytes) {
                // Restore each reservation to the in-memory reservation manager
                for res in tx_res.reservations {
                    match res.reservation_type {
                        ReservationType::Debit(amt) => {
                            // Need to restore balance context - read from storage
                            let key = ResourceKey::Account(res.account.clone());
                            let balance = self
                                .storage
                                .read(&key, tx_res.txn_id)
                                .ok()
                                .flatten()
                                .and_then(|v| match v {
                                    ResourceValue::Balance(amt) => Some(amt),
                                    _ => None,
                                })
                                .unwrap_or_else(Amount::zero);

                            self.reservations
                                .reserve_debit(tx_res.txn_id, &res.account, amt, balance)
                                .ok();
                        }
                        ReservationType::Credit(amt) => {
                            self.reservations
                                .reserve_credit(tx_res.txn_id, &res.account, amt)
                                .ok();
                        }
                        ReservationType::MetadataUpdate => {
                            self.reservations.reserve_metadata(tx_res.txn_id).ok();
                        }
                    }
                }
            }
        }
    }

    /// Add reservations to batch for atomic persistence
    fn add_reservations_to_batch(
        &mut self,
        batch: &mut proven_mvcc::Batch,
        txn_id: HlcTimestamp,
    ) -> Result<(), String> {
        let reservations_held = self.reservations.reservations_held_by(txn_id);

        if !reservations_held.is_empty() {
            let mut tx_reservations = TransactionReservations::new(txn_id);
            for (account, res_type) in reservations_held {
                tx_reservations.add_reservation(account, res_type);
            }

            // Lock key: prefix + lexicographic timestamp bytes
            let mut lock_key = b"_locks_".to_vec();
            lock_key.extend_from_slice(&txn_id.to_lexicographic_bytes());
            let lock_bytes = encode_transaction_reservations(&tx_reservations)?;

            // Add to batch (will be committed atomically with data)
            let metadata = self.storage.metadata_partition();
            batch.insert(metadata, lock_key, lock_bytes);
        }

        Ok(())
    }

    /// Get metadata for a transaction (reads from storage)
    fn get_metadata(&self, txn_id: HlcTimestamp) -> ResourceMetadata {
        self.storage
            .read(&ResourceKey::Metadata, txn_id)
            .ok()
            .flatten()
            .and_then(|v| match v {
                ResourceValue::Metadata(m) => Some(m),
                _ => None,
            })
            .unwrap_or_default()
    }

    /// Get supply for a transaction (reads from storage)
    fn get_supply(&self, txn_id: HlcTimestamp) -> Amount {
        self.storage
            .read(&ResourceKey::Supply, txn_id)
            .ok()
            .flatten()
            .and_then(|v| match v {
                ResourceValue::Supply(amt) => Some(amt),
                _ => None,
            })
            .unwrap_or_else(Amount::zero)
    }

    /// Get balance for an account at a transaction (reads from storage)
    fn get_balance(&self, account: &str, txn_id: HlcTimestamp) -> Amount {
        let key = ResourceKey::Account(account.to_string());
        self.storage
            .read(&key, txn_id)
            .ok()
            .flatten()
            .and_then(|v| match v {
                ResourceValue::Balance(amt) => Some(amt),
                _ => None,
            })
            .unwrap_or_else(Amount::zero)
    }

    /// Execute a read-only operation at a specific timestamp
    fn execute_read_at_timestamp(
        &self,
        operation: &ResourceOperation,
        read_timestamp: HlcTimestamp,
    ) -> OperationResult<ResourceResponse> {
        match operation {
            ResourceOperation::GetBalance { account } => {
                // Check for conflicting reservations from earlier transactions
                let pending = self.reservations.get_blocking_transactions(account);
                let blockers: Vec<_> = pending
                    .iter()
                    .filter(|&&tx_id| tx_id < read_timestamp)
                    .map(|&txn| BlockingInfo {
                        txn,
                        retry_on: RetryOn::CommitOrAbort,
                    })
                    .collect();

                if !blockers.is_empty() {
                    return OperationResult::WouldBlock { blockers };
                }

                // Safe to read
                let balance = self.get_balance(account, read_timestamp);
                OperationResult::Complete(ResourceResponse::Balance {
                    account: account.clone(),
                    amount: balance,
                })
            }

            ResourceOperation::GetMetadata => {
                let metadata = self.get_metadata(read_timestamp);
                let supply = self.get_supply(read_timestamp);

                OperationResult::Complete(ResourceResponse::Metadata {
                    name: metadata.name,
                    symbol: metadata.symbol,
                    decimals: metadata.decimals,
                    total_supply: supply,
                })
            }

            ResourceOperation::GetTotalSupply => {
                let supply = self.get_supply(read_timestamp);
                OperationResult::Complete(ResourceResponse::TotalSupply { amount: supply })
            }

            _ => panic!("Must be read-only operation for snapshot reads"),
        }
    }

    /// Process a resource operation
    fn process_operation(
        &mut self,
        operation: &ResourceOperation,
        transaction_id: HlcTimestamp,
        log_index: u64,
    ) -> Result<ResourceResponse, String> {
        match operation {
            ResourceOperation::Initialize {
                name,
                symbol,
                decimals,
            } => {
                // Check for metadata reservation conflict
                if let Some(blocking_txs) = self.reservations.would_conflict(
                    "",
                    &ReservationType::MetadataUpdate,
                    Amount::zero(),
                ) {
                    return Err(format!(
                        "Metadata update blocked by transactions: {:?}",
                        blocking_txs
                    ));
                }

                // Reserve metadata
                self.reservations.reserve_metadata(transaction_id)?;

                // Get current metadata
                let old_metadata = self.get_metadata(transaction_id);

                // Check if already initialized
                if old_metadata.initialized {
                    return Err("Resource already initialized".to_string());
                }

                // Create new metadata
                let new_metadata = ResourceMetadata {
                    name: name.clone(),
                    symbol: symbol.clone(),
                    decimals: *decimals,
                    initialized: true,
                };

                // Create delta for metadata
                let delta = ResourceDelta::SetMetadata {
                    old: if old_metadata.initialized {
                        Some(old_metadata)
                    } else {
                        None
                    },
                    new: new_metadata.clone(),
                };

                // Write to storage
                let mut batch = self.storage.batch();
                self.storage
                    .write_to_batch(&mut batch, delta, transaction_id, log_index)
                    .expect("Write failed");

                // Add reservations to the batch
                if let Err(e) = self.add_reservations_to_batch(&mut batch, transaction_id) {
                    eprintln!("Failed to add reservations to batch: {}", e);
                }

                // Commit batch
                batch.commit().expect("Batch commit failed");

                Ok(ResourceResponse::Initialized {
                    name: name.clone(),
                    symbol: symbol.clone(),
                    decimals: *decimals,
                })
            }

            ResourceOperation::UpdateMetadata { name, symbol } => {
                // Check for metadata reservation conflict
                if let Some(blocking_txs) = self.reservations.would_conflict(
                    "",
                    &ReservationType::MetadataUpdate,
                    Amount::zero(),
                ) {
                    return Err(format!(
                        "Metadata update blocked by transactions: {:?}",
                        blocking_txs
                    ));
                }

                // Reserve metadata
                self.reservations.reserve_metadata(transaction_id)?;

                // Get current metadata
                let old_metadata = self.get_metadata(transaction_id);

                if !old_metadata.initialized {
                    return Err("Resource not initialized".to_string());
                }

                // Create new metadata with updates
                let mut new_metadata = old_metadata.clone();
                if let Some(n) = name {
                    new_metadata.name = n.clone();
                }
                if let Some(s) = symbol {
                    new_metadata.symbol = s.clone();
                }

                // Create delta
                let delta = ResourceDelta::SetMetadata {
                    old: Some(old_metadata),
                    new: new_metadata.clone(),
                };

                // Write to storage
                let mut batch = self.storage.batch();
                self.storage
                    .write_to_batch(&mut batch, delta, transaction_id, log_index)
                    .expect("Write failed");

                // Add reservations to the batch
                if let Err(e) = self.add_reservations_to_batch(&mut batch, transaction_id) {
                    eprintln!("Failed to add reservations to batch: {}", e);
                }

                // Commit batch
                batch.commit().expect("Batch commit failed");

                Ok(ResourceResponse::MetadataUpdated {
                    name: name.clone(),
                    symbol: symbol.clone(),
                })
            }

            ResourceOperation::Mint { to, amount, .. } => {
                // Reserve credit for the recipient
                self.reservations
                    .reserve_credit(transaction_id, to, *amount)?;

                // Get current balance and supply
                let old_balance = self.get_balance(to, transaction_id);
                let old_supply = self.get_supply(transaction_id);

                let new_balance = old_balance + *amount;
                let new_supply = old_supply + *amount;

                // Create TWO deltas - one for balance, one for supply
                let balance_delta = ResourceDelta::SetBalance {
                    account: to.clone(),
                    old: old_balance,
                    new: new_balance,
                };

                let supply_delta = ResourceDelta::SetSupply {
                    old: old_supply,
                    new: new_supply,
                };

                // Write both deltas to storage in the same batch
                let mut batch = self.storage.batch();
                self.storage
                    .write_to_batch(&mut batch, balance_delta, transaction_id, log_index)
                    .expect("Write balance failed");
                self.storage
                    .write_to_batch(&mut batch, supply_delta, transaction_id, log_index)
                    .expect("Write supply failed");

                // Add reservations to the batch
                if let Err(e) = self.add_reservations_to_batch(&mut batch, transaction_id) {
                    eprintln!("Failed to add reservations to batch: {}", e);
                }

                // Commit batch
                batch.commit().expect("Batch commit failed");

                Ok(ResourceResponse::Minted {
                    to: to.clone(),
                    amount: *amount,
                    new_balance,
                    total_supply: new_supply,
                })
            }

            ResourceOperation::Burn { from, amount, .. } => {
                // Get current balance
                let old_balance = self.get_balance(from, transaction_id);

                // Check for debit reservation conflict
                if let Some(blocking_txs) = self.reservations.would_conflict(
                    from,
                    &ReservationType::Debit(*amount),
                    old_balance,
                ) {
                    return Err(format!("Burn blocked by transactions: {:?}", blocking_txs));
                }

                // Reserve debit
                self.reservations
                    .reserve_debit(transaction_id, from, *amount, old_balance)?;

                // Check balance
                if old_balance < *amount {
                    return Err("Insufficient balance for burn".to_string());
                }

                // Get current supply
                let old_supply = self.get_supply(transaction_id);

                let new_balance = old_balance - *amount;
                let new_supply = old_supply - *amount;

                // Create TWO deltas - one for balance, one for supply
                let balance_delta = ResourceDelta::SetBalance {
                    account: from.clone(),
                    old: old_balance,
                    new: new_balance,
                };

                let supply_delta = ResourceDelta::SetSupply {
                    old: old_supply,
                    new: new_supply,
                };

                // Write both deltas to storage in the same batch
                let mut batch = self.storage.batch();
                self.storage
                    .write_to_batch(&mut batch, balance_delta, transaction_id, log_index)
                    .expect("Write balance failed");
                self.storage
                    .write_to_batch(&mut batch, supply_delta, transaction_id, log_index)
                    .expect("Write supply failed");

                // Add reservations to the batch
                if let Err(e) = self.add_reservations_to_batch(&mut batch, transaction_id) {
                    eprintln!("Failed to add reservations to batch: {}", e);
                }

                // Commit batch
                batch.commit().expect("Batch commit failed");

                Ok(ResourceResponse::Burned {
                    from: from.clone(),
                    amount: *amount,
                    new_balance,
                    total_supply: new_supply,
                })
            }

            ResourceOperation::Transfer {
                from, to, amount, ..
            } => {
                // Get current balances
                let old_from_balance = self.get_balance(from, transaction_id);

                // Check for debit reservation conflict
                if let Some(blocking_txs) = self.reservations.would_conflict(
                    from,
                    &ReservationType::Debit(*amount),
                    old_from_balance,
                ) {
                    return Err(format!(
                        "Transfer blocked by transactions: {:?}",
                        blocking_txs
                    ));
                }

                // Reserve debit and credit
                self.reservations
                    .reserve_debit(transaction_id, from, *amount, old_from_balance)?;
                self.reservations
                    .reserve_credit(transaction_id, to, *amount)?;

                // Check balance
                if old_from_balance < *amount {
                    return Err("Insufficient balance for transfer".to_string());
                }

                // Get to balance
                let old_to_balance = self.get_balance(to, transaction_id);

                let new_from_balance = old_from_balance - *amount;
                let new_to_balance = old_to_balance + *amount;

                // Create TWO deltas - one for each account
                let from_delta = ResourceDelta::SetBalance {
                    account: from.clone(),
                    old: old_from_balance,
                    new: new_from_balance,
                };

                let to_delta = ResourceDelta::SetBalance {
                    account: to.clone(),
                    old: old_to_balance,
                    new: new_to_balance,
                };

                // Write both deltas to storage in the same batch
                let mut batch = self.storage.batch();
                self.storage
                    .write_to_batch(&mut batch, from_delta, transaction_id, log_index)
                    .expect("Write from balance failed");
                self.storage
                    .write_to_batch(&mut batch, to_delta, transaction_id, log_index)
                    .expect("Write to balance failed");

                // Add reservations to the batch
                if let Err(e) = self.add_reservations_to_batch(&mut batch, transaction_id) {
                    eprintln!("Failed to add reservations to batch: {}", e);
                }

                // Commit batch
                batch.commit().expect("Batch commit failed");

                Ok(ResourceResponse::Transferred {
                    from: from.clone(),
                    to: to.clone(),
                    amount: *amount,
                    from_balance: new_from_balance,
                    to_balance: new_to_balance,
                })
            }

            ResourceOperation::GetBalance { account } => {
                let balance = self.get_balance(account, transaction_id);
                Ok(ResourceResponse::Balance {
                    account: account.clone(),
                    amount: balance,
                })
            }

            ResourceOperation::GetMetadata => {
                let metadata = self.get_metadata(transaction_id);
                let supply = self.get_supply(transaction_id);
                Ok(ResourceResponse::Metadata {
                    name: metadata.name,
                    symbol: metadata.symbol,
                    decimals: metadata.decimals,
                    total_supply: supply,
                })
            }

            ResourceOperation::GetTotalSupply => Ok(ResourceResponse::TotalSupply {
                amount: self.get_supply(transaction_id),
            }),
        }
    }
}

impl TransactionEngine for ResourceTransactionEngine {
    type Operation = ResourceOperation;
    type Response = ResourceResponse;

    fn read_at_timestamp(
        &mut self,
        operation: Self::Operation,
        read_timestamp: HlcTimestamp,
        _log_index: u64,
    ) -> OperationResult<Self::Response> {
        self.execute_read_at_timestamp(&operation, read_timestamp)
    }

    fn begin(&mut self, _txn_id: HlcTimestamp, _log_index: u64) {
        // Nothing to do - MVCC storage tracks transactions internally
    }

    fn apply_operation(
        &mut self,
        operation: Self::Operation,
        txn_id: HlcTimestamp,
        log_index: u64,
    ) -> OperationResult<Self::Response> {
        match self.process_operation(&operation, txn_id, log_index) {
            Ok(response) => OperationResult::Complete(response),
            Err(err) if err.contains("blocked by transactions") => {
                // Parse blocking transactions from error message
                let retry_on = if err.contains("Metadata") {
                    RetryOn::CommitOrAbort // Metadata locks are exclusive
                } else {
                    RetryOn::Prepare // Balance reservations can be released early
                };

                OperationResult::WouldBlock {
                    blockers: vec![BlockingInfo {
                        txn: txn_id,
                        retry_on,
                    }],
                }
            }
            Err(err) => OperationResult::Complete(ResourceResponse::Error(err)),
        }
    }

    fn prepare(&mut self, txn_id: HlcTimestamp, log_index: u64) {
        // Update persisted reservations atomically with log_index
        let mut batch = self.storage.batch();
        if let Err(e) = self.add_reservations_to_batch(&mut batch, txn_id) {
            eprintln!("Failed to add reservations to batch: {}", e);
        }

        // Update log_index in metadata atomically with reservations
        let metadata = self.storage.metadata_partition();
        batch.insert(metadata, "_log_index", log_index.to_le_bytes());

        batch.commit().expect("Batch commit failed");
    }

    fn commit(&mut self, txn_id: HlcTimestamp, log_index: u64) {
        // Commit to storage and clear persisted reservations atomically
        let mut batch = self.storage.batch();

        // Commit the transaction via storage
        self.storage
            .commit_transaction_to_batch(&mut batch, txn_id, log_index)
            .expect("Commit failed");

        // Clear persisted reservations in the same batch
        let mut lock_key = b"_locks_".to_vec();
        lock_key.extend_from_slice(&txn_id.to_lexicographic_bytes());
        let metadata = self.storage.metadata_partition();
        batch.remove(metadata.clone(), lock_key);

        // Commit atomically: transaction commit + reservations cleanup + log_index
        batch.commit().expect("Batch commit failed");

        // Cleanup old buckets if needed (throttled internally)
        self.storage.maybe_cleanup(txn_id).ok();

        // Release all reservations held by this transaction
        self.reservations.release_transaction(txn_id);
    }

    fn abort(&mut self, txn_id: HlcTimestamp, log_index: u64) {
        // Abort in storage and clear persisted reservations atomically
        let mut batch = self.storage.batch();

        // Abort the transaction via storage
        self.storage
            .abort_transaction_to_batch(&mut batch, txn_id, log_index)
            .expect("Abort failed");

        // Clear persisted reservations in the same batch
        let mut lock_key = b"_locks_".to_vec();
        lock_key.extend_from_slice(&txn_id.to_lexicographic_bytes());
        let metadata = self.storage.metadata_partition();
        batch.remove(metadata.clone(), lock_key);

        // Commit atomically: transaction abort + reservations cleanup + log_index
        batch.commit().expect("Batch commit failed");

        // Cleanup old buckets if needed (throttled internally)
        self.storage.maybe_cleanup(txn_id).ok();

        // Release all reservations
        self.reservations.release_transaction(txn_id);
    }

    fn engine_name(&self) -> &'static str {
        "resource"
    }

    fn get_log_index(&self) -> Option<u64> {
        let log_index = self.storage.get_log_index();
        if log_index > 0 { Some(log_index) } else { None }
    }
}

impl Default for ResourceTransactionEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proven_hlc::NodeId;

    fn make_timestamp(n: u64) -> HlcTimestamp {
        HlcTimestamp::new(n, 0, NodeId::new(0))
    }

    #[test]
    fn test_basic_operations() {
        let mut engine = ResourceTransactionEngine::new();
        let tx1 = make_timestamp(100);

        // Begin transaction
        engine.begin(tx1, 1);

        // Initialize resource
        let op = ResourceOperation::Initialize {
            name: "Test Token".to_string(),
            symbol: "TEST".to_string(),
            decimals: 8,
        };

        let result = engine.apply_operation(op, tx1, 2);
        assert!(matches!(result, OperationResult::Complete(_)));

        // Mint tokens
        let op = ResourceOperation::Mint {
            to: "alice".to_string(),
            amount: Amount::from_integer(1000, 0),
            memo: None,
        };

        let result = engine.apply_operation(op, tx1, 3);
        assert!(matches!(result, OperationResult::Complete(_)));

        // Commit transaction
        engine.prepare(tx1, 4);
        engine.commit(tx1, 5);

        // Check balance in new transaction
        let tx2 = make_timestamp(200);
        engine.begin(tx2, 6);

        let op = ResourceOperation::GetBalance {
            account: "alice".to_string(),
        };

        let result = engine.apply_operation(op, tx2, 7);
        if let OperationResult::Complete(ResourceResponse::Balance { amount, .. }) = result {
            assert_eq!(amount, Amount::from_integer(1000, 0));
        } else {
            panic!("Expected balance response");
        }
    }

    #[test]
    fn test_transfer() {
        let mut engine = ResourceTransactionEngine::new();
        let tx1 = make_timestamp(100);

        engine.begin(tx1, 1);

        // Initialize
        engine.apply_operation(
            ResourceOperation::Initialize {
                name: "Test".to_string(),
                symbol: "TST".to_string(),
                decimals: 2,
            },
            tx1,
            2,
        );

        // Mint to alice
        engine.apply_operation(
            ResourceOperation::Mint {
                to: "alice".to_string(),
                amount: Amount::from_integer(1000, 0),
                memo: None,
            },
            tx1,
            3,
        );

        engine.commit(tx1, 4);

        // Transfer
        let tx2 = make_timestamp(200);
        engine.begin(tx2, 5);

        let result = engine.apply_operation(
            ResourceOperation::Transfer {
                from: "alice".to_string(),
                to: "bob".to_string(),
                amount: Amount::from_integer(300, 0),
                memo: None,
            },
            tx2,
            6,
        );

        assert!(matches!(result, OperationResult::Complete(_)));

        engine.commit(tx2, 7);

        // Verify balances
        let tx3 = make_timestamp(300);
        engine.begin(tx3, 8);

        let alice_balance = engine.get_balance("alice", tx3);
        let bob_balance = engine.get_balance("bob", tx3);

        assert_eq!(alice_balance, Amount::from_integer(700, 0));
        assert_eq!(bob_balance, Amount::from_integer(300, 0));
    }

    #[test]
    fn test_burn() {
        let mut engine = ResourceTransactionEngine::new();
        let tx1 = make_timestamp(100);

        engine.begin(tx1, 1);

        // Initialize and mint
        engine.apply_operation(
            ResourceOperation::Initialize {
                name: "Test".to_string(),
                symbol: "TST".to_string(),
                decimals: 2,
            },
            tx1,
            2,
        );

        engine.apply_operation(
            ResourceOperation::Mint {
                to: "alice".to_string(),
                amount: Amount::from_integer(1000, 0),
                memo: None,
            },
            tx1,
            3,
        );

        engine.commit(tx1, 4);

        // Burn
        let tx2 = make_timestamp(200);
        engine.begin(tx2, 5);

        let result = engine.apply_operation(
            ResourceOperation::Burn {
                from: "alice".to_string(),
                amount: Amount::from_integer(400, 0),
                memo: None,
            },
            tx2,
            6,
        );

        assert!(matches!(result, OperationResult::Complete(_)));

        engine.commit(tx2, 7);

        // Verify balance and supply
        let tx3 = make_timestamp(300);
        let balance = engine.get_balance("alice", tx3);
        let supply = engine.get_supply(tx3);

        assert_eq!(balance, Amount::from_integer(600, 0));
        assert_eq!(supply, Amount::from_integer(600, 0));
    }
}
