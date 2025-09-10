//! Resource engine implementation

use crate::storage::{ReservationManager, ReservationType, ResourceStorage};
use crate::stream::{ResourceOperation, ResourceResponse, TransactionContext};
use crate::types::Amount;
use proven_hlc::HlcTimestamp;
use proven_stream::{OperationResult, RetryOn, TransactionEngine};
use std::collections::HashMap;

/// Resource engine for processing resource operations
pub struct ResourceTransactionEngine {
    /// Resource storage
    storage: ResourceStorage,

    /// Reservation manager for conflict detection
    reservations: ReservationManager,

    /// Active transaction contexts
    transactions: HashMap<HlcTimestamp, TransactionContext>,
}

impl ResourceTransactionEngine {
    pub fn new() -> Self {
        Self {
            storage: ResourceStorage::new(),
            reservations: ReservationManager::new(),
            transactions: HashMap::new(),
        }
    }

    /// Process a resource operation
    fn process_operation(
        &mut self,
        operation: &ResourceOperation,
        transaction_id: HlcTimestamp,
    ) -> Result<ResourceResponse, String> {
        let tx_ctx = self
            .transactions
            .get_mut(&transaction_id)
            .ok_or_else(|| "Transaction not found".to_string())?;

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
                tx_ctx.add_reservation("".to_string(), ReservationType::MetadataUpdate);
                tx_ctx.set_modifies_metadata();

                // Initialize in storage
                self.storage
                    .initialize(name.clone(), symbol.clone(), *decimals)?;

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
                tx_ctx.add_reservation("".to_string(), ReservationType::MetadataUpdate);
                tx_ctx.set_modifies_metadata();

                // Update in storage
                self.storage.update_metadata(name.clone(), symbol.clone())?;

                Ok(ResourceResponse::MetadataUpdated {
                    name: name.clone(),
                    symbol: symbol.clone(),
                })
            }

            ResourceOperation::Mint { to, amount, .. } => {
                // Reserve credit for the recipient
                self.reservations
                    .reserve_credit(transaction_id, to, *amount)?;
                tx_ctx.add_reservation(to.clone(), ReservationType::Credit(*amount));
                tx_ctx.add_modified_account(to.clone());
                tx_ctx.set_modifies_supply();

                // Update pending balance
                let current_balance = self.storage.get_pending_balance(to, transaction_id);
                let new_balance = current_balance + *amount;
                self.storage
                    .update_pending_balance(transaction_id, to, new_balance)?;

                // Update pending supply
                let new_supply =
                    self.storage
                        .update_pending_supply(transaction_id, *amount, true)?;

                Ok(ResourceResponse::Minted {
                    to: to.clone(),
                    amount: *amount,
                    new_balance,
                    total_supply: new_supply,
                })
            }

            ResourceOperation::Burn { from, amount, .. } => {
                // Get current balance
                let current_balance = self.storage.get_pending_balance(from, transaction_id);

                // Check for debit reservation conflict
                if let Some(blocking_txs) = self.reservations.would_conflict(
                    from,
                    &ReservationType::Debit(*amount),
                    current_balance,
                ) {
                    return Err(format!("Burn blocked by transactions: {:?}", blocking_txs));
                }

                // Reserve debit
                self.reservations
                    .reserve_debit(transaction_id, from, *amount, current_balance)?;
                tx_ctx.add_reservation(from.clone(), ReservationType::Debit(*amount));
                tx_ctx.add_modified_account(from.clone());
                tx_ctx.set_modifies_supply();

                // Check balance
                if current_balance < *amount {
                    return Err("Insufficient balance for burn".to_string());
                }

                // Update pending balance
                let new_balance = current_balance - *amount;
                self.storage
                    .update_pending_balance(transaction_id, from, new_balance)?;

                // Update pending supply
                let new_supply =
                    self.storage
                        .update_pending_supply(transaction_id, *amount, false)?;

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
                let from_balance = self.storage.get_pending_balance(from, transaction_id);

                // Check for debit reservation conflict
                if let Some(blocking_txs) = self.reservations.would_conflict(
                    from,
                    &ReservationType::Debit(*amount),
                    from_balance,
                ) {
                    return Err(format!(
                        "Transfer blocked by transactions: {:?}",
                        blocking_txs
                    ));
                }

                // Reserve debit and credit
                self.reservations
                    .reserve_debit(transaction_id, from, *amount, from_balance)?;
                self.reservations
                    .reserve_credit(transaction_id, to, *amount)?;

                tx_ctx.add_reservation(from.clone(), ReservationType::Debit(*amount));
                tx_ctx.add_reservation(to.clone(), ReservationType::Credit(*amount));
                tx_ctx.add_modified_account(from.clone());
                tx_ctx.add_modified_account(to.clone());

                // Check balance
                if from_balance < *amount {
                    return Err("Insufficient balance for transfer".to_string());
                }

                // Update pending balances
                let new_from_balance = from_balance - *amount;
                self.storage
                    .update_pending_balance(transaction_id, from, new_from_balance)?;

                let to_balance = self.storage.get_pending_balance(to, transaction_id);
                let new_to_balance = to_balance + *amount;
                self.storage
                    .update_pending_balance(transaction_id, to, new_to_balance)?;

                Ok(ResourceResponse::Transferred {
                    from: from.clone(),
                    to: to.clone(),
                    amount: *amount,
                    from_balance: new_from_balance,
                    to_balance: new_to_balance,
                })
            }

            ResourceOperation::GetBalance { account } => {
                let balance = self.storage.get_balance(account, transaction_id);
                Ok(ResourceResponse::Balance {
                    account: account.clone(),
                    amount: balance,
                })
            }

            ResourceOperation::GetMetadata => {
                let metadata = self.storage.get_metadata();
                Ok(ResourceResponse::Metadata {
                    name: metadata.name.clone(),
                    symbol: metadata.symbol.clone(),
                    decimals: metadata.decimals,
                    total_supply: self.storage.get_total_supply(),
                })
            }

            ResourceOperation::GetTotalSupply => Ok(ResourceResponse::TotalSupply {
                amount: self.storage.get_total_supply(),
            }),
        }
    }
}

impl TransactionEngine for ResourceTransactionEngine {
    type Operation = ResourceOperation;
    type Response = ResourceResponse;

    fn begin_transaction(&mut self, txn_id: HlcTimestamp) {
        self.storage.begin_transaction(txn_id);
        self.transactions
            .insert(txn_id, TransactionContext::new(txn_id));
    }

    fn apply_operation(
        &mut self,
        operation: Self::Operation,
        txn_id: HlcTimestamp,
    ) -> OperationResult<Self::Response> {
        match self.process_operation(&operation, txn_id) {
            Ok(response) => OperationResult::Success(response),
            Err(err) if err.contains("blocked by transactions") => {
                // Parse blocking transactions from error message
                // In a real implementation, we'd return them more cleanly
                let retry_on = if err.contains("Metadata") {
                    RetryOn::CommitOrAbort // Metadata locks are exclusive
                } else {
                    RetryOn::Prepare // Balance reservations can be released early
                };

                OperationResult::WouldBlock {
                    blocking_txn: txn_id, // Would need to parse from error
                    retry_on,
                }
            }
            Err(err) => OperationResult::Error(err),
        }
    }

    fn prepare(&mut self, txn_id: HlcTimestamp) -> Result<(), String> {
        let tx_ctx = self
            .transactions
            .get_mut(&txn_id)
            .ok_or_else(|| "Transaction not found".to_string())?;

        tx_ctx.mark_prepared();

        // In the reservation model, we keep reservations until commit
        // This ensures consistency

        Ok(())
    }

    fn commit(&mut self, txn_id: HlcTimestamp) -> Result<(), String> {
        // Commit storage changes
        self.storage.commit_transaction(txn_id)?;

        // Release reservations
        self.reservations.release_transaction(txn_id);

        // Remove transaction context
        self.transactions.remove(&txn_id);

        Ok(())
    }

    fn abort(&mut self, txn_id: HlcTimestamp) -> Result<(), String> {
        // Abort storage changes
        self.storage.abort_transaction(txn_id);

        // Release reservations
        self.reservations.release_transaction(txn_id);

        // Remove transaction context
        self.transactions.remove(&txn_id);

        Ok(())
    }

    fn is_transaction_active(&self, txn_id: &HlcTimestamp) -> bool {
        self.transactions.contains_key(txn_id)
    }

    fn engine_name(&self) -> &str {
        "resource"
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
        engine.begin_transaction(tx1);

        // Initialize resource
        let op = ResourceOperation::Initialize {
            name: "Test Token".to_string(),
            symbol: "TEST".to_string(),
            decimals: 8,
        };

        let result = engine.apply_operation(op, tx1);
        assert!(matches!(result, OperationResult::Success(_)));

        // Mint tokens
        let op = ResourceOperation::Mint {
            to: "alice".to_string(),
            amount: Amount::from_integer(1000, 0),
            memo: None,
        };

        let result = engine.apply_operation(op, tx1);
        assert!(matches!(result, OperationResult::Success(_)));

        // Commit transaction
        engine.prepare(tx1).unwrap();
        engine.commit(tx1).unwrap();

        // Check balance in new transaction
        let tx2 = make_timestamp(200);
        engine.begin_transaction(tx2);

        let op = ResourceOperation::GetBalance {
            account: "alice".to_string(),
        };

        let result = engine.apply_operation(op, tx2);
        if let OperationResult::Success(ResourceResponse::Balance { amount, .. }) = result {
            assert_eq!(amount, Amount::from_integer(1000, 0));
        } else {
            panic!("Expected balance response");
        }
    }
}
