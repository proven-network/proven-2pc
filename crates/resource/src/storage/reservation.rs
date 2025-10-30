//! Reservation-based conflict detection for resource operations

use crate::types::Amount;
use proven_common::TransactionId;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// Type of reservation
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ReservationType {
    /// Reserve balance for spending (debit)
    Debit(Amount),
    /// Reserve space for receiving (credit)
    Credit(Amount),
    /// Reserve for metadata update
    MetadataUpdate,
}

/// A balance reservation for a specific account
#[derive(Debug, Clone)]
pub struct BalanceReservation {
    pub transaction_id: TransactionId,
    pub account: String,
    pub reservation_type: ReservationType,
    pub timestamp: TransactionId,
}

/// Manages reservations for conflict detection
pub struct ReservationManager {
    /// Active reservations by account
    /// Map<account, Vec<Reservation>>
    account_reservations: HashMap<String, Vec<BalanceReservation>>,

    /// Metadata reservations (only one transaction can update metadata at a time)
    metadata_reservation: Option<TransactionId>,

    /// Track which accounts each transaction has reservations on
    transaction_accounts: HashMap<TransactionId, HashSet<String>>,
}

impl ReservationManager {
    pub fn new() -> Self {
        Self {
            account_reservations: HashMap::new(),
            metadata_reservation: None,
            transaction_accounts: HashMap::new(),
        }
    }

    /// Check if a debit reservation can be made
    pub fn can_reserve_debit(
        &self,
        account: &str,
        amount: Amount,
        current_balance: Amount,
    ) -> bool {
        // Calculate total already reserved for debits
        let total_reserved = self.get_total_reserved_debits(account);

        // Check if we have enough balance after existing reservations
        current_balance >= total_reserved + amount
    }

    /// Get total amount reserved for debits on an account
    fn get_total_reserved_debits(&self, account: &str) -> Amount {
        self.account_reservations
            .get(account)
            .map(|reservations| {
                reservations
                    .iter()
                    .filter_map(|r| match &r.reservation_type {
                        ReservationType::Debit(amt) => Some(*amt),
                        _ => None,
                    })
                    .fold(Amount::zero(), |acc, amt| acc + amt)
            })
            .unwrap_or_else(Amount::zero)
    }

    /// Reserve balance for a debit operation
    pub fn reserve_debit(
        &mut self,
        transaction_id: TransactionId,
        account: &str,
        amount: Amount,
        current_balance: Amount,
    ) -> Result<(), String> {
        if !self.can_reserve_debit(account, amount, current_balance) {
            return Err("Insufficient balance for reservation".to_string());
        }

        let reservation = BalanceReservation {
            transaction_id,
            account: account.to_string(),
            reservation_type: ReservationType::Debit(amount),
            timestamp: transaction_id,
        };

        self.account_reservations
            .entry(account.to_string())
            .or_default()
            .push(reservation);

        self.transaction_accounts
            .entry(transaction_id)
            .or_default()
            .insert(account.to_string());

        Ok(())
    }

    /// Reserve balance for a credit operation
    pub fn reserve_credit(
        &mut self,
        transaction_id: TransactionId,
        account: &str,
        amount: Amount,
    ) -> Result<(), String> {
        let reservation = BalanceReservation {
            transaction_id,
            account: account.to_string(),
            reservation_type: ReservationType::Credit(amount),
            timestamp: transaction_id,
        };

        self.account_reservations
            .entry(account.to_string())
            .or_default()
            .push(reservation);

        self.transaction_accounts
            .entry(transaction_id)
            .or_default()
            .insert(account.to_string());

        Ok(())
    }

    /// Reserve metadata update
    pub fn reserve_metadata(&mut self, transaction_id: TransactionId) -> Result<(), String> {
        if let Some(existing_tx) = self.metadata_reservation {
            return Err(format!(
                "Metadata already reserved by transaction {}",
                existing_tx
            ));
        }

        self.metadata_reservation = Some(transaction_id);
        Ok(())
    }

    /// Check if operations would conflict
    pub fn would_conflict(
        &self,
        account: &str,
        operation: &ReservationType,
        current_balance: Amount,
    ) -> Option<Vec<TransactionId>> {
        match operation {
            ReservationType::Debit(amount) => {
                // Check if debit would exceed available balance
                if !self.can_reserve_debit(account, *amount, current_balance) {
                    // Return transactions that are blocking this debit
                    self.account_reservations.get(account).map(|reservations| {
                        reservations
                            .iter()
                            .filter(|r| matches!(r.reservation_type, ReservationType::Debit(_)))
                            .map(|r| r.transaction_id)
                            .collect()
                    })
                } else {
                    None
                }
            }
            ReservationType::Credit(_) => {
                // Credits don't conflict with other operations
                None
            }
            ReservationType::MetadataUpdate => {
                // Check if metadata is already reserved
                self.metadata_reservation.map(|tx| vec![tx])
            }
        }
    }

    /// Release all reservations for a transaction
    pub fn release_transaction(&mut self, transaction_id: TransactionId) {
        // Release metadata reservation if held
        if self.metadata_reservation == Some(transaction_id) {
            self.metadata_reservation = None;
        }

        // Get accounts this transaction has reservations on
        if let Some(accounts) = self.transaction_accounts.remove(&transaction_id) {
            for account in accounts {
                if let Some(reservations) = self.account_reservations.get_mut(&account) {
                    reservations.retain(|r| r.transaction_id != transaction_id);
                    if reservations.is_empty() {
                        self.account_reservations.remove(&account);
                    }
                }
            }
        }
    }

    /// Get all transactions that have reservations on an account
    pub fn get_blocking_transactions(&self, account: &str) -> Vec<TransactionId> {
        self.account_reservations
            .get(account)
            .map(|reservations| {
                reservations
                    .iter()
                    .map(|r| r.transaction_id)
                    .collect::<HashSet<_>>()
                    .into_iter()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get all reservations held by a transaction
    /// Returns list of (account, reservation_type) pairs
    pub fn reservations_held_by(
        &self,
        transaction_id: TransactionId,
    ) -> Vec<(String, ReservationType)> {
        let mut result = Vec::new();

        // Check metadata reservation
        if self.metadata_reservation == Some(transaction_id) {
            result.push((String::new(), ReservationType::MetadataUpdate));
        }

        // Check account reservations
        if let Some(accounts) = self.transaction_accounts.get(&transaction_id) {
            for account in accounts {
                if let Some(reservations) = self.account_reservations.get(account) {
                    for reservation in reservations {
                        if reservation.transaction_id == transaction_id {
                            result.push((account.clone(), reservation.reservation_type.clone()));
                        }
                    }
                }
            }
        }

        result
    }
}

impl Default for ReservationManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn make_timestamp(n: u64) -> TransactionId {
        TransactionId::from_uuid(Uuid::from_u128(n as u128))
    }

    #[test]
    fn test_debit_reservations() {
        let mut manager = ReservationManager::new();
        let balance = Amount::from_integer(100, 0);

        // First reservation should succeed
        let tx1 = make_timestamp(1);
        manager
            .reserve_debit(tx1, "alice", Amount::from_integer(50, 0), balance)
            .unwrap();

        // Second reservation should succeed (50 + 30 = 80 < 100)
        let tx2 = make_timestamp(2);
        manager
            .reserve_debit(tx2, "alice", Amount::from_integer(30, 0), balance)
            .unwrap();

        // Third reservation should fail (50 + 30 + 30 = 110 > 100)
        let tx3 = make_timestamp(3);
        let result = manager.reserve_debit(tx3, "alice", Amount::from_integer(30, 0), balance);
        assert!(result.is_err());

        // Release first transaction
        manager.release_transaction(tx1);

        // Now third reservation should succeed (30 + 30 = 60 < 100)
        manager
            .reserve_debit(tx3, "alice", Amount::from_integer(30, 0), balance)
            .unwrap();
    }

    #[test]
    fn test_metadata_reservation() {
        let mut manager = ReservationManager::new();

        let tx1 = make_timestamp(1);
        manager.reserve_metadata(tx1).unwrap();

        // Second metadata reservation should fail
        let tx2 = make_timestamp(2);
        let result = manager.reserve_metadata(tx2);
        assert!(result.is_err());

        // Release first transaction
        manager.release_transaction(tx1);

        // Now second should succeed
        manager.reserve_metadata(tx2).unwrap();
    }
}
