//! Transaction context for resource operations

use crate::storage::ReservationType;
use proven_hlc::HlcTimestamp;
use std::collections::HashSet;

/// Context for tracking transaction state
#[derive(Debug, Clone)]
pub struct TransactionContext {
    /// Transaction ID
    pub transaction_id: HlcTimestamp,

    /// Accounts that have been modified in this transaction
    pub modified_accounts: HashSet<String>,

    /// Reservations held by this transaction
    pub reservations: Vec<(String, ReservationType)>,

    /// Whether the transaction has been prepared
    pub is_prepared: bool,

    /// Whether the transaction modifies metadata
    pub modifies_metadata: bool,

    /// Whether the transaction modifies supply (mint/burn)
    pub modifies_supply: bool,
}

impl TransactionContext {
    pub fn new(transaction_id: HlcTimestamp) -> Self {
        Self {
            transaction_id,
            modified_accounts: HashSet::new(),
            reservations: Vec::new(),
            is_prepared: false,
            modifies_metadata: false,
            modifies_supply: false,
        }
    }

    /// Add an account that has been modified
    pub fn add_modified_account(&mut self, account: String) {
        self.modified_accounts.insert(account);
    }

    /// Add a reservation
    pub fn add_reservation(&mut self, account: String, reservation_type: ReservationType) {
        self.reservations.push((account, reservation_type));
    }

    /// Mark that this transaction modifies metadata
    pub fn set_modifies_metadata(&mut self) {
        self.modifies_metadata = true;
    }

    /// Mark that this transaction modifies supply
    pub fn set_modifies_supply(&mut self) {
        self.modifies_supply = true;
    }

    /// Mark the transaction as prepared
    pub fn mark_prepared(&mut self) {
        self.is_prepared = true;
    }
}
