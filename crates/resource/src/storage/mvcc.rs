//! MVCC storage for resource data

use crate::types::Amount;
use proven_hlc::HlcTimestamp;
use std::collections::BTreeMap;

/// Resource metadata
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct ResourceMetadata {
    pub name: String,
    pub symbol: String,
    pub decimals: u32,
    pub initialized: bool,
}

/// Versioned balance entry
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct BalanceVersion {
    amount: Amount,
    timestamp: HlcTimestamp,
}

/// MVCC storage for resource data
pub struct ResourceStorage {
    /// Resource metadata
    metadata: ResourceMetadata,

    /// Total supply of the resource
    total_supply: Amount,

    /// Account balances with MVCC
    /// BTreeMap<account, Vec<BalanceVersion>> where versions are sorted by timestamp
    balances: BTreeMap<String, Vec<BalanceVersion>>,

    /// Active transactions and their uncommitted changes
    /// Map<transaction_id, Map<account, pending_balance>>
    pending_changes: BTreeMap<HlcTimestamp, BTreeMap<String, Amount>>,

    /// Pending supply changes per transaction
    /// Map<transaction_id, new_supply>
    pending_supply: BTreeMap<HlcTimestamp, Amount>,
}

impl ResourceStorage {
    /// Create a new resource storage
    pub fn new() -> Self {
        Self {
            metadata: ResourceMetadata::default(),
            total_supply: Amount::zero(),
            balances: BTreeMap::new(),
            pending_changes: BTreeMap::new(),
            pending_supply: BTreeMap::new(),
        }
    }

    /// Initialize the resource metadata
    pub fn initialize(
        &mut self,
        name: String,
        symbol: String,
        decimals: u32,
    ) -> Result<(), String> {
        if self.metadata.initialized {
            return Err("Resource already initialized".to_string());
        }

        self.metadata = ResourceMetadata {
            name,
            symbol,
            decimals,
            initialized: true,
        };

        Ok(())
    }

    /// Update resource metadata
    pub fn update_metadata(
        &mut self,
        name: Option<String>,
        symbol: Option<String>,
    ) -> Result<(), String> {
        if !self.metadata.initialized {
            return Err("Resource not initialized".to_string());
        }

        if let Some(name) = name {
            self.metadata.name = name;
        }
        if let Some(symbol) = symbol {
            self.metadata.symbol = symbol;
        }

        Ok(())
    }

    /// Get resource metadata
    pub fn get_metadata(&self) -> &ResourceMetadata {
        &self.metadata
    }

    /// Get total supply
    pub fn get_total_supply(&self) -> Amount {
        self.total_supply
    }

    /// Get balance for an account at a specific timestamp (MVCC read)
    pub fn get_balance(&self, account: &str, timestamp: HlcTimestamp) -> Amount {
        // First check if there are any versions for this account
        if let Some(versions) = self.balances.get(account) {
            // Find the latest version that is <= timestamp
            for version in versions.iter().rev() {
                if version.timestamp <= timestamp {
                    return version.amount;
                }
            }
        }

        // No balance history means zero balance
        Amount::zero()
    }

    /// Get pending balance for an account in a transaction
    pub fn get_pending_balance(&self, account: &str, transaction_id: HlcTimestamp) -> Amount {
        if let Some(tx_changes) = self.pending_changes.get(&transaction_id)
            && let Some(&amount) = tx_changes.get(account)
        {
            return amount;
        }

        // If no pending changes, return current balance
        self.get_balance(account, transaction_id)
    }

    /// Begin tracking changes for a transaction
    pub fn begin_transaction(&mut self, transaction_id: HlcTimestamp) {
        self.pending_changes.insert(transaction_id, BTreeMap::new());
    }

    /// Update pending balance for an account in a transaction
    pub fn update_pending_balance(
        &mut self,
        transaction_id: HlcTimestamp,
        account: &str,
        new_balance: Amount,
    ) -> Result<(), String> {
        let tx_changes = self
            .pending_changes
            .get_mut(&transaction_id)
            .ok_or_else(|| format!("Transaction {} not found", transaction_id))?;

        tx_changes.insert(account.to_string(), new_balance);
        Ok(())
    }

    /// Update pending total supply for mints/burns
    pub fn update_pending_supply(
        &mut self,
        transaction_id: HlcTimestamp,
        delta: Amount,
        is_mint: bool,
    ) -> Result<Amount, String> {
        // Get current pending supply for this transaction or use the global supply
        let current_supply = self
            .pending_supply
            .get(&transaction_id)
            .copied()
            .unwrap_or(self.total_supply);

        let new_supply = if is_mint {
            current_supply + delta
        } else {
            // Check for underflow on burn
            if current_supply < delta {
                return Err("Burn amount exceeds total supply".to_string());
            }
            current_supply - delta
        };

        // Store the pending supply for this transaction
        self.pending_supply.insert(transaction_id, new_supply);

        Ok(new_supply)
    }

    /// Commit a transaction's changes
    pub fn commit_transaction(&mut self, transaction_id: HlcTimestamp) -> Result<(), String> {
        let tx_changes = self
            .pending_changes
            .remove(&transaction_id)
            .ok_or_else(|| format!("Transaction {} not found", transaction_id))?;

        // Apply all pending changes to the main storage
        for (account, new_balance) in tx_changes {
            let version = BalanceVersion {
                amount: new_balance,
                timestamp: transaction_id,
            };

            self.balances.entry(account).or_default().push(version);
        }

        // Commit supply change if any
        if let Some(new_supply) = self.pending_supply.remove(&transaction_id) {
            self.total_supply = new_supply;
        }

        Ok(())
    }

    /// Abort a transaction and discard its changes
    pub fn abort_transaction(&mut self, transaction_id: HlcTimestamp) {
        self.pending_changes.remove(&transaction_id);
        self.pending_supply.remove(&transaction_id);
    }

    /// Get a compacted view of the storage for snapshots
    /// Returns only the latest committed state for each account
    pub fn get_compacted_data(&self) -> CompactedResourceData {
        let mut latest_balances = BTreeMap::new();

        // For each account, get only the latest balance version
        for (account, versions) in &self.balances {
            if let Some(latest) = versions.last() {
                latest_balances.insert(account.clone(), latest.amount);
            }
        }

        CompactedResourceData {
            metadata: self.metadata.clone(),
            total_supply: self.total_supply,
            balances: latest_balances,
        }
    }

    /// Restore from compacted data
    /// Should only be called on a fresh storage instance
    pub fn restore_from_compacted(&mut self, data: CompactedResourceData) {
        use proven_hlc::NodeId;

        // Clear any existing data
        self.metadata = ResourceMetadata::default();
        self.total_supply = Amount::zero();
        self.balances.clear();
        self.pending_changes.clear();
        self.pending_supply.clear();

        // Restore metadata and supply
        self.metadata = data.metadata;
        self.total_supply = data.total_supply;

        // Create a special "restore" timestamp
        let restore_timestamp = HlcTimestamp::new(0, 0, NodeId::new(0));

        // Restore all balances with the restore timestamp
        for (account, amount) in data.balances {
            let version = BalanceVersion {
                amount,
                timestamp: restore_timestamp,
            };
            self.balances.insert(account, vec![version]);
        }
    }
}

/// Compacted resource data for snapshots
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CompactedResourceData {
    pub metadata: ResourceMetadata,
    pub total_supply: Amount,
    pub balances: BTreeMap<String, Amount>,
}

impl Default for ResourceStorage {
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
    fn test_basic_storage() {
        let mut storage = ResourceStorage::new();

        // Initialize resource
        storage
            .initialize("Test Token".to_string(), "TEST".to_string(), 8)
            .unwrap();
        assert_eq!(storage.get_metadata().name, "Test Token");
        assert_eq!(storage.get_metadata().decimals, 8);

        // Test balance operations
        let tx1 = make_timestamp(100);
        storage.begin_transaction(tx1);

        storage
            .update_pending_balance(tx1, "alice", Amount::from_integer(100, 0))
            .unwrap();
        assert_eq!(
            storage.get_pending_balance("alice", tx1),
            Amount::from_integer(100, 0)
        );

        storage.commit_transaction(tx1).unwrap();
        assert_eq!(
            storage.get_balance("alice", make_timestamp(200)),
            Amount::from_integer(100, 0)
        );
    }
}
