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
    created_by: HlcTimestamp, // Transaction that created this version
}

/// Versioned metadata entry
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct MetadataVersion {
    metadata: ResourceMetadata,
    timestamp: HlcTimestamp,
    created_by: HlcTimestamp,
}

/// Versioned supply entry
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct SupplyVersion {
    amount: Amount,
    timestamp: HlcTimestamp,
    created_by: HlcTimestamp,
}

/// MVCC storage for resource data
pub struct ResourceStorage {
    // === Committed versions (for snapshot reads) ===
    /// Resource metadata versions
    metadata_versions: Vec<MetadataVersion>,

    /// Total supply versions
    supply_versions: Vec<SupplyVersion>,

    /// Account balances with MVCC
    /// BTreeMap<account, Vec<BalanceVersion>> where versions are sorted by timestamp
    balance_versions: BTreeMap<String, Vec<BalanceVersion>>,

    // === Pending changes (for transaction isolation) ===
    /// Pending metadata changes per transaction
    pending_metadata: BTreeMap<HlcTimestamp, ResourceMetadata>,

    /// Pending supply changes per transaction
    pending_supply: BTreeMap<HlcTimestamp, Amount>,

    /// Pending balance changes per transaction
    /// Map<transaction_id, Map<account, pending_balance>>
    pending_balances: BTreeMap<HlcTimestamp, BTreeMap<String, Amount>>,

    /// Transaction start times for visibility checks
    transaction_start_times: BTreeMap<HlcTimestamp, HlcTimestamp>,
}

impl ResourceStorage {
    /// Create a new resource storage
    pub fn new() -> Self {
        use proven_hlc::NodeId;

        // Initialize with default versions at timestamp 0
        let init_timestamp = HlcTimestamp::new(0, 0, NodeId::new(0));

        Self {
            metadata_versions: vec![MetadataVersion {
                metadata: ResourceMetadata::default(),
                timestamp: init_timestamp,
                created_by: init_timestamp,
            }],
            supply_versions: vec![SupplyVersion {
                amount: Amount::zero(),
                timestamp: init_timestamp,
                created_by: init_timestamp,
            }],
            balance_versions: BTreeMap::new(),
            pending_metadata: BTreeMap::new(),
            pending_supply: BTreeMap::new(),
            pending_balances: BTreeMap::new(),
            transaction_start_times: BTreeMap::new(),
        }
    }

    /// Initialize the resource metadata
    pub fn initialize(
        &mut self,
        txn_id: HlcTimestamp,
        name: String,
        symbol: String,
        decimals: u32,
    ) -> Result<(), String> {
        // Check current metadata (considering pending changes)
        let current = self.get_pending_metadata(txn_id);
        if current.initialized {
            return Err("Resource already initialized".to_string());
        }

        let metadata = ResourceMetadata {
            name,
            symbol,
            decimals,
            initialized: true,
        };

        self.pending_metadata.insert(txn_id, metadata);
        Ok(())
    }

    /// Update resource metadata
    pub fn update_metadata(
        &mut self,
        txn_id: HlcTimestamp,
        name: Option<String>,
        symbol: Option<String>,
    ) -> Result<(), String> {
        let mut metadata = self.get_pending_metadata(txn_id);

        if !metadata.initialized {
            return Err("Resource not initialized".to_string());
        }

        if let Some(name) = name {
            metadata.name = name;
        }
        if let Some(symbol) = symbol {
            metadata.symbol = symbol;
        }

        self.pending_metadata.insert(txn_id, metadata);
        Ok(())
    }

    /// Get resource metadata for a transaction (considers pending changes)
    pub fn get_pending_metadata(&self, txn_id: HlcTimestamp) -> ResourceMetadata {
        // First check pending changes
        if let Some(pending) = self.pending_metadata.get(&txn_id) {
            return pending.clone();
        }

        // Otherwise return latest committed
        self.metadata_versions
            .last()
            .map(|v| v.metadata.clone())
            .unwrap_or_default()
    }

    /// Get resource metadata at a specific timestamp (snapshot read)
    /// Returns None if there are earlier pending writes that haven't committed
    pub fn get_metadata_at_timestamp(
        &self,
        read_timestamp: HlcTimestamp,
    ) -> Option<ResourceMetadata> {
        // Check for pending writes from earlier transactions
        for &tx_id in self.pending_metadata.keys() {
            if tx_id < read_timestamp {
                // There's a pending metadata change from an earlier transaction
                // Must wait for it to commit/abort before reading
                return None;
            }
        }

        // Find the latest version visible at read_timestamp
        for version in self.metadata_versions.iter().rev() {
            if version.timestamp <= read_timestamp {
                return Some(version.metadata.clone());
            }
        }

        Some(ResourceMetadata::default())
    }

    /// Get total supply for a transaction (considers pending changes)
    pub fn get_pending_supply(&self, txn_id: HlcTimestamp) -> Amount {
        // First check pending changes
        if let Some(&pending) = self.pending_supply.get(&txn_id) {
            return pending;
        }

        // Otherwise return latest committed
        self.supply_versions
            .last()
            .map(|v| v.amount)
            .unwrap_or_else(Amount::zero)
    }

    /// Get total supply at a specific timestamp (snapshot read)
    /// Returns None if there are earlier pending writes that haven't committed
    pub fn get_supply_at_timestamp(&self, read_timestamp: HlcTimestamp) -> Option<Amount> {
        // Check for pending writes from earlier transactions
        for &tx_id in self.pending_supply.keys() {
            if tx_id < read_timestamp {
                // There's a pending supply change from an earlier transaction
                // Must wait for it to commit/abort before reading
                return None;
            }
        }

        // Find the latest version visible at read_timestamp
        for version in self.supply_versions.iter().rev() {
            if version.timestamp <= read_timestamp {
                return Some(version.amount);
            }
        }

        Some(Amount::zero())
    }

    /// Get balance at a specific timestamp (snapshot read)
    /// Returns None if there are earlier pending writes that haven't committed
    pub fn get_balance_at_timestamp(
        &self,
        account: &str,
        read_timestamp: HlcTimestamp,
    ) -> Option<Amount> {
        // Check for pending writes from earlier transactions
        for (&tx_id, balances) in &self.pending_balances {
            if tx_id < read_timestamp && balances.contains_key(account) {
                // There's a pending balance change from an earlier transaction
                // Must wait for it to commit/abort before reading
                return None;
            }
        }

        // Find the latest committed version visible at read_timestamp
        if let Some(versions) = self.balance_versions.get(account) {
            for version in versions.iter().rev() {
                if version.timestamp <= read_timestamp {
                    return Some(version.amount);
                }
            }
        }

        Some(Amount::zero())
    }

    /// Get pending balance for an account in a transaction
    pub fn get_pending_balance(&self, account: &str, transaction_id: HlcTimestamp) -> Amount {
        // First check pending changes for this transaction
        if let Some(tx_balances) = self.pending_balances.get(&transaction_id)
            && let Some(&amount) = tx_balances.get(account)
        {
            return amount;
        }

        // Get transaction start time or use transaction_id as fallback
        let tx_start = self
            .transaction_start_times
            .get(&transaction_id)
            .copied()
            .unwrap_or(transaction_id);

        // Return committed balance visible at transaction start
        if let Some(versions) = self.balance_versions.get(account) {
            for version in versions.iter().rev() {
                if version.timestamp <= tx_start {
                    return version.amount;
                }
            }
        }

        Amount::zero()
    }

    /// Begin tracking changes for a transaction
    pub fn begin_transaction(&mut self, transaction_id: HlcTimestamp) {
        self.transaction_start_times
            .insert(transaction_id, transaction_id);
        self.pending_balances
            .insert(transaction_id, BTreeMap::new());
    }

    /// Update pending balance for an account in a transaction
    pub fn update_pending_balance(
        &mut self,
        transaction_id: HlcTimestamp,
        account: &str,
        new_balance: Amount,
    ) -> Result<(), String> {
        let tx_balances = self
            .pending_balances
            .get_mut(&transaction_id)
            .ok_or_else(|| format!("Transaction {} not found", transaction_id))?;

        tx_balances.insert(account.to_string(), new_balance);
        Ok(())
    }

    /// Update pending total supply for mints/burns
    pub fn update_pending_supply(
        &mut self,
        transaction_id: HlcTimestamp,
        delta: Amount,
        is_mint: bool,
    ) -> Result<Amount, String> {
        // Get current supply (pending or committed)
        let current_supply = self.get_pending_supply(transaction_id);

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
        // Commit metadata changes
        if let Some(metadata) = self.pending_metadata.remove(&transaction_id) {
            self.metadata_versions.push(MetadataVersion {
                metadata,
                timestamp: transaction_id,
                created_by: transaction_id,
            });
        }

        // Commit supply changes
        if let Some(supply) = self.pending_supply.remove(&transaction_id) {
            self.supply_versions.push(SupplyVersion {
                amount: supply,
                timestamp: transaction_id,
                created_by: transaction_id,
            });
        }

        // Commit balance changes
        if let Some(tx_balances) = self.pending_balances.remove(&transaction_id) {
            for (account, new_balance) in tx_balances {
                let version = BalanceVersion {
                    amount: new_balance,
                    timestamp: transaction_id,
                    created_by: transaction_id,
                };

                self.balance_versions
                    .entry(account)
                    .or_default()
                    .push(version);
            }
        }

        // Clean up transaction metadata
        self.transaction_start_times.remove(&transaction_id);

        Ok(())
    }

    /// Abort a transaction and discard its changes
    pub fn abort_transaction(&mut self, transaction_id: HlcTimestamp) {
        // Just remove all pending data - super cheap!
        self.pending_metadata.remove(&transaction_id);
        self.pending_supply.remove(&transaction_id);
        self.pending_balances.remove(&transaction_id);
        self.transaction_start_times.remove(&transaction_id);
    }

    /// Get a compacted view of the storage for snapshots
    /// Returns only the latest committed state for each account
    pub fn get_compacted_data(&self) -> CompactedResourceData {
        let mut latest_balances = BTreeMap::new();

        // For each account, get only the latest balance version
        for (account, versions) in &self.balance_versions {
            if let Some(latest) = versions.last() {
                latest_balances.insert(account.clone(), latest.amount);
            }
        }

        CompactedResourceData {
            metadata: self
                .metadata_versions
                .last()
                .map(|v| v.metadata.clone())
                .unwrap_or_default(),
            total_supply: self
                .supply_versions
                .last()
                .map(|v| v.amount)
                .unwrap_or_else(Amount::zero),
            balances: latest_balances,
        }
    }

    /// Check if there are pending balance writes from a transaction
    pub fn has_pending_balance_write(&self, tx_id: &HlcTimestamp, account: &str) -> bool {
        self.pending_balances
            .get(tx_id)
            .is_some_and(|balances| balances.contains_key(account))
    }

    /// Get all transactions with pending balance writes for an account
    pub fn get_pending_balance_writers(&self, account: &str) -> Vec<HlcTimestamp> {
        self.pending_balances
            .iter()
            .filter_map(|(tx_id, balances)| {
                if balances.contains_key(account) {
                    Some(*tx_id)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Check if there are pending metadata writes
    pub fn has_pending_metadata_write(&self) -> Vec<HlcTimestamp> {
        self.pending_metadata.keys().copied().collect()
    }

    /// Check if there are pending supply writes
    pub fn has_pending_supply_write(&self) -> Vec<HlcTimestamp> {
        self.pending_supply.keys().copied().collect()
    }

    /// Restore from compacted data
    /// Should only be called on a fresh storage instance
    pub fn restore_from_compacted(&mut self, data: CompactedResourceData) {
        use proven_hlc::NodeId;

        // Clear any existing data
        self.metadata_versions.clear();
        self.supply_versions.clear();
        self.balance_versions.clear();
        self.pending_metadata.clear();
        self.pending_supply.clear();
        self.pending_balances.clear();
        self.transaction_start_times.clear();

        // Create a special "restore" timestamp
        let restore_timestamp = HlcTimestamp::new(0, 0, NodeId::new(0));

        // Restore metadata
        self.metadata_versions.push(MetadataVersion {
            metadata: data.metadata,
            timestamp: restore_timestamp,
            created_by: restore_timestamp,
        });

        // Restore supply
        self.supply_versions.push(SupplyVersion {
            amount: data.total_supply,
            timestamp: restore_timestamp,
            created_by: restore_timestamp,
        });

        // Restore all balances
        for (account, amount) in data.balances {
            let version = BalanceVersion {
                amount,
                timestamp: restore_timestamp,
                created_by: restore_timestamp,
            };
            self.balance_versions.insert(account, vec![version]);
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
        let tx1 = make_timestamp(100);
        storage.begin_transaction(tx1);

        storage
            .initialize(tx1, "Test Token".to_string(), "TEST".to_string(), 8)
            .unwrap();

        let metadata = storage.get_pending_metadata(tx1);
        assert_eq!(metadata.name, "Test Token");
        assert_eq!(metadata.decimals, 8);

        // Test balance operations
        storage
            .update_pending_balance(tx1, "alice", Amount::from_integer(100, 0))
            .unwrap();
        assert_eq!(
            storage.get_pending_balance("alice", tx1),
            Amount::from_integer(100, 0)
        );

        storage.commit_transaction(tx1).unwrap();

        // Read committed data at a later timestamp
        assert_eq!(
            storage.get_balance_at_timestamp("alice", make_timestamp(200)),
            Some(Amount::from_integer(100, 0))
        );
    }
}
