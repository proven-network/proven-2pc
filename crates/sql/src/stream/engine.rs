//! SQL Transaction Engine implementation for the generic stream processor
//!
//! This module implements the TransactionEngine trait, providing SQL-specific
//! operation execution while delegating message handling to the generic processor.

use proven_hlc::HlcTimestamp;
use proven_stream::{OperationResult, TransactionEngine};

use crate::execution::Executor;
use crate::planning::planner::Planner;
use crate::storage::lock::LockManager;
use crate::storage::mvcc::MvccStorage;

use super::operation::SqlOperation;
use super::response::{SqlResponse, convert_execution_result};
use super::stats_cache::StatisticsCache;
use super::transaction::{TransactionContext, TransactionState};

use std::collections::HashMap;

/// SQL-specific transaction engine
pub struct SqlTransactionEngine {
    /// MVCC storage for versioned data
    pub storage: MvccStorage,

    /// Lock manager for pessimistic concurrency control
    pub lock_manager: LockManager,

    /// Active transaction contexts
    active_transactions: HashMap<HlcTimestamp, TransactionContext>,

    /// SQL executor (stateless)
    executor: Executor,

    /// Statistics cache for query optimization
    stats_cache: StatisticsCache,

    /// Current migration version
    migration_version: u32,
}

impl SqlTransactionEngine {
    /// Create a new SQL transaction engine
    pub fn new() -> Self {
        Self {
            storage: MvccStorage::new(),
            lock_manager: LockManager::new(),
            active_transactions: HashMap::new(),
            executor: Executor::new(),
            stats_cache: StatisticsCache::new(100), // Update stats every 100 commits
            migration_version: 0,
        }
    }

    /// Execute SQL and handle lock conflicts
    fn execute_sql(&mut self, sql: &str, txn_id: HlcTimestamp) -> OperationResult<SqlResponse> {
        // Transaction wound checking is now handled by stream processor

        // Parse SQL
        let statement = match crate::parsing::parse_sql(sql) {
            Ok(stmt) => stmt,
            Err(e) => return OperationResult::Error(format!("Parse error: {:?}", e)),
        };

        // Get schemas for planning
        let schemas = self.storage.get_schemas();

        // Create a stateless planner for this query
        let mut planner = Planner::new(schemas);

        // Add cached statistics if available for optimization
        if let Some(stats) = self.stats_cache.get() {
            planner.update_statistics(stats.clone());
        }

        // Plan the query
        let plan = match planner.plan(statement) {
            Ok(p) => p,
            Err(e) => return OperationResult::Error(format!("Planning error: {:?}", e)),
        };

        // Get transaction context
        let tx_ctx = match self.active_transactions.get_mut(&txn_id) {
            Some(ctx) => ctx,
            None => return OperationResult::Error(format!("Transaction {:?} not found", txn_id)),
        };

        // Execute with direct references to storage and lock manager
        // This is where lock conflicts will be discovered during execution
        match self.executor.execute(
            plan.clone(),
            &mut self.storage,
            &mut self.lock_manager,
            tx_ctx,
        ) {
            Ok(result) => {
                // After successful DDL operations, invalidate statistics cache
                if plan.is_ddl() {
                    self.stats_cache.invalidate();
                }
                OperationResult::Success(convert_execution_result(result))
            }
            Err(crate::error::Error::LockConflict { holder, mode: _ }) => {
                // Just report the conflict - stream processor handles wound-wait
                OperationResult::WouldBlock {
                    blocking_txn: holder,
                }
            }
            Err(e) => OperationResult::Error(format!("Execution error: {:?}", e)),
        }
    }
}

impl TransactionEngine for SqlTransactionEngine {
    type Operation = SqlOperation;
    type Response = SqlResponse;

    fn apply_operation(
        &mut self,
        operation: Self::Operation,
        txn_id: HlcTimestamp,
    ) -> OperationResult<Self::Response> {
        match operation {
            SqlOperation::Execute { sql } => self.execute_sql(&sql, txn_id),
            SqlOperation::Query { sql } => self.execute_sql(&sql, txn_id),
            SqlOperation::Migrate { version, sql } => {
                // Check if migration is needed
                if version <= self.migration_version {
                    return OperationResult::Success(SqlResponse::ExecuteResult {
                        result_type: "migration_skip".to_string(),
                        rows_affected: None,
                        message: Some(format!("Migration {} already applied", version)),
                    });
                }

                // Execute the migration
                match self.execute_sql(&sql, txn_id) {
                    OperationResult::Success(response) => {
                        // Update migration version on success
                        self.migration_version = version;
                        OperationResult::Success(response)
                    }
                    other => other,
                }
            }
        }
    }

    fn prepare(&mut self, txn_id: HlcTimestamp) -> std::result::Result<(), String> {
        // Get transaction context
        let tx_ctx = self
            .active_transactions
            .get_mut(&txn_id)
            .ok_or_else(|| format!("Transaction {} not found", txn_id))?;

        // Try to prepare the transaction
        if tx_ctx.prepare() {
            Ok(())
        } else {
            Err("Transaction cannot be prepared".to_string())
        }
    }

    fn commit(&mut self, txn_id: HlcTimestamp) -> std::result::Result<(), String> {
        // Remove transaction context
        let tx_ctx = self
            .active_transactions
            .remove(&txn_id)
            .ok_or_else(|| format!("Transaction {} not found", txn_id))?;

        // For auto-commit, we don't require prepare
        if tx_ctx.state == TransactionState::Active || tx_ctx.state == TransactionState::Preparing {
            // Commit to storage
            self.storage
                .commit_transaction(txn_id)
                .map_err(|e| format!("Storage commit error: {:?}", e))?;

            // Record the commit for statistics tracking
            self.stats_cache.record_commit();

            // Update statistics if needed
            let active_count = self.active_transactions.len();
            self.stats_cache.maybe_update(&self.storage, active_count);

            // Release all locks held by this transaction
            self.lock_manager
                .release_all(txn_id)
                .map_err(|e| format!("Lock release error: {:?}", e))?;

            Ok(())
        } else {
            Err("Transaction is not in a committable state".to_string())
        }
    }

    fn abort(&mut self, txn_id: HlcTimestamp) -> std::result::Result<(), String> {
        // Remove transaction context
        self.active_transactions.remove(&txn_id);

        // Abort in storage
        self.storage
            .abort_transaction(txn_id)
            .map_err(|e| format!("Storage abort error: {:?}", e))?;

        // Release all locks
        self.lock_manager
            .release_all(txn_id)
            .map_err(|e| format!("Lock release error: {:?}", e))?;

        Ok(())
    }

    fn begin_transaction(&mut self, txn_id: HlcTimestamp) {
        // Create new transaction context
        let tx_ctx = TransactionContext::new(txn_id);

        // Store context
        self.active_transactions.insert(txn_id, tx_ctx);
    }

    fn is_transaction_active(&self, txn_id: &HlcTimestamp) -> bool {
        self.active_transactions.contains_key(txn_id)
    }

    fn engine_name(&self) -> &str {
        "sql"
    }
}

impl Default for SqlTransactionEngine {
    fn default() -> Self {
        Self::new()
    }
}
