//! SQL Engine with predicate-based conflict detection
//!
//! This engine uses predicates for conflict detection at planning time,
//! eliminating the need for row-level locks.

use proven_hlc::HlcTimestamp;
use proven_stream::{OperationResult, RetryOn, TransactionEngine};

use crate::execution::Executor;
use crate::planning::planner::Planner;
use crate::storage::mvcc::MvccStorage;
use crate::stream::{
    operation::SqlOperation,
    response::{SqlResponse, convert_execution_result},
    transaction::TransactionContext,
};

use std::collections::HashMap;

/// SQL transaction engine with predicate-based conflict detection
pub struct SqlTransactionEngine {
    /// MVCC storage for versioned data
    pub storage: MvccStorage,

    /// Active transactions with their predicates
    active_transactions: HashMap<HlcTimestamp, TransactionContext>,

    /// SQL executor (stateless)
    executor: Executor,

    /// Current migration version
    migration_version: u32,
}

impl SqlTransactionEngine {
    /// Create a new SQL transaction engine
    pub fn new() -> Self {
        Self {
            storage: MvccStorage::new(),
            active_transactions: HashMap::new(),
            executor: Executor::new(),
            migration_version: 0,
        }
    }

    /// Execute SQL with predicate-based conflict detection
    fn execute_sql(&mut self, sql: &str, txn_id: HlcTimestamp) -> OperationResult<SqlResponse> {
        // Verify transaction exists
        if !self.active_transactions.contains_key(&txn_id) {
            return OperationResult::Error(format!("Transaction {:?} not found", txn_id));
        }

        // Phase 1: Parse SQL
        let statement = match crate::parsing::parse_sql(sql) {
            Ok(stmt) => stmt,
            Err(e) => return OperationResult::Error(format!("Parse error: {:?}", e)),
        };

        // Phase 2: Plan and extract predicates
        let planner = Planner::new(self.storage.get_schemas());
        let plan = match planner.plan(statement) {
            Ok(p) => p,
            Err(e) => return OperationResult::Error(format!("Planning error: {:?}", e)),
        };

        // Phase 3: Extract predicates from the plan
        let plan_predicates = planner.extract_predicates(&plan);

        for (other_tx_id, other_tx) in &self.active_transactions {
            if other_tx_id == &txn_id {
                continue; // Skip self
            }

            // Check if our predicates conflict with theirs
            if let Some(conflict) = plan_predicates.conflicts_with(&other_tx.predicates) {
                // Determine when we can retry based on the conflict type
                use crate::planning::predicate::ConflictInfo;
                let retry_on = match conflict {
                    // If they're reading and we want to write, we can retry after they prepare
                    ConflictInfo::WriteRead { .. } => RetryOn::Prepare,
                    // For all other conflicts, we must wait until commit/abort
                    ConflictInfo::ReadWrite { .. } 
                    | ConflictInfo::WriteWrite { .. } 
                    | ConflictInfo::InsertInsert { .. } => RetryOn::CommitOrAbort,
                };
                
                return OperationResult::WouldBlock {
                    blocking_txn: *other_tx_id,
                    retry_on,
                };
            }
        }

        // Phase 4: Add predicates to transaction context
        let tx_ctx = self.active_transactions.get_mut(&txn_id).unwrap();
        tx_ctx.add_predicates(plan_predicates);

        // Phase 5: Execute
        match self
            .executor
            .execute(plan.clone(), &mut self.storage, tx_ctx)
        {
            Ok(result) => {
                // Update schema cache if DDL operation
                if plan.is_ddl() {
                    // Schema updates are handled internally by storage
                }
                OperationResult::Success(convert_execution_result(result))
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
            SqlOperation::Query { sql } => self.execute_sql(&sql, txn_id),
            SqlOperation::Execute { sql } => self.execute_sql(&sql, txn_id),
            SqlOperation::Migrate { version, sql } => {
                // Check if migration is needed
                if version <= self.migration_version {
                    return OperationResult::Success(SqlResponse::ExecuteResult {
                        result_type: "migration".to_string(),
                        rows_affected: Some(0),
                        message: Some("Migration already applied".to_string()),
                    });
                }

                // Execute migration
                match self.execute_sql(&sql, txn_id) {
                    OperationResult::Success(response) => {
                        self.migration_version = version;
                        OperationResult::Success(response)
                    }
                    other => other,
                }
            }
        }
    }

    fn prepare(&mut self, txn_id: HlcTimestamp) -> std::result::Result<(), String> {
        // Get transaction and release read predicates
        let tx_ctx = self
            .active_transactions
            .get_mut(&txn_id)
            .ok_or_else(|| format!("Transaction {} not found", txn_id))?;

        // Prepare the transaction (releases read predicates)
        if !tx_ctx.prepare() {
            return Err(format!("Transaction {} is not in active state", txn_id));
        }

        Ok(())
    }

    fn commit(&mut self, txn_id: HlcTimestamp) -> std::result::Result<(), String> {
        // Remove transaction
        self.active_transactions
            .remove(&txn_id)
            .ok_or_else(|| format!("Transaction {} not found", txn_id))?;

        // Commit in storage (MVCC handles this)
        // The storage layer already has the transaction registered

        Ok(())
    }

    fn abort(&mut self, txn_id: HlcTimestamp) -> std::result::Result<(), String> {
        // Remove transaction
        self.active_transactions.remove(&txn_id);

        // Abort in storage (MVCC handles rollback)

        Ok(())
    }

    fn begin_transaction(&mut self, txn_id: HlcTimestamp) {
        // Create transaction context
        self.active_transactions
            .insert(txn_id, TransactionContext::new(txn_id));
    }

    fn is_transaction_active(&self, txn_id: &HlcTimestamp) -> bool {
        self.active_transactions.contains_key(txn_id)
    }

    fn engine_name(&self) -> &str {
        "sql-newer"
    }
}

impl Default for SqlTransactionEngine {
    fn default() -> Self {
        Self::new()
    }
}
