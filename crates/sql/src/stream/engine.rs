//! SQL Engine with predicate-based conflict detection
//!
//! This engine uses predicates for conflict detection at planning time,
//! eliminating the need for row-level locks.

use proven_hlc::HlcTimestamp;
use proven_stream::{OperationResult, RetryOn, TransactionEngine};

use crate::execution;
use crate::parsing::CachingParser;
use crate::planning::caching_planner::CachingPlanner;
use crate::semantic::CachingSemanticAnalyzer;
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

    /// Current migration version
    migration_version: u32,

    /// Caching parser for SQL statements
    parser: CachingParser,

    /// Caching semantic analyzer for validation and type checking
    analyzer: CachingSemanticAnalyzer,

    /// Caching planner for query plans
    planner: CachingPlanner,
}

impl SqlTransactionEngine {
    /// Create a new SQL transaction engine
    pub fn new() -> Self {
        let storage = MvccStorage::new();
        let schemas = storage.get_schemas();
        let indexes = storage.get_index_metadata();

        let parser = CachingParser::new();
        let analyzer = CachingSemanticAnalyzer::new(schemas.clone());
        let planner = CachingPlanner::new(schemas, indexes);

        Self {
            storage,
            active_transactions: HashMap::new(),
            migration_version: 0,
            parser,
            analyzer,
            planner,
        }
    }

    /// Execute SQL with predicate-based conflict detection
    fn execute_sql(
        &mut self,
        sql: &str,
        params: Option<Vec<crate::types::value::Value>>,
        txn_id: HlcTimestamp,
    ) -> OperationResult<SqlResponse> {
        // Verify transaction exists
        if !self.active_transactions.contains_key(&txn_id) {
            return OperationResult::Complete(SqlResponse::Error(format!(
                "Transaction {:?} not found",
                txn_id
            )));
        }

        // Step 1: Parse SQL with caching
        let statement = match self.parser.parse(sql) {
            Ok(stmt) => stmt,
            Err(e) => {
                return OperationResult::Complete(SqlResponse::Error(format!(
                    "Parse error: {:?}",
                    e
                )));
            }
        };

        // Step 2: Semantic analysis with caching
        let analyzed = match self.analyzer.analyze(statement) {
            Ok(a) => a,
            Err(e) => {
                return OperationResult::Complete(SqlResponse::Error(format!(
                    "Semantic error: {:?}",
                    e
                )));
            }
        };

        // Validate and bind parameters if provided (but don't replace in AST)
        let bound_params = if let Some(params) = params {
            // Check parameter count
            let expected_count = analyzed.parameter_count();

            if params.len() != expected_count {
                return OperationResult::Complete(SqlResponse::Error(format!(
                    "Expected {} parameters, got {}",
                    expected_count,
                    params.len()
                )));
            }

            // Bind parameters for validation (but don't replace in AST)
            match crate::semantic::bind_parameters(&*analyzed, params.to_vec()) {
                Ok(bound) => Some(bound),
                Err(e) => {
                    return OperationResult::Complete(SqlResponse::Error(format!(
                        "Parameter binding failed: {}",
                        e
                    )));
                }
            }
        } else {
            None
        };

        // Step 3: Plan the statement with caching
        let plan = match self.planner.plan(analyzed.clone()) {
            Ok(p) => p,
            Err(e) => {
                return OperationResult::Complete(SqlResponse::Error(format!(
                    "Planning error: {:?}",
                    e
                )));
            }
        };

        // Phase 4: Extract predicates from the plan
        let plan_predicates = self.planner.extract_predicates(&*plan);

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

        // Phase 5: Add predicates to transaction context
        let tx_ctx = self.active_transactions.get_mut(&txn_id).unwrap();
        tx_ctx.add_predicates(plan_predicates);

        // Phase 6: Execute with parameters
        match execution::execute_with_params(
            (*plan).clone(),
            &mut self.storage,
            tx_ctx,
            bound_params.as_ref(),
        ) {
            Ok(result) => {
                // Update schema cache if DDL operation
                if plan.is_ddl() {
                    // Schema updates are handled internally by storage
                    // Update all caches with new schemas
                    let schemas = self.storage.get_schemas();
                    let indexes = self.storage.get_index_metadata();

                    // Clear parser cache on schema change
                    self.parser.clear();
                    // Update analyzer with new schemas
                    self.analyzer.update_schemas(schemas.clone());
                    // Update planner with new schemas and indexes
                    self.planner.update_schemas(schemas);
                    self.planner.update_indexes(indexes);
                }
                OperationResult::Complete(convert_execution_result(result))
            }
            Err(e) => {
                OperationResult::Complete(SqlResponse::Error(format!("Execution error: {:?}", e)))
            }
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
            SqlOperation::Query { sql, params } => self.execute_sql(&sql, params, txn_id),
            SqlOperation::Execute { sql, params } => self.execute_sql(&sql, params, txn_id),
            SqlOperation::Migrate { version, sql } => {
                // Check if migration is needed
                if version <= self.migration_version {
                    return OperationResult::Complete(SqlResponse::ExecuteResult {
                        result_type: "migration".to_string(),
                        rows_affected: Some(0),
                        message: Some("Migration already applied".to_string()),
                    });
                }

                // Execute migration (no parameters for migrations)
                match self.execute_sql(&sql, None, txn_id) {
                    OperationResult::Complete(response) => {
                        self.migration_version = version;
                        OperationResult::Complete(response)
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

        // Commit in storage
        self.storage
            .commit_transaction(txn_id)
            .map_err(|e| format!("Failed to commit transaction: {:?}", e))?;

        Ok(())
    }

    fn abort(&mut self, txn_id: HlcTimestamp) -> std::result::Result<(), String> {
        // Remove transaction
        self.active_transactions.remove(&txn_id);

        // Abort in storage
        self.storage
            .abort_transaction(txn_id)
            .map_err(|e| format!("Failed to abort transaction: {:?}", e))?;

        Ok(())
    }

    fn begin_transaction(&mut self, txn_id: HlcTimestamp) {
        // Register transaction with storage
        self.storage.register_transaction(txn_id, txn_id);

        // Create transaction context
        self.active_transactions
            .insert(txn_id, TransactionContext::new(txn_id));
    }

    fn is_transaction_active(&self, txn_id: &HlcTimestamp) -> bool {
        self.active_transactions.contains_key(txn_id)
    }

    fn engine_name(&self) -> &str {
        "sql"
    }

    fn snapshot(&self) -> std::result::Result<Vec<u8>, String> {
        // Only snapshot when no active transactions
        if !self.active_transactions.is_empty() {
            return Err("Cannot snapshot with active transactions".to_string());
        }

        // Get compacted data from storage
        let compacted = self.storage.get_compacted_data();

        // Serialize with CBOR
        let mut buf = Vec::new();
        ciborium::into_writer(&compacted, &mut buf)
            .map_err(|e| format!("Failed to serialize snapshot: {}", e))?;

        // Compress with zstd (level 3 is a good balance)
        let compressed = zstd::encode_all(&buf[..], 3)
            .map_err(|e| format!("Failed to compress snapshot: {}", e))?;

        Ok(compressed)
    }

    fn restore_from_snapshot(&mut self, data: &[u8]) -> std::result::Result<(), String> {
        // Decompress the data
        let decompressed =
            zstd::decode_all(data).map_err(|e| format!("Failed to decompress snapshot: {}", e))?;

        // Deserialize snapshot
        let compacted: crate::storage::mvcc::CompactedSqlData =
            ciborium::from_reader(&decompressed[..])
                .map_err(|e| format!("Failed to deserialize snapshot: {}", e))?;

        // Clear existing state
        self.storage = MvccStorage::new();
        self.active_transactions.clear();
        self.migration_version = 0; // Reset migration version

        // Restore storage from compacted data
        self.storage.restore_from_compacted(compacted);

        // Reinitialize all caches with new storage schemas
        let schemas = self.storage.get_schemas();
        let indexes = self.storage.get_index_metadata();

        self.parser.clear();
        self.analyzer = CachingSemanticAnalyzer::new(schemas.clone());
        self.planner = CachingPlanner::new(schemas, indexes);

        Ok(())
    }
}

impl Default for SqlTransactionEngine {
    fn default() -> Self {
        Self::new()
    }
}
