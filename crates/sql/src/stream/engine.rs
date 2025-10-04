//! SQL Engine with predicate-based conflict detection
//!
//! This engine uses predicates for conflict detection at planning time,
//! eliminating the need for row-level locks.

use proven_hlc::HlcTimestamp;
use proven_stream::engine::BlockingInfo;
use proven_stream::{OperationResult, RetryOn, TransactionEngine};

use crate::execution;
use crate::parsing::CachingParser;
use crate::planning::caching_planner::CachingPlanner;
use crate::semantic::CachingSemanticAnalyzer;
use crate::storage::{Storage, StorageConfig};
use crate::stream::{
    operation::SqlOperation,
    predicate_index::PredicateIndex,
    response::{SqlResponse, convert_execution_result},
    transaction::TransactionContext,
};

use std::collections::HashMap;

/// SQL transaction engine with predicate-based conflict detection
pub struct SqlTransactionEngine {
    /// Storage engine with persistence
    storage: Storage,

    /// Active transactions with their predicates
    active_transactions: HashMap<HlcTimestamp, TransactionContext>,

    /// Fast predicate index for conflict detection
    predicate_index: PredicateIndex,

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
    pub fn new(config: StorageConfig) -> Self {
        // Create storage with default config
        let storage = Storage::new(config).expect("Failed to create storage");
        let schemas = storage.get_schemas();

        // Get index metadata directly from new storage
        let indexes = storage.get_index_metadata();

        let parser = CachingParser::new();
        let analyzer = CachingSemanticAnalyzer::new(schemas.clone());
        let planner = CachingPlanner::new(schemas, indexes);

        let mut engine = Self {
            storage,
            active_transactions: HashMap::new(),
            predicate_index: PredicateIndex::new(),
            migration_version: 0,
            parser,
            analyzer,
            planner,
        };

        // Recover active transactions from storage (crash recovery)
        engine.recover_from_storage();

        engine
    }

    /// Recover active transactions and predicates from storage (crash recovery)
    fn recover_from_storage(&mut self) {
        // Use current wall clock time for recovery
        // We create a timestamp with node_id 0 since this is just for scanning
        let current_physical = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        let current_time = proven_hlc::HlcTimestamp::from_physical_time(
            current_physical,
            proven_hlc::NodeId::new(0),
        );

        // Get all active transactions from predicate storage
        match self
            .storage
            .get_active_transactions_from_predicates(current_time)
        {
            Ok(transactions) => {
                for (txn_id, predicates) in transactions {
                    // Create transaction context
                    let mut tx_ctx = TransactionContext::new(txn_id);

                    // Rebuild predicates for this transaction
                    let mut query_predicates = crate::semantic::predicate::QueryPredicates::new();
                    for predicate in predicates {
                        // We don't know if these were reads or writes from storage
                        // To be safe, treat them all as writes (more restrictive)
                        query_predicates.writes.push(predicate);
                    }

                    tx_ctx.predicates = query_predicates.clone();

                    // Add to active transactions
                    self.active_transactions.insert(txn_id, tx_ctx);

                    // Add to predicate index
                    self.predicate_index
                        .add_transaction(txn_id, &query_predicates);
                }
            }
            Err(e) => {
                eprintln!(
                    "Warning: Failed to recover predicates from storage: {:?}",
                    e
                );
                // Continue without recovery - predicates will be rebuilt as operations execute
            }
        }
    }

    /// Execute SQL snapshot read (read-only, no mutations)
    fn execute_sql_snapshot(
        &mut self,
        sql: &str,
        params: Option<Vec<crate::types::value::Value>>,
        read_timestamp: HlcTimestamp,
    ) -> OperationResult<SqlResponse> {
        // Parse SQL with caching
        let statement = match self.parser.parse(sql) {
            Ok(stmt) => stmt,
            Err(e) => {
                return OperationResult::Complete(SqlResponse::Error(format!(
                    "Parse error: {:?}",
                    e
                )));
            }
        };

        // Get parameter types if provided
        let param_types = params
            .as_ref()
            .map(|p| p.iter().map(|v| v.data_type()).collect())
            .unwrap_or_default();

        // Semantic analysis
        let analyzed = match self.analyzer.analyze(statement, param_types) {
            Ok(a) => a,
            Err(e) => {
                return OperationResult::Complete(SqlResponse::Error(format!(
                    "Semantic error: {:?}",
                    e
                )));
            }
        };

        // Check parameter count
        if let Some(ref param_values) = params {
            let expected_count = analyzed.parameter_count();
            if param_values.len() != expected_count {
                return OperationResult::Complete(SqlResponse::Error(format!(
                    "Expected {} parameters, got {}",
                    expected_count,
                    param_values.len()
                )));
            }
        }

        // Extract predicates
        let query_predicates = if let Some(ref param_values) = params {
            analyzed.extract_predicates(param_values)
        } else {
            analyzed.extract_predicates(&[])
        };

        // Check for conflicts with earlier transactions only
        let mut blockers = Vec::new();
        let potential_conflicts = self
            .predicate_index
            .find_potential_conflicts(read_timestamp, &query_predicates);

        for candidate_tx_id in potential_conflicts {
            // Only consider EARLIER transactions as blockers
            if candidate_tx_id >= read_timestamp {
                continue;
            }

            if let Some(other_tx) = self.active_transactions.get(&candidate_tx_id)
                && query_predicates
                    .conflicts_with(&other_tx.predicates)
                    .is_some()
            {
                blockers.push(BlockingInfo {
                    txn: candidate_tx_id,
                    retry_on: RetryOn::CommitOrAbort,
                });
            }
        }

        if !blockers.is_empty() {
            blockers.sort_by_key(|b| b.txn);
            return OperationResult::WouldBlock { blockers };
        }

        // Plan the statement
        let plan = match self.planner.plan(analyzed.clone()) {
            Ok(p) => p,
            Err(e) => {
                return OperationResult::Complete(SqlResponse::Error(format!(
                    "Planning error: {:?}",
                    e
                )));
            }
        };

        // Execute with a temporary context (snapshot reads are stateless)
        let mut temp_ctx = TransactionContext::new(read_timestamp);

        let result = execution::execute_with_params(
            (*plan).clone(),
            &mut self.storage,
            &mut temp_ctx,
            params.as_ref(),
        );

        // Clean up the temporary transaction (use read_timestamp as log_index since this is a snapshot read)
        let _ = self.storage.abort_transaction(read_timestamp, 0);

        match result {
            Ok(result) => OperationResult::Complete(convert_execution_result(result)),
            Err(e) => {
                OperationResult::Complete(SqlResponse::Error(format!("Execution error: {:?}", e)))
            }
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

        // Get parameter types if provided
        let param_types = params
            .as_ref()
            .map(|p| p.iter().map(|v| v.data_type()).collect())
            .unwrap_or_default();

        // Step 2: Semantic analysis with caching and parameter types
        let analyzed = match self.analyzer.analyze(statement, param_types) {
            Ok(a) => a,
            Err(e) => {
                return OperationResult::Complete(SqlResponse::Error(format!(
                    "Semantic error: {:?}",
                    e
                )));
            }
        };

        // Check parameter count if parameters were provided
        if let Some(ref param_values) = params {
            let expected_count = analyzed.parameter_count();
            if param_values.len() != expected_count {
                return OperationResult::Complete(SqlResponse::Error(format!(
                    "Expected {} parameters, got {}",
                    expected_count,
                    param_values.len()
                )));
            }
        }

        // Step 3: Extract predicates from analyzed statement BEFORE planning
        // This avoids generating a physical plan just to check for conflicts
        let query_predicates = if let Some(ref param_values) = params {
            analyzed.extract_predicates(param_values)
        } else {
            // No parameters - use empty params for extraction
            analyzed.extract_predicates(&[])
        };

        // Step 4: Check for conflicts using the predicate index
        let mut blockers = Vec::new();

        // Use index to find potential conflicts - O(1) or O(log n) instead of O(n)
        let potential_conflicts = self
            .predicate_index
            .find_potential_conflicts(txn_id, &query_predicates);

        // Only check the candidates identified by the index
        for candidate_tx_id in potential_conflicts {
            if let Some(other_tx) = self.active_transactions.get(&candidate_tx_id) {
                // Check if our predicates actually conflict with theirs
                if let Some(conflict) = query_predicates.conflicts_with(&other_tx.predicates) {
                    // Determine when we can retry based on the conflict type
                    use crate::semantic::predicate::ConflictInfo;
                    let retry_on = match conflict {
                        // If they're reading and we want to write, we can retry after they prepare
                        ConflictInfo::WriteRead => RetryOn::Prepare,
                        // For all other conflicts, we must wait until commit/abort
                        ConflictInfo::ReadWrite
                        | ConflictInfo::WriteWrite
                        | ConflictInfo::InsertInsert => RetryOn::CommitOrAbort,
                    };

                    blockers.push(BlockingInfo {
                        txn: candidate_tx_id,
                        retry_on,
                    });
                }
            }
        }

        if !blockers.is_empty() {
            // Sort by transaction ID (which embeds timestamp - oldest first)
            blockers.sort_by_key(|b| b.txn);

            return OperationResult::WouldBlock { blockers };
        }

        // Step 5: Add predicates to transaction context and update index
        let tx_ctx = self.active_transactions.get_mut(&txn_id).unwrap();
        tx_ctx.add_predicates(query_predicates.clone());

        // Update the predicate index with the new predicates
        self.predicate_index
            .update_transaction(txn_id, &query_predicates);

        // Step 6: NOW plan the statement (only after conflict checking passes)
        let plan = match self.planner.plan(analyzed.clone()) {
            Ok(p) => p,
            Err(e) => {
                return OperationResult::Complete(SqlResponse::Error(format!(
                    "Planning error: {:?}",
                    e
                )));
            }
        };

        // Step 7: Execute with parameters
        match execution::execute_with_params(
            (*plan).clone(),
            &mut self.storage,
            tx_ctx,
            params.as_ref(),
        ) {
            Ok(result) => {
                // Step 8: Persist predicates to storage (crash-safe)
                // For write operations (INSERT/UPDATE/DELETE), predicates are already persisted
                // in the same batch as the data. For read operations (SELECT), we need to
                // persist them separately.
                if !plan.is_write() {
                    let tx_ctx = self.active_transactions.get_mut(&txn_id).unwrap();

                    // Persist read predicates (handled internally by storage)
                    if let Err(e) = self.storage.persist_read_predicates(tx_ctx) {
                        return OperationResult::Complete(SqlResponse::Error(format!(
                            "Failed to persist predicates: {:?}",
                            e
                        )));
                    }
                }

                // Update schema cache if DDL operation
                if plan.is_ddl() {
                    // Schema updates are handled internally by storage
                    // Update all caches with new schemas
                    let schemas = self.storage.get_schemas();

                    // Get index metadata directly from new storage
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

    fn read_at_timestamp(
        &mut self,
        operation: Self::Operation,
        read_timestamp: HlcTimestamp,
        _log_index: u64,
    ) -> OperationResult<Self::Response> {
        match operation {
            SqlOperation::Query { sql, params } => {
                // Execute as a snapshot read using the read_timestamp as txn_id
                self.execute_sql_snapshot(&sql, params, read_timestamp)
            }
            _ => panic!("Must be read-only operation"),
        }
    }

    fn apply_operation(
        &mut self,
        operation: Self::Operation,
        txn_id: HlcTimestamp,
        log_index: u64,
    ) -> OperationResult<Self::Response> {
        // Set log_index in transaction context
        if let Some(tx_ctx) = self.active_transactions.get_mut(&txn_id) {
            tx_ctx.set_log_index(log_index);
        }

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

    fn begin(&mut self, txn_id: HlcTimestamp, _log_index: u64) {
        // Create transaction context
        self.active_transactions
            .insert(txn_id, TransactionContext::new(txn_id));
    }

    fn prepare(&mut self, txn_id: HlcTimestamp, _log_index: u64) {
        // Get transaction and release read predicates
        if let Some(tx_ctx) = self.active_transactions.get_mut(&txn_id) {
            // Prepare the transaction (releases read predicates in-memory)
            tx_ctx.prepare();

            // Also remove read predicates from storage
            let _ = self.storage.prepare_transaction(txn_id);
        }
    }

    fn commit(&mut self, txn_id: HlcTimestamp, log_index: u64) {
        // Remove transaction if it exists
        if self.active_transactions.remove(&txn_id).is_some() {
            // Remove from predicate index (in-memory cache)
            self.predicate_index.remove_transaction(&txn_id);

            // Commit in storage with log_index (this also cleans up predicates internally)
            let _ = self.storage.commit_transaction(txn_id, log_index);
        }
        // If transaction doesn't exist, that's fine - may have been committed already
    }

    fn abort(&mut self, txn_id: HlcTimestamp, log_index: u64) {
        // Remove transaction if it exists
        if self.active_transactions.remove(&txn_id).is_some() {
            // Remove from predicate index (in-memory cache)
            self.predicate_index.remove_transaction(&txn_id);

            // Abort in storage with log_index (this also cleans up predicates internally)
            let _ = self.storage.abort_transaction(txn_id, log_index);
        }
        // If transaction doesn't exist, that's fine - may have been aborted already
    }

    fn engine_name(&self) -> &'static str {
        "sql"
    }

    fn get_log_index(&self) -> Option<u64> {
        if self.storage.get_log_index() > 0 {
            Some(self.storage.get_log_index())
        } else {
            None
        }
    }
}

impl Default for SqlTransactionEngine {
    fn default() -> Self {
        Self::new(StorageConfig::default())
    }
}
