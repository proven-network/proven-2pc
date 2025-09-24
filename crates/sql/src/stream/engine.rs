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
use crate::storage::mvcc::MvccStorage;
use crate::stream::{
    operation::SqlOperation,
    predicate_index::PredicateIndex,
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
            predicate_index: PredicateIndex::new(),
            migration_version: 0,
            parser,
            analyzer,
            planner,
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

        match execution::execute_with_params(
            (*plan).clone(),
            &mut self.storage,
            &mut temp_ctx,
            params.as_ref(),
        ) {
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

    fn read_at_timestamp(
        &mut self,
        operation: Self::Operation,
        read_timestamp: HlcTimestamp,
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

    fn prepare(&mut self, txn_id: HlcTimestamp) {
        // Get transaction and release read predicates
        if let Some(tx_ctx) = self.active_transactions.get_mut(&txn_id) {
            // Prepare the transaction (releases read predicates)
            tx_ctx.prepare();
        }
        // If transaction doesn't exist, that's fine - it may have been aborted already
    }

    fn commit(&mut self, txn_id: HlcTimestamp) {
        // Remove transaction if it exists
        if self.active_transactions.remove(&txn_id).is_some() {
            // Remove from predicate index
            self.predicate_index.remove_transaction(&txn_id);

            // Commit in storage (ignore errors - best effort)
            let _ = self.storage.commit_transaction(txn_id);
        }
        // If transaction doesn't exist, that's fine - may have been committed already
    }

    fn abort(&mut self, txn_id: HlcTimestamp) {
        // Remove transaction if it exists
        if self.active_transactions.remove(&txn_id).is_some() {
            // Remove from predicate index
            self.predicate_index.remove_transaction(&txn_id);

            // Abort in storage (ignore errors - best effort)
            let _ = self.storage.abort_transaction(txn_id);
        }
        // If transaction doesn't exist, that's fine - may have been aborted already
    }

    fn begin(&mut self, txn_id: HlcTimestamp) {
        // Create transaction context
        self.active_transactions
            .insert(txn_id, TransactionContext::new(txn_id));
    }

    fn engine_name(&self) -> &'static str {
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
        self.predicate_index = PredicateIndex::new();
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
