//! SQL Engine with predicate-based conflict detection
//!
//! This engine uses predicates for conflict detection at planning time,
//! eliminating the need for row-level locks.

pub mod predicate_index;
// pub mod stats_cache;

use proven_common::TransactionId;
use proven_stream::engine::BlockingInfo;
use proven_stream::{OperationResult, RetryOn, TransactionEngine};

use crate::execution;
use crate::parsing::CachingParser;
use crate::planning::caching_planner::CachingPlanner;
use crate::semantic::CachingSemanticAnalyzer;
use crate::storage::{SqlStorage, SqlStorageConfig};
use crate::types::ValueExt;
use crate::types::context::{ExecutionContext, TransactionContext, TransactionState};
use crate::types::operation::SqlOperation;
use crate::types::response::{SqlResponse, convert_execution_result};
use predicate_index::PredicateIndex;

use std::collections::HashMap;

/// SQL transaction engine with predicate-based conflict detection
pub struct SqlTransactionEngine {
    /// Storage engine with persistence
    storage: SqlStorage,

    /// Active transactions with their predicates
    active_transactions: HashMap<TransactionId, TransactionContext>,

    /// Transaction states (separate from predicates)
    transaction_states: HashMap<TransactionId, TransactionState>,

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
    pub fn new(config: SqlStorageConfig) -> Self {
        // Create storage with default config
        let storage = SqlStorage::new(config).expect("Failed to create storage");
        let schemas = storage.get_schemas();

        // Get index metadata directly from new storage
        let indexes = storage.get_index_metadata();

        // Convert Arc<Table> schemas to Table for analyzer/planner
        let schemas_plain: HashMap<String, crate::types::schema::Table> = schemas
            .iter()
            .map(|(k, v)| (k.clone(), v.as_ref().clone()))
            .collect();

        let parser = CachingParser::new();
        let analyzer = CachingSemanticAnalyzer::new(schemas_plain.clone(), indexes.clone());
        let planner = CachingPlanner::new(schemas_plain, indexes);

        let mut engine = Self {
            storage,
            active_transactions: HashMap::new(),
            transaction_states: HashMap::new(),
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
        // Get all active transactions from predicate storage
        match self.storage.get_active_transactions() {
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
                    self.transaction_states
                        .insert(txn_id, TransactionState::Active);

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

        // Get all active DDL operations from DDL storage
        match self.storage.get_active_ddls() {
            Ok(ddl_map) => {
                for (txn_id, pending_ddls) in ddl_map {
                    // Get or create transaction context
                    let tx_ctx = self.active_transactions.entry(txn_id).or_insert_with(|| {
                        let ctx = TransactionContext::new(txn_id);
                        self.transaction_states
                            .insert(txn_id, TransactionState::Active);
                        ctx
                    });

                    // Restore pending DDLs
                    for ddl in pending_ddls {
                        tx_ctx.add_pending_ddl(ddl);
                    }
                }
            }
            Err(e) => {
                eprintln!(
                    "Warning: Failed to recover pending DDLs from storage: {:?}",
                    e
                );
                // Continue without recovery
            }
        }
    }

    /// Execute SQL snapshot read (read-only, no mutations)
    fn execute_sql_snapshot(
        &mut self,
        sql: &str,
        params: Option<Vec<crate::types::Value>>,
        read_timestamp: TransactionId,
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

        // Check if this is an EXPLAIN statement - if so, return the plan as text
        if matches!(&*analyzed.ast, crate::parsing::ast::Statement::Explain(_)) {
            return OperationResult::Complete(SqlResponse::ExplainPlan {
                plan: plan.to_string(),
            });
        }

        // Execute with a temporary context (snapshot reads are stateless)
        // Use log_index=0 since snapshot reads don't have a position in the ordered stream
        let mut exec_ctx = ExecutionContext::new(read_timestamp, 0);

        // Create batch (even for reads, for consistency - will be empty for SELECT)
        let mut batch = self.storage.batch();

        let result = execution::execute_with_params(
            (*plan).clone(),
            &mut self.storage,
            &mut batch,
            &mut exec_ctx,
            params.as_ref(),
        );

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
        params: Option<Vec<crate::types::Value>>,
        txn_id: TransactionId,
        log_index: u64,
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

        // Check if this is an EXPLAIN statement - if so, return the plan as text
        if matches!(&*analyzed.ast, crate::parsing::ast::Statement::Explain(_)) {
            return OperationResult::Complete(SqlResponse::ExplainPlan {
                plan: plan.to_string(),
            });
        }

        // Create execution context for this operation
        let tx_ctx = self.active_transactions.get(&txn_id).unwrap();
        let mut exec_ctx = tx_ctx.create_execution_context(log_index, query_predicates.clone());

        // Step 7: Record pending DDL BEFORE execution (to capture old schema)
        // Also persist to storage for crash recovery
        if plan.is_ddl()
            && let Some(tx_ctx) = self.active_transactions.get_mut(&txn_id)
        {
            use crate::types::context::PendingDdl;

            let pending_ddl = match plan.as_ref() {
                crate::types::plan::Plan::CreateTable { name, .. } => {
                    Some(PendingDdl::Create { name: name.clone() })
                }
                crate::types::plan::Plan::DropTable { names, .. } => {
                    // Capture schema BEFORE drop
                    names.iter().find_map(|name| {
                        self.storage
                            .get_schemas()
                            .get(name)
                            .map(|old_schema| PendingDdl::Drop {
                                name: name.clone(),
                                old_schema: old_schema.as_ref().clone(),
                            })
                    })
                }
                crate::types::plan::Plan::AlterTable {
                    name, operation, ..
                } => {
                    // Capture schema BEFORE alter
                    self.storage.get_schemas().get(name).map(|old_schema| {
                        // Check if this is a RENAME TABLE (needs special rollback handling)
                        if let crate::parsing::ast::ddl::AlterTableOperation::RenameTable {
                            new_table_name,
                        } = operation
                        {
                            PendingDdl::Rename {
                                old_name: name.clone(),
                                new_name: new_table_name.clone(),
                                old_schema: old_schema.as_ref().clone(),
                            }
                        } else {
                            PendingDdl::Alter {
                                name: name.clone(),
                                old_schema: old_schema.as_ref().clone(),
                            }
                        }
                    })
                }
                crate::types::plan::Plan::CreateIndex { name, .. } => {
                    Some(PendingDdl::CreateIndex { name: name.clone() })
                }
                crate::types::plan::Plan::DropIndex { name, .. } => {
                    // Capture index metadata BEFORE drop
                    self.storage.get_index_metadata().get(name).map(|metadata| {
                        PendingDdl::DropIndex {
                            name: name.clone(),
                            metadata: metadata.clone(),
                        }
                    })
                }
                _ => None,
            };

            if let Some(ddl) = pending_ddl {
                tx_ctx.add_pending_ddl(ddl.clone());
                // Persist to storage immediately for crash recovery
                if let Err(e) = self.storage.persist_pending_ddl(txn_id, &ddl) {
                    eprintln!("Warning: Failed to persist pending DDL: {:?}", e);
                }
            }
        }

        // Step 8: Create batch for atomic commit (data + predicates + log_index)
        let mut batch = self.storage.batch();

        // Step 9: Execute with batch
        match execution::execute_with_params(
            (*plan).clone(),
            &mut self.storage,
            &mut batch,
            &mut exec_ctx,
            params.as_ref(),
        ) {
            Ok(result) => {
                // Step 9: Add predicates to batch (atomic with data for writes, standalone for reads)
                if let Err(e) =
                    self.storage
                        .add_predicates_to_batch(&mut batch, txn_id, &query_predicates)
                {
                    return OperationResult::Complete(SqlResponse::Error(format!(
                        "Failed to add predicates to batch: {:?}",
                        e
                    )));
                }

                // Step 10: Commit batch atomically (data + predicates + log_index)
                if let Err(e) = batch.commit() {
                    return OperationResult::Complete(SqlResponse::Error(format!(
                        "Failed to commit batch: {:?}",
                        e
                    )));
                }

                // Update schema cache if DDL operation
                if plan.is_ddl() {
                    // Schema updates are handled internally by storage
                    // Update all caches with new schemas
                    let schemas_arc = self.storage.get_schemas();

                    // Get index metadata directly from new storage
                    let indexes = self.storage.get_index_metadata();

                    // Convert Arc<Table> to Table for analyzer/planner
                    let schemas: HashMap<String, crate::types::schema::Table> = schemas_arc
                        .iter()
                        .map(|(k, v)| (k.clone(), v.as_ref().clone()))
                        .collect();

                    // Clear parser cache on schema change
                    self.parser.clear();
                    // Update analyzer with new schemas and indexes
                    self.analyzer.update_schemas(schemas.clone());
                    self.analyzer.update_index_metadata(indexes.clone());
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
        read_timestamp: TransactionId,
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
        txn_id: TransactionId,
        log_index: u64,
    ) -> OperationResult<Self::Response> {
        match operation {
            SqlOperation::Query { sql, params } => {
                self.execute_sql(&sql, params, txn_id, log_index)
            }
            SqlOperation::Execute { sql, params } => {
                self.execute_sql(&sql, params, txn_id, log_index)
            }
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
                match self.execute_sql(&sql, None, txn_id, log_index) {
                    OperationResult::Complete(response) => {
                        self.migration_version = version;
                        OperationResult::Complete(response)
                    }
                    other => other,
                }
            }
        }
    }

    fn begin(&mut self, txn_id: TransactionId, _log_index: u64) {
        // Create transaction context
        self.active_transactions
            .insert(txn_id, TransactionContext::new(txn_id));
        self.transaction_states
            .insert(txn_id, TransactionState::Active);
    }

    fn prepare(&mut self, txn_id: TransactionId, _log_index: u64) {
        // Get transaction and release read predicates
        if let Some(tx_ctx) = self.active_transactions.get_mut(&txn_id) {
            // Prepare the transaction (releases read predicates in-memory)
            tx_ctx.prepare();

            // Update state
            self.transaction_states
                .insert(txn_id, TransactionState::Preparing);

            // Also remove read predicates from storage
            let _ = self.storage.prepare_transaction(txn_id);
        }
    }

    fn commit(&mut self, txn_id: TransactionId, log_index: u64) {
        // Remove transaction if it exists
        if self.active_transactions.remove(&txn_id).is_some() {
            self.transaction_states.remove(&txn_id);

            // Remove from predicate index (in-memory cache)
            self.predicate_index.remove_transaction(&txn_id);

            // Commit in storage with log_index (this also cleans up predicates internally)
            let _ = self.storage.commit_transaction(txn_id, log_index);
        }
        // If transaction doesn't exist, that's fine - may have been committed already
    }

    fn abort(&mut self, txn_id: TransactionId, log_index: u64) {
        // Remove transaction if it exists
        if let Some(tx_ctx) = self.active_transactions.remove(&txn_id) {
            self.transaction_states.remove(&txn_id);

            // Remove from predicate index (in-memory cache)
            self.predicate_index.remove_transaction(&txn_id);

            // Create single atomic batch for abort + DDL rollback
            let mut batch = self.storage.batch();

            // Abort in storage (MVCC + predicates + pending DDLs cleanup)
            if let Err(e) = self
                .storage
                .abort_transaction_to_batch(&mut batch, txn_id, log_index)
            {
                eprintln!("Warning: Failed to abort transaction: {:?}", e);
                return;
            }

            // Roll back DDL metadata changes (in same batch for atomicity)
            if !tx_ctx.pending_ddls.is_empty() {
                // Process pending DDLs in reverse order
                for ddl in tx_ctx.pending_ddls.iter().rev() {
                    use crate::types::context::PendingDdl;

                    if let Err(e) = match ddl {
                        PendingDdl::Create { name } => {
                            // Undo CREATE TABLE: remove schema from metadata
                            self.storage.rollback_create_table(&mut batch, name)
                        }
                        PendingDdl::Drop { name, old_schema } => {
                            // Undo DROP TABLE: restore schema to metadata
                            self.storage
                                .rollback_drop_table(&mut batch, name, old_schema)
                        }
                        PendingDdl::Alter { name, old_schema } => {
                            // Undo ALTER TABLE: restore old schema to metadata
                            self.storage
                                .rollback_alter_table(&mut batch, name, old_schema)
                        }
                        PendingDdl::Rename {
                            old_name,
                            new_name,
                            old_schema,
                        } => {
                            // Undo RENAME TABLE: restore old schema AND remove new schema
                            self.storage
                                .rollback_rename_table(&mut batch, old_name, new_name, old_schema)
                        }
                        PendingDdl::CreateIndex { name } => {
                            // Undo CREATE INDEX: remove index from metadata
                            self.storage.rollback_create_index(&mut batch, name)
                        }
                        PendingDdl::DropIndex { name, metadata } => {
                            // Undo DROP INDEX: restore index metadata
                            self.storage.rollback_drop_index(&mut batch, name, metadata)
                        }
                    } {
                        eprintln!("Warning: Failed to add DDL rollback to batch: {:?}", e);
                    }
                }
            }

            // Commit the entire abort atomically (MVCC + predicates + metadata)
            if let Err(e) = batch.commit() {
                eprintln!("Warning: Failed to commit abort batch: {:?}", e);
                return;
            }

            // Reload all metadata to sync in-memory state (only if we had DDL)
            if !tx_ctx.pending_ddls.is_empty() {
                if let Err(e) = self.storage.reload_metadata() {
                    eprintln!("Warning: Failed to reload metadata after rollback: {:?}", e);
                }

                // Update caches with reloaded schemas
                let schemas_arc = self.storage.get_schemas();
                let indexes = self.storage.get_index_metadata();

                let schemas: HashMap<String, crate::types::schema::Table> = schemas_arc
                    .iter()
                    .map(|(k, v)| (k.clone(), v.as_ref().clone()))
                    .collect();

                self.parser.clear();
                self.analyzer.update_schemas(schemas.clone());
                self.analyzer.update_index_metadata(indexes.clone());
                self.planner.update_schemas(schemas);
                self.planner.update_indexes(indexes);
            }
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
        Self::new(SqlStorageConfig::default())
    }
}
