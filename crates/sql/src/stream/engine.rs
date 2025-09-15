//! SQL Engine with predicate-based conflict detection
//!
//! This engine uses predicates for conflict detection at planning time,
//! eliminating the need for row-level locks.

use proven_hlc::HlcTimestamp;
use proven_stream::{OperationResult, RetryOn, TransactionEngine};

use crate::execution::Executor;
use crate::planning::planner::Planner;
use crate::planning::prepared_cache::{PreparedCache, PreparedPlan, bind_parameters};
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

    /// Prepared statement cache
    prepared_cache: PreparedCache,
}

impl SqlTransactionEngine {
    /// Create a new SQL transaction engine
    pub fn new() -> Self {
        Self {
            storage: MvccStorage::new(),
            active_transactions: HashMap::new(),
            executor: Executor::new(),
            migration_version: 0,
            prepared_cache: PreparedCache::new(),
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

        // Check if we have parameters
        let plan = if params.is_some() {
            // Try to get from cache
            if let Some(prepared) = self.prepared_cache.get(sql) {
                // Use cached plan and bind parameters
                let params = params.unwrap_or_default();
                if params.len() != prepared.param_count {
                    return OperationResult::Complete(SqlResponse::Error(format!(
                        "Expected {} parameters, got {}",
                        prepared.param_count,
                        params.len()
                    )));
                }
                match bind_parameters(&prepared.plan_template, &params) {
                    Ok(bound_plan) => bound_plan,
                    Err(e) => {
                        return OperationResult::Complete(SqlResponse::Error(format!(
                            "Parameter binding error: {:?}",
                            e
                        )));
                    }
                }
            } else {
                // Parse, plan, and cache
                let statement = match crate::parsing::parse_sql(sql) {
                    Ok(stmt) => stmt,
                    Err(e) => {
                        return OperationResult::Complete(SqlResponse::Error(format!(
                            "Parse error: {:?}",
                            e
                        )));
                    }
                };

                // Count parameters in the ORIGINAL statement (before optimization)
                // This ensures we always expect the same number of parameters the user wrote
                let original_param_count = count_statement_parameters(&statement);

                let planner = Planner::new(
                    self.storage.get_schemas(),
                    self.storage.get_index_metadata(),
                );
                let plan_template = match planner.plan(statement) {
                    Ok(p) => p,
                    Err(e) => {
                        return OperationResult::Complete(SqlResponse::Error(format!(
                            "Planning error: {:?}",
                            e
                        )));
                    }
                };

                // Cache the prepared plan with the ORIGINAL parameter count
                self.prepared_cache.insert(
                    sql.to_string(),
                    PreparedPlan {
                        plan_template: plan_template.clone(),
                        param_count: original_param_count,
                        param_locations: Vec::new(), // Not tracking locations for now
                    },
                );

                // Bind parameters
                let params = params.unwrap_or_default();
                if params.len() != original_param_count {
                    return OperationResult::Complete(SqlResponse::Error(format!(
                        "Expected {} parameters, got {}",
                        original_param_count,
                        params.len()
                    )));
                }
                match bind_parameters(&plan_template, &params) {
                    Ok(bound_plan) => bound_plan,
                    Err(e) => {
                        return OperationResult::Complete(SqlResponse::Error(format!(
                            "Parameter binding error: {:?}",
                            e
                        )));
                    }
                }
            }
        } else {
            // No parameters - regular execution
            let statement = match crate::parsing::parse_sql(sql) {
                Ok(stmt) => stmt,
                Err(e) => {
                    return OperationResult::Complete(SqlResponse::Error(format!(
                        "Parse error: {:?}",
                        e
                    )));
                }
            };

            let planner = Planner::new(
                self.storage.get_schemas(),
                self.storage.get_index_metadata(),
            );
            match planner.plan(statement) {
                Ok(p) => p,
                Err(e) => {
                    return OperationResult::Complete(SqlResponse::Error(format!(
                        "Planning error: {:?}",
                        e
                    )));
                }
            }
        };

        // Phase 3: Extract predicates from the plan
        let planner = Planner::new(
            self.storage.get_schemas(),
            self.storage.get_index_metadata(),
        );
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
        self.prepared_cache = PreparedCache::new();
        self.migration_version = 0; // Reset migration version

        // Restore storage from compacted data
        self.storage.restore_from_compacted(compacted);

        Ok(())
    }
}

/// Count the number of parameter placeholders in an AST statement (before planning)
fn count_statement_parameters(stmt: &crate::parsing::Statement) -> usize {
    use crate::parsing::{Expression, Statement, ast::InsertSource};

    let mut max_idx = 0;

    fn count_expr_params(expr: &Expression, max_idx: &mut usize) {
        match expr {
            Expression::Parameter(idx) => {
                *max_idx = (*max_idx).max(*idx + 1);
            }
            Expression::Operator(op) => {
                use crate::parsing::Operator;
                match op {
                    Operator::And(l, r)
                    | Operator::Or(l, r)
                    | Operator::Equal(l, r)
                    | Operator::NotEqual(l, r)
                    | Operator::GreaterThan(l, r)
                    | Operator::GreaterThanOrEqual(l, r)
                    | Operator::LessThan(l, r)
                    | Operator::LessThanOrEqual(l, r)
                    | Operator::Add(l, r)
                    | Operator::Subtract(l, r)
                    | Operator::Multiply(l, r)
                    | Operator::Divide(l, r)
                    | Operator::Remainder(l, r)
                    | Operator::Exponentiate(l, r)
                    | Operator::Like(l, r) => {
                        count_expr_params(l, max_idx);
                        count_expr_params(r, max_idx);
                    }
                    Operator::Not(e)
                    | Operator::Factorial(e)
                    | Operator::Identity(e)
                    | Operator::Negate(e) => {
                        count_expr_params(e, max_idx);
                    }
                    Operator::Is(e, _) => {
                        count_expr_params(e, max_idx);
                    }
                    Operator::InList { expr, list, .. } => {
                        count_expr_params(expr, max_idx);
                        for e in list {
                            count_expr_params(e, max_idx);
                        }
                    }
                    Operator::Between {
                        expr, low, high, ..
                    } => {
                        count_expr_params(expr, max_idx);
                        count_expr_params(low, max_idx);
                        count_expr_params(high, max_idx);
                    }
                }
            }
            Expression::Function(_, args) => {
                for arg in args {
                    count_expr_params(arg, max_idx);
                }
            }
            Expression::All | Expression::Column(_, _) | Expression::Literal(_) => {}
            Expression::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                if let Some(op) = operand {
                    count_expr_params(op, max_idx);
                }
                for (cond, result) in when_clauses {
                    count_expr_params(cond, max_idx);
                    count_expr_params(result, max_idx);
                }
                if let Some(else_expr) = else_clause {
                    count_expr_params(else_expr, max_idx);
                }
            }
        }
    }

    match stmt {
        Statement::Select(select) => {
            // Check all parts of SELECT
            for (expr, _) in &select.select {
                count_expr_params(expr, &mut max_idx);
            }
            if let Some(where_clause) = &select.r#where {
                count_expr_params(where_clause, &mut max_idx);
            }
            for expr in &select.group_by {
                count_expr_params(expr, &mut max_idx);
            }
            if let Some(having) = &select.having {
                count_expr_params(having, &mut max_idx);
            }
            for (expr, _) in &select.order_by {
                count_expr_params(expr, &mut max_idx);
            }
        }
        Statement::Insert { source, .. } => {
            match source {
                InsertSource::Values(values) => {
                    for row in values {
                        for expr in row {
                            count_expr_params(expr, &mut max_idx);
                        }
                    }
                }
                InsertSource::DefaultValues => {
                    // No parameters in DEFAULT VALUES
                }
                InsertSource::Select(select) => {
                    // Count params in the SELECT statement
                    for (expr, _) in &select.select {
                        count_expr_params(expr, &mut max_idx);
                    }
                    if let Some(where_expr) = &select.r#where {
                        count_expr_params(where_expr, &mut max_idx);
                    }
                    for expr in &select.group_by {
                        count_expr_params(expr, &mut max_idx);
                    }
                    if let Some(having_expr) = &select.having {
                        count_expr_params(having_expr, &mut max_idx);
                    }
                    for (expr, _) in &select.order_by {
                        count_expr_params(expr, &mut max_idx);
                    }
                    if let Some(limit_expr) = &select.limit {
                        count_expr_params(limit_expr, &mut max_idx);
                    }
                    if let Some(offset_expr) = &select.offset {
                        count_expr_params(offset_expr, &mut max_idx);
                    }
                }
            }
        }
        Statement::Update { set, r#where, .. } => {
            for expr in set.values().flatten() {
                count_expr_params(expr, &mut max_idx);
            }

            if let Some(where_clause) = r#where {
                count_expr_params(where_clause, &mut max_idx);
            }
        }
        Statement::Delete {
            r#where: Some(where_clause),
            ..
        } => {
            count_expr_params(where_clause, &mut max_idx);
        }
        Statement::Delete { r#where: None, .. } => {}
        _ => {} // DDL statements don't have parameters
    }

    max_idx
}

impl Default for SqlTransactionEngine {
    fn default() -> Self {
        Self::new()
    }
}
