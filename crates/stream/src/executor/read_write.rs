//! Read-write execution path for 2PC transactions
//!
//! This executor handles transactions that:
//! - Participate in two-phase commit (2PC)
//! - Can block and wait on locks (wound-wait for deadlock prevention)
//! - Must be prepared before commit
//! - Require coordinator approval to commit

use crate::engine::{BatchOperations, OperationResult, TransactionEngine};
use crate::error::Result;
use crate::support::ResponseSender;
use crate::transaction::TransactionManager;
use proven_common::{Response, TransactionId};
use serde::{Deserialize, Serialize};

/// Acknowledgment response for begin/prepare/commit/abort operations
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AckResponse {
    pub success: bool,
}

impl Response for AckResponse {}

impl AckResponse {
    pub fn success() -> Self {
        Self { success: true }
    }
}

/// Executes read-write operations using 2PC
pub struct ReadWriteExecutor<'a, E: TransactionEngine> {
    engine: &'a mut E,
    tx_manager: &'a mut TransactionManager<E>,
    response: &'a ResponseSender,
}

impl<'a, E: TransactionEngine> ReadWriteExecutor<'a, E> {
    /// Create a new read-write executor
    pub fn new(
        engine: &'a mut E,
        tx_manager: &'a mut TransactionManager<E>,
        response: &'a ResponseSender,
    ) -> Self {
        Self {
            engine,
            tx_manager,
            response,
        }
    }

    /// Begin a new transaction
    ///
    /// # Arguments
    /// * `batch` - Batch to accumulate operations into
    /// * `txn_id` - Transaction ID
    /// * `coordinator_id` - Coordinator to send response to
    /// * `request_id` - Request ID for matching responses
    pub fn begin(
        &mut self,
        batch: &mut E::Batch,
        txn_id: TransactionId,
        coordinator_id: String,
        request_id: String,
        deadline: proven_common::Timestamp,
        participants: std::collections::HashMap<String, u64>,
    ) -> Result<()> {
        // Register transaction with manager
        self.tx_manager
            .begin(txn_id, coordinator_id.clone(), deadline, participants);

        // Begin in engine (adds to batch)
        self.engine.begin(batch, txn_id);

        // Add transaction metadata to batch
        let state = self.tx_manager.get(txn_id)?;
        let metadata_key = crate::transaction::state::TransactionState::metadata_key(txn_id);
        let metadata_value = state.to_bytes()?;
        batch.insert_metadata(metadata_key, metadata_value);

        // Send success response
        self.response.send_success(
            &coordinator_id,
            Some(&txn_id.to_string()),
            request_id,
            AckResponse::success(),
        );

        Ok(())
    }

    /// Execute an operation within a transaction
    ///
    /// # Arguments
    /// * `batch` - Batch to accumulate operations into
    /// * `operation` - The operation to execute
    /// * `txn_id` - Transaction ID
    /// * `coordinator_id` - Coordinator to send response to
    /// * `request_id` - Request ID for matching responses
    pub fn execute(
        &mut self,
        batch: &mut E::Batch,
        operation: E::Operation,
        txn_id: TransactionId,
        coordinator_id: String,
        request_id: String,
    ) -> Result<()> {
        // Ensure transaction is registered (needed for wound-wait to find coordinator)
        if !self.tx_manager.exists(txn_id) {
            // Transaction not started yet - would need deadline and participants
            // For now, just log a warning
            tracing::warn!(
                "Transaction {} not registered, cannot wound-wait properly",
                txn_id
            );
        }

        // Try to execute with wound-wait protocol
        self.execute_with_wound_wait(batch, operation, txn_id, coordinator_id, request_id)
    }

    /// Execute with wound-wait deadlock prevention
    fn execute_with_wound_wait(
        &mut self,
        batch: &mut E::Batch,
        operation: E::Operation,
        txn_id: TransactionId,
        coordinator_id: String,
        request_id: String,
    ) -> Result<()> {
        match self
            .engine
            .apply_operation(batch, operation.clone(), txn_id)
        {
            OperationResult::Complete(response) => {
                // Add transaction metadata to batch
                let state = self.tx_manager.get(txn_id)?;
                let metadata_key =
                    crate::transaction::state::TransactionState::metadata_key(txn_id);
                let metadata_value = state.to_bytes()?;
                batch.insert_metadata(metadata_key, metadata_value);

                // Send successful response
                self.response.send_success(
                    &coordinator_id,
                    Some(&txn_id.to_string()),
                    request_id,
                    response,
                );
                Ok(())
            }
            OperationResult::WouldBlock { blockers } => {
                // Wound-Wait Protocol:
                // - If blocked by younger transactions (txn > current), wound them
                // - If blocked by older transactions (txn < current), defer

                // Find younger blockers to wound
                let younger_victims: Vec<TransactionId> = blockers
                    .iter()
                    .filter(|b| b.txn > txn_id)
                    .map(|b| b.txn)
                    .collect();

                if !younger_victims.is_empty() {
                    tracing::debug!(
                        "Transaction {} wounding younger transactions: {:?}",
                        txn_id,
                        younger_victims
                            .iter()
                            .map(|v| v.to_string())
                            .collect::<Vec<_>>()
                    );

                    // Wound each younger transaction (all go in same batch)
                    for victim in &younger_victims {
                        self.wound_transaction(batch, *victim, txn_id)?;
                    }

                    // Retry operation after wounding (use same batch)
                    match self
                        .engine
                        .apply_operation(batch, operation.clone(), txn_id)
                    {
                        OperationResult::Complete(response) => {
                            // Add transaction metadata to batch
                            let state = self.tx_manager.get(txn_id)?;
                            let metadata_key =
                                crate::transaction::state::TransactionState::metadata_key(txn_id);
                            let metadata_value = state.to_bytes()?;
                            batch.insert_metadata(metadata_key, metadata_value);

                            self.response.send_success(
                                &coordinator_id,
                                Some(&txn_id.to_string()),
                                request_id,
                                response,
                            );
                            return Ok(());
                        }
                        OperationResult::WouldBlock {
                            blockers: new_blockers,
                        } => {
                            // Still blocked (by older transactions) - defer
                            tracing::debug!(
                                "Transaction {} still blocked after wounding, deferring",
                                txn_id
                            );

                            self.tx_manager.defer_operation(
                                txn_id,
                                operation.clone(),
                                new_blockers,
                                coordinator_id,
                                request_id,
                                false, // ReadWrite operations are not atomic
                            );

                            // Persist transaction metadata (for crash recovery)
                            let state = self.tx_manager.get(txn_id)?;
                            let metadata_key =
                                crate::transaction::state::TransactionState::metadata_key(txn_id);
                            let metadata_value = state.to_bytes()?;
                            batch.insert_metadata(metadata_key, metadata_value);

                            return Ok(());
                        }
                    }
                }

                // All blockers are older - defer this operation
                tracing::debug!(
                    "Transaction {} deferring to older transactions: {:?}",
                    txn_id,
                    blockers
                        .iter()
                        .map(|b| b.txn.to_string())
                        .collect::<Vec<_>>()
                );

                self.tx_manager.defer_operation(
                    txn_id,
                    operation,
                    blockers,
                    coordinator_id,
                    request_id,
                    false, // ReadWrite operations are not atomic
                );

                // Persist transaction metadata (for crash recovery)
                let state = self.tx_manager.get(txn_id)?;
                let metadata_key =
                    crate::transaction::state::TransactionState::metadata_key(txn_id);
                let metadata_value = state.to_bytes()?;
                batch.insert_metadata(metadata_key, metadata_value);

                Ok(())
            }
        }
    }

    /// Wound a victim transaction (abort it)
    fn wound_transaction(
        &mut self,
        batch: &mut E::Batch,
        victim: TransactionId,
        wounded_by: TransactionId,
    ) -> Result<()> {
        // Check if victim transaction exists
        if !self.tx_manager.exists(victim) {
            // Victim transaction doesn't exist (might have already completed)
            // Just log and continue
            tracing::debug!(
                "Cannot wound transaction {} - not found (may have already completed)",
                victim
            );
            return Ok(());
        }

        // Mark the transaction as wounded
        self.tx_manager.mark_wounded(victim, wounded_by)?;

        // Send wounded notification to coordinator
        if let Some(victim_coord) = self.tx_manager.get_coordinator(victim) {
            self.response
                .send_wounded(&victim_coord, &victim.to_string(), wounded_by, None);
        }

        // Abort the victim transaction (adds to batch)
        self.engine.abort(batch, victim);

        // Transition to aborted state and release any deferred operations
        let ready_ops = self.tx_manager.transition_to_aborted(
            victim,
            crate::transaction::AbortReason::Wounded { by: wounded_by },
        )?;

        // Remove victim's transaction metadata
        let metadata_key = crate::transaction::state::TransactionState::metadata_key(victim);
        batch.remove_metadata(metadata_key);

        // Retry deferred operations that were blocked by this victim
        // They all go in the SAME batch (critical for atomicity!)
        if !ready_ops.is_empty() {
            tracing::debug!(
                "Released {} deferred operations after wounding victim {}",
                ready_ops.len(),
                victim
            );
            self.retry_deferred_to_batch(batch, ready_ops)?;
        }

        Ok(())
    }

    /// Prepare a transaction for commit (Phase 1 of 2PC)
    ///
    /// # Arguments
    /// * `batch` - Batch to accumulate operations into
    /// * `txn_id` - Transaction ID to prepare
    /// * `coordinator_id` - Coordinator to send response to
    /// * `request_id` - Request ID for matching responses
    /// * `participants` - Map of participant streams for recovery
    pub fn prepare(
        &mut self,
        batch: &mut E::Batch,
        txn_id: TransactionId,
        coordinator_id: String,
        request_id: String,
        participants: std::collections::HashMap<String, u64>,
    ) -> Result<()> {
        // Prepare in engine (adds to batch)
        self.engine.prepare(batch, txn_id);

        // Transition to prepared state - this releases operations waiting on prepare
        let ready_ops = self
            .tx_manager
            .transition_to_prepared_with_participants(txn_id, participants)?;

        // Add updated transaction metadata (with participants)
        let state = self.tx_manager.get(txn_id)?;
        let metadata_key = crate::transaction::state::TransactionState::metadata_key(txn_id);
        let metadata_value = state.to_bytes()?;
        batch.insert_metadata(metadata_key, metadata_value);

        // Retry deferred operations that were waiting on prepare
        // They all go in the SAME batch!
        if !ready_ops.is_empty() {
            tracing::debug!(
                "Released {} deferred operations on prepare",
                ready_ops.len()
            );
            self.retry_deferred_to_batch(batch, ready_ops)?;
        }

        // Send prepared response
        self.response
            .send_prepared(&coordinator_id, &txn_id.to_string(), Some(request_id));

        Ok(())
    }

    /// Commit a prepared transaction (Phase 2 of 2PC)
    ///
    /// # Arguments
    /// * `batch` - Batch to accumulate operations into
    /// * `txn_id` - Transaction ID to commit
    /// * `coordinator_id` - Coordinator to send response to
    /// * `request_id` - Request ID for matching responses
    pub fn commit(
        &mut self,
        batch: &mut E::Batch,
        txn_id: TransactionId,
        coordinator_id: String,
        request_id: String,
    ) -> Result<()> {
        // Commit in engine (adds to batch)
        self.engine.commit(batch, txn_id);

        // Transition to committed state - this releases operations waiting on completion
        let ready_ops = self.tx_manager.transition_to_committed(txn_id)?;

        // Remove transaction metadata (transaction is complete)
        let metadata_key = crate::transaction::state::TransactionState::metadata_key(txn_id);
        batch.remove_metadata(metadata_key);

        // Retry deferred operations that were waiting on commit
        // They all go in the SAME batch!
        if !ready_ops.is_empty() {
            tracing::debug!("Released {} deferred operations on commit", ready_ops.len());
            self.retry_deferred_to_batch(batch, ready_ops)?;
        }

        // Send committed response
        self.response.send_success(
            &coordinator_id,
            Some(&txn_id.to_string()),
            request_id,
            AckResponse::success(),
        );

        Ok(())
    }

    /// Abort a transaction
    ///
    /// # Arguments
    /// * `batch` - Batch to accumulate operations into
    /// * `txn_id` - Transaction ID to abort
    /// * `coordinator_id` - Coordinator to send response to
    /// * `request_id` - Request ID for matching responses
    pub fn abort(
        &mut self,
        batch: &mut E::Batch,
        txn_id: TransactionId,
        coordinator_id: String,
        request_id: String,
    ) -> Result<()> {
        // Abort in engine (adds to batch)
        self.engine.abort(batch, txn_id);

        // Transition to aborted state - this releases operations waiting on completion
        let ready_ops = self
            .tx_manager
            .transition_to_aborted(txn_id, crate::transaction::AbortReason::Explicit)?;

        // Remove transaction metadata (transaction is complete)
        let metadata_key = crate::transaction::state::TransactionState::metadata_key(txn_id);
        batch.remove_metadata(metadata_key);

        // Retry deferred operations that were waiting on abort
        // They all go in the SAME batch!
        if !ready_ops.is_empty() {
            tracing::debug!(
                "Released {} deferred operations after abort",
                ready_ops.len()
            );
            self.retry_deferred_to_batch(batch, ready_ops)?;
        }

        // Send aborted response
        self.response.send_success(
            &coordinator_id,
            Some(&txn_id.to_string()),
            request_id,
            AckResponse::success(),
        );

        Ok(())
    }

    /// Retry deferred operations in the SAME batch
    ///
    /// CRITICAL: All deferred operations that become ready are retried
    /// in the SAME batch as the operation that unblocked them.
    /// This ensures atomic persistence of cascading operations.
    fn retry_deferred_to_batch(
        &mut self,
        batch: &mut E::Batch,
        ready_ops: Vec<crate::transaction::DeferredOp<E::Operation>>,
    ) -> Result<()> {
        for deferred in ready_ops {
            // Check if this is an atomic operation (AdHoc)
            if deferred.is_atomic {
                // AdHoc operations need begin → apply → commit sequence
                self.engine.begin(batch, deferred.owner_txn_id);

                match self.engine.apply_operation(
                    batch,
                    deferred.operation.clone(),
                    deferred.owner_txn_id,
                ) {
                    OperationResult::Complete(response) => {
                        // Commit in same batch
                        self.engine.commit(batch, deferred.owner_txn_id);

                        // Update transaction metadata
                        self.tx_manager
                            .transition_to_committed(deferred.owner_txn_id)?;
                        let metadata_key =
                            crate::transaction::state::TransactionState::metadata_key(
                                deferred.owner_txn_id,
                            );
                        batch.remove_metadata(metadata_key);

                        self.response.send_success(
                            &deferred.coordinator_id,
                            None,
                            deferred.request_id,
                            response,
                        );
                    }
                    OperationResult::WouldBlock { blockers } => {
                        // Still blocked - re-defer
                        self.tx_manager.defer_operation(
                            deferred.owner_txn_id,
                            deferred.operation,
                            blockers,
                            deferred.coordinator_id,
                            deferred.request_id,
                            true, // Still atomic
                        );

                        // Update transaction metadata
                        let state = self.tx_manager.get(deferred.owner_txn_id)?;
                        let metadata_key =
                            crate::transaction::state::TransactionState::metadata_key(
                                deferred.owner_txn_id,
                            );
                        let metadata_value = state.to_bytes()?;
                        batch.insert_metadata(metadata_key, metadata_value);
                    }
                }
            } else {
                // ReadWrite operations - just apply (already begun)
                match self.engine.apply_operation(
                    batch,
                    deferred.operation.clone(),
                    deferred.owner_txn_id,
                ) {
                    OperationResult::Complete(response) => {
                        // Update transaction metadata
                        let state = self.tx_manager.get(deferred.owner_txn_id)?;
                        let metadata_key =
                            crate::transaction::state::TransactionState::metadata_key(
                                deferred.owner_txn_id,
                            );
                        let metadata_value = state.to_bytes()?;
                        batch.insert_metadata(metadata_key, metadata_value);

                        self.response.send_success(
                            &deferred.coordinator_id,
                            Some(&deferred.owner_txn_id.to_string()),
                            deferred.request_id,
                            response,
                        );
                    }
                    OperationResult::WouldBlock { blockers } => {
                        // Re-defer with new blockers
                        self.tx_manager.defer_operation(
                            deferred.owner_txn_id,
                            deferred.operation,
                            blockers,
                            deferred.coordinator_id,
                            deferred.request_id,
                            false, // Still not atomic
                        );

                        // Update transaction metadata
                        let state = self.tx_manager.get(deferred.owner_txn_id)?;
                        let metadata_key =
                            crate::transaction::state::TransactionState::metadata_key(
                                deferred.owner_txn_id,
                            );
                        let metadata_value = state.to_bytes()?;
                        batch.insert_metadata(metadata_key, metadata_value);
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{BlockingInfo, RetryOn};
    use proven_common::{Operation, OperationType, Response};
    use proven_engine::MockClient;
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct TestOp(String);
    impl Operation for TestOp {
        fn operation_type(&self) -> OperationType {
            OperationType::Write
        }
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestResponse(String);
    impl Response for TestResponse {}

    struct TestBatch;
    impl crate::engine::BatchOperations for TestBatch {
        fn insert_metadata(&mut self, _key: Vec<u8>, _value: Vec<u8>) {}
        fn remove_metadata(&mut self, _key: Vec<u8>) {}
    }

    struct TestEngine {
        should_block: bool,
        operations_executed: Vec<String>,
        begun: Vec<TransactionId>,
        prepared: Vec<TransactionId>,
        committed: Vec<TransactionId>,
        aborted: Vec<TransactionId>,
    }

    impl TransactionEngine for TestEngine {
        type Operation = TestOp;
        type Response = TestResponse;
        type Batch = TestBatch;

        fn start_batch(&mut self) -> Self::Batch {
            TestBatch
        }

        fn commit_batch(&mut self, _batch: Self::Batch, _log_index: u64) {}

        fn read_at_timestamp(
            &mut self,
            _operation: Self::Operation,
            _read_txn_id: TransactionId,
        ) -> OperationResult<Self::Response> {
            unimplemented!()
        }

        fn apply_operation(
            &mut self,
            _batch: &mut Self::Batch,
            operation: Self::Operation,
            _txn_id: TransactionId,
        ) -> OperationResult<Self::Response> {
            self.operations_executed.push(operation.0.clone());

            if self.should_block {
                OperationResult::WouldBlock {
                    blockers: vec![BlockingInfo {
                        txn: TransactionId::new(),
                        retry_on: RetryOn::CommitOrAbort,
                    }],
                }
            } else {
                OperationResult::Complete(TestResponse(format!("executed: {}", operation.0)))
            }
        }

        fn begin(&mut self, _batch: &mut Self::Batch, txn_id: TransactionId) {
            self.begun.push(txn_id);
        }

        fn prepare(&mut self, _batch: &mut Self::Batch, txn_id: TransactionId) {
            self.prepared.push(txn_id);
        }

        fn commit(&mut self, _batch: &mut Self::Batch, txn_id: TransactionId) {
            self.committed.push(txn_id);
        }

        fn abort(&mut self, _batch: &mut Self::Batch, txn_id: TransactionId) {
            self.aborted.push(txn_id);
        }

        fn get_log_index(&self) -> Option<u64> {
            None
        }
        fn scan_transaction_metadata(&self) -> Vec<(TransactionId, Vec<u8>)> {
            vec![]
        }

        fn engine_name(&self) -> &str {
            "test"
        }
    }

    fn setup() -> (TestEngine, TransactionManager<TestEngine>, ResponseSender) {
        let mock_engine_for_client = Arc::new(proven_engine::MockEngine::new());
        let client = Arc::new(MockClient::new(
            "test-node".to_string(),
            mock_engine_for_client,
        ));
        let engine = TestEngine {
            should_block: false,
            operations_executed: Vec::new(),
            begun: Vec::new(),
            prepared: Vec::new(),
            committed: Vec::new(),
            aborted: Vec::new(),
        };
        let tx_manager = TransactionManager::new();
        let response = ResponseSender::new(client, "test-stream".to_string(), "test".to_string());

        (engine, tx_manager, response)
    }

    #[tokio::test]
    async fn test_begin_transaction() {
        let (mut engine, mut tx_manager, response) = setup();

        let txn_id = TransactionId::new();
        let result = {
            let mut batch = engine.start_batch();
            let mut executor = ReadWriteExecutor::new(&mut engine, &mut tx_manager, &response);
            let res = executor.begin(
                &mut batch,
                txn_id,
                "coord-1".to_string(),
                "req-1".to_string(),
                proven_common::Timestamp::from_micros(10000),
                std::collections::HashMap::new(),
            );
            engine.commit_batch(batch, 1);
            res
        };

        assert!(result.is_ok());
        assert_eq!(engine.begun.len(), 1);
        assert_eq!(engine.begun[0], txn_id);

        tokio::task::yield_now().await;
    }

    #[tokio::test]
    async fn test_execute_operation() {
        let (mut engine, mut tx_manager, response) = setup();
        let txn_id = TransactionId::new();

        // Begin transaction first
        {
            let mut batch = engine.start_batch();
            let mut executor = ReadWriteExecutor::new(&mut engine, &mut tx_manager, &response);
            executor
                .begin(
                    &mut batch,
                    txn_id,
                    "coord-1".to_string(),
                    "req-1".to_string(),
                    proven_common::Timestamp::from_micros(10000),
                    std::collections::HashMap::new(),
                )
                .unwrap();
            engine.commit_batch(batch, 1);
        }

        // Execute operation
        {
            let mut batch = engine.start_batch();
            let mut executor = ReadWriteExecutor::new(&mut engine, &mut tx_manager, &response);
            let result = executor.execute(
                &mut batch,
                TestOp("write1".to_string()),
                txn_id,
                "coord-1".to_string(),
                "req-2".to_string(),
            );
            engine.commit_batch(batch, 2);
            assert!(result.is_ok());
        }

        assert_eq!(engine.operations_executed.len(), 1);
        assert_eq!(engine.operations_executed[0], "write1");

        tokio::task::yield_now().await;
    }

    #[tokio::test]
    async fn test_prepare_commit_flow() {
        let (mut engine, mut tx_manager, response) = setup();
        let txn_id = TransactionId::new();

        // Begin -> Execute -> Prepare -> Commit
        {
            let mut batch = engine.start_batch();
            let mut executor = ReadWriteExecutor::new(&mut engine, &mut tx_manager, &response);
            executor
                .begin(
                    &mut batch,
                    txn_id,
                    "coord-1".to_string(),
                    "req-1".to_string(),
                    proven_common::Timestamp::from_micros(10000),
                    std::collections::HashMap::new(),
                )
                .unwrap();
            engine.commit_batch(batch, 1);
        }
        {
            let mut batch = engine.start_batch();
            let mut executor = ReadWriteExecutor::new(&mut engine, &mut tx_manager, &response);
            executor
                .execute(
                    &mut batch,
                    TestOp("write1".to_string()),
                    txn_id,
                    "coord-1".to_string(),
                    "req-2".to_string(),
                )
                .unwrap();
            engine.commit_batch(batch, 2);
        }
        {
            let mut batch = engine.start_batch();
            let mut executor = ReadWriteExecutor::new(&mut engine, &mut tx_manager, &response);
            executor
                .prepare(
                    &mut batch,
                    txn_id,
                    "coord-1".to_string(),
                    "req-3".to_string(),
                    std::collections::HashMap::new(),
                )
                .unwrap();
            engine.commit_batch(batch, 3);
        }
        {
            let mut batch = engine.start_batch();
            let mut executor = ReadWriteExecutor::new(&mut engine, &mut tx_manager, &response);
            executor
                .commit(
                    &mut batch,
                    txn_id,
                    "coord-1".to_string(),
                    "req-4".to_string(),
                )
                .unwrap();
            engine.commit_batch(batch, 4);
        }

        assert_eq!(engine.begun.len(), 1);
        assert_eq!(engine.operations_executed.len(), 1);
        assert_eq!(engine.prepared.len(), 1);
        assert_eq!(engine.committed.len(), 1);
        assert_eq!(engine.aborted.len(), 0);

        tokio::task::yield_now().await;
    }

    #[tokio::test]
    async fn test_abort_transaction() {
        let (mut engine, mut tx_manager, response) = setup();
        let txn_id = TransactionId::new();

        // Begin -> Execute -> Abort
        {
            let mut batch = engine.start_batch();
            let mut executor = ReadWriteExecutor::new(&mut engine, &mut tx_manager, &response);
            executor
                .begin(
                    &mut batch,
                    txn_id,
                    "coord-1".to_string(),
                    "req-1".to_string(),
                    proven_common::Timestamp::from_micros(10000),
                    std::collections::HashMap::new(),
                )
                .unwrap();
            engine.commit_batch(batch, 1);
        }
        {
            let mut batch = engine.start_batch();
            let mut executor = ReadWriteExecutor::new(&mut engine, &mut tx_manager, &response);
            executor
                .execute(
                    &mut batch,
                    TestOp("write1".to_string()),
                    txn_id,
                    "coord-1".to_string(),
                    "req-2".to_string(),
                )
                .unwrap();
            engine.commit_batch(batch, 2);
        }
        {
            let mut batch = engine.start_batch();
            let mut executor = ReadWriteExecutor::new(&mut engine, &mut tx_manager, &response);
            executor
                .abort(
                    &mut batch,
                    txn_id,
                    "coord-1".to_string(),
                    "req-3".to_string(),
                )
                .unwrap();
            engine.commit_batch(batch, 3);
        }

        assert_eq!(engine.begun.len(), 1);
        assert_eq!(engine.operations_executed.len(), 1);
        assert_eq!(engine.aborted.len(), 1);
        assert_eq!(engine.committed.len(), 0);

        tokio::task::yield_now().await;
    }

    #[tokio::test]
    async fn test_blocked_operation_defers() {
        let (mut engine, mut tx_manager, response) = setup();

        // Make engine block
        engine.should_block = true;

        let txn_id = TransactionId::new();
        {
            let mut batch = engine.start_batch();
            let mut executor = ReadWriteExecutor::new(&mut engine, &mut tx_manager, &response);
            executor
                .begin(
                    &mut batch,
                    txn_id,
                    "coord-1".to_string(),
                    "req-1".to_string(),
                    proven_common::Timestamp::from_micros(10000),
                    std::collections::HashMap::new(),
                )
                .unwrap();
            engine.commit_batch(batch, 1);
        }

        let result = {
            let mut batch = engine.start_batch();
            let mut executor = ReadWriteExecutor::new(&mut engine, &mut tx_manager, &response);
            let res = executor.execute(
                &mut batch,
                TestOp("write1".to_string()),
                txn_id,
                "coord-1".to_string(),
                "req-2".to_string(),
            );
            engine.commit_batch(batch, 2);
            res
        };

        assert!(result.is_ok());
        // With wound-wait, operation is tried twice: once initially, once after wounding
        assert_eq!(engine.operations_executed.len(), 2);

        // Should have deferred the operation (still blocked after wounding)
        assert_eq!(tx_manager.deferred_count(txn_id), 1);

        tokio::task::yield_now().await;
    }
}
