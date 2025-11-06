//! Stream processing kernel - core transaction processing without I/O orchestration
//!
//! This module provides the pure processing logic for stream messages,
//! separated from stream subscription and event loop orchestration.
//! The runner crate owns the orchestration and uses this kernel to process messages.

use crate::engine::TransactionEngine;
use crate::error::{Error, Result};
use crate::flow::{OrderedFlow, UnorderedFlow};
use crate::support::ResponseSender;
use crate::transaction::TransactionManager;
use proven_common::Timestamp;
use proven_engine::{Message, MockClient};
use proven_protocol::{OrderedMessage, ReadOnlyMessage};
use std::sync::Arc;

/// Response mode for message processing
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseMode {
    /// Suppress responses (typically used during replay/catch-up)
    Suppress,
    /// Send responses normally (live processing)
    Send,
}

/// Stream processing kernel - handles message processing without I/O orchestration
///
/// This is the core transaction processing engine that:
/// - Processes ordered and read-only messages
/// - Manages transaction state
/// - Handles crash recovery
/// - Tracks processing offsets
///
/// It does NOT handle:
/// - Stream subscriptions (runner's responsibility)
/// - Event loop orchestration (runner's responsibility)
/// - Shutdown signaling (runner's responsibility)
pub struct StreamProcessingKernel<E: TransactionEngine> {
    /// Transaction engine
    engine: E,

    /// Transaction manager
    tx_manager: TransactionManager<E>,

    /// Recovery manager for handling coordinator failures
    recovery_manager: crate::transaction::RecoveryManager,

    /// Response sender
    response: ResponseSender,

    /// Current response mode
    response_mode: ResponseMode,

    /// Stream name (for logging)
    stream_name: String,

    /// Current offset in the stream
    current_offset: u64,
}

impl<E: TransactionEngine> StreamProcessingKernel<E> {
    /// Create a new stream processing kernel
    pub fn new(engine: E, client: Arc<MockClient>, stream_name: String) -> Self {
        // Create response sender internally
        let response = ResponseSender::new(
            client.clone(),
            stream_name.clone(),
            engine.engine_name().to_string(),
        );

        // Get starting offset from engine's persisted log index
        let engine_offset = engine.get_log_index().unwrap_or(0);

        tracing::info!(
            "[{}] Kernel initialized at log index {}, will replay from offset {}",
            stream_name,
            engine_offset,
            engine_offset + 1
        );

        Self {
            engine,
            recovery_manager: crate::transaction::RecoveryManager::new(client, stream_name.clone()),
            tx_manager: TransactionManager::new(),
            response,
            response_mode: ResponseMode::Suppress, // Start with responses suppressed (for replay)
            stream_name,
            current_offset: engine_offset,
        }
    }

    /// Get current response mode
    pub fn response_mode(&self) -> ResponseMode {
        self.response_mode
    }

    /// Set response mode (called by orchestrator)
    pub fn set_response_mode(&mut self, mode: ResponseMode) {
        if self.response_mode != mode {
            tracing::info!(
                "[{}] Response mode transition: {:?} -> {:?}",
                self.stream_name,
                self.response_mode,
                mode
            );
            self.response_mode = mode;

            // Update response sender
            match mode {
                ResponseMode::Suppress => self.response.set_suppress(true),
                ResponseMode::Send => self.response.set_suppress(false),
            }
        }
    }

    /// Get current offset
    pub fn current_offset(&self) -> u64 {
        self.current_offset
    }

    /// Get stream name
    pub fn stream_name(&self) -> &str {
        &self.stream_name
    }

    /// Process a message from the ordered stream
    pub async fn process_ordered(
        &mut self,
        message: Message,
        timestamp: Timestamp,
        offset: u64,
    ) -> Result<()> {
        // Update offset
        self.current_offset = offset;

        // Parse message
        let coord_msg = OrderedMessage::from_message(message)
            .map_err(|e| Error::InvalidOperation(e.to_string()))?;

        // Use ordered flow (8-step deterministic processing)
        OrderedFlow::process(
            &mut self.engine,
            &mut self.tx_manager,
            &self.recovery_manager,
            &self.response,
            coord_msg,
            timestamp,
            offset,
            self.response_mode,
        )
        .await
    }

    /// Process a read-only message from pubsub
    pub fn process_readonly(&mut self, message: Message) -> Result<()> {
        // Parse message
        let coord_msg = ReadOnlyMessage::from_message(message)
            .map_err(|e| Error::InvalidOperation(e.to_string()))?;

        // Use read-only flow
        UnorderedFlow::process(
            &mut self.engine,
            &mut self.tx_manager,
            &self.response,
            coord_msg,
        )
    }

    /// Recover transaction state from persisted metadata (called on startup)
    ///
    /// This method scans all persisted transaction metadata and rebuilds
    /// the TransactionManager's in-memory state. This is critical for:
    /// - Restoring active transactions with their coordinator information
    /// - Restoring prepared transactions with their participant lists
    /// - Restoring completed transactions to completed map and rebuilding deadline index
    pub fn recover_transaction_state(&mut self) -> Result<()> {
        let metadata_entries = self.engine.scan_transaction_metadata();

        if metadata_entries.is_empty() {
            tracing::info!(
                "[{}] No persisted transactions to recover",
                self.stream_name
            );
            return Ok(());
        }

        tracing::info!(
            "[{}] Recovering {} persisted transactions...",
            self.stream_name,
            metadata_entries.len()
        );

        let mut recovered_active = 0;
        let mut recovered_prepared = 0;
        let mut recovered_completed = 0;

        for (txn_id, metadata_bytes) in metadata_entries {
            match crate::transaction::TransactionState::<E::Operation>::from_bytes(&metadata_bytes)
            {
                Ok(state) => {
                    match state.phase {
                        crate::transaction::TransactionPhase::Active => {
                            // Restore active transaction with deferred operations
                            self.tx_manager.begin(
                                txn_id,
                                state.coordinator_id.clone(),
                                state.deadline,
                                state.participants.clone(),
                            );

                            // Restore deferred operations
                            for deferred_op in state.deferred_operations {
                                let blockers = deferred_op.waiting_for.to_blocking_info();
                                self.tx_manager.defer_operation(
                                    txn_id,
                                    deferred_op.operation,
                                    blockers,
                                    deferred_op.coordinator_id,
                                    deferred_op.request_id,
                                    deferred_op.is_atomic,
                                );
                            }

                            recovered_active += 1;
                            tracing::debug!(
                                "[{}] Recovered Active transaction {} (coord: {})",
                                self.stream_name,
                                txn_id,
                                state.coordinator_id
                            );
                        }

                        crate::transaction::TransactionPhase::Prepared => {
                            // Restore prepared transaction with deferred operations
                            // First begin, then transition to prepared
                            self.tx_manager.begin(
                                txn_id,
                                state.coordinator_id.clone(),
                                state.deadline,
                                std::collections::HashMap::new(),
                            );

                            self.tx_manager.transition_to_prepared_with_participants(
                                txn_id,
                                state.participants.clone(),
                            )?;

                            // Restore deferred operations
                            for deferred_op in state.deferred_operations {
                                let blockers = deferred_op.waiting_for.to_blocking_info();
                                self.tx_manager.defer_operation(
                                    txn_id,
                                    deferred_op.operation,
                                    blockers,
                                    deferred_op.coordinator_id,
                                    deferred_op.request_id,
                                    deferred_op.is_atomic,
                                );
                            }

                            recovered_prepared += 1;
                            tracing::debug!(
                                "[{}] Recovered Prepared transaction {} (coord: {}, {} participants)",
                                self.stream_name,
                                txn_id,
                                state.coordinator_id,
                                state.participants.len()
                            );
                        }

                        crate::transaction::TransactionPhase::Committed => {
                            // Restore completed transaction to completed map and rebuild deadline index
                            // These are kept until their deadline passes for late message handling
                            self.tx_manager.restore_completed(
                                txn_id,
                                state.coordinator_id.clone(),
                                state.phase,
                                Timestamp::now(), // Use current time as completed_at
                                state.deadline,
                            );

                            recovered_completed += 1;
                            tracing::debug!(
                                "[{}] Recovered Completed transaction {} (deadline: {})",
                                self.stream_name,
                                txn_id,
                                state.deadline.as_micros()
                            );
                        }

                        crate::transaction::TransactionPhase::Aborted { aborted_at, reason } => {
                            // Restore completed transaction to completed map and rebuild deadline index
                            self.tx_manager.restore_completed(
                                txn_id,
                                state.coordinator_id.clone(),
                                crate::transaction::TransactionPhase::Aborted {
                                    aborted_at,
                                    reason,
                                },
                                aborted_at,
                                state.deadline,
                            );

                            recovered_completed += 1;
                            tracing::debug!(
                                "[{}] Recovered Aborted transaction {} (deadline: {})",
                                self.stream_name,
                                txn_id,
                                state.deadline.as_micros()
                            );
                        }
                    }
                }

                Err(e) => {
                    tracing::warn!(
                        "[{}] Failed to deserialize transaction {}: {}",
                        self.stream_name,
                        txn_id,
                        e
                    );
                }
            }
        }

        tracing::info!(
            "[{}] Recovery complete: {} Active, {} Prepared, {} Completed",
            self.stream_name,
            recovered_active,
            recovered_prepared,
            recovered_completed
        );

        Ok(())
    }

    /// Check if the kernel needs processing (for idle detection)
    ///
    /// Returns true if there are transactions that need recovery/GC at the given timestamp
    pub fn needs_processing(&self, now: Timestamp) -> bool {
        self.tx_manager.needs_processing(now)
    }

    /// Get reference to transaction manager (for testing)
    #[cfg(test)]
    pub fn transaction_manager(&self) -> &TransactionManager<E> {
        &self.tx_manager
    }

    /// Get reference to engine (for testing)
    #[cfg(test)]
    pub fn engine(&self) -> &E {
        &self.engine
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{OperationResult, TransactionEngine};
    use proven_common::{Operation, OperationType, ProcessorType, Response, TransactionId};
    use proven_engine::MockClient;
    use proven_protocol::TransactionPhase;
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct TestOp(String);
    impl Operation for TestOp {
        fn operation_type(&self) -> OperationType {
            OperationType::Write
        }

        fn processor_type(&self) -> ProcessorType {
            ProcessorType::Kv
        }
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestResponse(String);
    impl Response for TestResponse {}

    struct TestEngine {
        operations: Vec<String>,
        begun: Vec<TransactionId>,
        committed: Vec<TransactionId>,
    }

    struct TestBatch;
    impl crate::engine::BatchOperations for TestBatch {
        fn insert_transaction_metadata(&mut self, _txn_id: TransactionId, _value: Vec<u8>) {}
        fn remove_transaction_metadata(&mut self, _txn_id: TransactionId) {}
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
            OperationResult::Complete(TestResponse("read".to_string()))
        }

        fn apply_operation(
            &mut self,
            _batch: &mut Self::Batch,
            operation: Self::Operation,
            _txn_id: TransactionId,
        ) -> OperationResult<Self::Response> {
            self.operations.push(operation.0.clone());
            OperationResult::Complete(TestResponse(format!("executed: {}", operation.0)))
        }

        fn begin(&mut self, _batch: &mut Self::Batch, txn_id: TransactionId) {
            self.begun.push(txn_id);
        }

        fn prepare(&mut self, _batch: &mut Self::Batch, _txn_id: TransactionId) {}
        fn commit(&mut self, _batch: &mut Self::Batch, txn_id: TransactionId) {
            self.committed.push(txn_id);
        }
        fn abort(&mut self, _batch: &mut Self::Batch, _txn_id: TransactionId) {}
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

    fn create_kernel() -> StreamProcessingKernel<TestEngine> {
        let mock_engine_for_client = Arc::new(proven_engine::MockEngine::new());
        let client = Arc::new(MockClient::new(
            "test-node".to_string(),
            mock_engine_for_client,
        ));
        let engine = TestEngine {
            operations: Vec::new(),
            begun: Vec::new(),
            committed: Vec::new(),
        };

        let mut kernel = StreamProcessingKernel::new(engine, client, "test-stream".to_string());

        // For tests, enable responses (Live mode)
        kernel.set_response_mode(ResponseMode::Send);
        kernel
    }

    #[tokio::test]
    async fn test_process_adhoc_operation() {
        let mut kernel = create_kernel();

        let op_msg = OrderedMessage::AutoCommitOperation {
            txn_id: TransactionId::new(),
            coordinator_id: "coord-1".to_string(),
            request_id: "req-1".to_string(),
            txn_deadline: Timestamp::now().add_micros(10_000_000),
            operation: TestOp("write1".to_string()),
        };

        let message = op_msg.into_message();
        let result = kernel.process_ordered(message, Timestamp::now(), 1).await;

        assert!(result.is_ok());
        assert_eq!(kernel.engine().operations.len(), 1);
        assert_eq!(kernel.engine().operations[0], "write1");
        assert_eq!(kernel.current_offset(), 1);

        tokio::task::yield_now().await;
    }

    #[tokio::test]
    async fn test_replay_executes_operations() {
        let mut kernel = create_kernel();
        kernel.set_response_mode(ResponseMode::Suppress);

        let txn_id = TransactionId::new();
        let op_msg = OrderedMessage::TransactionOperation {
            txn_id,
            coordinator_id: "coord-1".to_string(),
            request_id: "req-1".to_string(),
            txn_deadline: Timestamp::now().add_micros(10_000_000),
            operation: TestOp("write1".to_string()),
        };

        let message = op_msg.into_message();
        let result = kernel.process_ordered(message, Timestamp::now(), 1).await;

        assert!(result.is_ok());

        // During replay, operations ARE executed to rebuild engine state
        assert_eq!(kernel.engine().operations.len(), 1);
        assert_eq!(kernel.engine().operations[0], "write1");

        // Transaction should be tracked
        assert!(kernel.transaction_manager().exists(txn_id));

        tokio::task::yield_now().await;
    }

    #[tokio::test]
    async fn test_response_mode_transitions() {
        let mut kernel = create_kernel();

        assert_eq!(kernel.response_mode(), ResponseMode::Send);

        kernel.set_response_mode(ResponseMode::Suppress);
        assert_eq!(kernel.response_mode(), ResponseMode::Suppress);

        kernel.set_response_mode(ResponseMode::Send);
        assert_eq!(kernel.response_mode(), ResponseMode::Send);
    }

    #[tokio::test]
    async fn test_replay_tracks_prepare() {
        let mut kernel = create_kernel();
        kernel.set_response_mode(ResponseMode::Suppress);

        let txn_id = TransactionId::new();

        // Use consistent timestamps to avoid expiration
        let ts1 = Timestamp::from_micros(1000);
        let ts2 = Timestamp::from_micros(2000);
        let deadline = Timestamp::from_micros(1_000_000_000); // Far future

        // First, track begin
        let op_msg = OrderedMessage::TransactionOperation {
            txn_id,
            coordinator_id: "coord-1".to_string(),
            request_id: "req-1".to_string(),
            txn_deadline: deadline,
            operation: TestOp("write1".to_string()),
        };

        kernel
            .process_ordered(op_msg.into_message(), ts1, 1)
            .await
            .unwrap();

        // Then track prepare
        let ctrl_msg = OrderedMessage::<TestOp>::TransactionControl {
            txn_id,
            phase: TransactionPhase::Prepare(std::collections::HashMap::new()),
            coordinator_id: Some("coord-1".to_string()),
            request_id: Some("req-1".to_string()),
            txn_deadline: proven_common::Timestamp::now().add_micros(10_000_000),
        };

        kernel
            .process_ordered(ctrl_msg.into_message(), ts2, 2)
            .await
            .unwrap();

        // Transaction should exist and be in prepared state
        let state = kernel.transaction_manager().get(txn_id).unwrap();
        assert_eq!(state.phase, crate::transaction::TransactionPhase::Prepared);
    }

    #[tokio::test]
    async fn test_crash_recovery_active_transaction() {
        // Test that active transactions are recovered from persisted metadata

        // Create a test engine that simulates persisted metadata
        struct RecoveryTestEngine {
            operations: Vec<String>,
            metadata: Vec<(TransactionId, Vec<u8>)>,
        }

        impl TransactionEngine for RecoveryTestEngine {
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
                OperationResult::Complete(TestResponse("read".to_string()))
            }

            fn apply_operation(
                &mut self,
                _batch: &mut Self::Batch,
                operation: Self::Operation,
                _txn_id: TransactionId,
            ) -> OperationResult<Self::Response> {
                self.operations.push(operation.0.clone());
                OperationResult::Complete(TestResponse(format!("executed: {}", operation.0)))
            }

            fn begin(&mut self, _batch: &mut Self::Batch, _txn_id: TransactionId) {}
            fn prepare(&mut self, _batch: &mut Self::Batch, _txn_id: TransactionId) {}
            fn commit(&mut self, _batch: &mut Self::Batch, _txn_id: TransactionId) {}
            fn abort(&mut self, _batch: &mut Self::Batch, _txn_id: TransactionId) {}
            fn get_log_index(&self) -> Option<u64> {
                Some(42)
            }

            fn scan_transaction_metadata(&self) -> Vec<(TransactionId, Vec<u8>)> {
                // Return the persisted metadata
                self.metadata.clone()
            }

            fn engine_name(&self) -> &str {
                "recovery-test"
            }
        }

        // Create persisted transaction state
        let txn_id = TransactionId::new();
        let state = crate::transaction::TransactionState::<TestOp> {
            coordinator_id: "coord-1".to_string(),
            deadline: Timestamp::from_micros(10000),
            participants: std::collections::HashMap::new(),
            phase: crate::transaction::TransactionPhase::Active,
            deferred_operations: Vec::new(),
        };

        let metadata_bytes = state.to_bytes().unwrap();

        let engine = RecoveryTestEngine {
            operations: Vec::new(),
            metadata: vec![(txn_id, metadata_bytes)],
        };

        let mock_engine_for_client = Arc::new(proven_engine::MockEngine::new());
        let client = Arc::new(MockClient::new(
            "test-node".to_string(),
            mock_engine_for_client,
        ));

        let mut kernel = StreamProcessingKernel::new(engine, client, "test-stream".to_string());

        // Recover transaction state
        kernel.recover_transaction_state().unwrap();

        // Verify transaction was recovered
        assert!(kernel.transaction_manager().exists(txn_id));
        let recovered_state = kernel.transaction_manager().get(txn_id).unwrap();
        assert_eq!(recovered_state.coordinator_id, "coord-1");
        assert_eq!(
            recovered_state.phase,
            crate::transaction::TransactionPhase::Active
        );

        tokio::task::yield_now().await;
    }

    #[tokio::test]
    async fn test_crash_recovery_prepared_transaction() {
        // Test that prepared transactions with participants are recovered

        struct RecoveryTestEngine {
            metadata: Vec<(TransactionId, Vec<u8>)>,
        }

        impl TransactionEngine for RecoveryTestEngine {
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
                OperationResult::Complete(TestResponse("read".to_string()))
            }

            fn apply_operation(
                &mut self,
                _batch: &mut Self::Batch,
                operation: Self::Operation,
                _txn_id: TransactionId,
            ) -> OperationResult<Self::Response> {
                OperationResult::Complete(TestResponse(format!("executed: {}", operation.0)))
            }

            fn begin(&mut self, _batch: &mut Self::Batch, _txn_id: TransactionId) {}
            fn prepare(&mut self, _batch: &mut Self::Batch, _txn_id: TransactionId) {}
            fn commit(&mut self, _batch: &mut Self::Batch, _txn_id: TransactionId) {}
            fn abort(&mut self, _batch: &mut Self::Batch, _txn_id: TransactionId) {}
            fn get_log_index(&self) -> Option<u64> {
                Some(42)
            }

            fn scan_transaction_metadata(&self) -> Vec<(TransactionId, Vec<u8>)> {
                self.metadata.clone()
            }

            fn engine_name(&self) -> &str {
                "recovery-test"
            }
        }

        // Create prepared transaction with participants
        let txn_id = TransactionId::new();
        let mut participants = std::collections::HashMap::new();
        participants.insert("participant1".to_string(), 10);
        participants.insert("participant2".to_string(), 11);

        let state = crate::transaction::TransactionState::<TestOp> {
            coordinator_id: "coord-1".to_string(),
            deadline: Timestamp::from_micros(10000),
            participants: participants.clone(),
            phase: crate::transaction::TransactionPhase::Prepared,
            deferred_operations: Vec::new(),
        };

        let metadata_bytes = state.to_bytes().unwrap();

        let engine = RecoveryTestEngine {
            metadata: vec![(txn_id, metadata_bytes)],
        };

        let mock_engine_for_client = Arc::new(proven_engine::MockEngine::new());
        let client = Arc::new(MockClient::new(
            "test-node".to_string(),
            mock_engine_for_client,
        ));

        let mut kernel = StreamProcessingKernel::new(engine, client, "test-stream".to_string());

        // Recover transaction state
        kernel.recover_transaction_state().unwrap();

        // Verify prepared transaction was recovered with participants
        assert!(kernel.transaction_manager().exists(txn_id));
        let recovered_state = kernel.transaction_manager().get(txn_id).unwrap();
        assert_eq!(recovered_state.coordinator_id, "coord-1");
        assert_eq!(
            recovered_state.phase,
            crate::transaction::TransactionPhase::Prepared
        );

        let recovered_participants = kernel.transaction_manager().get_participants(txn_id);
        assert_eq!(recovered_participants.len(), 2);
        assert_eq!(recovered_participants.get("participant1"), Some(&10));
        assert_eq!(recovered_participants.get("participant2"), Some(&11));

        tokio::task::yield_now().await;
    }

    #[tokio::test]
    async fn test_crash_recovery_restores_completed_transactions() {
        // Test that committed/aborted transactions are restored to completed map
        // and deadline index is rebuilt

        struct RecoveryTestEngine {
            metadata: Vec<(TransactionId, Vec<u8>)>,
        }

        impl TransactionEngine for RecoveryTestEngine {
            type Operation = TestOp;
            type Response = TestResponse;
            type Batch = TestBatch;

            fn start_batch(&mut self) -> Self::Batch {
                TestBatch
            }

            fn commit_batch(&mut self, _batch: Self::Batch, _log_index: u64) {
                // In a real implementation, this would actually clean up the metadata
            }

            fn read_at_timestamp(
                &mut self,
                _operation: Self::Operation,
                _read_txn_id: TransactionId,
            ) -> OperationResult<Self::Response> {
                OperationResult::Complete(TestResponse("read".to_string()))
            }

            fn apply_operation(
                &mut self,
                _batch: &mut Self::Batch,
                operation: Self::Operation,
                _txn_id: TransactionId,
            ) -> OperationResult<Self::Response> {
                OperationResult::Complete(TestResponse(format!("executed: {}", operation.0)))
            }

            fn begin(&mut self, _batch: &mut Self::Batch, _txn_id: TransactionId) {}
            fn prepare(&mut self, _batch: &mut Self::Batch, _txn_id: TransactionId) {}
            fn commit(&mut self, _batch: &mut Self::Batch, _txn_id: TransactionId) {}
            fn abort(&mut self, _batch: &mut Self::Batch, _txn_id: TransactionId) {}
            fn get_log_index(&self) -> Option<u64> {
                Some(42)
            }

            fn scan_transaction_metadata(&self) -> Vec<(TransactionId, Vec<u8>)> {
                self.metadata.clone()
            }

            fn engine_name(&self) -> &str {
                "recovery-test"
            }
        }

        // Create committed and aborted transactions
        let committed_txn = TransactionId::new();
        let aborted_txn = TransactionId::new();

        let committed_state = crate::transaction::TransactionState::<TestOp> {
            coordinator_id: "coord-1".to_string(),
            deadline: Timestamp::from_micros(10000),
            participants: std::collections::HashMap::new(),
            phase: crate::transaction::TransactionPhase::Committed,
            deferred_operations: Vec::new(),
        };

        let aborted_state = crate::transaction::TransactionState::<TestOp> {
            coordinator_id: "coord-2".to_string(),
            deadline: Timestamp::from_micros(10000),
            participants: std::collections::HashMap::new(),
            phase: crate::transaction::TransactionPhase::Aborted {
                aborted_at: Timestamp::now(),
                reason: crate::transaction::AbortReason::Explicit,
            },
            deferred_operations: Vec::new(),
        };

        let engine = RecoveryTestEngine {
            metadata: vec![
                (committed_txn, committed_state.to_bytes().unwrap()),
                (aborted_txn, aborted_state.to_bytes().unwrap()),
            ],
        };

        let mock_engine_for_client = Arc::new(proven_engine::MockEngine::new());
        let client = Arc::new(MockClient::new(
            "test-node".to_string(),
            mock_engine_for_client,
        ));

        let mut kernel = StreamProcessingKernel::new(engine, client, "test-stream".to_string());

        // Recover transaction state
        kernel.recover_transaction_state().unwrap();

        // Verify completed transactions were restored to completed map
        assert!(kernel.transaction_manager().is_completed(committed_txn));
        assert!(kernel.transaction_manager().is_completed(aborted_txn));

        // Verify they exist in the system (any state including completed)
        assert!(kernel.transaction_manager().exists(committed_txn));
        assert!(kernel.transaction_manager().exists(aborted_txn));

        tokio::task::yield_now().await;
    }

    #[tokio::test]
    async fn test_crash_recovery_with_deferred_operations() {
        // Test that deferred operations are recovered after crash

        use crate::transaction::deferral::{DeferredOp, WaitingFor};
        use std::collections::HashSet;

        struct RecoveryTestEngine {
            metadata: Vec<(TransactionId, Vec<u8>)>,
        }

        impl TransactionEngine for RecoveryTestEngine {
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
                OperationResult::Complete(TestResponse("read".to_string()))
            }
            fn apply_operation(
                &mut self,
                _batch: &mut Self::Batch,
                operation: Self::Operation,
                _txn_id: TransactionId,
            ) -> OperationResult<Self::Response> {
                OperationResult::Complete(TestResponse(format!("executed: {}", operation.0)))
            }
            fn begin(&mut self, _batch: &mut Self::Batch, _txn_id: TransactionId) {}
            fn prepare(&mut self, _batch: &mut Self::Batch, _txn_id: TransactionId) {}
            fn commit(&mut self, _batch: &mut Self::Batch, _txn_id: TransactionId) {}
            fn abort(&mut self, _batch: &mut Self::Batch, _txn_id: TransactionId) {}
            fn get_log_index(&self) -> Option<u64> {
                Some(42)
            }
            fn scan_transaction_metadata(&self) -> Vec<(TransactionId, Vec<u8>)> {
                self.metadata.clone()
            }
            fn engine_name(&self) -> &str {
                "recovery-test"
            }
        }

        // Create a transaction with deferred operations
        let txn_id = TransactionId::new();
        let blocker_txn = TransactionId::new();

        // Create a deferred operation waiting on another transaction
        let mut waiting_for_complete = HashSet::new();
        waiting_for_complete.insert(blocker_txn);

        let deferred_op = DeferredOp::new(
            TestOp("deferred-write".to_string()),
            "coord-1".to_string(),
            "req-1".to_string(),
            txn_id,
            WaitingFor {
                prepare: HashSet::new(),
                complete: waiting_for_complete,
            },
            false,
        );

        let state = crate::transaction::TransactionState::<TestOp> {
            coordinator_id: "coord-1".to_string(),
            deadline: Timestamp::from_micros(1_000_000_000),
            participants: std::collections::HashMap::new(),
            phase: crate::transaction::TransactionPhase::Active,
            deferred_operations: vec![deferred_op],
        };

        let metadata_bytes = state.to_bytes().unwrap();

        let engine = RecoveryTestEngine {
            metadata: vec![(txn_id, metadata_bytes)],
        };

        let mock_engine_for_client = Arc::new(proven_engine::MockEngine::new());
        let client = Arc::new(MockClient::new(
            "test-node".to_string(),
            mock_engine_for_client,
        ));

        let mut kernel = StreamProcessingKernel::new(engine, client, "test-stream".to_string());

        // Recover transaction state
        kernel.recover_transaction_state().unwrap();

        // Verify transaction was recovered
        assert!(kernel.transaction_manager().exists(txn_id));

        // Verify deferred operation was recovered
        // The deferred operation should be waiting on blocker_txn
        let deferred_count = kernel.transaction_manager().deferred_count(txn_id);
        assert_eq!(deferred_count, 1, "Deferred operation should be recovered");

        tokio::task::yield_now().await;
    }
}
