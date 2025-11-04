//! Stream processor - main processing loop
//!
//! Manages stream consumption, phase transitions, and delegates message
//! processing to the flow layer.

use crate::engine::TransactionEngine;
use crate::error::{Error, Result};
use crate::flow::{OrderedFlow, ReadOnlyFlow};
use crate::support::ResponseSender;
use crate::transaction::TransactionManager;
use proven_common::Timestamp;
use proven_engine::{Message, MockClient};
use proven_protocol::{OrderedMessage, ReadOnlyMessage};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;

/// Processing phases
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessorPhase {
    /// Replaying from snapshot - execute operations but suppress responses
    Replay,
    /// Live processing - normal operation with responses
    Live,
}

/// Stream processor for distributed transactions
pub struct StreamProcessor<E: TransactionEngine> {
    /// Transaction engine
    engine: E,

    /// Client for stream/pubsub
    client: Arc<MockClient>,

    /// Transaction manager
    tx_manager: TransactionManager<E>,

    /// Recovery manager for handling coordinator failures
    recovery_manager: crate::transaction::RecoveryManager,

    /// Response sender
    response: ResponseSender,

    /// Current processing phase
    phase: ProcessorPhase,

    /// Stream name
    stream_name: String,

    /// Current offset in the stream
    current_offset: u64,
}

impl<E: TransactionEngine> StreamProcessor<E> {
    /// Create a new stream processor
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
            "[{}] Engine is at log index {}, will replay from offset {}",
            stream_name,
            engine_offset,
            engine_offset + 1
        );

        Self {
            engine,
            recovery_manager: crate::transaction::RecoveryManager::new(
                client.clone(),
                stream_name.clone(),
            ),
            client,
            tx_manager: TransactionManager::new(),
            response,
            phase: ProcessorPhase::Replay, // Always start in replay
            stream_name,
            current_offset: engine_offset,
        }
    }

    /// Get current processing phase
    pub fn phase(&self) -> ProcessorPhase {
        self.phase
    }

    /// Transition to a new phase
    pub fn transition_to(&mut self, phase: ProcessorPhase) {
        tracing::info!(
            "[{}] Phase transition: {:?} -> {:?}",
            self.stream_name,
            self.phase,
            phase
        );
        self.phase = phase;
    }

    /// Get current offset
    pub fn current_offset(&self) -> u64 {
        self.current_offset
    }

    /// Process a message from the ordered stream
    async fn process_ordered(
        &mut self,
        message: Message,
        timestamp: Timestamp,
        offset: u64,
    ) -> Result<()> {
        // Update offset
        self.current_offset = offset;

        // Both replay and live use same processing path now
        // Phase is passed to dispatcher to control response suppression
        self.process_live_ordered(message, timestamp, offset).await
    }

    /// Process a read-only message from pubsub
    fn process_readonly(&mut self, message: Message) -> Result<()> {
        // Parse message
        let coord_msg = ReadOnlyMessage::from_message(message)
            .map_err(|e| Error::InvalidOperation(e.to_string()))?;

        // Use read-only flow
        ReadOnlyFlow::process(
            &mut self.engine,
            &mut self.tx_manager,
            &self.response,
            coord_msg,
        )
    }

    /// Process a live ordered message
    async fn process_live_ordered(
        &mut self,
        message: Message,
        timestamp: Timestamp,
        offset: u64,
    ) -> Result<()> {
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
            self.phase, // Pass current phase (Replay or Live)
        )
        .await
    }

    /// Run recovery for expired transactions (both active and prepared)
    pub async fn run_recovery(&mut self, current_time: Timestamp) -> Result<()> {
        // Check both active and prepared transactions for expiry
        let mut expired_active = self.tx_manager.get_expired_active(current_time);
        let expired_prepared = self.tx_manager.get_expired_prepared(current_time);

        expired_active.extend(expired_prepared);
        let expired = expired_active;

        if !expired.is_empty() {
            tracing::info!(
                "[{}] Running recovery for {} expired transactions",
                self.stream_name,
                expired.len()
            );
        }

        for txn_id in expired {
            // Get participants to determine recovery decision
            let participants = self.tx_manager.get_participants(txn_id);

            if participants.is_empty() {
                // No participants - just abort locally
                tracing::debug!(
                    "[{}] Aborting expired transaction {} (no participants)",
                    self.stream_name,
                    txn_id
                );

                // Publish ABORT message to stream (so it gets logged and processed normally)
                let mut headers = std::collections::HashMap::new();
                headers.insert("txn_id".to_string(), txn_id.to_string());
                headers.insert("txn_phase".to_string(), "abort".to_string());
                headers.insert("recovery".to_string(), "true".to_string());

                let message = Message::new(Vec::new(), headers);
                if let Err(e) = self
                    .client
                    .publish_to_stream(self.stream_name.clone(), vec![message])
                    .await
                {
                    tracing::error!(
                        "[{}] Failed to publish recovery ABORT: {:?}",
                        self.stream_name,
                        e
                    );
                } else {
                    // Mark as resolved to prevent duplicate recovery attempts
                    let _ = self.tx_manager.transition_to_aborted(
                        txn_id,
                        crate::transaction::AbortReason::DeadlineExceeded,
                    );
                }
            } else {
                // Query participants to determine decision
                tracing::debug!(
                    "[{}] Recovering transaction {} with {} participants",
                    self.stream_name,
                    txn_id,
                    participants.len()
                );

                let decision = self
                    .recovery_manager
                    .recover(txn_id, &participants, current_time)
                    .await;

                match decision {
                    crate::transaction::TransactionDecision::Commit => {
                        tracing::info!(
                            "[{}] Recovery decision: COMMIT for transaction {}",
                            self.stream_name,
                            txn_id
                        );

                        // Publish COMMIT message to stream (so it gets logged and processed normally)
                        let mut headers = std::collections::HashMap::new();
                        headers.insert("txn_id".to_string(), txn_id.to_string());
                        headers.insert("txn_phase".to_string(), "commit".to_string());
                        headers.insert("recovery".to_string(), "true".to_string());

                        let message = Message::new(Vec::new(), headers);
                        if let Err(e) = self
                            .client
                            .publish_to_stream(self.stream_name.clone(), vec![message])
                            .await
                        {
                            tracing::error!(
                                "[{}] Failed to publish recovery COMMIT: {:?}",
                                self.stream_name,
                                e
                            );
                        } else {
                            tracing::debug!(
                                "[{}] Published recovery COMMIT message for transaction {}",
                                self.stream_name,
                                txn_id
                            );
                            // Mark as resolved to prevent duplicate recovery attempts
                            // The actual commit will be processed when the message comes through
                            let _ = self.tx_manager.transition_to_committed(txn_id);
                        }
                    }
                    crate::transaction::TransactionDecision::Abort
                    | crate::transaction::TransactionDecision::Unknown => {
                        tracing::info!(
                            "[{}] Recovery decision: ABORT for transaction {} (decision: {:?})",
                            self.stream_name,
                            txn_id,
                            decision
                        );

                        // Publish ABORT message to stream (so it gets logged and processed normally)
                        let mut headers = std::collections::HashMap::new();
                        headers.insert("txn_id".to_string(), txn_id.to_string());
                        headers.insert("txn_phase".to_string(), "abort".to_string());
                        headers.insert("recovery".to_string(), "true".to_string());

                        let message = Message::new(Vec::new(), headers);
                        if let Err(e) = self
                            .client
                            .publish_to_stream(self.stream_name.clone(), vec![message])
                            .await
                        {
                            tracing::error!(
                                "[{}] Failed to publish recovery ABORT: {:?}",
                                self.stream_name,
                                e
                            );
                        } else {
                            tracing::debug!(
                                "[{}] Published recovery ABORT message for transaction {}",
                                self.stream_name,
                                txn_id
                            );
                            // Mark as resolved to prevent duplicate recovery attempts
                            let _ = self.tx_manager.transition_to_aborted(
                                txn_id,
                                crate::transaction::AbortReason::Recovery,
                            );
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Get reference to transaction manager
    #[cfg(test)]
    fn transaction_manager(&self) -> &TransactionManager<E> {
        &self.tx_manager
    }

    /// Get reference to engine
    #[cfg(test)]
    fn engine(&self) -> &E {
        &self.engine
    }

    /// Recover transaction state from persisted metadata (called on startup)
    ///
    /// This method scans all persisted transaction metadata and rebuilds
    /// the TransactionManager's in-memory state. This is critical for:
    /// - Restoring active transactions with their coordinator information
    /// - Restoring prepared transactions with their participant lists
    /// - Cleaning up stale metadata from completed transactions
    fn recover_transaction_state(&mut self) -> Result<()> {
        use crate::engine::BatchOperations;
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
        let mut cleaned_stale = 0;

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

                        crate::transaction::TransactionPhase::Committed
                        | crate::transaction::TransactionPhase::Aborted { .. } => {
                            // Stale metadata - should have been cleaned up but wasn't
                            // Clean it up now
                            cleaned_stale += 1;
                            tracing::debug!(
                                "[{}] Cleaning up stale transaction {} ({:?})",
                                self.stream_name,
                                txn_id,
                                state.phase
                            );

                            let mut batch = self.engine.start_batch();
                            let metadata_key =
                                crate::transaction::TransactionState::<E::Operation>::metadata_key(
                                    txn_id,
                                );
                            batch.remove_metadata(metadata_key);

                            let log_index = self.engine.get_log_index().unwrap_or(0);
                            self.engine.commit_batch(batch, log_index);
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
            "[{}] Recovery complete: {} Active, {} Prepared, {} stale cleaned",
            self.stream_name,
            recovered_active,
            recovered_prepared,
            cleaned_stale
        );

        Ok(())
    }

    /// Start the processor with replay
    pub async fn start_with_replay(mut self, shutdown_rx: oneshot::Receiver<()>) -> Result<()>
    where
        E: Send + 'static,
    {
        // Step 1: Recover persisted transaction state from metadata
        // This rebuilds TransactionManager state from crash
        self.recover_transaction_state()?;

        // Step 2: Perform replay phase to catch up with stream
        // This rebuilds deferred operation queue by replaying messages
        self.perform_replay().await?;

        // Step 3: Transition to live processing
        self.transition_to(ProcessorPhase::Live);
        self.run_live_processing(shutdown_rx).await
    }

    /// Perform replay from engine's current position to stream head
    async fn perform_replay(&mut self) -> Result<()> {
        let start_offset = self.current_offset + 1;

        tracing::info!(
            "[{}] Starting replay from offset {}",
            self.stream_name,
            start_offset
        );

        // Suppress responses during replay (coordinators are long gone)
        self.response.set_suppress(true);

        // Subscribe to stream from where engine left off
        let mut replay_stream = self
            .client
            .stream_messages(self.stream_name.clone(), Some(start_offset))
            .await
            .map_err(|e| Error::EngineError(e.to_string()))?;

        let mut count = 0;
        let start_time = std::time::Instant::now();

        // Process messages until we catch up
        loop {
            tokio::select! {
                result = replay_stream.recv() => {
                    match result {
                        Some((message, timestamp, offset)) => {
                            // Process in replay mode (tracks state without full execution)
                            if let Err(e) = self.process_ordered(message, timestamp, offset).await {
                                tracing::error!(
                                    "[{}] Error during replay at offset {}: {:?}",
                                    self.stream_name, offset, e
                                );
                            }
                            count += 1;

                            // Log progress every 10k messages
                            if count % 10_000 == 0 {
                                tracing::info!(
                                    "[{}] Replay progress: {} messages, at offset {}",
                                    self.stream_name, count, offset
                                );
                            }
                        }
                        None => {
                            // Stream ended (caught up)
                            break;
                        }
                    }
                }

                // Add a timeout check - if we haven't received a message in 1 second,
                // assume we're caught up
                _ = tokio::time::sleep(Duration::from_secs(1)) => {
                    tracing::debug!(
                        "[{}] No messages for 1s during replay, assuming caught up",
                        self.stream_name
                    );
                    break;
                }
            }
        }

        let elapsed = start_time.elapsed();
        tracing::info!(
            "[{}] Replay complete. Processed {} messages in {:.2}s, now at offset {}",
            self.stream_name,
            count,
            elapsed.as_secs_f64(),
            self.current_offset
        );

        // Re-enable responses for live processing
        self.response.set_suppress(false);

        // Note: Recovery now happens in dispatcher's 8-step flow
        // No need for explicit recovery call here

        Ok(())
    }

    /// Run live processing with event loop
    async fn run_live_processing(mut self, mut shutdown_rx: oneshot::Receiver<()>) -> Result<()> {
        tracing::info!(
            "[{}] Starting live processing from offset {}",
            self.stream_name,
            self.current_offset
        );

        // Subscribe to ordered stream (start from next offset after engine's position)
        let start_offset = if self.current_offset > 0 {
            Some(self.current_offset + 1)
        } else {
            Some(0)
        };

        let mut ordered_stream = self
            .client
            .stream_messages(self.stream_name.clone(), start_offset)
            .await
            .map_err(|e| Error::EngineError(e.to_string()))?;

        // Subscribe to readonly pubsub (ReadOnly + AdHoc reads)
        let mut readonly_stream = self
            .client
            .subscribe(&format!("stream.{}.readonly", self.stream_name), None)
            .await
            .map_err(|e| Error::EngineError(e.to_string()))?;

        // Idle detection for noop injection (check every 100ms)
        let mut idle_check = tokio::time::interval(Duration::from_millis(100));
        idle_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                // Prioritize shutdown and ordered stream
                biased;

                // Shutdown signal
                _ = &mut shutdown_rx => {
                    tracing::info!("[{}] Shutdown signal received", self.stream_name);
                    break;
                }

                // Ordered stream messages (ReadWrite + AdHoc writes)
                result = ordered_stream.recv() => {
                    match result {
                        Some((message, timestamp, offset)) => {
                            if let Err(e) = self.process_ordered(message, timestamp, offset).await {
                                tracing::error!(
                                    "[{}] Error processing ordered message at offset {}: {:?}",
                                    self.stream_name, offset, e
                                );
                            }
                        }
                        None => {
                            tracing::warn!("[{}] Ordered stream ended", self.stream_name);
                            break;
                        }
                    }
                }

                // Unordered pubsub messages (ReadOnly + AdHoc reads)
                Some(message) = readonly_stream.recv() => {
                    if let Err(e) = self.process_readonly(message) {
                        tracing::error!(
                            "[{}] Error processing readonly message: {:?}",
                            self.stream_name, e
                        );
                    }
                }

                // Idle detection for noop injection
                // NOTE: Recovery and GC now happen in dispatcher's 8-step flow
                // We just need to inject noops when stream is idle with expired transactions
                _ = idle_check.tick() => {
                    let now = Timestamp::now();

                    // Check if there are transactions that need processing (expired or GC-able)
                    if self.tx_manager.needs_processing(now) {
                        // Stream is idle with expired transactions - inject a noop
                        tracing::debug!(
                            "[{}] Injecting noop message to trigger recovery/GC",
                            self.stream_name
                        );

                        let noop_msg = OrderedMessage::<E::Operation>::Noop;
                        if let Err(e) = self.client.publish_to_stream(
                            self.stream_name.clone(),
                            vec![noop_msg.into_message()],
                        ).await {
                            tracing::warn!(
                                "[{}] Failed to inject noop: {:?}",
                                self.stream_name, e
                            );
                        }
                    }
                }
            }
        }

        tracing::info!("[{}] Live processing stopped", self.stream_name);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{OperationResult, TransactionEngine};
    use proven_common::{Operation, OperationType, Response, TransactionId};
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
        fn insert_metadata(&mut self, _key: Vec<u8>, _value: Vec<u8>) {}
        fn remove_metadata(&mut self, _key: Vec<u8>) {}
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

    fn create_processor() -> StreamProcessor<TestEngine> {
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

        let mut processor = StreamProcessor::new(engine, client, "test-stream".to_string());

        // For tests, start in Live phase directly (skip replay)
        processor.phase = ProcessorPhase::Live;
        processor
    }

    #[tokio::test]
    async fn test_process_adhoc_operation() {
        let mut processor = create_processor();

        let op_msg = OrderedMessage::AutoCommitOperation {
            txn_id: TransactionId::new(),
            coordinator_id: "coord-1".to_string(),
            request_id: "req-1".to_string(),
            txn_deadline: Timestamp::now().add_micros(10_000_000),
            operation: TestOp("write1".to_string()),
        };

        let message = op_msg.into_message();
        let result = processor
            .process_ordered(message, Timestamp::now(), 1)
            .await;

        assert!(result.is_ok());
        assert_eq!(processor.engine().operations.len(), 1);
        assert_eq!(processor.engine().operations[0], "write1");
        assert_eq!(processor.current_offset(), 1);

        tokio::task::yield_now().await;
    }

    #[tokio::test]
    async fn test_replay_executes_operations() {
        let mut processor = create_processor();
        processor.transition_to(ProcessorPhase::Replay);

        let txn_id = TransactionId::new();
        let op_msg = OrderedMessage::TransactionOperation {
            txn_id,
            coordinator_id: "coord-1".to_string(),
            request_id: "req-1".to_string(),
            txn_deadline: Timestamp::from_micros(10000),
            operation: TestOp("write1".to_string()),
        };

        let message = op_msg.into_message();
        let result = processor
            .process_ordered(message, Timestamp::now(), 1)
            .await;

        assert!(result.is_ok());

        // During replay, operations ARE executed to rebuild engine state
        assert_eq!(processor.engine().operations.len(), 1);
        assert_eq!(processor.engine().operations[0], "write1");

        // Transaction should be tracked
        assert!(processor.transaction_manager().exists(txn_id));

        tokio::task::yield_now().await;
    }

    #[tokio::test]
    async fn test_phase_transitions() {
        let mut processor = create_processor();

        assert_eq!(processor.phase(), ProcessorPhase::Live);

        processor.transition_to(ProcessorPhase::Replay);
        assert_eq!(processor.phase(), ProcessorPhase::Replay);

        processor.transition_to(ProcessorPhase::Live);
        assert_eq!(processor.phase(), ProcessorPhase::Live);
    }

    #[tokio::test]
    async fn test_replay_tracks_prepare() {
        let mut processor = create_processor();
        processor.transition_to(ProcessorPhase::Replay);

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

        processor
            .process_ordered(op_msg.into_message(), ts1, 1)
            .await
            .unwrap();

        // Then track prepare
        let ctrl_msg = OrderedMessage::<TestOp>::TransactionControl {
            txn_id,
            phase: TransactionPhase::Prepare(std::collections::HashMap::new()),
            coordinator_id: Some("coord-1".to_string()),
            request_id: Some("req-1".to_string()),
        };

        processor
            .process_ordered(ctrl_msg.into_message(), ts2, 2)
            .await
            .unwrap();

        // Transaction should exist and be in prepared state
        let state = processor.transaction_manager().get(txn_id).unwrap();
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

        let mut processor = StreamProcessor::new(engine, client, "test-stream".to_string());

        // Recover transaction state
        processor.recover_transaction_state().unwrap();

        // Verify transaction was recovered
        assert!(processor.transaction_manager().exists(txn_id));
        let recovered_state = processor.transaction_manager().get(txn_id).unwrap();
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

        let mut processor = StreamProcessor::new(engine, client, "test-stream".to_string());

        // Recover transaction state
        processor.recover_transaction_state().unwrap();

        // Verify prepared transaction was recovered with participants
        assert!(processor.transaction_manager().exists(txn_id));
        let recovered_state = processor.transaction_manager().get(txn_id).unwrap();
        assert_eq!(recovered_state.coordinator_id, "coord-1");
        assert_eq!(
            recovered_state.phase,
            crate::transaction::TransactionPhase::Prepared
        );

        let recovered_participants = processor.transaction_manager().get_participants(txn_id);
        assert_eq!(recovered_participants.len(), 2);
        assert_eq!(recovered_participants.get("participant1"), Some(&10));
        assert_eq!(recovered_participants.get("participant2"), Some(&11));

        tokio::task::yield_now().await;
    }

    #[tokio::test]
    async fn test_crash_recovery_cleans_stale_metadata() {
        // Test that committed/aborted transactions are cleaned up
        // (not added back to manager, and cleanup batch is created)

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

        let mut processor = StreamProcessor::new(engine, client, "test-stream".to_string());

        // Recover transaction state
        processor.recover_transaction_state().unwrap();

        // Verify stale transactions were NOT added to transaction manager
        // (the important invariant - they should be cleaned up, not restored)
        assert!(!processor.transaction_manager().exists(committed_txn));
        assert!(!processor.transaction_manager().exists(aborted_txn));

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

        let mut processor = StreamProcessor::new(engine, client, "test-stream".to_string());

        // Recover transaction state
        processor.recover_transaction_state().unwrap();

        // Verify transaction was recovered
        assert!(processor.transaction_manager().exists(txn_id));

        // Verify deferred operation was recovered
        // The deferred operation should be waiting on blocker_txn
        let deferred_count = processor.transaction_manager().deferred_count(txn_id);
        assert_eq!(deferred_count, 1, "Deferred operation should be recovered");

        tokio::task::yield_now().await;
    }
}
