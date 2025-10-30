//! Message dispatcher - routes messages by transaction mode
//!
//! The dispatcher is a pure routing layer that creates the appropriate executor
//! for each message and delegates to it.

use crate::engine::TransactionEngine;
use crate::error::Result;
use crate::executor::{AdHocExecutor, ReadOnlyExecutor, ReadWriteExecutor};
use crate::support::ResponseSender;
use crate::transaction::TransactionManager;
use proven_common::Timestamp;
use proven_protocol::{CoordinatorMessage, TransactionMode};

/// Routes messages to appropriate executors based on transaction mode
pub struct MessageDispatcher;

impl MessageDispatcher {
    /// Dispatch a message from the ordered stream
    pub fn dispatch_ordered<E: TransactionEngine>(
        engine: &mut E,
        tx_manager: &mut TransactionManager<E>,
        response: &ResponseSender,
        message: CoordinatorMessage<E::Operation>,
        timestamp: Timestamp,
        log_index: u64,
    ) -> Result<()> {
        match message {
            CoordinatorMessage::ReadOnly { .. } => {
                // Read-only should use pubsub, not ordered stream
                Err("Read-only messages should use pubsub, not ordered stream".into())
            }
            CoordinatorMessage::Operation(op) => {
                match op.mode {
                    TransactionMode::ReadOnly => {
                        // Shouldn't happen - read-only uses pubsub
                        Err("Read-only operations should use pubsub".into())
                    }
                    TransactionMode::AdHoc => {
                        // Ad-hoc: auto-begin, execute, auto-commit
                        let mut executor = AdHocExecutor::new(engine, tx_manager, response);
                        executor.execute(
                            op.operation,
                            op.coordinator_id,
                            op.request_id,
                            timestamp,
                            log_index,
                        )
                    }
                    TransactionMode::ReadWrite => {
                        // Two-phase commit transaction

                        // Ensure transaction is registered (needed for wound-wait)
                        // Check before creating executor to avoid borrow issues
                        if !tx_manager.exists(op.txn_id)
                            && let Some(deadline) = op.txn_deadline
                        {
                            // Begin the transaction
                            // Note: participants are sent with prepare, not begin
                            tx_manager.begin(
                                op.txn_id,
                                op.coordinator_id.clone(),
                                deadline,
                                std::collections::HashMap::new(), // Empty until prepare
                            );
                            engine.begin(op.txn_id, log_index);
                        }

                        let mut executor = ReadWriteExecutor::new(engine, tx_manager, response);
                        executor.execute(
                            op.operation,
                            op.txn_id,
                            op.coordinator_id,
                            op.request_id,
                            log_index,
                        )
                    }
                }
            }
            CoordinatorMessage::Control(ctrl) => {
                // All control messages go to ReadWriteExecutor
                match ctrl.phase {
                    proven_protocol::TransactionPhase::Prepare(participants) => {
                        // Prepare phase with participants
                        let mut executor = ReadWriteExecutor::new(engine, tx_manager, response);
                        executor.prepare(
                            ctrl.txn_id,
                            ctrl.coordinator_id.unwrap_or_default(),
                            ctrl.request_id.unwrap_or_default(),
                            participants,
                            log_index,
                        )
                    }
                    proven_protocol::TransactionPhase::PrepareAndCommit => {
                        // Optimization for single-participant transactions
                        let mut executor = ReadWriteExecutor::new(engine, tx_manager, response);
                        executor.prepare(
                            ctrl.txn_id,
                            ctrl.coordinator_id.clone().unwrap_or_default(),
                            ctrl.request_id.clone().unwrap_or_default(),
                            std::collections::HashMap::new(), // No participants for single-participant optimization
                            log_index,
                        )?;
                        executor.commit(
                            ctrl.txn_id,
                            ctrl.coordinator_id.unwrap_or_default(),
                            ctrl.request_id.unwrap_or_default(),
                            log_index,
                        )
                    }
                    proven_protocol::TransactionPhase::Commit => {
                        let mut executor = ReadWriteExecutor::new(engine, tx_manager, response);
                        executor.commit(
                            ctrl.txn_id,
                            ctrl.coordinator_id.unwrap_or_default(),
                            ctrl.request_id.unwrap_or_default(),
                            log_index,
                        )
                    }
                    proven_protocol::TransactionPhase::Abort => {
                        let mut executor = ReadWriteExecutor::new(engine, tx_manager, response);
                        executor.abort(
                            ctrl.txn_id,
                            ctrl.coordinator_id.unwrap_or_default(),
                            ctrl.request_id.unwrap_or_default(),
                            log_index,
                        )
                    }
                }
            }
        }
    }

    /// Dispatch a message from pubsub (read-only operations)
    pub fn dispatch_readonly<E: TransactionEngine>(
        engine: &mut E,
        tx_manager: &mut TransactionManager<E>,
        response: &ResponseSender,
        message: CoordinatorMessage<E::Operation>,
    ) -> Result<()> {
        match message {
            CoordinatorMessage::ReadOnly {
                read_timestamp,
                coordinator_id,
                request_id,
                operation,
            } => {
                let mut executor = ReadOnlyExecutor::new(engine, tx_manager, response);
                executor.execute(operation, read_timestamp, coordinator_id, request_id)
            }
            _ => Err("Only read-only messages allowed on pubsub channel".into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::OperationResult;
    use proven_common::{Operation, OperationType, Response, TransactionId};
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

    struct TestEngine {
        operations: Vec<(String, TransactionId)>,
        begun: Vec<TransactionId>,
        committed: Vec<TransactionId>,
    }

    impl TransactionEngine for TestEngine {
        type Operation = TestOp;
        type Response = TestResponse;

        fn read_at_timestamp(
            &mut self,
            operation: Self::Operation,
            read_txn_id: TransactionId,
        ) -> OperationResult<Self::Response> {
            self.operations.push((operation.0.clone(), read_txn_id));
            OperationResult::Complete(TestResponse(format!("read: {}", operation.0)))
        }

        fn apply_operation(
            &mut self,
            operation: Self::Operation,
            txn_id: TransactionId,
            _log_index: u64,
        ) -> OperationResult<Self::Response> {
            self.operations.push((operation.0.clone(), txn_id));
            OperationResult::Complete(TestResponse(format!("executed: {}", operation.0)))
        }

        fn begin(&mut self, txn_id: TransactionId, _log_index: u64) {
            self.begun.push(txn_id);
        }

        fn prepare(&mut self, _txn_id: TransactionId, _log_index: u64) {}
        fn commit(&mut self, txn_id: TransactionId, _log_index: u64) {
            self.committed.push(txn_id);
        }
        fn abort(&mut self, _txn_id: TransactionId, _log_index: u64) {}
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
            operations: Vec::new(),
            begun: Vec::new(),
            committed: Vec::new(),
        };
        let tx_manager = TransactionManager::new();
        let response = ResponseSender::new(client, "test-stream".to_string(), "test".to_string());

        (engine, tx_manager, response)
    }

    #[tokio::test]
    async fn test_dispatch_adhoc() {
        let (mut engine, mut tx_manager, response) = setup();

        let msg = CoordinatorMessage::Operation(proven_protocol::OperationMessage {
            txn_id: TransactionId::new(),
            coordinator_id: "coord-1".to_string(),
            request_id: "req-1".to_string(),
            txn_deadline: None,
            mode: TransactionMode::AdHoc,
            operation: TestOp("write1".to_string()),
        });

        let result = MessageDispatcher::dispatch_ordered(
            &mut engine,
            &mut tx_manager,
            &response,
            msg,
            Timestamp::now(),
            1,
        );

        assert!(result.is_ok());
        assert_eq!(engine.operations.len(), 1);
        assert_eq!(engine.operations[0].0, "write1");
        assert_eq!(engine.begun.len(), 1);
        assert_eq!(engine.committed.len(), 1);

        tokio::task::yield_now().await;
    }

    #[tokio::test]
    async fn test_dispatch_readonly() {
        let (mut engine, mut tx_manager, response) = setup();

        let msg = CoordinatorMessage::ReadOnly {
            read_timestamp: TransactionId::new(),
            coordinator_id: "coord-1".to_string(),
            request_id: "req-1".to_string(),
            operation: TestOp("read1".to_string()),
        };

        let result =
            MessageDispatcher::dispatch_readonly(&mut engine, &mut tx_manager, &response, msg);

        assert!(result.is_ok());
        assert_eq!(engine.operations.len(), 1);
        assert_eq!(engine.operations[0].0, "read1");

        tokio::task::yield_now().await;
    }
}
