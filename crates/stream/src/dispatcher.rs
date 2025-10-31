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
    ///
    /// CRITICAL: Creates ONE batch for the entire message processing.
    /// All operations triggered by this message (including deferred ops that become ready)
    /// are accumulated in this batch and committed atomically at the end.
    pub fn dispatch_ordered<E: TransactionEngine>(
        engine: &mut E,
        tx_manager: &mut TransactionManager<E>,
        response: &ResponseSender,
        message: CoordinatorMessage<E::Operation>,
        timestamp: Timestamp,
        log_index: u64,
    ) -> Result<()> {
        // ═══════════════════════════════════════════════════════════
        // ONE BATCH FOR ENTIRE MESSAGE
        // ═══════════════════════════════════════════════════════════
        let mut batch = engine.start_batch();

        // Process the message and accumulate all operations in batch
        Self::dispatch_message(engine, tx_manager, response, &mut batch, message, timestamp)?;

        // ═══════════════════════════════════════════════════════════
        // SINGLE ATOMIC COMMIT
        // ═══════════════════════════════════════════════════════════
        engine.commit_batch(batch, log_index);

        Ok(())
    }

    /// Internal message routing (batch is passed through)
    fn dispatch_message<E: TransactionEngine>(
        engine: &mut E,
        tx_manager: &mut TransactionManager<E>,
        response: &ResponseSender,
        batch: &mut E::Batch,
        message: CoordinatorMessage<E::Operation>,
        timestamp: Timestamp,
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
                            batch,
                            op.operation,
                            op.coordinator_id,
                            op.request_id,
                            timestamp,
                        )
                    }
                    TransactionMode::ReadWrite => {
                        // Two-phase commit transaction

                        // Ensure transaction is registered (needed for wound-wait)
                        // Check before creating executor to avoid borrow issues
                        if !tx_manager.exists(op.txn_id)
                            && let Some(deadline) = op.txn_deadline
                        {
                            // Register transaction in manager
                            // Note: participants are sent with prepare, not begin
                            tx_manager.begin(
                                op.txn_id,
                                op.coordinator_id.clone(),
                                deadline,
                                std::collections::HashMap::new(), // Empty until prepare
                            );

                            // Also begin in engine (add to active transactions)
                            engine.begin(batch, op.txn_id);
                        }

                        let mut executor = ReadWriteExecutor::new(engine, tx_manager, response);
                        executor.execute(
                            batch,
                            op.operation,
                            op.txn_id,
                            op.coordinator_id,
                            op.request_id,
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
                            batch,
                            ctrl.txn_id,
                            ctrl.coordinator_id.unwrap_or_default(),
                            ctrl.request_id.unwrap_or_default(),
                            participants,
                        )
                    }
                    proven_protocol::TransactionPhase::PrepareAndCommit => {
                        // Optimization for single-participant transactions
                        let mut executor = ReadWriteExecutor::new(engine, tx_manager, response);
                        executor.prepare(
                            batch,
                            ctrl.txn_id,
                            ctrl.coordinator_id.clone().unwrap_or_default(),
                            ctrl.request_id.clone().unwrap_or_default(),
                            std::collections::HashMap::new(), // No participants for single-participant optimization
                        )?;
                        executor.commit(
                            batch,
                            ctrl.txn_id,
                            ctrl.coordinator_id.unwrap_or_default(),
                            ctrl.request_id.unwrap_or_default(),
                        )
                    }
                    proven_protocol::TransactionPhase::Commit => {
                        let mut executor = ReadWriteExecutor::new(engine, tx_manager, response);
                        executor.commit(
                            batch,
                            ctrl.txn_id,
                            ctrl.coordinator_id.unwrap_or_default(),
                            ctrl.request_id.unwrap_or_default(),
                        )
                    }
                    proven_protocol::TransactionPhase::Abort => {
                        let mut executor = ReadWriteExecutor::new(engine, tx_manager, response);
                        executor.abort(
                            batch,
                            ctrl.txn_id,
                            ctrl.coordinator_id.unwrap_or_default(),
                            ctrl.request_id.unwrap_or_default(),
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
