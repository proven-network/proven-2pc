//! Ordered flow - 8-step deterministic processing for ordered stream messages
//!
//! This module implements the core processing flow for all ordered messages:
//! 1. Create batch
//! 2. Abort expired active transactions
//! 3. Recover expired prepared transactions
//! 4. GC completed transactions past deadline
//! 5. LOOP: Process unblocked deferrals (age order)
//! 6. Validate & process original message
//! 7. LOOP: Process newly unblocked deferrals (age order)
//! 8. Commit batch atomically

use crate::engine::{BatchOperations, TransactionEngine};
use crate::error::Result;
use crate::executor::context::ExecutionContext;
use crate::executor::{AdHocExecution, ReadWriteExecution};
use crate::processor::ProcessorPhase;
use crate::transaction::{AbortReason, RecoveryManager, TransactionDecision, TransactionState};
use proven_common::{Timestamp, TransactionId};
use proven_protocol::{CoordinatorMessage, TransactionMode};
use std::collections::HashMap;

/// Orchestrates the 8-step deterministic flow for ordered messages
pub struct OrderedFlow;

impl OrderedFlow {
    /// Process ordered message with full 8-step deterministic flow
    ///
    /// This is the CORE of stream processing. Every ordered message goes through
    /// all 8 steps to ensure deterministic, atomic, and recoverable execution.
    #[allow(clippy::too_many_arguments)]
    pub async fn process<E: TransactionEngine>(
        engine: &mut E,
        tx_manager: &mut crate::transaction::TransactionManager<E>,
        recovery_manager: &RecoveryManager,
        response: &crate::support::ResponseSender,
        message: CoordinatorMessage<E::Operation>,
        timestamp: Timestamp,
        log_index: u64,
        phase: ProcessorPhase,
    ) -> Result<()> {
        // ═══════════════════════════════════════════════════════════
        // STEP 1: CREATE BATCH
        // ═══════════════════════════════════════════════════════════
        let mut batch = engine.start_batch();

        // ═══════════════════════════════════════════════════════════
        // STEP 2: ABORT EXPIRED ACTIVE TRANSACTIONS
        // ═══════════════════════════════════════════════════════════
        let mut expired_active = tx_manager.get_expired_active(timestamp);
        expired_active.sort(); // Deterministic order

        let mut ctx = ExecutionContext::new(engine, tx_manager, response);
        for txn_id in expired_active {
            ctx.abort_transaction(&mut batch, txn_id, AbortReason::DeadlineExceeded, phase)?;
        }

        // ═══════════════════════════════════════════════════════════
        // STEP 3: RECOVER EXPIRED PREPARED TRANSACTIONS
        // ═══════════════════════════════════════════════════════════
        let mut expired_prepared = ctx.tx_manager.get_expired_prepared(timestamp);
        expired_prepared.sort(); // Deterministic order

        for txn_id in expired_prepared {
            Self::recover_prepared_transaction(
                &mut batch,
                &mut ctx,
                recovery_manager,
                txn_id,
                timestamp,
                phase,
            )
            .await?;
        }

        // ═══════════════════════════════════════════════════════════
        // STEP 4: GC COMPLETED TRANSACTIONS PAST DEADLINE
        // ═══════════════════════════════════════════════════════════
        let to_gc = ctx.tx_manager.gc_completed_by_deadline(timestamp);
        for txn_id in to_gc {
            let metadata_key = TransactionState::<E::Operation>::metadata_key(txn_id);
            batch.remove_metadata(metadata_key);
        }

        // ═══════════════════════════════════════════════════════════
        // STEP 5: LOOP - PROCESS UNBLOCKED DEFERRALS (AGE ORDER)
        // ═══════════════════════════════════════════════════════════
        loop {
            let ready_ops = ctx.tx_manager.take_all_ready_deferrals_sorted();
            if ready_ops.is_empty() {
                break;
            }

            for deferred in ready_ops {
                ctx.execute_deferred(&mut batch, deferred, phase)?;
            }
        }

        // ═══════════════════════════════════════════════════════════
        // STEP 6: VALIDATE & PROCESS ORIGINAL MESSAGE
        // ═══════════════════════════════════════════════════════════
        if !matches!(message, CoordinatorMessage::Noop) {
            Self::dispatch_message(&mut ctx, &mut batch, message, phase)?;
        }

        // ═══════════════════════════════════════════════════════════
        // STEP 7: LOOP - PROCESS NEWLY UNBLOCKED DEFERRALS (AGE ORDER)
        // ═══════════════════════════════════════════════════════════
        loop {
            let ready_ops = ctx.tx_manager.take_all_ready_deferrals_sorted();
            if ready_ops.is_empty() {
                break;
            }

            for deferred in ready_ops {
                ctx.execute_deferred(&mut batch, deferred, phase)?;
            }
        }

        // ═══════════════════════════════════════════════════════════
        // STEP 8: COMMIT BATCH ATOMICALLY
        // ═══════════════════════════════════════════════════════════
        ctx.engine.commit_batch(batch, log_index);

        Ok(())
    }

    /// Recover an expired prepared transaction
    async fn recover_prepared_transaction<E: TransactionEngine>(
        batch: &mut E::Batch,
        ctx: &mut ExecutionContext<'_, E>,
        recovery_manager: &RecoveryManager,
        txn_id: TransactionId,
        timestamp: Timestamp,
        phase: ProcessorPhase,
    ) -> Result<()> {
        // Get participants to query
        let participants = ctx.tx_manager.get_participants(txn_id);

        if participants.is_empty() {
            // No participants - just abort (shouldn't happen for prepared, but handle it)
            tracing::warn!(
                "Prepared transaction {} has no participants, aborting",
                txn_id
            );
            return ctx.abort_transaction(batch, txn_id, AbortReason::DeadlineExceeded, phase);
        }

        // Query participants for decision
        tracing::debug!(
            "Recovering prepared transaction {} by querying {} participants",
            txn_id,
            participants.len()
        );

        let decision = recovery_manager
            .recover(txn_id, &participants, timestamp)
            .await;

        match decision {
            TransactionDecision::Commit => {
                // Commit the prepared transaction
                tracing::info!("Recovery decision: COMMIT for transaction {}", txn_id);

                ctx.commit_transaction(
                    batch,
                    txn_id,
                    ctx.tx_manager.get_coordinator(txn_id).unwrap_or_default(),
                    "recovery".to_string(),
                    phase,
                )?;
            }
            TransactionDecision::Abort | TransactionDecision::Unknown => {
                // Abort the prepared transaction
                tracing::info!(
                    "Recovery decision: {:?} for transaction {}, aborting",
                    decision,
                    txn_id
                );

                ctx.abort_transaction(batch, txn_id, AbortReason::Recovery, phase)?;
            }
        }

        Ok(())
    }

    /// Dispatch message to appropriate executor
    fn dispatch_message<E: TransactionEngine>(
        ctx: &mut ExecutionContext<'_, E>,
        batch: &mut E::Batch,
        message: CoordinatorMessage<E::Operation>,
        phase: ProcessorPhase,
    ) -> Result<()> {
        match message {
            CoordinatorMessage::Noop => {
                // Noop message - just triggers processing without actual work
                Ok(())
            }
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
                        AdHocExecution::execute(
                            ctx,
                            batch,
                            op.operation,
                            op.coordinator_id,
                            op.request_id,
                            phase,
                        )
                    }
                    TransactionMode::ReadWrite => {
                        // Two-phase commit transaction

                        // Ensure transaction is registered (needed for wound-wait)
                        if !ctx.tx_manager.exists(op.txn_id)
                            && let Some(deadline) = op.txn_deadline
                        {
                            // Register transaction in manager
                            ctx.tx_manager.begin(
                                op.txn_id,
                                op.coordinator_id.clone(),
                                deadline,
                                HashMap::new(), // Empty until prepare
                            );

                            // Also begin in engine (add to active transactions)
                            ctx.engine.begin(batch, op.txn_id);

                            // Persist initial state
                            ctx.persist_transaction_state(batch, op.txn_id)?;
                        }

                        ReadWriteExecution::execute(
                            ctx,
                            batch,
                            op.operation,
                            op.txn_id,
                            op.coordinator_id,
                            op.request_id,
                            phase,
                        )
                    }
                }
            }
            CoordinatorMessage::Control(ctrl) => {
                // All control messages go to ReadWriteExecutor
                match ctrl.phase {
                    proven_protocol::TransactionPhase::Prepare(participants) => {
                        ReadWriteExecution::prepare(
                            ctx,
                            batch,
                            ctrl.txn_id,
                            ctrl.coordinator_id.unwrap_or_default(),
                            ctrl.request_id.unwrap_or_default(),
                            participants,
                            phase,
                        )
                    }
                    proven_protocol::TransactionPhase::PrepareAndCommit => {
                        ReadWriteExecution::prepare(
                            ctx,
                            batch,
                            ctrl.txn_id,
                            ctrl.coordinator_id.clone().unwrap_or_default(),
                            ctrl.request_id.clone().unwrap_or_default(),
                            HashMap::new(),
                            phase,
                        )?;
                        ReadWriteExecution::commit(
                            ctx,
                            batch,
                            ctrl.txn_id,
                            ctrl.coordinator_id.unwrap_or_default(),
                            ctrl.request_id.unwrap_or_default(),
                            phase,
                        )
                    }
                    proven_protocol::TransactionPhase::Commit => ReadWriteExecution::commit(
                        ctx,
                        batch,
                        ctrl.txn_id,
                        ctrl.coordinator_id.unwrap_or_default(),
                        ctrl.request_id.unwrap_or_default(),
                        phase,
                    ),
                    proven_protocol::TransactionPhase::Abort => ReadWriteExecution::abort(
                        ctx,
                        batch,
                        ctrl.txn_id,
                        ctrl.coordinator_id.unwrap_or_default(),
                        ctrl.request_id.unwrap_or_default(),
                        phase,
                    ),
                }
            }
        }
    }
}
