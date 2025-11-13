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
use crate::kernel::ResponseMode;
use crate::transaction::{AbortReason, RecoveryManager, TransactionDecision};
use proven_common::{Timestamp, TransactionId};
use proven_protocol::OrderedMessage;
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
        message: OrderedMessage<E::Operation>,
        timestamp: Timestamp,
        log_index: u64,
        response_mode: ResponseMode,
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
            ctx.abort_transaction(
                &mut batch,
                txn_id,
                AbortReason::DeadlineExceeded,
                response_mode,
            )?;
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
                response_mode,
            )
            .await?;
        }

        // ═══════════════════════════════════════════════════════════
        // STEP 4: GC COMPLETED TRANSACTIONS PAST DEADLINE
        // ═══════════════════════════════════════════════════════════
        let to_gc = ctx.tx_manager.gc_completed_by_deadline(timestamp);
        for txn_id in to_gc {
            batch.remove_transaction_metadata(txn_id);
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
                ctx.execute_deferred(&mut batch, deferred, response_mode)?;
            }
        }

        // ═══════════════════════════════════════════════════════════
        // STEP 6: VALIDATE & PROCESS ORIGINAL MESSAGE
        // ═══════════════════════════════════════════════════════════
        let mut did_process = false;

        if !matches!(message, OrderedMessage::Noop) {
            // Check if message is past its deadline
            let is_expired = match &message {
                OrderedMessage::AutoCommitOperation { txn_deadline, .. } => {
                    *txn_deadline < timestamp
                }
                OrderedMessage::TransactionOperation { txn_deadline, .. } => {
                    *txn_deadline < timestamp
                }
                OrderedMessage::TransactionControl { txn_deadline, .. } => {
                    *txn_deadline < timestamp
                }
                OrderedMessage::Noop => false,
            };

            if is_expired {
                // Silently skip messages that arrive after their deadline
                tracing::debug!(
                    "Skipping expired message for transaction {:?} (deadline passed)",
                    message.txn_id()
                );
            } else {
                did_process = true;
                Self::dispatch_message(&mut ctx, &mut batch, message, response_mode)?;
            }
        }

        // ═══════════════════════════════════════════════════════════
        // STEP 7: LOOP - PROCESS NEWLY UNBLOCKED DEFERRALS (AGE ORDER)
        // ═══════════════════════════════════════════════════════════
        if did_process {
            loop {
                let ready_ops = ctx.tx_manager.take_all_ready_deferrals_sorted();
                if ready_ops.is_empty() {
                    break;
                }

                for deferred in ready_ops {
                    ctx.execute_deferred(&mut batch, deferred, response_mode)?;
                }
            }
        }

        // ═══════════════════════════════════════════════════════════
        // STEP 8: COMMIT BATCH ATOMICALLY
        // ═══════════════════════════════════════════════════════════

        // Flush all dirty transaction states before commit (lazy persistence optimization)
        ctx.flush_dirty_states(&mut batch)?;

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
        phase: ResponseMode,
    ) -> Result<()> {
        // Get participants to query
        let participants = ctx.tx_manager.get_participants(txn_id);

        if participants.is_empty() {
            // No participants - just abort (shouldn't happen for prepared, but handle it)
            println!(
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
                println!("Recovery decision: COMMIT for transaction {}", txn_id);

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
                tracing::debug!(
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
        message: OrderedMessage<E::Operation>,
        phase: ResponseMode,
    ) -> Result<()> {
        match message {
            OrderedMessage::Noop => {
                // Noop message - just triggers processing without actual work
                Ok(())
            }
            OrderedMessage::AutoCommitOperation {
                txn_id,
                coordinator_id,
                request_id,
                txn_deadline,
                operation,
            } => {
                // Auto-commit: begin, execute, commit in single batch
                AdHocExecution::execute(
                    ctx,
                    batch,
                    txn_id,
                    txn_deadline,
                    operation,
                    coordinator_id,
                    request_id,
                    phase,
                )
            }
            OrderedMessage::TransactionOperation {
                txn_id,
                coordinator_id,
                request_id,
                txn_deadline,
                operation,
            } => {
                // Two-phase commit transaction

                // Ensure transaction is registered (needed for wound-wait)
                if !ctx.tx_manager.exists(txn_id) {
                    // Register transaction in manager
                    ctx.tx_manager.begin(
                        txn_id,
                        coordinator_id.clone(),
                        txn_deadline,
                        HashMap::new(), // Empty until prepare
                    );

                    // Also begin in engine (add to active transactions)
                    ctx.engine.begin(batch, txn_id);

                    // Mark dirty for lazy persistence
                    ctx.mark_dirty(txn_id);
                }

                ReadWriteExecution::execute(
                    ctx,
                    batch,
                    operation,
                    txn_id,
                    coordinator_id,
                    request_id,
                    phase,
                )
            }
            OrderedMessage::TransactionControl {
                txn_id,
                phase: tx_phase,
                coordinator_id,
                request_id,
                txn_deadline: _,
            } => {
                // All control messages go to ReadWriteExecutor
                match tx_phase {
                    proven_protocol::TransactionPhase::Prepare(participants) => {
                        ReadWriteExecution::prepare(
                            ctx,
                            batch,
                            txn_id,
                            coordinator_id.unwrap_or_default(),
                            request_id.unwrap_or_default(),
                            participants,
                            phase,
                        )
                    }
                    proven_protocol::TransactionPhase::PrepareAndCommit => {
                        ReadWriteExecution::prepare_and_commit(
                            ctx,
                            batch,
                            txn_id,
                            coordinator_id.unwrap_or_default(),
                            request_id.unwrap_or_default(),
                            phase,
                        )
                    }
                    proven_protocol::TransactionPhase::Commit => ReadWriteExecution::commit(
                        ctx,
                        batch,
                        txn_id,
                        coordinator_id.unwrap_or_default(),
                        request_id.unwrap_or_default(),
                        phase,
                    ),
                    proven_protocol::TransactionPhase::Abort => ReadWriteExecution::abort(
                        ctx,
                        batch,
                        txn_id,
                        coordinator_id.unwrap_or_default(),
                        request_id.unwrap_or_default(),
                        phase,
                    ),
                }
            }
        }
    }
}
