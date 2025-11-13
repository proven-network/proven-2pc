//! Execution context - bundles common state and patterns for operation execution

use crate::engine::{BatchOperations, BlockingInfo, OperationResult, TransactionEngine};
use crate::error::Result;
use crate::executor::read_write::AckResponse;
use crate::kernel::ResponseMode;
use crate::support::ResponseSender;
use crate::transaction::{AbortReason, DeferredOp, TransactionManager};
use proven_common::TransactionId;

/// Execution context bundles all state needed for operation execution
pub struct ExecutionContext<'a, E: TransactionEngine> {
    pub engine: &'a mut E,
    pub tx_manager: &'a mut TransactionManager<E>,
    pub response: &'a ResponseSender,
    /// Transactions with dirty state that need persistence (optimization)
    dirty_transactions: std::collections::HashSet<TransactionId>,
}

impl<'a, E: TransactionEngine> ExecutionContext<'a, E> {
    /// Create a new execution context
    pub fn new(
        engine: &'a mut E,
        tx_manager: &'a mut TransactionManager<E>,
        response: &'a ResponseSender,
    ) -> Self {
        use std::collections::HashSet;
        Self {
            engine,
            tx_manager,
            response,
            dirty_transactions: HashSet::new(),
        }
    }

    // ═══════════════════════════════════════════════════════════
    // COMMON EXECUTION PATTERNS
    // ═══════════════════════════════════════════════════════════

    /// Execute a deferred operation (handles both atomic and non-atomic)
    pub fn execute_deferred(
        &mut self,
        batch: &mut E::Batch,
        deferred: DeferredOp<E::Operation>,
        response_mode: ResponseMode,
    ) -> Result<()> {
        if deferred.is_atomic {
            // AdHoc: begin → apply → commit in same batch
            self.execute_atomic_deferred(batch, deferred, response_mode)
        } else {
            // ReadWrite: just apply (already begun)
            self.execute_transactional_deferred(batch, deferred, response_mode)
        }
    }

    /// Execute an atomic deferred operation (AdHoc)
    fn execute_atomic_deferred(
        &mut self,
        batch: &mut E::Batch,
        deferred: DeferredOp<E::Operation>,
        response_mode: ResponseMode,
    ) -> Result<()> {
        // Begin transaction
        self.engine.begin(batch, deferred.owner_txn_id);

        match self
            .engine
            .apply_operation(batch, deferred.operation.clone(), deferred.owner_txn_id)
        {
            OperationResult::Complete(resp) => {
                // Commit in same batch
                self.engine.commit(batch, deferred.owner_txn_id);
                self.tx_manager
                    .transition_to_committed(deferred.owner_txn_id)?;

                // Mark as dirty (lazy persistence)
                self.mark_dirty(deferred.owner_txn_id);

                // Send response
                if response_mode == ResponseMode::Send {
                    self.response.send_success(
                        &deferred.coordinator_id,
                        None,
                        deferred.request_id,
                        resp,
                    );
                }
                Ok(())
            }
            OperationResult::WouldBlock { blockers } => {
                // Re-blocks - defer again
                self.handle_blocked(
                    batch,
                    deferred.owner_txn_id,
                    deferred.operation,
                    blockers,
                    deferred.coordinator_id,
                    deferred.request_id,
                    true, // is_atomic
                    response_mode,
                )?;
                Ok(())
            }
        }
    }

    /// Execute a transactional deferred operation (ReadWrite)
    fn execute_transactional_deferred(
        &mut self,
        batch: &mut E::Batch,
        deferred: DeferredOp<E::Operation>,
        response_mode: ResponseMode,
    ) -> Result<()> {
        match self
            .engine
            .apply_operation(batch, deferred.operation.clone(), deferred.owner_txn_id)
        {
            OperationResult::Complete(resp) => {
                // Mark transaction state as dirty (lazy persistence)
                self.mark_dirty(deferred.owner_txn_id);

                // Send response
                if response_mode == ResponseMode::Send {
                    self.response.send_success(
                        &deferred.coordinator_id,
                        Some(&deferred.owner_txn_id.to_string()),
                        deferred.request_id,
                        resp,
                    );
                }
                Ok(())
            }
            OperationResult::WouldBlock { blockers } => {
                // Re-blocks - defer again
                self.handle_blocked(
                    batch,
                    deferred.owner_txn_id,
                    deferred.operation,
                    blockers,
                    deferred.coordinator_id,
                    deferred.request_id,
                    false, // not atomic
                    response_mode,
                )?;
                Ok(())
            }
        }
    }

    /// Handle a blocked operation using wound-wait protocol
    #[allow(clippy::too_many_arguments)]
    pub fn handle_blocked(
        &mut self,
        batch: &mut E::Batch,
        txn_id: TransactionId,
        operation: E::Operation,
        blockers: Vec<BlockingInfo>,
        coordinator_id: String,
        request_id: String,
        is_atomic: bool,
        response_mode: ResponseMode,
    ) -> Result<()> {
        // Find younger victims to wound
        let younger_victims: Vec<TransactionId> = blockers
            .iter()
            .filter(|b| b.txn > txn_id)
            .map(|b| b.txn)
            .collect();

        // Wound each younger victim
        for victim in younger_victims {
            self.wound_transaction(batch, victim, txn_id, response_mode)?;
        }

        // Defer for older blockers
        let older_blockers: Vec<BlockingInfo> =
            blockers.into_iter().filter(|b| b.txn < txn_id).collect();

        if !older_blockers.is_empty() {
            self.tx_manager.defer_operation(
                txn_id,
                operation,
                older_blockers,
                coordinator_id,
                request_id,
                is_atomic,
            );

            // Mark as dirty (lazy persistence)
            self.mark_dirty(txn_id);
        }

        Ok(())
    }

    /// Wound a victim transaction (abort it)
    pub fn wound_transaction(
        &mut self,
        batch: &mut E::Batch,
        victim: TransactionId,
        wounded_by: TransactionId,
        response_mode: ResponseMode,
    ) -> Result<()> {
        // Check if victim exists
        if !self.tx_manager.exists(victim) {
            return Ok(()); // Already gone
        }

        // Get coordinator for notification
        let victim_coord = self.tx_manager.get_coordinator(victim);

        // Mark as wounded
        self.tx_manager.mark_wounded(victim, wounded_by)?;

        // Send wounded notification
        if let Some(coord) = victim_coord.as_ref()
            && response_mode == ResponseMode::Send
        {
            self.response
                .send_wounded(coord, &victim.to_string(), wounded_by, None);
        }

        // Abort in engine
        self.engine.abort(batch, victim);

        // Transition to aborted
        self.tx_manager
            .transition_to_aborted(victim, AbortReason::Wounded { by: wounded_by })?;

        // Mark as dirty (lazy persistence)
        self.mark_dirty(victim);

        Ok(())
    }

    /// Abort a transaction (used by recovery, expiration, wound-wait)
    pub fn abort_transaction(
        &mut self,
        batch: &mut E::Batch,
        txn_id: TransactionId,
        reason: AbortReason,
        response_mode: ResponseMode,
    ) -> Result<()> {
        if !self.tx_manager.exists(txn_id) {
            return Ok(());
        }

        let coordinator_id = self.tx_manager.get_coordinator(txn_id);

        // Abort in engine
        self.engine.abort(batch, txn_id);

        // Transition state
        self.tx_manager.transition_to_aborted(txn_id, reason)?;

        // Mark as dirty (lazy persistence)
        self.mark_dirty(txn_id);

        // Send notification
        if let Some(coord) = coordinator_id
            && response_mode == ResponseMode::Send
        {
            self.response.send_error(
                &coord,
                Some(&txn_id.to_string()),
                "deadline_exceeded".to_string(),
                "Transaction deadline exceeded".to_string(),
            );
        }

        Ok(())
    }

    /// Commit a prepared transaction
    pub fn commit_transaction(
        &mut self,
        batch: &mut E::Batch,
        txn_id: TransactionId,
        coordinator_id: String,
        request_id: String,
        response_mode: ResponseMode,
    ) -> Result<()> {
        // Commit in engine
        self.engine.commit(batch, txn_id);

        // Transition to committed
        self.tx_manager.transition_to_committed(txn_id)?;

        // Mark as dirty (lazy persistence)
        self.mark_dirty(txn_id);

        // Send response
        if response_mode == ResponseMode::Send {
            self.response.send_success(
                &coordinator_id,
                Some(&txn_id.to_string()),
                request_id,
                AckResponse::success(),
            );
        }

        Ok(())
    }

    // ═══════════════════════════════════════════════════════════
    // METADATA PERSISTENCE HELPERS
    // ═══════════════════════════════════════════════════════════

    /// Mark a transaction as needing persistence (lazy approach)
    pub fn mark_dirty(&mut self, txn_id: TransactionId) {
        self.dirty_transactions.insert(txn_id);
    }

    /// Persist all dirty transaction states in one batch (lazy flush)
    pub fn flush_dirty_states(&mut self, batch: &mut E::Batch) -> Result<()> {
        for txn_id in self.dirty_transactions.drain() {
            // Check if transaction still exists (active/prepared)
            if self.tx_manager.exists(txn_id) {
                // Check if it's completed
                if let Ok(completed_state) =
                    self.tx_manager.get_completed_state_for_persistence(txn_id)
                {
                    // Persist completed state
                    let value = completed_state.to_bytes()?;
                    batch.insert_transaction_metadata(txn_id, value);
                } else if let Ok(state) = self.tx_manager.get_state_for_persistence(txn_id) {
                    // Persist active/prepared state
                    let value = state.to_bytes()?;
                    batch.insert_transaction_metadata(txn_id, value);
                }
            }
        }
        Ok(())
    }
}
