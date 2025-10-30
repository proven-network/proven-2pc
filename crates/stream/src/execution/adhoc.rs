//! Ad-hoc execution path for auto-commit operations

use crate::engine::{OperationResult, TransactionEngine};
use crate::error::{ProcessorError, Result};
use crate::response::ResponseSender;
use crate::transaction::DeferredOperationsManager;
use proven_common::{Timestamp, TransactionId};
use proven_engine::Message;
use proven_protocol::CoordinatorMessage;

/// Execute an ad-hoc operation with auto-commit
///
/// This path:
/// - Creates minimal transaction state
/// - Auto-begins and auto-commits
/// - Handles blocking with immediate abort
/// - Does not participate in 2PC
pub fn execute_adhoc<E: TransactionEngine>(
    engine: &mut E,
    message: Message,
    _msg_timestamp: Timestamp,
    log_index: u64,
    response_sender: &ResponseSender,
    deferred_manager: &mut DeferredOperationsManager<E::Operation>,
) -> Result<()> {
    // Generate a transaction ID for this ad-hoc operation
    let txn_id = TransactionId::new();

    // Parse message into typed structure
    let coord_msg = CoordinatorMessage::from_message(message)
        .map_err(|e| ProcessorError::InvalidOperation(e.to_string()))?;

    // Extract adhoc fields
    let (coordinator_id, request_id, operation_bytes) = match coord_msg {
        CoordinatorMessage::Operation(op) => (op.coordinator_id, Some(op.request_id), op.operation),
        _ => {
            return Err(ProcessorError::InvalidOperation(
                "Expected operation message for ad-hoc execution".to_string(),
            ));
        }
    };

    // Deserialize the operation
    let operation: E::Operation = serde_json::from_slice(&operation_bytes)
        .map_err(|e| ProcessorError::InvalidOperation(format!("Failed to deserialize: {}", e)))?;

    // Begin transaction
    engine.begin(txn_id, log_index);

    // Execute the operation
    match engine.apply_operation(operation.clone(), txn_id, log_index) {
        OperationResult::Complete(response) => {
            // Immediately commit the transaction
            engine.commit(txn_id, log_index);

            // Send successful response
            response_sender.send_success(&coordinator_id, None, request_id, response);

            // Retry any deferred operations that were waiting on this transaction
            retry_deferred_for_transaction(engine, deferred_manager, txn_id, response_sender);

            Ok(())
        }
        OperationResult::WouldBlock { blockers } => {
            // Ad-hoc operations should not block - abort immediately
            engine.abort(txn_id, log_index);

            let blocker_list: Vec<String> = blockers.iter().map(|b| b.txn.to_string()).collect();
            tracing::warn!("Ad-hoc operation blocked by {:?}, aborting", blocker_list);

            response_sender.send_error(
                &coordinator_id,
                None,
                format!(
                    "Ad-hoc operation blocked by transactions: {:?}",
                    blocker_list
                ),
                request_id,
            );

            Ok(())
        }
    }
}

/// Retry deferred operations after a transaction commits
fn retry_deferred_for_transaction<E: TransactionEngine>(
    engine: &mut E,
    deferred_manager: &mut DeferredOperationsManager<E::Operation>,
    completed_txn: TransactionId,
    response_sender: &ResponseSender,
) {
    // Get operations that can be retried after this transaction
    let ready_operations = deferred_manager.take_commit_waiting_operations(&completed_txn);

    for deferred_op in ready_operations {
        let txn_id = deferred_op.txn_id;
        let operation = deferred_op.operation;
        let coordinator_id = deferred_op.coordinator_id;
        let request_id = deferred_op.request_id;
        match engine.apply_operation(operation.clone(), txn_id, 0) {
            OperationResult::Complete(response) => {
                // Send successful response using ResponseSender
                response_sender.send_success(&coordinator_id, None, request_id, response);
            }
            OperationResult::WouldBlock { blockers } => {
                // Re-defer with all blockers
                deferred_manager.defer_operation(
                    operation,
                    txn_id,
                    blockers,
                    coordinator_id,
                    request_id,
                );
            }
        }
    }
}
