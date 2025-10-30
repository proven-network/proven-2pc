//! Read-only execution path for snapshot isolation reads

use crate::engine::{OperationResult, TransactionEngine};
use crate::error::{ProcessorError, Result};
use crate::response::ResponseSender;
use crate::transaction::DeferredOperationsManager;
use proven_engine::Message;
use proven_protocol::CoordinatorMessage;

/// Execute a read-only operation using snapshot isolation
///
/// This path:
/// - Does not create transaction state
/// - Does not acquire locks
/// - Can be blocked by exclusive write locks
/// - Does not participate in 2PC
///
/// Read-only operations come via pubsub and must have a read_timestamp header
/// set by the coordinator (ReadOnlyExecutor).
pub async fn execute_read_only<E: TransactionEngine>(
    engine: &mut E,
    message: Message,
    response_sender: &ResponseSender,
    deferred_manager: &mut DeferredOperationsManager<E::Operation>,
) -> Result<()> {
    // Parse message into typed structure
    let coord_msg = CoordinatorMessage::from_message(message)
        .map_err(|e| ProcessorError::InvalidOperation(e.to_string()))?;

    // Extract read-only specific fields
    let (read_txn_id, coordinator_id, request_id, operation_bytes) = match coord_msg {
        CoordinatorMessage::ReadOnly {
            read_timestamp,
            coordinator_id,
            request_id,
            operation,
        } => (read_timestamp, coordinator_id, Some(request_id), operation),
        _ => {
            return Err(ProcessorError::InvalidOperation(
                "Expected read-only message".to_string(),
            ));
        }
    };

    // Deserialize the operation
    let operation: E::Operation = serde_json::from_slice(&operation_bytes)
        .map_err(|e| ProcessorError::InvalidOperation(format!("Failed to deserialize: {}", e)))?;

    // Execute the read at the specified transaction ID (snapshot)
    match engine.read_at_timestamp(operation.clone(), read_txn_id) {
        OperationResult::Complete(response) => {
            // Send successful response
            response_sender.send_success(&coordinator_id, None, request_id, response);
            Ok(())
        }
        OperationResult::WouldBlock { blockers } => {
            // Read-only operations can be blocked by exclusive write locks
            // Defer the operation to retry when the blocking transaction completes
            tracing::debug!(
                txn_id = %read_txn_id,
                blockers = ?blockers.iter().map(|b| b.txn.to_string()).collect::<Vec<_>>(),
                "Read operation blocked"
            );

            // For read-only ops, we use the read transaction ID for tracking
            // Defer to retry when ALL blockers complete
            deferred_manager.defer_operation(
                operation,
                read_txn_id, // Use read transaction ID as the operation ID
                blockers,
                coordinator_id,
                request_id,
            );
            Ok(())
        }
    }
}
