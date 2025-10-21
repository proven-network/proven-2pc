//! Read-only execution path for snapshot isolation reads

use crate::engine::{OperationResult, TransactionEngine};
use crate::error::{ProcessorError, Result};
use crate::transaction::DeferredOperationsManager;
use proven_engine::Message;
use proven_hlc::HlcTimestamp;
use std::sync::Arc;

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
    client: &Arc<proven_engine::MockClient>,
    stream_name: &str,
    deferred_manager: &mut DeferredOperationsManager<E::Operation>,
) -> Result<()> {
    // Extract read timestamp from message header (required)
    let read_timestamp = message
        .get_header("read_timestamp")
        .and_then(|s| HlcTimestamp::parse(s).ok())
        .ok_or({
            ProcessorError::MissingHeader("read_timestamp header required for read-only operations")
        })?;

    // Extract coordinator and request ID for response
    let coordinator_id = message
        .get_header("coordinator_id")
        .ok_or(ProcessorError::MissingHeader("coordinator_id"))?;
    let request_id = message.get_header("request_id").map(String::from);

    // Deserialize the operation
    let operation: E::Operation = serde_json::from_slice(&message.body)
        .map_err(|e| ProcessorError::InvalidOperation(format!("Failed to deserialize: {}", e)))?;

    // Execute the read at the specified timestamp
    match engine.read_at_timestamp(operation.clone(), read_timestamp) {
        OperationResult::Complete(response) => {
            // Send successful response
            send_read_response(
                client,
                stream_name,
                coordinator_id,
                request_id,
                response,
                read_timestamp,
            )
            .await;
            Ok(())
        }
        OperationResult::WouldBlock { blockers } => {
            // Read-only operations can be blocked by exclusive write locks
            // Defer the operation to retry when the blocking transaction completes
            tracing::debug!(
                stream = %stream_name,
                timestamp = %read_timestamp,
                blockers = ?blockers.iter().map(|b| b.txn.to_string()).collect::<Vec<_>>(),
                "Read operation blocked"
            );

            // For read-only ops, we use the read timestamp as the "transaction ID" for tracking
            // Defer to retry when ALL blockers complete
            deferred_manager.defer_operation(
                operation,
                read_timestamp, // Use read timestamp as the operation ID
                blockers,
                coordinator_id.to_string(),
                request_id,
            );
            Ok(())
        }
    }
}

/// Send a successful read response
async fn send_read_response<R: proven_common::Response>(
    client: &Arc<proven_engine::MockClient>,
    stream_name: &str,
    coordinator_id: &str,
    request_id: Option<String>,
    response: R,
    _read_timestamp: HlcTimestamp,
) {
    let serialized = match serde_json::to_vec(&response) {
        Ok(data) => data,
        Err(e) => {
            tracing::error!("[{}] Failed to serialize response: {}", stream_name, e);
            return;
        }
    };

    // Build response headers
    let mut headers = std::collections::HashMap::new();
    headers.insert("participant".to_string(), stream_name.to_string());
    if let Some(req_id) = request_id {
        headers.insert("request_id".to_string(), req_id);
    }
    headers.insert("status".to_string(), "complete".to_string());

    let message = proven_engine::Message::new(serialized, headers);
    let subject = format!("coordinator.{}.response", coordinator_id);

    let client = client.clone();
    let stream_name = stream_name.to_string();
    tokio::spawn(async move {
        if let Err(e) = client.publish(&subject, vec![message]).await {
            tracing::warn!(
                "[{}] Failed to send read response to coordinator: {}",
                stream_name,
                e
            );
        }
    });
}
