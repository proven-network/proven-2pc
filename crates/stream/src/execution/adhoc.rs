//! Ad-hoc execution path for auto-commit operations

use crate::engine::{OperationResult, TransactionEngine};
use crate::error::{ProcessorError, Result};
use crate::transaction::DeferredOperationsManager;
use proven_engine::Message;
use proven_hlc::HlcTimestamp;
use std::sync::Arc;

/// Execute an ad-hoc operation with auto-commit
///
/// This path:
/// - Creates minimal transaction state
/// - Auto-begins and auto-commits
/// - Handles blocking with immediate abort
/// - Does not participate in 2PC
pub async fn execute_adhoc<E: TransactionEngine>(
    engine: &mut E,
    message: Message,
    msg_timestamp: HlcTimestamp,
    log_index: u64,
    client: &Arc<proven_engine::MockClient>,
    stream_name: &str,
    deferred_manager: &mut DeferredOperationsManager<E::Operation>,
) -> Result<()> {
    // Extract operation timestamp (use as transaction ID)
    let operation_timestamp = super::get_operation_timestamp(&message, msg_timestamp);

    // Extract coordinator and request ID for response
    let coordinator_id = message
        .get_header("coordinator_id")
        .ok_or(ProcessorError::MissingHeader("coordinator_id"))?;
    let request_id = message.get_header("request_id").map(String::from);

    // Deserialize the operation
    let operation: E::Operation = serde_json::from_slice(&message.body)
        .map_err(|e| ProcessorError::InvalidOperation(format!("Failed to deserialize: {}", e)))?;

    // Begin transaction using operation timestamp as ID
    engine.begin(operation_timestamp, log_index);

    // Execute the operation
    match engine.apply_operation(operation.clone(), operation_timestamp, log_index) {
        OperationResult::Complete(response) => {
            // Immediately commit the transaction
            engine.commit(operation_timestamp, log_index);

            // Send successful response
            send_adhoc_response(
                client,
                stream_name,
                coordinator_id,
                request_id,
                response,
                operation_timestamp,
            )
            .await;

            // Retry any deferred operations that were waiting on this transaction
            retry_deferred_for_transaction(
                engine,
                deferred_manager,
                operation_timestamp,
                client,
                stream_name,
            )
            .await;

            Ok(())
        }
        OperationResult::WouldBlock { blockers } => {
            // Ad-hoc operations should not block - abort immediately
            engine.abort(operation_timestamp, log_index);

            let blocker_list: Vec<String> = blockers.iter().map(|b| b.txn.to_string()).collect();
            tracing::warn!(
                "[{}] Ad-hoc operation blocked by {:?}, aborting",
                stream_name,
                blocker_list
            );

            send_error_response(
                client,
                stream_name,
                coordinator_id,
                request_id,
                &format!(
                    "Ad-hoc operation blocked by transactions: {:?}",
                    blocker_list
                ),
                operation_timestamp,
            )
            .await;

            Ok(())
        }
    }
}

/// Retry deferred operations after a transaction commits
async fn retry_deferred_for_transaction<E: TransactionEngine>(
    engine: &mut E,
    deferred_manager: &mut DeferredOperationsManager<E::Operation>,
    completed_txn: HlcTimestamp,
    client: &Arc<proven_engine::MockClient>,
    stream_name: &str,
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
                // Send successful response
                let serialized = match serde_json::to_vec(&response) {
                    Ok(data) => data,
                    Err(e) => {
                        tracing::error!("[{}] Failed to serialize response: {}", stream_name, e);
                        continue;
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

                let client_clone = client.clone();
                let stream_name_clone = stream_name.to_string();
                tokio::spawn(async move {
                    if let Err(e) = client_clone.publish(&subject, vec![message]).await {
                        tracing::warn!(
                            "[{}] Failed to send deferred response to coordinator: {}",
                            stream_name_clone,
                            e
                        );
                    }
                });
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

/// Send a successful response
async fn send_adhoc_response<R: proven_common::Response>(
    client: &Arc<proven_engine::MockClient>,
    stream_name: &str,
    coordinator_id: &str,
    request_id: Option<String>,
    response: R,
    _operation_timestamp: HlcTimestamp,
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
                "[{}] Failed to send adhoc response to coordinator: {}",
                stream_name,
                e
            );
        }
    });
}

/// Send an error response
async fn send_error_response(
    client: &Arc<proven_engine::MockClient>,
    stream_name: &str,
    coordinator_id: &str,
    request_id: Option<String>,
    error: &str,
    _operation_timestamp: HlcTimestamp,
) {
    // Build error response headers
    let mut headers = std::collections::HashMap::new();
    headers.insert("participant".to_string(), stream_name.to_string());
    if let Some(req_id) = request_id {
        headers.insert("request_id".to_string(), req_id);
    }
    headers.insert("status".to_string(), "error".to_string());
    headers.insert("error".to_string(), error.to_string());

    let message = proven_engine::Message::new(Vec::new(), headers);
    let subject = format!("coordinator.{}.response", coordinator_id);

    let client = client.clone();
    let stream_name = stream_name.to_string();
    tokio::spawn(async move {
        if let Err(e) = client.publish(&subject, vec![message]).await {
            tracing::warn!(
                "[{}] Failed to send error response to coordinator: {}",
                stream_name,
                e
            );
        }
    });
}
