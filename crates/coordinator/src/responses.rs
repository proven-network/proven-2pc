//! Response collection and handling

use crate::error::{CoordinatorError, Result};
use parking_lot::Mutex;
use proven_engine::{Message, MockClient};
use proven_hlc::HlcTimestamp;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;

/// Status of a response (protocol level, no storage types)
#[derive(Debug, Clone)]
pub enum ResponseStatus {
    /// Operation completed successfully
    Success,

    /// Transaction was wounded
    Wounded { wounded_by: HlcTimestamp },

    /// Operation failed with an error
    Error(String),
}

/// A response message from a storage engine
#[derive(Debug)]
pub struct ResponseMessage {
    pub status: ResponseStatus,
    pub body: Vec<u8>,
    pub engine: String,
    pub participant: Option<String>,
}

/// Collects responses from storage engines
pub struct ResponseCollector {
    /// Responses indexed by request_id (can have multiple responses per request)
    responses: Arc<Mutex<HashMap<String, Vec<ResponseMessage>>>>,

    /// Notify channels for when new responses arrive
    notifiers: Arc<Mutex<HashMap<String, Arc<tokio::sync::Notify>>>>,

    /// Client for subscribing to responses
    client: Arc<MockClient>,

    /// Coordinator ID for routing
    coordinator_id: String,

    /// Background task handle
    task: Mutex<Option<JoinHandle<()>>>,
}

impl ResponseCollector {
    pub fn new(client: Arc<MockClient>, coordinator_id: String) -> Self {
        Self {
            responses: Arc::new(Mutex::new(HashMap::new())),
            notifiers: Arc::new(Mutex::new(HashMap::new())),
            client,
            coordinator_id,
            task: Mutex::new(None),
        }
    }

    /// Start the background response collection task
    pub fn start(&self) {
        let responses = self.responses.clone();
        let notifiers = self.notifiers.clone();
        let client = self.client.clone();
        let subject = format!("coordinator.{}.response", self.coordinator_id);

        let task = tokio::spawn(async move {
            // Subscribe to response subject
            let mut stream = match client.subscribe(&subject, None).await {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!("Failed to subscribe to responses: {}", e);
                    return;
                }
            };

            while let Some(msg) = stream.recv().await {
                if let Some(request_id) = msg.headers.get("request_id") {
                    // Parse the response
                    let response = Self::parse_response(&msg);

                    // Store the response
                    responses
                        .lock()
                        .entry(request_id.clone())
                        .or_default()
                        .push(response);

                    // Notify any waiters - get notifier without holding lock
                    let notifier = notifiers.lock().get(request_id).cloned();
                    if let Some(notifier) = notifier {
                        notifier.notify_waiters();
                    }
                }
            }
        });

        *self.task.lock() = Some(task);
    }

    /// Parse a message into a ResponseMessage
    fn parse_response(msg: &Message) -> ResponseMessage {
        let engine = msg
            .headers
            .get("engine")
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());

        let participant = msg.headers.get("participant").cloned();

        // Check status header for protocol-level responses
        let status = if let Some(status) = msg.headers.get("status") {
            match status.as_str() {
                "wounded" => {
                    let wounded_by = msg
                        .headers
                        .get("wounded_by")
                        .and_then(|s| HlcTimestamp::parse(s).ok())
                        .unwrap_or_else(|| HlcTimestamp::new(0, 0, proven_hlc::NodeId::new(0)));

                    ResponseStatus::Wounded { wounded_by }
                }
                "error" => {
                    let error = msg
                        .headers
                        .get("error")
                        .cloned()
                        .unwrap_or_else(|| "Unknown error".to_string());

                    ResponseStatus::Error(error)
                }
                "prepared" => {
                    // Prepare vote - treat as success with empty body
                    ResponseStatus::Success
                }
                _ => ResponseStatus::Success,
            }
        } else {
            // No status header means success
            ResponseStatus::Success
        };

        ResponseMessage {
            status,
            body: msg.body.clone(),
            engine,
            participant,
        }
    }

    /// Wait for a response with the given request ID
    pub async fn wait_for_response(
        &self,
        request_id: String,
        timeout: Duration,
    ) -> Result<ResponseMessage> {
        // Create or get notifier for this request FIRST
        let notifier = {
            let mut notifiers = self.notifiers.lock();
            notifiers
                .entry(request_id.clone())
                .or_insert_with(|| Arc::new(tokio::sync::Notify::new()))
                .clone()
        };

        // Wait for notification with timeout
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            // CRITICAL: Create the notified future BEFORE checking for responses
            // This prevents lost wakeups
            let notified = notifier.notified();

            // Check if response already arrived
            // Use a block to ensure the lock is dropped before waiting
            let response_opt = {
                let mut responses = self.responses.lock();
                if let Some(response_list) = responses.get_mut(&request_id) {
                    if !response_list.is_empty() {
                        Some(response_list.remove(0))
                    } else {
                        None
                    }
                } else {
                    None
                }
            };

            // If we got a response, clean up if needed and return
            if let Some(response) = response_opt {
                // Check if we should clean up empty entries
                {
                    let mut responses = self.responses.lock();
                    if let Some(response_list) = responses.get(&request_id)
                        && response_list.is_empty()
                    {
                        responses.remove(&request_id);
                        // Also remove notifier
                        self.notifiers.lock().remove(&request_id);
                    }
                }
                return Ok(response);
            }

            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                // Clean up notifier on timeout
                self.notifiers.lock().remove(&request_id);
                return Err(CoordinatorError::ResponseTimeout);
            }

            // Wait for notification or timeout
            match tokio::time::timeout(remaining, notified).await {
                Ok(()) => {
                    // Loop will check for response again
                    continue;
                }
                Err(_) => {
                    // Timeout - clean up notifier
                    self.notifiers.lock().remove(&request_id);
                    return Err(CoordinatorError::ResponseTimeout);
                }
            }
        }
    }

    /// Stop the background task
    pub async fn stop(&self) {
        let task = self.task.lock().take();
        if let Some(task) = task {
            task.abort();
            let _ = task.await;
        }
    }
}

impl Drop for ResponseCollector {
    fn drop(&mut self) {
        // Abort the background task if still running
        if let Some(task) = self.task.lock().take() {
            task.abort();
        }
    }
}
