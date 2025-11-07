//! Response sending for participant-to-coordinator communication

use proven_common::{Response, TransactionId};
use proven_engine::{Message, MockClient};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Helper for building and sending responses to coordinators
pub struct ResponseSender {
    /// The client for sending messages
    client: Arc<MockClient>,
    /// Name of this participant (stream name)
    stream_name: String,
    /// Name of the engine
    engine_name: String,
    /// Whether to suppress responses (used during replay)
    /// Uses AtomicBool for interior mutability to allow sharing in Arc
    suppress: AtomicBool,
}

impl ResponseSender {
    /// Create a new response sender
    pub fn new(client: Arc<MockClient>, stream_name: String, engine_name: String) -> Self {
        Self {
            client,
            stream_name,
            engine_name,
            suppress: AtomicBool::new(false),
        }
    }

    /// Set whether to suppress responses (for replay phase)
    pub fn set_suppress(&self, suppress: bool) {
        self.suppress.store(suppress, Ordering::Relaxed);
    }

    /// Send a successful operation response
    pub fn send_success<R: Response>(
        &self,
        coordinator_id: &str,
        txn_id: Option<&str>,
        request_id: String,
        response: R,
    ) {
        // Suppress responses during replay
        if self.suppress.load(Ordering::Relaxed) {
            return;
        }

        let body = match serde_json::to_vec(&response) {
            Ok(bytes) => bytes,
            Err(e) => {
                tracing::error!("[{}] Failed to serialize response: {}", self.stream_name, e);
                self.send_error(
                    coordinator_id,
                    txn_id,
                    format!("Serialization failed: {}", e),
                    request_id,
                );
                return;
            }
        };

        let mut headers = HashMap::with_capacity(4);
        headers.insert("participant".to_string(), self.stream_name.clone());
        headers.insert("engine".to_string(), self.engine_name.clone());
        headers.insert("status".to_string(), "complete".to_string());

        if let Some(txn_id) = txn_id {
            headers.insert("txn_id".to_string(), txn_id.to_string());
        }

        headers.insert("request_id".to_string(), request_id);

        let message = Message::new(body, headers);
        let subject = format!("coordinator.{}.response", coordinator_id);

        let client = self.client.clone();
        let stream_name = self.stream_name.clone();
        tokio::spawn(async move {
            if let Err(e) = client.publish(&subject, vec![message]).await {
                tracing::warn!("[{}] Failed to send success response: {}", stream_name, e);
            }
        });
    }

    /// Send a prepared response (2PC phase 1)
    pub fn send_prepared(&self, coordinator_id: &str, txn_id: &str, request_id: Option<String>) {
        // Suppress responses during replay
        if self.suppress.load(Ordering::Relaxed) {
            return;
        }

        let mut headers = HashMap::with_capacity(5);
        headers.insert("txn_id".to_string(), txn_id.to_string());
        headers.insert("participant".to_string(), self.stream_name.clone());
        headers.insert("engine".to_string(), self.engine_name.clone());
        headers.insert("status".to_string(), "prepared".to_string());

        if let Some(req_id) = request_id {
            headers.insert("request_id".to_string(), req_id);
        }

        let message = Message::new(Vec::new(), headers);
        let subject = format!("coordinator.{}.response", coordinator_id);

        let client = self.client.clone();
        tokio::spawn(async move {
            let _ = client.publish(&subject, vec![message]).await;
        });
    }

    /// Send a wounded response (wound-wait protocol)
    pub fn send_wounded(
        &self,
        coordinator_id: &str,
        txn_id: &str,
        wounded_by: TransactionId,
        request_id: Option<String>,
    ) {
        // Suppress responses during replay
        if self.suppress.load(Ordering::Relaxed) {
            return;
        }

        let mut headers = HashMap::with_capacity(6);
        headers.insert("txn_id".to_string(), txn_id.to_string());
        headers.insert("participant".to_string(), self.stream_name.clone());
        headers.insert("engine".to_string(), self.engine_name.clone());
        headers.insert("status".to_string(), "wounded".to_string());
        headers.insert("wounded_by".to_string(), wounded_by.to_string());

        if let Some(req_id) = request_id {
            headers.insert("request_id".to_string(), req_id);
        }

        let message = Message::new(Vec::new(), headers);
        let subject = format!("coordinator.{}.response", coordinator_id);

        let client = self.client.clone();
        tokio::spawn(async move {
            let _ = client.publish(&subject, vec![message]).await;
        });
    }

    /// Send an error response
    pub fn send_error(
        &self,
        coordinator_id: &str,
        txn_id: Option<&str>,
        error: String,
        request_id: String,
    ) {
        // Suppress responses during replay
        if self.suppress.load(Ordering::Relaxed) {
            return;
        }

        let mut headers = HashMap::with_capacity(6);
        headers.insert("participant".to_string(), self.stream_name.clone());
        headers.insert("engine".to_string(), self.engine_name.clone());
        headers.insert("status".to_string(), "error".to_string());
        headers.insert("error".to_string(), error);

        if let Some(txn_id) = txn_id {
            headers.insert("txn_id".to_string(), txn_id.to_string());
        }

        headers.insert("request_id".to_string(), request_id);

        let message = Message::new(Vec::new(), headers);
        let subject = format!("coordinator.{}.response", coordinator_id);

        let client = self.client.clone();
        tokio::spawn(async move {
            let _ = client.publish(&subject, vec![message]).await;
        });
    }
}
