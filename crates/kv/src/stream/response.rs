//! Response handling for KV stream processing
//!
//! This module defines response types and channels for sending
//! results back to coordinators.

use crate::types::Value;
use proven_hlc::HlcTimestamp;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Response sent back to coordinator
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KvResponse {
    /// Successful get operation
    GetResult { key: String, value: Option<Value> },

    /// Successful put operation
    PutResult {
        key: String,
        previous: Option<Value>,
    },

    /// Successful delete operation
    DeleteResult { key: String, deleted: bool },

    /// Error occurred
    Error(String),

    /// Transaction was wounded by an older transaction
    Wounded {
        /// Transaction that wounded us
        wounded_by: HlcTimestamp,
        /// Reason for wounding
        reason: String,
    },

    /// Operation is deferred waiting for locks
    Deferred {
        /// Transaction we're waiting for
        waiting_for: HlcTimestamp,
        /// Key that we need lock for
        lock_key: String,
    },
}

/// Channel for sending responses back to coordinators
pub trait ResponseChannel: Send + Sync {
    fn send(&self, coordinator_id: &str, txn_id: &str, response: KvResponse);

    fn as_any(&self) -> &dyn std::any::Any {
        panic!("as_any not implemented for this ResponseChannel")
    }
}

/// Mock implementation for testing
pub struct MockResponseChannel {
    responses: Arc<parking_lot::Mutex<Vec<(String, String, KvResponse)>>>,
}

impl MockResponseChannel {
    pub fn new() -> Self {
        Self {
            responses: Arc::new(parking_lot::Mutex::new(Vec::new())),
        }
    }

    pub fn get_responses(&self) -> Vec<(String, String, KvResponse)> {
        self.responses.lock().clone()
    }

    pub fn clear(&mut self) {
        self.responses.lock().clear();
    }
}

impl ResponseChannel for Arc<MockResponseChannel> {
    fn send(&self, coordinator_id: &str, txn_id: &str, response: KvResponse) {
        self.responses
            .lock()
            .push((coordinator_id.to_string(), txn_id.to_string(), response));
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
