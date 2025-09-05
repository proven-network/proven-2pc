//! Response handling for KV stream processing
//!
//! This module defines response types and channels for sending
//! results back to coordinators.

use crate::types::Value;
use proven_hlc::HlcTimestamp;
use serde::{Deserialize, Serialize};

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

    /// Transaction is prepared and ready to commit
    Prepared,
}
