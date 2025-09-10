//! Message types for pubsub communication

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Heartbeat message sent periodically by each node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Heartbeat {
    /// Node sending the heartbeat
    pub node_id: String,

    /// Processors currently running on this node
    pub processors: Vec<ProcessorStatus>,

    /// Raft groups this node is part of
    pub raft_groups: Vec<String>,

    /// Timestamp (milliseconds since epoch)
    pub timestamp_ms: u64,
}

/// Status of a running processor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessorStatus {
    /// Stream this processor handles
    pub stream: String,

    /// Guaranteed to run until this time (ms since epoch)
    pub guaranteed_until_ms: u64,

    /// Current state
    pub state: ProcessorState,
}

/// State of a processor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ProcessorState {
    /// Actively processing
    Active,
    /// Preparing to shut down
    ShuttingDown,
    /// Starting up
    Starting,
}

/// Request to start a processor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessorRequest {
    /// Unique request ID for correlation
    pub request_id: Uuid,

    /// Stream to process
    pub stream: String,

    /// Minimum duration to run (milliseconds)
    pub min_duration_ms: u64,

    /// Node requesting the processor
    pub requester: String,

    /// Target node that should handle this (if specified)
    pub target_node: Option<String>,
}

/// Acknowledgment that a processor has started
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessorAck {
    /// Request ID this is responding to
    pub request_id: Uuid,

    /// Node that started the processor
    pub node_id: String,

    /// Stream being processed
    pub stream: String,

    /// Guaranteed to run until (ms since epoch)
    pub guaranteed_until_ms: u64,
}

/// Processor extension notification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessorExtension {
    /// Node that owns the processor
    pub node_id: String,

    /// Stream being processed
    pub stream: String,

    /// New guaranteed until time (ms since epoch)
    pub new_guaranteed_until_ms: u64,
}
