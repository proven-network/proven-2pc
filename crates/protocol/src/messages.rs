//! Typed message wrappers for coordinator-to-participant communication

use proven_common::{Operation, Timestamp, TransactionId};
use proven_engine::Message;
use serde_json;
use std::collections::HashMap;

/// Transaction phases in 2PC protocol
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactionPhase {
    /// Prepare phase (vote request) - includes participant list for recovery
    Prepare(HashMap<String, u64>),
    /// Combined prepare and commit (single-participant optimization)
    PrepareAndCommit,
    /// Commit phase (decision)
    Commit,
    /// Abort phase (decision)
    Abort,
}

impl TransactionPhase {
    /// Get the phase name (without participants data)
    pub fn phase_name(&self) -> &'static str {
        match self {
            Self::Prepare(_) => "prepare",
            Self::PrepareAndCommit => "prepare_and_commit",
            Self::Commit => "commit",
            Self::Abort => "abort",
        }
    }

    /// Get participants if this is a prepare phase (only regular Prepare has participants)
    pub fn participants(&self) -> Option<&HashMap<String, u64>> {
        match self {
            Self::Prepare(p) => Some(p),
            _ => None,
        }
    }
}

/// Transaction mode for different execution paths
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionMode {
    /// Read-only transaction using snapshot isolation
    ReadOnly,
    /// Ad-hoc operation with auto-commit
    AdHoc,
    /// Full read-write transaction with 2PC
    ReadWrite,
}

impl TransactionMode {
    /// Parse from string header value
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "read_only" => Some(Self::ReadOnly),
            "adhoc" => Some(Self::AdHoc),
            _ => None, // Default is read-write
        }
    }

    /// Convert to string header value (None means read-write, which is the default)
    pub fn as_str(&self) -> Option<&'static str> {
        match self {
            Self::ReadOnly => Some("read_only"),
            Self::AdHoc => Some("adhoc"),
            Self::ReadWrite => None, // Default mode doesn't need explicit header
        }
    }
}

/// Typed wrapper around Message for coordinator-to-participant communication
#[derive(Debug, Clone)]
pub enum CoordinatorMessage<O: Operation> {
    /// Regular operation message
    Operation(OperationMessage<O>),
    /// Transaction control message (prepare/commit/abort)
    Control(TransactionControlMessage),
    /// Read-only operation (via pubsub)
    ReadOnly {
        /// Read timestamp for snapshot isolation
        read_timestamp: TransactionId,
        /// Coordinator ID for responses
        coordinator_id: String,
        /// Request ID for matching responses
        request_id: String,
        /// Parsed operation
        operation: O,
    },
    /// No-op message for triggering recovery/GC without real work
    /// Used when stream is idle but has expired transactions
    Noop,
}

/// Operation message in a transaction
#[derive(Debug, Clone)]
pub struct OperationMessage<O: Operation> {
    /// Transaction ID
    pub txn_id: TransactionId,
    /// Coordinator ID for responses
    pub coordinator_id: String,
    /// Request ID for matching responses
    pub request_id: String,
    /// Transaction deadline (sent on first contact with stream)
    pub txn_deadline: Option<Timestamp>,
    /// Transaction mode (read_only, adhoc, or read-write as default)
    pub mode: TransactionMode,
    /// Parsed operation
    pub operation: O,
}

/// Transaction control message (prepare/commit/abort)
#[derive(Debug, Clone)]
pub struct TransactionControlMessage {
    /// Transaction ID
    pub txn_id: TransactionId,
    /// Transaction phase (includes participants for prepare phases)
    pub phase: TransactionPhase,
    /// Coordinator ID for responses (optional for commit/abort)
    pub coordinator_id: Option<String>,
    /// Request ID for matching responses (optional for commit/abort)
    pub request_id: Option<String>,
}

impl<O: Operation> CoordinatorMessage<O> {
    /// Parse a Message into a typed CoordinatorMessage
    pub fn from_message(msg: Message) -> Result<Self, ParseError> {
        // Check for noop message
        if msg.get_header("noop").is_some() {
            return Ok(CoordinatorMessage::Noop);
        }

        // Check if this is a control message (empty body)
        if msg.body.is_empty() {
            // Must have txn_phase header
            let phase_str = msg
                .get_header("txn_phase")
                .ok_or(ParseError::MissingHeader("txn_phase"))?;

            let txn_id_str = msg
                .get_header("txn_id")
                .ok_or(ParseError::MissingHeader("txn_id"))?;
            let txn_id = TransactionId::parse(txn_id_str)
                .map_err(|_| ParseError::InvalidTransactionId(txn_id_str.to_string()))?;

            let coordinator_id = msg.get_header("coordinator_id").map(String::from);
            let request_id = msg.get_header("request_id").map(String::from);

            // Parse phase and participants together
            let phase = match phase_str {
                "prepare" => {
                    // Prepare must have participants
                    let participants = msg
                        .get_header("participants")
                        .and_then(|s| serde_json::from_str(s).ok())
                        .ok_or(ParseError::MissingParticipants)?;
                    TransactionPhase::Prepare(participants)
                }
                "prepare_and_commit" => TransactionPhase::PrepareAndCommit,
                "commit" => TransactionPhase::Commit,
                "abort" => TransactionPhase::Abort,
                _ => return Err(ParseError::InvalidPhase(phase_str.to_string())),
            };

            return Ok(CoordinatorMessage::Control(TransactionControlMessage {
                txn_id,
                phase,
                coordinator_id,
                request_id,
            }));
        }

        // Check for read-only mode (should come via pubsub, not ordered stream)
        if let Some(read_ts_str) = msg.get_header("read_timestamp") {
            let read_timestamp = TransactionId::parse(read_ts_str)
                .map_err(|_| ParseError::InvalidTransactionId(read_ts_str.to_string()))?;

            let coordinator_id = msg
                .get_header("coordinator_id")
                .ok_or(ParseError::MissingHeader("coordinator_id"))?
                .to_string();

            let request_id = msg
                .get_header("request_id")
                .ok_or(ParseError::MissingHeader("request_id"))?
                .to_string();

            // Deserialize the operation
            let operation: O = serde_json::from_slice(&msg.body)
                .map_err(|e| ParseError::InvalidOperation(e.to_string()))?;

            return Ok(CoordinatorMessage::ReadOnly {
                read_timestamp,
                coordinator_id,
                request_id,
                operation,
            });
        }

        // Regular operation message
        let txn_id_str = msg
            .get_header("txn_id")
            .ok_or(ParseError::MissingHeader("txn_id"))?;
        let txn_id = TransactionId::parse(txn_id_str)
            .map_err(|_| ParseError::InvalidTransactionId(txn_id_str.to_string()))?;

        let coordinator_id = msg
            .get_header("coordinator_id")
            .ok_or(ParseError::MissingHeader("coordinator_id"))?
            .to_string();

        let request_id = msg
            .get_header("request_id")
            .ok_or(ParseError::MissingHeader("request_id"))?
            .to_string();

        // Parse optional deadline
        let txn_deadline = msg
            .get_header("txn_deadline")
            .and_then(|s| Timestamp::parse(s).ok());

        // Parse transaction mode
        let mode = msg
            .get_header("txn_mode")
            .and_then(TransactionMode::parse)
            .unwrap_or(TransactionMode::ReadWrite);

        // Deserialize the operation
        let operation: O = serde_json::from_slice(&msg.body)
            .map_err(|e| ParseError::InvalidOperation(e.to_string()))?;

        Ok(CoordinatorMessage::Operation(OperationMessage {
            txn_id,
            coordinator_id,
            request_id,
            txn_deadline,
            mode,
            operation,
        }))
    }

    /// Convert to a raw Message for sending
    pub fn into_message(self) -> Message {
        match self {
            CoordinatorMessage::Operation(op) => {
                let mut headers = HashMap::new();
                headers.insert("txn_id".to_string(), op.txn_id.to_string());
                headers.insert("coordinator_id".to_string(), op.coordinator_id);
                headers.insert("request_id".to_string(), op.request_id);

                if let Some(deadline) = op.txn_deadline {
                    headers.insert("txn_deadline".to_string(), deadline.to_string());
                }

                if let Some(mode_str) = op.mode.as_str() {
                    headers.insert("txn_mode".to_string(), mode_str.to_string());
                }

                // Serialize the operation
                let body = serde_json::to_vec(&op.operation).unwrap();
                Message::new(body, headers)
            }
            CoordinatorMessage::Control(ctrl) => {
                let mut headers = HashMap::new();
                headers.insert("txn_id".to_string(), ctrl.txn_id.to_string());
                headers.insert("txn_phase".to_string(), ctrl.phase.phase_name().to_string());

                if let Some(coordinator_id) = ctrl.coordinator_id {
                    headers.insert("coordinator_id".to_string(), coordinator_id);
                }

                if let Some(request_id) = ctrl.request_id {
                    headers.insert("request_id".to_string(), request_id);
                }

                // Add participants if this is a prepare phase
                if let Some(participants) = ctrl.phase.participants() {
                    headers.insert(
                        "participants".to_string(),
                        serde_json::to_string(&participants).unwrap(),
                    );
                }

                Message::new(Vec::new(), headers)
            }
            CoordinatorMessage::ReadOnly {
                read_timestamp,
                coordinator_id,
                request_id,
                operation,
            } => {
                let mut headers = HashMap::new();
                headers.insert("read_timestamp".to_string(), read_timestamp.to_string());
                headers.insert("coordinator_id".to_string(), coordinator_id);
                headers.insert("request_id".to_string(), request_id);

                // Serialize the operation
                let body = serde_json::to_vec(&operation).unwrap();
                Message::new(body, headers)
            }
            CoordinatorMessage::Noop => {
                let mut headers = HashMap::new();
                headers.insert("noop".to_string(), "true".to_string());
                Message::new(Vec::new(), headers)
            }
        }
    }

    /// Get transaction ID if this message has one
    pub fn txn_id(&self) -> Option<TransactionId> {
        match self {
            CoordinatorMessage::Operation(op) => Some(op.txn_id),
            CoordinatorMessage::Control(ctrl) => Some(ctrl.txn_id),
            CoordinatorMessage::ReadOnly { .. } => None,
            CoordinatorMessage::Noop => None,
        }
    }

    /// Get coordinator ID for sending responses
    pub fn coordinator_id(&self) -> Option<&str> {
        match self {
            CoordinatorMessage::Operation(op) => Some(&op.coordinator_id),
            CoordinatorMessage::Control(ctrl) => ctrl.coordinator_id.as_deref(),
            CoordinatorMessage::ReadOnly { coordinator_id, .. } => Some(coordinator_id),
            CoordinatorMessage::Noop => None,
        }
    }

    /// Get request ID for matching responses
    pub fn request_id(&self) -> Option<&str> {
        match self {
            CoordinatorMessage::Operation(op) => Some(&op.request_id),
            CoordinatorMessage::Control(ctrl) => ctrl.request_id.as_deref(),
            CoordinatorMessage::ReadOnly { request_id, .. } => Some(request_id),
            CoordinatorMessage::Noop => None,
        }
    }
}

/// Errors that can occur when parsing messages
#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("Missing required header: {0}")]
    MissingHeader(&'static str),

    #[error("Invalid transaction ID: {0}")]
    InvalidTransactionId(String),

    #[error("Invalid transaction phase: {0}")]
    InvalidPhase(String),

    #[error("Invalid operation: {0}")]
    InvalidOperation(String),

    #[error("Missing participants for prepare phase")]
    MissingParticipants,
}
