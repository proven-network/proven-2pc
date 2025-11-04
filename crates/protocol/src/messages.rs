//! Typed message wrappers for coordinator-to-participant communication
//!
//! Messages are split by delivery channel:
//! - OrderedMessage: For operations requiring deterministic ordering (sent via stream)
//! - ReadOnlyMessage: For snapshot isolation reads (sent via pubsub)

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

/// Messages sent via ordered stream (deterministic, durable)
#[derive(Debug, Clone)]
pub enum OrderedMessage<O: Operation> {
    /// Operation in a read-write transaction
    TransactionOperation {
        txn_id: TransactionId,
        coordinator_id: String,
        request_id: String,
        txn_deadline: Timestamp,
        operation: O,
    },

    /// Single write operation with auto-commit
    AutoCommitOperation {
        txn_id: TransactionId,
        coordinator_id: String,
        request_id: String,
        txn_deadline: Timestamp,
        operation: O,
    },

    /// 2PC control message (prepare/commit/abort)
    TransactionControl {
        txn_id: TransactionId,
        phase: TransactionPhase,
        coordinator_id: Option<String>,
        request_id: Option<String>,
    },

    /// No-op message for triggering recovery/GC without real work
    Noop,
}

/// Read-only message sent via pubsub (snapshot isolation)
#[derive(Debug, Clone)]
pub struct ReadOnlyMessage<O: Operation> {
    /// Read timestamp for snapshot isolation (also serves as txn_id for MVCC)
    pub read_timestamp: TransactionId,
    /// Coordinator ID for responses
    pub coordinator_id: String,
    /// Request ID for matching responses
    pub request_id: String,
    /// Parsed operation
    pub operation: O,
}

impl<O: Operation> OrderedMessage<O> {
    /// Parse a Message from the ordered stream into a typed OrderedMessage
    pub fn from_message(msg: Message) -> Result<Self, ParseError> {
        // Check for noop message
        if msg.get_header("noop").is_some() {
            return Ok(OrderedMessage::Noop);
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

            return Ok(OrderedMessage::TransactionControl {
                txn_id,
                phase,
                coordinator_id,
                request_id,
            });
        }

        // Operation message - check for auto-commit
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

        // Parse deadline (required for all operations)
        let txn_deadline_str = msg
            .get_header("txn_deadline")
            .ok_or(ParseError::MissingHeader("txn_deadline"))?;
        let txn_deadline = Timestamp::parse(txn_deadline_str)
            .map_err(|_| ParseError::InvalidTimestamp(txn_deadline_str.to_string()))?;

        // Deserialize the operation
        let operation: O = serde_json::from_slice(&msg.body)
            .map_err(|e| ParseError::InvalidOperation(e.to_string()))?;

        // Check if this is auto-commit
        if msg.get_header("auto_commit").is_some() {
            return Ok(OrderedMessage::AutoCommitOperation {
                txn_id,
                coordinator_id,
                request_id,
                txn_deadline,
                operation,
            });
        }

        // Regular transaction operation
        Ok(OrderedMessage::TransactionOperation {
            txn_id,
            coordinator_id,
            request_id,
            txn_deadline,
            operation,
        })
    }

    /// Convert to a raw Message for sending
    pub fn into_message(self) -> Message {
        match self {
            OrderedMessage::TransactionOperation {
                txn_id,
                coordinator_id,
                request_id,
                txn_deadline,
                operation,
            } => {
                let mut headers = HashMap::new();
                headers.insert("txn_id".to_string(), txn_id.to_string());
                headers.insert("coordinator_id".to_string(), coordinator_id);
                headers.insert("request_id".to_string(), request_id);
                headers.insert("txn_deadline".to_string(), txn_deadline.to_string());

                let body = serde_json::to_vec(&operation).unwrap();
                Message::new(body, headers)
            }
            OrderedMessage::AutoCommitOperation {
                txn_id,
                coordinator_id,
                request_id,
                txn_deadline,
                operation,
            } => {
                let mut headers = HashMap::new();
                headers.insert("txn_id".to_string(), txn_id.to_string());
                headers.insert("coordinator_id".to_string(), coordinator_id);
                headers.insert("request_id".to_string(), request_id);
                headers.insert("txn_deadline".to_string(), txn_deadline.to_string());
                headers.insert("auto_commit".to_string(), "1".to_string());

                let body = serde_json::to_vec(&operation).unwrap();
                Message::new(body, headers)
            }
            OrderedMessage::TransactionControl {
                txn_id,
                phase,
                coordinator_id,
                request_id,
            } => {
                let mut headers = HashMap::new();
                headers.insert("txn_id".to_string(), txn_id.to_string());
                headers.insert("txn_phase".to_string(), phase.phase_name().to_string());

                if let Some(coordinator_id) = coordinator_id {
                    headers.insert("coordinator_id".to_string(), coordinator_id);
                }

                if let Some(request_id) = request_id {
                    headers.insert("request_id".to_string(), request_id);
                }

                // Add participants if this is a prepare phase
                if let Some(participants) = phase.participants() {
                    headers.insert(
                        "participants".to_string(),
                        serde_json::to_string(&participants).unwrap(),
                    );
                }

                Message::new(Vec::new(), headers)
            }
            OrderedMessage::Noop => {
                let mut headers = HashMap::new();
                headers.insert("noop".to_string(), "true".to_string());
                Message::new(Vec::new(), headers)
            }
        }
    }

    /// Get transaction ID if this message has one
    pub fn txn_id(&self) -> Option<TransactionId> {
        match self {
            OrderedMessage::TransactionOperation { txn_id, .. } => Some(*txn_id),
            OrderedMessage::AutoCommitOperation { txn_id, .. } => Some(*txn_id),
            OrderedMessage::TransactionControl { txn_id, .. } => Some(*txn_id),
            OrderedMessage::Noop => None,
        }
    }

    /// Get coordinator ID for sending responses
    pub fn coordinator_id(&self) -> Option<&str> {
        match self {
            OrderedMessage::TransactionOperation { coordinator_id, .. } => Some(coordinator_id),
            OrderedMessage::AutoCommitOperation { coordinator_id, .. } => Some(coordinator_id),
            OrderedMessage::TransactionControl { coordinator_id, .. } => coordinator_id.as_deref(),
            OrderedMessage::Noop => None,
        }
    }

    /// Get request ID for matching responses
    pub fn request_id(&self) -> Option<&str> {
        match self {
            OrderedMessage::TransactionOperation { request_id, .. } => Some(request_id),
            OrderedMessage::AutoCommitOperation { request_id, .. } => Some(request_id),
            OrderedMessage::TransactionControl { request_id, .. } => request_id.as_deref(),
            OrderedMessage::Noop => None,
        }
    }
}

impl<O: Operation> ReadOnlyMessage<O> {
    /// Parse a Message from pubsub into a typed ReadOnlyMessage
    pub fn from_message(msg: Message) -> Result<Self, ParseError> {
        let read_ts_str = msg
            .get_header("read_timestamp")
            .ok_or(ParseError::MissingHeader("read_timestamp"))?;
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

        let operation: O = serde_json::from_slice(&msg.body)
            .map_err(|e| ParseError::InvalidOperation(e.to_string()))?;

        Ok(ReadOnlyMessage {
            read_timestamp,
            coordinator_id,
            request_id,
            operation,
        })
    }

    /// Convert to a raw Message for sending
    pub fn into_message(self) -> Message {
        let mut headers = HashMap::new();
        headers.insert(
            "read_timestamp".to_string(),
            self.read_timestamp.to_string(),
        );
        headers.insert("coordinator_id".to_string(), self.coordinator_id);
        headers.insert("request_id".to_string(), self.request_id);

        let body = serde_json::to_vec(&self.operation).unwrap();
        Message::new(body, headers)
    }
}

/// Errors that can occur when parsing messages
#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("Missing required header: {0}")]
    MissingHeader(&'static str),

    #[error("Invalid transaction ID: {0}")]
    InvalidTransactionId(String),

    #[error("Invalid timestamp: {0}")]
    InvalidTimestamp(String),

    #[error("Invalid transaction phase: {0}")]
    InvalidPhase(String),

    #[error("Invalid operation: {0}")]
    InvalidOperation(String),

    #[error("Missing participants for prepare phase")]
    MissingParticipants,
}
