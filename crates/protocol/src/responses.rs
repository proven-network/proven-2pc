//! Typed response building for participant-to-coordinator communication

use proven_common::{Response, TransactionId};
use proven_engine::Message;
use serde_json;
use std::collections::HashMap;

/// Response status from participant
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResponseStatus {
    /// Operation completed successfully
    Complete,
    /// Transaction prepared successfully
    Prepared,
    /// Transaction was wounded by another transaction
    Wounded { wounded_by: TransactionId },
    /// Error occurred during execution
    Error { message: String },
}

/// Typed participant response
#[derive(Debug, Clone)]
pub struct ParticipantResponse {
    /// Participant (stream) name
    pub participant: String,
    /// Engine name (sql, kv, etc.)
    pub engine: String,
    /// Transaction ID (if applicable)
    pub txn_id: Option<String>,
    /// Request ID for matching
    pub request_id: Option<String>,
    /// Response status
    pub status: ResponseStatus,
    /// Response body (serialized result for Complete status)
    pub body: Vec<u8>,
}

impl ParticipantResponse {
    /// Convert to a raw Message for sending via pubsub
    pub fn into_message(self) -> Message {
        let mut headers = HashMap::new();
        headers.insert("participant".to_string(), self.participant);
        headers.insert("engine".to_string(), self.engine);

        if let Some(txn_id) = self.txn_id {
            headers.insert("txn_id".to_string(), txn_id);
        }

        if let Some(request_id) = self.request_id {
            headers.insert("request_id".to_string(), request_id);
        }

        match self.status {
            ResponseStatus::Complete => {
                headers.insert("status".to_string(), "complete".to_string());
            }
            ResponseStatus::Prepared => {
                headers.insert("status".to_string(), "prepared".to_string());
            }
            ResponseStatus::Wounded { wounded_by } => {
                headers.insert("status".to_string(), "wounded".to_string());
                headers.insert("wounded_by".to_string(), wounded_by.to_string());
            }
            ResponseStatus::Error { message } => {
                headers.insert("status".to_string(), "error".to_string());
                headers.insert("error".to_string(), message);
            }
        }

        Message::new(self.body, headers)
    }

    /// Parse from a raw Message
    pub fn from_message(msg: Message) -> Result<Self, ParseError> {
        let participant = msg
            .get_header("participant")
            .ok_or(ParseError::MissingHeader("participant"))?
            .to_string();

        let engine = msg.get_header("engine").unwrap_or("unknown").to_string();

        let txn_id = msg.get_header("txn_id").map(String::from);
        let request_id = msg.get_header("request_id").map(String::from);

        let status = match msg.get_header("status") {
            Some("complete") => ResponseStatus::Complete,
            Some("prepared") => ResponseStatus::Prepared,
            Some("wounded") => {
                let wounded_by_str = msg
                    .get_header("wounded_by")
                    .ok_or(ParseError::MissingHeader("wounded_by"))?;
                let wounded_by = TransactionId::parse(wounded_by_str)
                    .map_err(|_| ParseError::InvalidTransactionId(wounded_by_str.to_string()))?;
                ResponseStatus::Wounded { wounded_by }
            }
            Some("error") => {
                let message = msg
                    .get_header("error")
                    .unwrap_or("Unknown error")
                    .to_string();
                ResponseStatus::Error { message }
            }
            Some(other) => return Err(ParseError::InvalidStatus(other.to_string())),
            None => return Err(ParseError::MissingHeader("status")),
        };

        Ok(ParticipantResponse {
            participant,
            engine,
            txn_id,
            request_id,
            status,
            body: msg.body,
        })
    }
}

/// Builder for creating participant responses
pub struct ResponseBuilder {
    participant: String,
    engine: String,
}

impl ResponseBuilder {
    /// Create a new response builder
    pub fn new(participant: String, engine: String) -> Self {
        Self {
            participant,
            engine,
        }
    }

    /// Build a successful operation response
    pub fn success<R: Response>(
        &self,
        txn_id: Option<&str>,
        request_id: Option<String>,
        response: R,
    ) -> Result<ParticipantResponse, serde_json::Error> {
        let body = serde_json::to_vec(&response)?;
        Ok(ParticipantResponse {
            participant: self.participant.clone(),
            engine: self.engine.clone(),
            txn_id: txn_id.map(String::from),
            request_id,
            status: ResponseStatus::Complete,
            body,
        })
    }

    /// Build a prepared response
    pub fn prepared(&self, txn_id: &str, request_id: Option<String>) -> ParticipantResponse {
        ParticipantResponse {
            participant: self.participant.clone(),
            engine: self.engine.clone(),
            txn_id: Some(txn_id.to_string()),
            request_id,
            status: ResponseStatus::Prepared,
            body: Vec::new(),
        }
    }

    /// Build a wounded response
    pub fn wounded(
        &self,
        txn_id: &str,
        wounded_by: TransactionId,
        request_id: Option<String>,
    ) -> ParticipantResponse {
        ParticipantResponse {
            participant: self.participant.clone(),
            engine: self.engine.clone(),
            txn_id: Some(txn_id.to_string()),
            request_id,
            status: ResponseStatus::Wounded { wounded_by },
            body: Vec::new(),
        }
    }

    /// Build an error response
    pub fn error(
        &self,
        txn_id: Option<&str>,
        error: String,
        request_id: Option<String>,
    ) -> ParticipantResponse {
        ParticipantResponse {
            participant: self.participant.clone(),
            engine: self.engine.clone(),
            txn_id: txn_id.map(String::from),
            request_id,
            status: ResponseStatus::Error { message: error },
            body: Vec::new(),
        }
    }
}

/// Errors that can occur when parsing responses
#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("Missing required header: {0}")]
    MissingHeader(&'static str),

    #[error("Invalid transaction ID: {0}")]
    InvalidTransactionId(String),

    #[error("Invalid status: {0}")]
    InvalidStatus(String),
}
