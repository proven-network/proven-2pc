//! Queue client for coordinator-based transactions

use crate::stream::operation::QueueOperation;
use crate::stream::response::QueueResponse;
use crate::types::QueueValue;
use proven_coordinator::Transaction;

/// Queue client that works with coordinator transactions
#[derive(Clone)]
pub struct QueueClient {
    /// The transaction this client is associated with
    transaction: Transaction,
}

impl QueueClient {
    /// Create a new Queue client for a transaction
    pub fn new(transaction: Transaction) -> Self {
        Self { transaction }
    }

    /// Enqueue a value
    pub async fn enqueue(
        &self,
        stream_name: impl Into<String>,
        queue_name: impl Into<String>,
        value: QueueValue,
    ) -> Result<(), QueueError> {
        let stream_name = stream_name.into();
        let operation = QueueOperation::Enqueue {
            queue_name: queue_name.into(),
            value,
        };

        let response = self.execute_operation(stream_name, operation).await?;

        match response {
            QueueResponse::Enqueued => Ok(()),
            QueueResponse::Error(e) => Err(QueueError::OperationError(e)),
            _ => Err(QueueError::UnexpectedResponse),
        }
    }

    /// Enqueue bytes
    pub async fn enqueue_bytes(
        &self,
        stream_name: impl Into<String>,
        queue_name: impl Into<String>,
        value: Vec<u8>,
    ) -> Result<(), QueueError> {
        self.enqueue(stream_name, queue_name, QueueValue::Bytes(value))
            .await
    }

    /// Enqueue a string
    pub async fn enqueue_string(
        &self,
        stream_name: impl Into<String>,
        queue_name: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<(), QueueError> {
        self.enqueue(stream_name, queue_name, QueueValue::String(value.into()))
            .await
    }

    /// Enqueue JSON value
    pub async fn enqueue_json(
        &self,
        stream_name: impl Into<String>,
        queue_name: impl Into<String>,
        value: serde_json::Value,
    ) -> Result<(), QueueError> {
        self.enqueue(stream_name, queue_name, QueueValue::Json(value))
            .await
    }

    /// Dequeue a value
    pub async fn dequeue(
        &self,
        stream_name: impl Into<String>,
        queue_name: impl Into<String>,
    ) -> Result<Option<QueueValue>, QueueError> {
        let stream_name = stream_name.into();
        let operation = QueueOperation::Dequeue {
            queue_name: queue_name.into(),
        };

        let response = self.execute_operation(stream_name, operation).await?;

        match response {
            QueueResponse::Dequeued(value) => Ok(value),
            QueueResponse::Error(e) => Err(QueueError::OperationError(e)),
            _ => Err(QueueError::UnexpectedResponse),
        }
    }

    /// Dequeue bytes
    pub async fn dequeue_bytes(
        &self,
        stream_name: impl Into<String>,
        queue_name: impl Into<String>,
    ) -> Result<Option<Vec<u8>>, QueueError> {
        match self.dequeue(stream_name, queue_name).await? {
            Some(QueueValue::Bytes(b)) => Ok(Some(b)),
            Some(_) => Err(QueueError::TypeMismatch),
            None => Ok(None),
        }
    }

    /// Dequeue a string
    pub async fn dequeue_string(
        &self,
        stream_name: impl Into<String>,
        queue_name: impl Into<String>,
    ) -> Result<Option<String>, QueueError> {
        match self.dequeue(stream_name, queue_name).await? {
            Some(QueueValue::String(s)) => Ok(Some(s)),
            Some(_) => Err(QueueError::TypeMismatch),
            None => Ok(None),
        }
    }

    /// Peek at the next value without removing it
    pub async fn peek(
        &self,
        stream_name: impl Into<String>,
        queue_name: impl Into<String>,
    ) -> Result<Option<QueueValue>, QueueError> {
        let stream_name = stream_name.into();
        let operation = QueueOperation::Peek {
            queue_name: queue_name.into(),
        };

        let response = self.execute_operation(stream_name, operation).await?;

        match response {
            QueueResponse::Peeked(value) => Ok(value),
            QueueResponse::Error(e) => Err(QueueError::OperationError(e)),
            _ => Err(QueueError::UnexpectedResponse),
        }
    }

    /// Get queue size
    pub async fn size(
        &self,
        stream_name: impl Into<String>,
        queue_name: impl Into<String>,
    ) -> Result<usize, QueueError> {
        let stream_name = stream_name.into();
        let operation = QueueOperation::Size {
            queue_name: queue_name.into(),
        };

        let response = self.execute_operation(stream_name, operation).await?;

        match response {
            QueueResponse::Size(size) => Ok(size),
            QueueResponse::Error(e) => Err(QueueError::OperationError(e)),
            _ => Err(QueueError::UnexpectedResponse),
        }
    }

    /// Check if queue is empty
    pub async fn is_empty(
        &self,
        stream_name: impl Into<String>,
        queue_name: impl Into<String>,
    ) -> Result<bool, QueueError> {
        let stream_name = stream_name.into();
        let operation = QueueOperation::IsEmpty {
            queue_name: queue_name.into(),
        };

        let response = self.execute_operation(stream_name, operation).await?;

        match response {
            QueueResponse::IsEmpty(is_empty) => Ok(is_empty),
            QueueResponse::Error(e) => Err(QueueError::OperationError(e)),
            _ => Err(QueueError::UnexpectedResponse),
        }
    }

    /// Clear a queue
    pub async fn clear(
        &self,
        stream_name: impl Into<String>,
        queue_name: impl Into<String>,
    ) -> Result<(), QueueError> {
        let stream_name = stream_name.into();
        let operation = QueueOperation::Clear {
            queue_name: queue_name.into(),
        };

        let response = self.execute_operation(stream_name, operation).await?;

        match response {
            QueueResponse::Cleared => Ok(()),
            QueueResponse::Error(e) => Err(QueueError::OperationError(e)),
            _ => Err(QueueError::UnexpectedResponse),
        }
    }

    /// Execute an operation and deserialize the response
    async fn execute_operation(
        &self,
        stream_name: String,
        operation: QueueOperation,
    ) -> Result<QueueResponse, QueueError> {
        // Serialize the operation
        let operation_bytes = serde_json::to_vec(&operation)
            .map_err(|e| QueueError::SerializationError(e.to_string()))?;

        // Execute through the transaction
        let response_bytes = self
            .transaction
            .execute(stream_name, operation_bytes)
            .await
            .map_err(|e| QueueError::CoordinatorError(e.to_string()))?;

        // Deserialize the response
        let response = serde_json::from_slice(&response_bytes)
            .map_err(|e| QueueError::DeserializationError(e.to_string()))?;

        Ok(response)
    }
}

/// Queue-specific error type
#[derive(Debug, thiserror::Error)]
pub enum QueueError {
    #[error("Coordinator error: {0}")]
    CoordinatorError(String),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Deserialization error: {0}")]
    DeserializationError(String),

    #[error("Operation error: {0}")]
    OperationError(String),

    #[error("Type mismatch")]
    TypeMismatch,

    #[error("Unexpected response type")]
    UnexpectedResponse,
}
