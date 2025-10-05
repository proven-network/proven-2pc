//! KV client for coordinator-based transactions

use proven_coordinator::Executor;
use proven_kv::{KvOperation, KvResponse, Value};
use std::sync::Arc;

/// KV client that works with coordinator executors
pub struct KvClient<E: Executor> {
    /// The executor this client is associated with
    executor: Arc<E>,
}

impl<E: Executor> KvClient<E> {
    /// Create a new KV client for an executor
    pub fn new(executor: Arc<E>) -> Self {
        Self { executor }
    }

    /// Get a value by key
    pub async fn get(
        &self,
        stream_name: impl Into<String>,
        key: impl Into<String>,
    ) -> Result<Option<Value>, KvError> {
        let stream_name = stream_name.into();
        let key_str = key.into();
        let operation = KvOperation::Get {
            key: key_str.clone(),
        };

        let response = self.execute_operation(stream_name, operation).await?;

        match response {
            KvResponse::GetResult { key: _, value } => Ok(value),
            KvResponse::Error(e) => Err(KvError::OperationError(e)),
            _ => Err(KvError::UnexpectedResponse),
        }
    }

    /// Put a key-value pair
    pub async fn put(
        &self,
        stream_name: impl Into<String>,
        key: impl Into<String>,
        value: Value,
    ) -> Result<Option<Value>, KvError> {
        let stream_name = stream_name.into();
        let operation = KvOperation::Put {
            key: key.into(),
            value,
        };

        let response = self.execute_operation(stream_name, operation).await?;

        match response {
            KvResponse::PutResult { key: _, previous } => Ok(previous),
            KvResponse::Error(e) => Err(KvError::OperationError(e)),
            _ => Err(KvError::UnexpectedResponse),
        }
    }

    /// Delete a key
    pub async fn delete(
        &self,
        stream_name: impl Into<String>,
        key: impl Into<String>,
    ) -> Result<bool, KvError> {
        let stream_name = stream_name.into();
        let operation = KvOperation::Delete { key: key.into() };

        let response = self.execute_operation(stream_name, operation).await?;

        match response {
            KvResponse::DeleteResult { key: _, deleted } => Ok(deleted),
            KvResponse::Error(e) => Err(KvError::OperationError(e)),
            _ => Err(KvError::UnexpectedResponse),
        }
    }

    /// Put a string value
    pub async fn put_string(
        &self,
        stream_name: impl Into<String>,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<Option<Value>, KvError> {
        self.put(stream_name, key, Value::String(value.into()))
            .await
    }

    /// Put bytes value
    pub async fn put_bytes(
        &self,
        stream_name: impl Into<String>,
        key: impl Into<String>,
        value: impl Into<Vec<u8>>,
    ) -> Result<Option<Value>, KvError> {
        self.put(stream_name, key, Value::Bytes(value.into())).await
    }

    /// Put integer value
    pub async fn put_integer(
        &self,
        stream_name: impl Into<String>,
        key: impl Into<String>,
        value: i64,
    ) -> Result<Option<Value>, KvError> {
        self.put(stream_name, key, Value::Integer(value)).await
    }

    /// Get a string value
    pub async fn get_string(
        &self,
        stream_name: impl Into<String>,
        key: impl Into<String>,
    ) -> Result<Option<String>, KvError> {
        match self.get(stream_name, key).await? {
            Some(Value::String(s)) => Ok(Some(s)),
            Some(_) => Err(KvError::TypeMismatch),
            None => Ok(None),
        }
    }

    /// Get bytes value
    pub async fn get_bytes(
        &self,
        stream_name: impl Into<String>,
        key: impl Into<String>,
    ) -> Result<Option<Vec<u8>>, KvError> {
        match self.get(stream_name, key).await? {
            Some(Value::Bytes(b)) => Ok(Some(b)),
            Some(_) => Err(KvError::TypeMismatch),
            None => Ok(None),
        }
    }

    /// Get integer value
    pub async fn get_integer(
        &self,
        stream_name: impl Into<String>,
        key: impl Into<String>,
    ) -> Result<Option<i64>, KvError> {
        match self.get(stream_name, key).await? {
            Some(Value::Integer(i)) => Ok(Some(i)),
            Some(_) => Err(KvError::TypeMismatch),
            None => Ok(None),
        }
    }

    /// Execute an operation and deserialize the response
    async fn execute_operation(
        &self,
        stream_name: String,
        operation: KvOperation,
    ) -> Result<KvResponse, KvError> {
        // Execute through the executor with the operation object
        let response_bytes = self
            .executor
            .execute(stream_name, &operation)
            .await
            .map_err(|e| KvError::CoordinatorError(e.to_string()))?;

        // Deserialize the response
        let response = serde_json::from_slice(&response_bytes)
            .map_err(|e| KvError::DeserializationError(e.to_string()))?;

        Ok(response)
    }
}

/// KV-specific error type
#[derive(Debug, thiserror::Error)]
pub enum KvError {
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
