//! Transaction operation and response traits
//!
//! This crate defines the core abstractions for operations and responses
//! that can be executed within transactions, supporting both synchronous
//! execution and speculative pre-fetching.

use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// Type of operation - read or write
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperationType {
    /// Read operation - does not modify state
    Read,
    /// Write operation - modifies state
    Write,
}

/// Trait for operations that can be executed within a transaction
pub trait Operation:
    serde::de::DeserializeOwned + serde::Serialize + Send + Sync + Debug + Clone + PartialEq + Eq
{
    /// Get the type of this operation (read or write)
    fn operation_type(&self) -> OperationType;

    /// Convert this operation to a JSON value for pattern analysis
    fn as_json_value(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap_or(serde_json::Value::Null)
    }
}

/// Trait for responses from operations
pub trait Response: serde::Serialize + Send + Sync + Debug {
    /// Convert to bytes for backward compatibility with existing code
    fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }
}

/// Example implementation for testing
#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct TestOperation {
        query: String,
        stream: String,
        is_write: bool,
    }

    impl Operation for TestOperation {
        fn operation_type(&self) -> OperationType {
            if self.is_write {
                OperationType::Write
            } else {
                OperationType::Read
            }
        }
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct TestResponse {
        success: bool,
        data: Option<String>,
    }

    impl Response for TestResponse {}

    #[test]
    fn test_operation_traits() {
        let op = TestOperation {
            query: "SELECT * FROM users".to_string(),
            stream: "users".to_string(),
            is_write: false,
        };

        assert_eq!(op.operation_type(), OperationType::Read);

        let json = op.as_json_value();
        assert!(json.is_object());
    }

    #[test]
    fn test_response_traits() {
        let resp = TestResponse {
            success: true,
            data: Some("result".to_string()),
        };

        assert!(!resp.to_bytes().is_empty());
    }

    #[test]
    fn test_operation_clone_and_eq() {
        let op1 = TestOperation {
            query: "SELECT * FROM users".to_string(),
            stream: "users".to_string(),
            is_write: false,
        };

        let op2 = op1.clone();
        assert_eq!(op1, op2);
    }
}
