use serde::Serialize;
use std::fmt::Debug;

/// Trait for responses from operations
pub trait Response: Serialize + Send + Sync + Debug {
    /// Convert to bytes for backward compatibility with existing code
    fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }
}

/// Example implementation for testing
#[cfg(test)]
mod tests {
    use super::*;

    use serde::Deserialize;

    #[derive(Debug, Serialize, Deserialize)]
    struct TestResponse {
        success: bool,
        data: Option<String>,
    }

    impl Response for TestResponse {}

    #[test]
    fn test_response_traits() {
        let resp = TestResponse {
            success: true,
            data: Some("result".to_string()),
        };

        assert!(!resp.to_bytes().is_empty());
    }
}
