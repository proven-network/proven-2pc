use serde::Serialize;
use std::fmt::Debug;

/// Trait for change data from commits
pub trait ChangeData: Serialize + Send + Sync + Debug {
    /// Merge two change data objects
    fn merge(self, other: Self) -> Self;
}

/// Example implementation for testing
#[cfg(test)]
mod tests {
    use super::*;

    use serde::Deserialize;

    #[derive(Debug, Serialize, Deserialize)]
    struct TestChangeData {
        success: bool,
        data: Option<String>,
    }

    impl ChangeData for TestChangeData {
        fn merge(self, _other: Self) -> Self {
            self
        }
    }

    #[test]
    fn test_response_traits() {
        let data = TestChangeData {
            success: true,
            data: Some("result".to_string()),
        };
        let other = TestChangeData {
            success: false,
            data: Some("other".to_string()),
        };
        let merged = data.merge(other);
        assert!(merged.success);
        assert_eq!(merged.data, Some("result".to_string()));
    }
}
