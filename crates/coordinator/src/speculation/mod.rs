//! Dynamic speculation system for learning and predicting transaction patterns
//!
//! This module provides a learning-based speculation system that discovers correlations
//! between transaction arguments and operations, enabling prediction of database operations
//! before they're explicitly requested.

// V2 modules (sequence-based)
pub mod args;
pub mod learning;
pub mod prediction_context;
pub mod predictor;
pub mod template;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::HashMap, sync::Arc};

// Re-export V2 types as primary
pub use learning::{Learner, SequencePattern};
pub use prediction_context::{CheckResult, PredictionContext};
pub use predictor::Predictor;

/// Configuration for the speculation system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpeculationConfig {
    /// Minimum confidence threshold for read operations
    pub min_confidence_read: f64,

    /// Minimum confidence threshold for write operations
    pub min_confidence_write: f64,

    /// Maximum operations to speculate per transaction
    pub max_speculation_per_txn: usize,

    /// Enable pattern detection in strings (e.g., "user_123" -> "user_*")
    pub detect_string_patterns: bool,

    /// Minimum occurrences before a pattern is considered
    pub min_occurrences: u32,

    /// Minimum confidence threshold for auto-commit read optimization
    pub auto_commit_read_confidence: f64,

    /// Minimum number of samples before considering auto-commit optimization
    pub auto_commit_min_samples: u64,
}

impl Default for SpeculationConfig {
    fn default() -> Self {
        Self {
            min_confidence_read: 0.8,
            min_confidence_write: 0.95,
            max_speculation_per_txn: 100,
            detect_string_patterns: true,
            min_occurrences: 5,
            auto_commit_read_confidence: 0.95,
            auto_commit_min_samples: 50,
        }
    }
}

/// Speculation context using sequence-based learning
///
/// ## Usage Pattern
///
/// ```rust,ignore
/// // At transaction start:
/// let mut pred_context = speculation.create_prediction_context(category, &args);
///
/// // For each operation:
/// match pred_context.check(stream, &operation, is_write) {
///     CheckResult::Match { response, .. } => {
///         // Use speculated response
///     }
///     CheckResult::SpeculationFailed { .. } => {
///         // Abort transaction
///         pred_context.report_outcome(category, &args, false);
///         return Err(SpeculationFailed);
///     }
///     CheckResult::NoPrediction => {
///         // Execute operation normally
///     }
/// }
///
/// // At transaction end (success or failure):
/// pred_context.report_outcome(category, &args, transaction_succeeded);
/// ```
///
/// This ensures ALL transactions go through PredictionContext for consistent learning.
pub struct SpeculationContext {
    learner: Arc<RwLock<Learner>>,
    predictor: Predictor,
}

impl SpeculationContext {
    /// Create a new speculation context
    pub fn new(config: SpeculationConfig) -> Self {
        let learner = Arc::new(RwLock::new(Learner::new(config.clone())));
        let predictor = Predictor::new(config.clone());

        Self { learner, predictor }
    }

    /// Create a prediction context for a new transaction
    pub fn create_prediction_context(&self, category: &str, args: &[Value]) -> PredictionContext {
        let learner = self.learner.read();

        if let Some(prediction) = self.predictor.predict(category, args, &learner) {
            PredictionContext::new(prediction, args.to_vec(), self.learner.clone())
        } else {
            PredictionContext::empty(category.to_string(), args.to_vec(), self.learner.clone())
        }
    }

    /// Get current patterns (for debugging/monitoring)
    #[allow(dead_code)]
    pub fn get_patterns(&self) -> HashMap<String, SequencePattern> {
        let learner = self.learner.read();
        learner.get_all_patterns().clone()
    }

    /// Create an empty prediction context for transactions without speculation
    ///
    /// This is used for retry transactions after speculation failure, or when
    /// we explicitly want to avoid speculation while still learning from the operations.
    pub fn create_empty_context(&self, category: &str, args: &[Value]) -> PredictionContext {
        PredictionContext::empty(category.to_string(), args.to_vec(), self.learner.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn test_sequence_learning_and_prediction() {
        let config = SpeculationConfig {
            min_confidence_read: 0.0,
            min_confidence_write: 0.0,
            min_occurrences: 3,
            ..Default::default()
        };

        let context = SpeculationContext::new(config.clone());

        // Learn from similar transactions using PredictionContext
        for i in 0..4 {
            let user_id = format!("user_{}", i);
            let args = vec![json!({
                "user_id": user_id.clone(),
                "amount": 100 + i,
            })];

            // Create prediction context (even for learning - ensures consistency)
            let mut pred_ctx = context.create_prediction_context("transfer", &args);

            // For learning, we don't need responses, just track operations
            // Simulate executing operations (they'll return NoPrediction since no responses set)
            let _ = pred_ctx
                .check(
                    "kv_stream",
                    &json!({"Get": {"key": format!("user:{}:balance", user_id)}}),
                    false,
                )
                .await;
            let _ = pred_ctx
                .check(
                    "kv_stream",
                    &json!({"Put": {"key": format!("user:{}:balance", user_id), "value": 100 + i}}),
                    true,
                )
                .await;

            // First 3 have same pattern
            if i < 3 {
                let _ = pred_ctx
                    .check(
                        "kv_stream",
                        &json!({"Get": {"key": format!("user:{}:history", user_id)}}),
                        false,
                    )
                    .await;
            }
            // 4th diverges after first two operations

            // Report outcome - this is where learning happens
            pred_ctx.report_outcome(true);
        }

        // Create prediction context for new transaction
        let new_args = vec![json!({
            "user_id": "alice",
            "amount": 500,
        })];

        let mut pred_context = context.create_prediction_context("transfer", &new_args);

        // Should have learned common prefix of 2 operations
        assert_eq!(
            pred_context.prediction_count(),
            2,
            "Should learn common prefix"
        );

        // Simulate speculation execution by creating fake receivers
        {
            let (tx0, rx0) = tokio::sync::oneshot::channel();
            let (tx1, rx1) = tokio::sync::oneshot::channel();
            let _ = tx0.send(Ok(b"fake_response_1".to_vec()));
            let _ = tx1.send(Ok(b"fake_response_2".to_vec()));
            pred_context.set_test_receiver(0, rx0);
            pred_context.set_test_receiver(1, rx1);
        }

        // Simulate transaction execution - operations match predictions
        let result1 = pred_context
            .check(
                "kv_stream",
                &json!({"Get": {"key": "user:alice:balance"}}),
                false,
            )
            .await;
        assert!(matches!(result1, CheckResult::Match { .. }));

        let result2 = pred_context
            .check(
                "kv_stream",
                &json!({"Put": {"key": "user:alice:balance", "value": 500}}),
                true,
            )
            .await;
        assert!(matches!(result2, CheckResult::Match { .. }));

        // Should have no more predictions
        let result3 = pred_context
            .check(
                "kv_stream",
                &json!({"Get": {"key": "user:alice:history"}}),
                false,
            )
            .await;
        assert!(matches!(result3, CheckResult::NoPrediction));
    }

    #[tokio::test]
    async fn test_speculation_failure_detection() {
        let config = SpeculationConfig {
            min_confidence_read: 0.0,
            min_confidence_write: 0.0,
            min_occurrences: 1,
            ..Default::default()
        };

        let context = SpeculationContext::new(config.clone());

        // Learn a pattern using PredictionContext
        let args = vec![json!({"user": "bob", "amount": 100})];
        let mut pred_ctx = context.create_prediction_context("test", &args);
        let _ = pred_ctx
            .check("kv", &json!({"Get": {"key": "user:bob"}}), false)
            .await;
        let _ = pred_ctx
            .check(
                "kv",
                &json!({"Put": {"key": "user:bob", "value": 100}}),
                true,
            )
            .await;
        pred_ctx.report_outcome(true);

        // Create prediction for new transaction
        let new_args = vec![json!({"user": "alice", "amount": 200})];
        let mut pred_context = context.create_prediction_context("test", &new_args);

        // Simulate speculation execution for first operation only
        {
            let (tx0, rx0) = tokio::sync::oneshot::channel();
            let _ = tx0.send(Ok(b"fake_response".to_vec()));
            pred_context.set_test_receiver(0, rx0);
        }

        // First operation matches
        let result1 = pred_context
            .check("kv", &json!({"Get": {"key": "user:alice"}}), false)
            .await;
        assert!(matches!(result1, CheckResult::Match { .. }));

        // Second operation doesn't match (different amount)
        let result2 = pred_context
            .check(
                "kv",
                &json!({"Put": {"key": "user:alice", "value": 999}}),
                true,
            )
            .await;
        assert!(matches!(result2, CheckResult::SpeculationMismatch { .. }));

        assert!(pred_context.has_failed());
    }
}
