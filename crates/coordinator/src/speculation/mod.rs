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

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

// Re-export V2 types as primary
pub use learning::{SequenceLearner, SequencePattern};
pub use prediction_context::{CheckResult, PredictionContext};
pub use predictor::SequencePredictor;

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
}

impl Default for SpeculationConfig {
    fn default() -> Self {
        Self {
            min_confidence_read: 0.8,
            min_confidence_write: 0.95,
            max_speculation_per_txn: 100,
            detect_string_patterns: true,
            min_occurrences: 5,
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
    learner: std::sync::Arc<std::sync::Mutex<SequenceLearner>>,
    predictor: SequencePredictor,
}

impl SpeculationContext {
    /// Create a new speculation context
    pub fn new(config: SpeculationConfig) -> Self {
        use std::sync::{Arc, Mutex};

        let learner = Arc::new(Mutex::new(SequenceLearner::new(config.clone())));
        let predictor = SequencePredictor::new(config.clone());

        Self { learner, predictor }
    }

    /// Create a prediction context for a new transaction
    pub fn create_prediction_context(&self, category: &str, args: &[Value]) -> PredictionContext {
        let learner = self.learner.lock().unwrap();

        if let Some(prediction) = self.predictor.predict(category, args, &learner) {
            PredictionContext::new(prediction, self.learner.clone())
        } else {
            PredictionContext::empty(self.learner.clone())
        }
    }

    /// Get current patterns (for debugging/monitoring)
    #[allow(dead_code)]
    pub fn get_patterns(&self) -> HashMap<String, SequencePattern> {
        let learner = self.learner.lock().unwrap();
        learner.get_all_patterns().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_sequence_learning_and_prediction() {
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

            // Simulate executing operations
            pred_ctx.check(
                "kv_stream",
                &json!({"Get": {"key": format!("user:{}:balance", user_id)}}),
                false,
            );
            pred_ctx.check(
                "kv_stream",
                &json!({"Put": {"key": format!("user:{}:balance", user_id), "value": 100 + i}}),
                true,
            );

            // First 3 have same pattern
            if i < 3 {
                pred_ctx.check(
                    "kv_stream",
                    &json!({"Get": {"key": format!("user:{}:history", user_id)}}),
                    false,
                );
            }
            // 4th diverges after first two operations

            // Report outcome - this is where learning happens
            pred_ctx.report_outcome("transfer", &args, true);
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

        // Simulate transaction execution - operations match predictions
        let result1 = pred_context.check(
            "kv_stream",
            &json!({"Get": {"key": "user:alice:balance"}}),
            false,
        );
        assert!(matches!(result1, CheckResult::Match { .. }));

        let result2 = pred_context.check(
            "kv_stream",
            &json!({"Put": {"key": "user:alice:balance", "value": 500}}),
            true,
        );
        assert!(matches!(result2, CheckResult::Match { .. }));

        // Should have no more predictions
        let result3 = pred_context.check(
            "kv_stream",
            &json!({"Get": {"key": "user:alice:history"}}),
            false,
        );
        assert!(matches!(result3, CheckResult::NoPrediction));
    }

    #[test]
    fn test_speculation_failure_detection() {
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
        pred_ctx.check("kv", &json!({"Get": {"key": "user:bob"}}), false);
        pred_ctx.check(
            "kv",
            &json!({"Put": {"key": "user:bob", "value": 100}}),
            true,
        );
        pred_ctx.report_outcome("test", &args, true);

        // Create prediction for new transaction
        let new_args = vec![json!({"user": "alice", "amount": 200})];
        let mut pred_context = context.create_prediction_context("test", &new_args);

        // First operation matches
        let result1 = pred_context.check("kv", &json!({"Get": {"key": "user:alice"}}), false);
        assert!(matches!(result1, CheckResult::Match { .. }));

        // Second operation doesn't match (different amount)
        let result2 = pred_context.check(
            "kv",
            &json!({"Put": {"key": "user:alice", "value": 999}}),
            true,
        );
        assert!(matches!(result2, CheckResult::SpeculationFailed { .. }));

        assert!(pred_context.has_failed());
    }
}
