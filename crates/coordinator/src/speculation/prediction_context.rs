//! Prediction context that acts as a guard during transaction execution
//!
//! This module provides a stateful context that validates operations against
//! predicted sequences and provides speculated responses when operations match.

use crate::speculation::learning::SequenceLearner;
use crate::speculation::predictor::{PredictedOperation, PredictionResult};
use serde_json::Value;
use std::sync::{Arc, Mutex};

/// Result of checking an operation against predictions
#[derive(Debug)]
pub enum CheckResult {
    /// Operation matches prediction - here's the speculated response
    Match { response: Value, is_write: bool },

    /// Operation doesn't match - speculation failed
    SpeculationFailed {
        expected: String,
        actual: String,
        position: usize,
    },

    /// No more predictions (transaction continues beyond speculation)
    NoPrediction,
}

/// Tracks the state of speculation during transaction execution
#[derive(Clone)]
pub struct PredictionContext {
    /// The predicted sequence
    predictions: Vec<PredictedOperation>,

    /// Current position in the sequence
    current_position: usize,

    /// Pattern ID for feedback
    pattern_id: String,

    /// Category for learning
    category: String,

    /// Where speculation failed (if it did)
    failure_point: Option<usize>,

    /// Reference to learner for feedback
    learner: Arc<Mutex<SequenceLearner>>,

    /// Operations that were actually executed
    executed_operations: Vec<(String, Value, bool)>,
}

impl PredictionContext {
    /// Create a new prediction context from a prediction result
    pub fn new(prediction: PredictionResult, learner: Arc<Mutex<SequenceLearner>>) -> Self {
        Self {
            predictions: prediction.operations,
            current_position: 0,
            pattern_id: prediction.pattern_id,
            category: prediction.category,
            failure_point: None,
            learner,
            executed_operations: Vec::new(),
        }
    }

    /// Create an empty context when no speculation is available
    pub fn empty(learner: Arc<Mutex<SequenceLearner>>) -> Self {
        Self {
            predictions: Vec::new(),
            current_position: 0,
            pattern_id: String::new(),
            category: String::new(),
            failure_point: None,
            learner,
            executed_operations: Vec::new(),
        }
    }

    /// Check if an operation matches the next predicted operation
    pub fn check(&mut self, stream: &str, operation: &Value, is_write: bool) -> CheckResult {
        // Record this operation for learning
        self.executed_operations
            .push((stream.to_string(), operation.clone(), is_write));

        // Check if we have a prediction for this position
        if self.current_position >= self.predictions.len() {
            return CheckResult::NoPrediction;
        }

        let predicted = &self.predictions[self.current_position];

        // Check if operation matches prediction
        if self.operations_match(predicted, stream, operation, is_write) {
            // Match! Return the speculated response
            let response = predicted
                .expected_response
                .clone()
                .unwrap_or_else(|| self.generate_default_response(is_write));

            self.current_position += 1;

            CheckResult::Match { response, is_write }
        } else {
            // Mismatch - speculation failed
            self.failure_point = Some(self.current_position);

            CheckResult::SpeculationFailed {
                expected: format!(
                    "{}/{}/{}",
                    predicted.stream, predicted.operation, predicted.is_write
                ),
                actual: format!("{}/{}/{}", stream, operation, is_write),
                position: self.current_position,
            }
        }
    }

    /// Check if two operations are equivalent
    fn operations_match(
        &self,
        predicted: &PredictedOperation,
        stream: &str,
        operation: &Value,
        is_write: bool,
    ) -> bool {
        // Basic checks
        if predicted.stream != stream || predicted.is_write != is_write {
            return false;
        }

        // Compare operation structure
        // This is simplified - in reality we might want fuzzy matching
        self.values_equivalent(&predicted.operation, operation)
    }

    /// Check if two JSON values are equivalent (allowing some flexibility)
    fn values_equivalent(&self, v1: &Value, v2: &Value) -> bool {
        match (v1, v2) {
            (Value::Object(m1), Value::Object(m2)) => {
                // Check all keys match
                if m1.len() != m2.len() || !m1.keys().all(|k| m2.contains_key(k)) {
                    return false;
                }

                // Check all values match recursively
                m1.iter()
                    .all(|(k, v1)| m2.get(k).map_or(false, |v2| self.values_equivalent(v1, v2)))
            }
            (Value::Array(a1), Value::Array(a2)) => {
                a1.len() == a2.len()
                    && a1
                        .iter()
                        .zip(a2.iter())
                        .all(|(v1, v2)| self.values_equivalent(v1, v2))
            }
            _ => v1 == v2,
        }
    }

    /// Generate a default response for when we don't have a speculated one
    fn generate_default_response(&self, is_write: bool) -> Value {
        if is_write {
            // Write operations typically return success/acknowledgment
            serde_json::json!({
                "status": "ok",
                "speculated": true
            })
        } else {
            // Read operations return empty/null by default
            // In practice, speculation execution would fill this
            serde_json::json!(null)
        }
    }

    /// Set the expected response for a position (called by speculation executor)
    pub fn set_response(&mut self, position: usize, response: Value) {
        if position < self.predictions.len() {
            self.predictions[position].expected_response = Some(response);
        }
    }

    /// Check if all predicted operations were executed
    pub fn all_predictions_used(&self) -> bool {
        self.current_position >= self.predictions.len()
    }

    /// Check if speculation failed
    pub fn has_failed(&self) -> bool {
        self.failure_point.is_some()
    }

    /// Get the failure point if speculation failed
    pub fn get_failure_point(&self) -> Option<usize> {
        self.failure_point
    }

    /// Report results back to the learner
    /// This is the PRIMARY way to record all transaction outcomes
    pub fn report_outcome(self, category: &str, args: &[Value], transaction_succeeded: bool) {
        let mut learner = self.learner.lock().unwrap();

        if let Some(failure_pos) = self.failure_point {
            // Speculation failed - truncate pattern at failure point
            if !self.pattern_id.is_empty() {
                learner.record_speculation_failure(&self.pattern_id, failure_pos);
            }

            // Still learn from the actual operations executed (up to failure)
            if transaction_succeeded && !self.executed_operations.is_empty() {
                learner.learn_from_transaction(category, args, &self.executed_operations);
            }
        } else if transaction_succeeded {
            // Transaction succeeded - learn from ALL executed operations
            // This handles both:
            // 1. Transactions that matched predictions
            // 2. Transactions that went beyond predictions (NoPrediction)
            // 3. Transactions with no predictions at all
            if !self.executed_operations.is_empty() {
                learner.learn_from_transaction(category, args, &self.executed_operations);
            }
        }
        // If transaction failed (aborted), we don't learn from it
    }

    /// Get predicted operations (for debugging/monitoring)
    pub fn get_predictions(&self) -> &[PredictedOperation] {
        &self.predictions
    }

    /// Get the number of predictions
    pub fn prediction_count(&self) -> usize {
        self.predictions.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::speculation::SpeculationConfig;
    use crate::speculation::learning::SequenceLearner;
    use crate::speculation::predictor::SequencePredictor;
    use serde_json::json;

    #[test]
    fn test_prediction_context_matching() {
        let config = SpeculationConfig {
            min_occurrences: 1,
            min_confidence_read: 0.0,
            min_confidence_write: 0.0,
            ..Default::default()
        };

        let mut learner = SequenceLearner::new(config.clone());
        let predictor = SequencePredictor::new(config);

        // Learn a pattern
        let args = vec![json!({"user": "alice"})];
        let ops = vec![
            (
                "kv".to_string(),
                json!({"Get": {"key": "user:alice"}}),
                false,
            ),
            (
                "kv".to_string(),
                json!({"Put": {"key": "user:alice", "value": 100}}),
                true,
            ),
        ];
        learner.learn_from_transaction("test", &args, &ops);

        // Create prediction
        let new_args = vec![json!({"user": "bob"})];
        let prediction = predictor.predict("test", &new_args, &learner).unwrap();

        let learner_arc = Arc::new(Mutex::new(learner));
        let mut context = PredictionContext::new(prediction, learner_arc.clone());

        // Check matching operations
        let result1 = context.check("kv", &json!({"Get": {"key": "user:bob"}}), false);
        assert!(matches!(result1, CheckResult::Match { .. }));

        let result2 = context.check(
            "kv",
            &json!({"Put": {"key": "user:bob", "value": 100}}),
            true,
        );
        assert!(matches!(result2, CheckResult::Match { .. }));

        // All predictions used
        assert!(context.all_predictions_used());
    }

    #[test]
    fn test_speculation_failure() {
        let config = SpeculationConfig {
            min_occurrences: 1,
            min_confidence_read: 0.0,
            min_confidence_write: 0.0,
            ..Default::default()
        };

        let mut learner = SequenceLearner::new(config.clone());
        let predictor = SequencePredictor::new(config);

        // Learn a pattern
        let args = vec![json!({"user": "alice"})];
        let ops = vec![
            (
                "kv".to_string(),
                json!({"Get": {"key": "user:alice"}}),
                false,
            ),
            (
                "kv".to_string(),
                json!({"Put": {"key": "user:alice", "value": 100}}),
                true,
            ),
        ];
        learner.learn_from_transaction("test", &args, &ops);

        // Create prediction
        let new_args = vec![json!({"user": "bob"})];
        let prediction = predictor.predict("test", &new_args, &learner).unwrap();

        let learner_arc = Arc::new(Mutex::new(learner));
        let mut context = PredictionContext::new(prediction, learner_arc.clone());

        // First operation matches
        let result1 = context.check("kv", &json!({"Get": {"key": "user:bob"}}), false);
        assert!(matches!(result1, CheckResult::Match { .. }));

        // Second operation doesn't match (different value)
        let result2 = context.check(
            "kv",
            &json!({"Put": {"key": "user:bob", "value": 200}}),
            true,
        );
        assert!(matches!(result2, CheckResult::SpeculationFailed { .. }));

        assert!(context.has_failed());
        assert_eq!(context.get_failure_point(), Some(1));
    }
}
