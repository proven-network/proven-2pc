//! Prediction context that acts as a guard during transaction execution
//!
//! This module provides a stateful context that validates operations against
//! predicted sequences and provides speculated responses when operations match.

use crate::error::Result;
use crate::executor::Executor;
use crate::speculation::learning::SequenceLearner;
use crate::speculation::predictor::{PredictedOperation, PredictionResult};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

/// Result of checking an operation against predictions
#[derive(Debug)]
pub enum CheckResult {
    /// Operation matches prediction - here's the speculated response
    Match { response: Vec<u8>, is_write: bool },

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
    pub(crate) predictions: Vec<PredictedOperation>,

    /// Current position in the sequence
    current_position: usize,

    /// Pattern ID for feedback
    pattern_id: String,

    /// Category for learning
    category: String,

    /// Transaction arguments for learning
    args: Vec<Value>,

    /// Where speculation failed (if it did)
    failure_point: Option<usize>,

    /// Reference to learner for feedback
    learner: Arc<Mutex<SequenceLearner>>,

    /// Operations that were actually executed
    executed_operations: Vec<(String, Value, bool)>,

    /// Response receivers for async execution
    /// We store them in an Arc<Mutex> to allow cloning and concurrent access
    response_receivers: Arc<Mutex<HashMap<usize, oneshot::Receiver<Result<Vec<u8>>>>>>,
}

impl PredictionContext {
    /// Create a new prediction context from a prediction result
    pub fn new(
        prediction: PredictionResult,
        args: Vec<Value>,
        learner: Arc<Mutex<SequenceLearner>>,
    ) -> Self {
        Self {
            predictions: prediction.operations,
            current_position: 0,
            pattern_id: prediction.pattern_id,
            category: prediction.category,
            args,
            failure_point: None,
            learner,
            executed_operations: Vec::new(),
            response_receivers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Create an empty context when no speculation is available
    pub fn empty(category: String, args: Vec<Value>, learner: Arc<Mutex<SequenceLearner>>) -> Self {
        Self {
            predictions: Vec::new(),
            current_position: 0,
            pattern_id: String::new(),
            category,
            args,
            failure_point: None,
            learner,
            executed_operations: Vec::new(),
            response_receivers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Check if an operation matches the next predicted operation
    pub async fn check(&mut self, stream: &str, operation: &Value, is_write: bool) -> CheckResult {
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
            // Match! Try to get the response from the receiver
            let position = self.current_position;
            self.current_position += 1;

            // Try to get and await the receiver for this position
            let receiver_opt = self.response_receivers.lock().unwrap().remove(&position);

            if let Some(receiver) = receiver_opt {
                // Await the response
                match receiver.await {
                    Ok(Ok(response)) => {
                        // Got successful response
                        CheckResult::Match { response, is_write }
                    }
                    Ok(Err(_err)) => {
                        // Execution failed
                        // tracing::warn!("Speculated execution failed for position {}: {}", position, err);
                        CheckResult::NoPrediction
                    }
                    Err(_) => {
                        // Channel was dropped (shouldn't happen)
                        // tracing::warn!("Response channel dropped for position {}", position);
                        CheckResult::NoPrediction
                    }
                }
            } else {
                // No receiver available - speculation execution likely didn't run
                // tracing::warn!(
                //     "Matched operation at position {} but no response available",
                //     position
                // );
                CheckResult::NoPrediction
            }
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
    pub fn report_outcome(&self, transaction_succeeded: bool) {
        let mut learner = self.learner.lock().unwrap();

        if let Some(failure_pos) = self.failure_point {
            // Speculation failed - truncate pattern at failure point
            if !self.pattern_id.is_empty() {
                learner.record_speculation_failure(&self.pattern_id, failure_pos);
            }

            // Still learn from the actual operations executed (up to failure)
            if transaction_succeeded && !self.executed_operations.is_empty() {
                learner.learn_from_transaction(
                    &self.category,
                    &self.args,
                    &self.executed_operations,
                );
            }
        } else if transaction_succeeded {
            // Transaction succeeded - learn from ALL executed operations
            // This handles both:
            // 1. Transactions that matched predictions
            // 2. Transactions that went beyond predictions (NoPrediction)
            // 3. Transactions with no predictions at all
            if !self.executed_operations.is_empty() {
                learner.learn_from_transaction(
                    &self.category,
                    &self.args,
                    &self.executed_operations,
                );
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

    /// Set a test response receiver (for testing only)
    #[cfg(test)]
    pub fn set_test_receiver(&mut self, position: usize, receiver: oneshot::Receiver<Result<Vec<u8>>>) {
        self.response_receivers.lock().unwrap().insert(position, receiver);
    }

    /// Execute all predicted operations speculatively using the executor
    ///
    /// This groups operations by stream and executes them in parallel batches,
    /// ensuring operations for the same stream are sent together in order.
    pub async fn execute_predictions(&mut self, executor: Arc<Executor>) -> Result<()> {
        if self.predictions.is_empty() {
            return Ok(());
        }

        // Group operations by stream while maintaining order
        let mut stream_operations: HashMap<String, Vec<Vec<u8>>> = HashMap::new();
        let mut position_map: HashMap<(String, usize), usize> = HashMap::new();

        for (i, pred_op) in self.predictions.iter().enumerate() {
            // Convert the operation Value to bytes for execution
            let operation_bytes = serde_json::to_vec(&pred_op.operation)
                .map_err(crate::error::CoordinatorError::SerializationError)?;

            let stream = pred_op.stream.clone();
            let op_index = stream_operations.entry(stream.clone()).or_default().len();
            stream_operations.get_mut(&stream).unwrap().push(operation_bytes);

            // Map (stream, op_index) to original position
            position_map.insert((stream, op_index), i);
        }

        // Execute all streams in a single batch (amortizes network latency)
        match executor.execute_multi_stream_batch(stream_operations).await {
            Ok(receivers_map) => {
                // Map receivers back to their original positions
                let mut all_receivers = HashMap::new();
                for ((stream, op_idx), receiver) in receivers_map {
                    if let Some(&pos) = position_map.get(&(stream, op_idx)) {
                        all_receivers.insert(pos, receiver);
                    }
                }
                // Store all receivers
                *self.response_receivers.lock().unwrap() = all_receivers;
            }
            Err(e) => {
                // Log but don't fail - speculation is best-effort
                tracing::debug!("Failed to execute multi-stream batch: {}", e);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::speculation::SpeculationConfig;
    use crate::speculation::learning::SequenceLearner;
    use crate::speculation::predictor::SequencePredictor;
    use serde_json::json;

    #[tokio::test]
    async fn test_prediction_context_matching() {
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
        let mut context = PredictionContext::new(prediction, new_args.clone(), learner_arc.clone());

        // Simulate speculation execution by creating fake receivers
        {
            let (tx0, rx0) = oneshot::channel();
            let (tx1, rx1) = oneshot::channel();

            // Send fake responses
            let _ = tx0.send(Ok(b"fake_response_1".to_vec()));
            let _ = tx1.send(Ok(b"fake_response_2".to_vec()));

            // Store receivers
            context.set_test_receiver(0, rx0);
            context.set_test_receiver(1, rx1);
        }

        // Check matching operations
        let result1 = context.check("kv", &json!({"Get": {"key": "user:bob"}}), false).await;
        assert!(matches!(result1, CheckResult::Match { .. }));

        let result2 = context.check(
            "kv",
            &json!({"Put": {"key": "user:bob", "value": 100}}),
            true,
        ).await;
        assert!(matches!(result2, CheckResult::Match { .. }));

        // All predictions used
        assert!(context.all_predictions_used());
    }

    #[tokio::test]
    async fn test_speculation_failure() {
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
        let mut context = PredictionContext::new(prediction, new_args.clone(), learner_arc.clone());

        // Simulate speculation execution for first operation only
        {
            let (tx0, rx0) = oneshot::channel();
            let _ = tx0.send(Ok(b"fake_response".to_vec()));
            context.set_test_receiver(0, rx0);
        }

        // First operation matches
        let result1 = context.check("kv", &json!({"Get": {"key": "user:bob"}}), false).await;
        assert!(matches!(result1, CheckResult::Match { .. }));

        // Second operation doesn't match (different value)
        let result2 = context.check(
            "kv",
            &json!({"Put": {"key": "user:bob", "value": 200}}),
            true,
        ).await;
        assert!(matches!(result2, CheckResult::SpeculationFailed { .. }));

        assert!(context.has_failed());
        assert_eq!(context.get_failure_point(), Some(1));
    }
}
