//! Sequence-based prediction engine
//!
//! This module generates predictions by instantiating learned sequence patterns
//! with new argument values. It returns entire sequences or nothing (all-or-nothing approach).

use crate::speculation::SpeculationConfig;
use crate::speculation::learning::Learner;
use crate::speculation::template::TemplateInstantiator;
use serde_json::Value;

/// A predicted operation with metadata for tracking
#[derive(Debug, Clone)]
pub struct PredictedOperation {
    /// The instantiated operation
    pub operation: Value,

    /// Which stream this targets
    pub stream: String,

    /// Whether this is a write
    pub is_write: bool,

    /// Position in the sequence
    pub position: usize,
}

/// Result of prediction attempt
#[derive(Debug)]
pub struct PredictionResult {
    /// The predicted operations in sequence order
    pub operations: Vec<PredictedOperation>,

    /// The pattern that generated these predictions
    pub pattern_id: String,

    /// Category of the prediction
    pub category: String,

    /// Confidence score
    pub confidence: f64,
}

/// Generates predictions from learned patterns
pub struct Predictor {
    config: SpeculationConfig,
    instantiator: TemplateInstantiator,
}

impl Predictor {
    pub fn new(config: SpeculationConfig) -> Self {
        Self {
            config,
            instantiator: TemplateInstantiator::new(),
        }
    }

    /// Generate predictions for a transaction
    pub fn predict(
        &self,
        category: &str,
        args: &[Value],
        learner: &Learner,
    ) -> Option<PredictionResult> {
        // Get the pattern for this category
        let pattern = learner.get_pattern(category)?;

        // Check if pattern meets our criteria
        if !pattern.is_usable(&self.config) {
            return None;
        }

        // Try to instantiate all operations in the sequence
        let mut predictions = Vec::new();

        for op_pattern in &pattern.operations {
            match self.instantiator.instantiate(&op_pattern.template, args) {
                Ok(operation) => {
                    predictions.push(PredictedOperation {
                        operation,
                        stream: op_pattern.stream.clone(),
                        is_write: op_pattern.is_write,
                        position: op_pattern.position,
                    });
                }
                Err(_) => {
                    // If any operation fails to instantiate, abort entire prediction
                    return None;
                }
            }
        }

        Some(PredictionResult {
            operations: predictions,
            pattern_id: pattern.pattern_id.clone(),
            category: pattern.category.clone(),
            confidence: pattern.confidence,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::speculation::learning::Learner;
    use serde_json::json;

    #[test]
    fn test_sequence_prediction() {
        let config = SpeculationConfig {
            min_occurrences: 2,
            min_confidence_read: 0.0,
            min_confidence_write: 0.0,
            ..Default::default()
        };

        let mut learner = Learner::new(config.clone());
        let predictor = Predictor::new(config);

        // Learn from two similar transactions
        for i in 0..2 {
            let user = format!("user_{}", i);
            let args = vec![json!({"user": user.clone(), "amount": 100 + i})];
            let ops = vec![
                (
                    "kv".to_string(),
                    json!({"Put": {"key": format!("user:{}:balance", user), "value": 100 + i}}),
                    true,
                ),
                (
                    "kv".to_string(),
                    json!({"Get": {"key": format!("user:{}:balance", user)}}),
                    false,
                ),
            ];
            learner.learn_from_transaction("transfer", &args, &ops);
        }

        // Predict for new transaction
        let new_args = vec![json!({"user": "alice", "amount": 500})];
        let result = predictor.predict("transfer", &new_args, &learner);

        assert!(result.is_some());
        let prediction = result.unwrap();
        assert_eq!(prediction.operations.len(), 2);
        assert_eq!(prediction.category, "transfer");

        // Check operations were instantiated correctly
        let put_op = &prediction.operations[0];
        assert!(put_op.is_write);
        assert!(put_op.operation.to_string().contains("alice"));
        assert!(put_op.operation.to_string().contains("500"));

        let get_op = &prediction.operations[1];
        assert!(!get_op.is_write);
        assert!(get_op.operation.to_string().contains("alice"));
    }

    #[test]
    fn test_template_preserves_operation_string_type() {
        // This test verifies that when operations have string values
        // but args have numeric values, the template system preserves
        // the string type in the operation
        let config = SpeculationConfig {
            min_occurrences: 2,
            min_confidence_read: 0.0,
            min_confidence_write: 0.0,
            ..Default::default()
        };

        let mut learner = Learner::new(config.clone());
        let predictor = Predictor::new(config);

        // Learn from transactions where operations have string "10"
        // but args have numeric 10
        for i in 0..2 {
            let args = vec![json!({
                "from": format!("account_{}", i),
                "to": format!("account_{}", (i + 1) % 2),
                "amount": 10  // Numeric in args
            })];

            let ops = vec![(
                "resource_stream".to_string(),
                json!({
                    "Transfer": {
                        "from": format!("account_{}", i),
                        "to": format!("account_{}", (i + 1) % 2),
                        "amount": "10",  // String in operation!
                        "memo": null
                    }
                }),
                true,
            )];

            learner.learn_from_transaction("transfer", &args, &ops);
        }

        // Now predict with numeric amount in args
        let new_args = vec![json!({
            "from": "account_99",
            "to": "account_0",
            "amount": 10  // Numeric value
        })];

        let result = predictor.predict("transfer", &new_args, &learner);
        assert!(result.is_some());

        let prediction = result.unwrap();
        assert_eq!(prediction.operations.len(), 1);

        // The predicted operation should have string "10" to match the template
        let predicted_op = &prediction.operations[0];
        let transfer = &predicted_op.operation["Transfer"];

        // This should be "10" (string), not 10 (number)
        assert_eq!(
            transfer["amount"],
            json!("10"), // Should be string to match operation format
            "Amount should be string to preserve operation format"
        );
    }

    #[test]
    fn test_type_mismatch_in_templates() {
        // This test replicates the issue where template instantiation
        // doesn't match due to type differences (e.g., number vs string)
        let config = SpeculationConfig {
            min_occurrences: 2,
            min_confidence_read: 0.0,
            min_confidence_write: 0.0,
            ..Default::default()
        };

        let mut learner = Learner::new(config.clone());
        let predictor = Predictor::new(config);

        // First transaction: Learn with string amounts (like from JSON input)
        let args1 = vec![json!({
            "from": "account_0",
            "to": "account_1",
            "amount": "10"  // String value
        })];

        let ops1 = vec![(
            "resource_stream".to_string(),
            json!({
                "Transfer": {
                    "from": "account_0",
                    "to": "account_1",
                    "amount": "10",  // String in operation too
                    "memo": null
                }
            }),
            true,
        )];
        learner.learn_from_transaction("transfer", &args1, &ops1);

        // Second transaction: Same pattern with string
        let args2 = vec![json!({
            "from": "account_1",
            "to": "account_0",
            "amount": "10"  // String value
        })];

        let ops2 = vec![(
            "resource_stream".to_string(),
            json!({
                "Transfer": {
                    "from": "account_1",
                    "to": "account_0",
                    "amount": "10",  // String
                    "memo": null
                }
            }),
            true,
        )];
        learner.learn_from_transaction("transfer", &args2, &ops2);

        // Now predict with numeric amount - this is what causes the mismatch
        let new_args = vec![json!({
            "from": "account_99",
            "to": "account_0",
            "amount": 10  // Numeric value this time!
        })];

        let result = predictor.predict("transfer", &new_args, &learner);
        assert!(result.is_some());

        let prediction = result.unwrap();
        assert_eq!(prediction.operations.len(), 1);

        // The predicted operation should preserve the string format from the learned pattern
        let predicted_op = &prediction.operations[0];
        let transfer = &predicted_op.operation["Transfer"];

        // This should be "10" (string) to preserve the operation format
        assert_eq!(
            transfer["amount"],
            json!("10"), // Should be string to preserve operation format
            "Amount should be string to preserve operation format"
        );
    }

    #[test]
    fn test_all_or_nothing_prediction() {
        let config = SpeculationConfig {
            min_occurrences: 1,
            min_confidence_read: 0.0,
            min_confidence_write: 0.0,
            ..Default::default()
        };
        let mut learner = Learner::new(config.clone());
        let predictor = Predictor::new(config);

        // Learn a pattern - with substring detection, we only need user
        let args = vec![json!({"user": "alice"})];
        let ops = vec![
            (
                "kv".to_string(),
                json!({"Get": {"key": "user:alice"}}),
                false,
            ),
            (
                "kv".to_string(),
                json!({"Put": {"key": "user:alice", "value": "data"}}),
                true,
            ),
        ];
        learner.learn_from_transaction("test", &args, &ops);

        // Should be able to predict with just user field
        let complete_args = vec![json!({"user": "bob"})];
        let result = predictor.predict("test", &complete_args, &learner);

        // Should work with substring pattern detection
        assert!(result.is_some(), "Should predict with substring pattern");
        let prediction = result.unwrap();
        assert_eq!(prediction.operations.len(), 2);

        // Check the operations have correct substitutions
        assert!(
            prediction.operations[0]
                .operation
                .to_string()
                .contains("user:bob")
        );
        assert!(
            prediction.operations[1]
                .operation
                .to_string()
                .contains("user:bob")
        );
    }

    #[test]
    fn test_missing_required_arg() {
        let config = SpeculationConfig {
            min_occurrences: 1,
            min_confidence_read: 0.0,
            min_confidence_write: 0.0,
            ..Default::default()
        };
        let mut learner = Learner::new(config.clone());
        let predictor = Predictor::new(config);

        // Learn a pattern that needs user field
        let args = vec![json!({"user": "alice", "amount": 100})];
        let ops = vec![
            (
                "kv".to_string(),
                json!({"Get": {"key": "user:alice:balance"}}),
                false,
            ),
            (
                "kv".to_string(),
                json!({"Put": {"key": "user:alice:balance", "value": 100}}),
                true,
            ),
        ];
        learner.learn_from_transaction("test", &args, &ops);

        // Try to predict without required user field
        let incomplete_args = vec![json!({"amount": 200})]; // Missing 'user'
        let result = predictor.predict("test", &incomplete_args, &learner);

        // Should return None since user field is required for the template
        assert!(result.is_none());
    }
}
