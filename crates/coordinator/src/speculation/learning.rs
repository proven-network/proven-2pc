//! Sequence-based pattern learning with common prefix detection
//!
//! This module learns operation sequences from transactions and identifies
//! reliable common prefixes that can be safely speculated.

use crate::speculation::SpeculationConfig;
use crate::speculation::template::{Template, TemplateExtractor};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// A single operation in a learned sequence
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OperationPattern {
    /// The template with placeholders
    pub template: Template,

    /// Position in the overall sequence
    pub position: usize,

    /// Stream this operation targets
    pub stream: String,

    /// Whether this is a write operation
    pub is_write: bool,
}

/// A learned sequence pattern with confidence metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SequencePattern {
    /// Category this pattern belongs to
    pub category: String,

    /// The common prefix of operations we're confident about
    pub operations: Vec<OperationPattern>,

    /// Total number of transactions in this category
    pub total_occurrences: u32,

    /// Number of transactions that match this exact prefix
    pub prefix_match_count: u32,

    /// Number of transactions that have this prefix then continue
    pub prefix_continuation_count: u32,

    /// Confidence score for the entire sequence
    pub confidence: f64,

    /// Unique identifier
    pub pattern_id: String,
}

impl SequencePattern {
    fn new(category: String, operations: Vec<OperationPattern>) -> Self {
        let pattern_id = Self::generate_id(&category, &operations);

        Self {
            category,
            operations,
            total_occurrences: 1,
            prefix_match_count: 1,
            prefix_continuation_count: 0,
            confidence: 0.0,
            pattern_id,
        }
    }

    fn generate_id(category: &str, operations: &[OperationPattern]) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        category.hash(&mut hasher);
        for op in operations {
            op.stream.hash(&mut hasher);
            op.is_write.hash(&mut hasher);
            op.position.hash(&mut hasher);
        }
        format!("{:x}", hasher.finish())
    }

    /// Update confidence based on consistency
    pub fn update_confidence(&mut self) {
        if self.total_occurrences < 3 {
            self.confidence = 0.0;
            return;
        }

        // Ratio of transactions that match our prefix
        let match_ratio = self.prefix_match_count as f64 / self.total_occurrences as f64;

        // Ratio of transactions that continue beyond (vs exact match)
        let continuation_ratio = if self.prefix_match_count > 0 {
            self.prefix_continuation_count as f64 / self.prefix_match_count as f64
        } else {
            0.0
        };

        // We want high match ratio and low continuation ratio
        // (means our prefix is a reliable stopping point)
        let reliability = match_ratio * (1.0 - continuation_ratio * 0.5);

        // Wilson score for confidence with small samples
        let n = self.total_occurrences as f64;
        let p = match_ratio;
        let z = 1.96; // 95% confidence interval
        let denominator = 1.0 + z * z / n;
        let center = (p + z * z / (2.0 * n)) / denominator;
        let spread = z * ((p * (1.0 - p) / n + z * z / (4.0 * n * n)).sqrt()) / denominator;

        // Use lower bound, adjusted by reliability
        self.confidence = ((center - spread) * reliability).max(0.0);
    }

    /// Check if this pattern is usable for speculation
    pub fn is_usable(&self, config: &SpeculationConfig) -> bool {
        // Check if we have enough occurrences
        if self.total_occurrences < config.min_occurrences {
            return false;
        }

        // Use different thresholds based on whether sequence contains writes
        let has_writes = self.operations.iter().any(|op| op.is_write);
        let threshold = if has_writes {
            config.min_confidence_write
        } else {
            config.min_confidence_read
        };

        self.confidence >= threshold
    }
}

/// Manages sequence pattern learning
pub struct SequenceLearner {
    /// Patterns organized by category
    patterns: HashMap<String, SequencePattern>,

    /// Configuration
    config: SpeculationConfig,

    /// Template extractor
    extractor: TemplateExtractor,
}

impl SequenceLearner {
    pub fn new(config: SpeculationConfig) -> Self {
        Self {
            patterns: HashMap::new(),
            config,
            extractor: TemplateExtractor::new(),
        }
    }

    /// Learn from a completed transaction
    pub fn learn_from_transaction(
        &mut self,
        category: &str,
        args: &[Value],
        operations: &[(String, Value, bool)],
    ) {
        // Extract templates for this transaction
        let mut templates = Vec::new();

        for (position, (stream, operation, is_write)) in operations.iter().enumerate() {
            if let Some(template) = self.extractor.extract(stream, operation, args, *is_write) {
                templates.push(OperationPattern {
                    template,
                    position,
                    stream: stream.clone(),
                    is_write: *is_write,
                });
            }
        }

        if templates.is_empty() {
            return;
        }

        // Update or create pattern for this category
        if self.patterns.contains_key(category) {
            // Get common prefix length first
            let common_len = {
                let pattern = self.patterns.get(category).unwrap();
                self.find_common_prefix_length(&pattern.operations, &templates)
            };

            // Now update the pattern
            let pattern = self.patterns.get_mut(category).unwrap();
            Self::update_pattern_with_sequence(pattern, &templates, common_len);
        } else {
            // First time seeing this category
            let new_pattern = SequencePattern::new(category.to_string(), templates);
            self.patterns.insert(category.to_string(), new_pattern);
        }
    }

    /// Update existing pattern with a new sequence observation
    fn update_pattern_with_sequence(
        pattern: &mut SequencePattern,
        new_sequence: &[OperationPattern],
        common_len: usize,
    ) {
        pattern.total_occurrences += 1;

        if common_len < pattern.operations.len() {
            // New sequence diverges earlier - shorten our pattern
            pattern.operations.truncate(common_len);
            pattern.prefix_match_count = 2; // This and the previous that established it
            pattern.prefix_continuation_count = 1; // The one we just truncated from
        } else if common_len == pattern.operations.len() {
            // New sequence matches our current prefix
            pattern.prefix_match_count += 1;

            if new_sequence.len() > pattern.operations.len() {
                // And continues beyond
                pattern.prefix_continuation_count += 1;
            }
        }

        // Recalculate confidence
        pattern.update_confidence();
    }

    /// Find how many operations match from the start
    fn find_common_prefix_length(
        &self,
        existing: &[OperationPattern],
        new_seq: &[OperationPattern],
    ) -> usize {
        let mut common = 0;

        for (e, n) in existing.iter().zip(new_seq.iter()) {
            // Check if operations are equivalent
            if e.stream == n.stream
                && e.is_write == n.is_write
                && self.templates_similar(&e.template, &n.template)
            {
                common += 1;
            } else {
                break;
            }
        }

        common
    }

    /// Check if two templates are similar enough to be considered the same pattern
    fn templates_similar(&self, t1: &Template, t2: &Template) -> bool {
        // Same stream and operation type
        if t1.stream != t2.stream || t1.is_write != t2.is_write {
            return false;
        }

        // Same required paths (using same arguments)
        if t1.required_paths != t2.required_paths {
            return false;
        }

        // Structurally equivalent patterns
        Self::patterns_structurally_equal(&t1.pattern, &t2.pattern)
    }

    fn patterns_structurally_equal(v1: &Value, v2: &Value) -> bool {
        match (v1, v2) {
            (Value::String(s1), Value::String(s2)) => {
                // Both should have placeholders in same positions
                let has_placeholder1 = s1.contains("{{");
                let has_placeholder2 = s2.contains("{{");
                has_placeholder1 == has_placeholder2
            }
            (Value::Object(m1), Value::Object(m2)) => {
                m1.len() == m2.len()
                    && m1.keys().all(|k| m2.contains_key(k))
                    && m1.iter().all(|(k, v1)| {
                        m2.get(k)
                            .is_some_and(|v2| Self::patterns_structurally_equal(v1, v2))
                    })
            }
            (Value::Array(a1), Value::Array(a2)) => {
                a1.len() == a2.len()
                    && a1
                        .iter()
                        .zip(a2.iter())
                        .all(|(v1, v2)| Self::patterns_structurally_equal(v1, v2))
            }
            _ => v1 == v2,
        }
    }

    /// Get the pattern for a category (if it exists and is usable)
    pub fn get_pattern(&self, category: &str) -> Option<&SequencePattern> {
        self.patterns
            .get(category)
            .filter(|p| p.is_usable(&self.config))
    }

    /// Get all patterns (for monitoring/debugging)
    pub fn get_all_patterns(&self) -> &HashMap<String, SequencePattern> {
        &self.patterns
    }

    /// Record feedback about a failed speculation
    pub fn record_speculation_failure(&mut self, pattern_id: &str, failed_at_position: usize) {
        // Find the pattern
        for pattern in self.patterns.values_mut() {
            if pattern.pattern_id == pattern_id {
                // Truncate pattern at failure point
                if failed_at_position < pattern.operations.len() {
                    pattern.operations.truncate(failed_at_position);
                    // Reset confidence since we changed the pattern
                    pattern.prefix_match_count = 1;
                    pattern.prefix_continuation_count = 0;
                    pattern.update_confidence();
                }
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_common_prefix_learning() {
        let config = SpeculationConfig {
            min_occurrences: 3,
            min_confidence_read: 0.0,
            min_confidence_write: 0.0,
            ..Default::default()
        };

        let mut learner = SequenceLearner::new(config);

        // Transaction 1: Put, Get, Put, Get
        let args1 = vec![json!({"user": "alice", "amount": 100})];
        let ops1 = vec![
            (
                "kv".to_string(),
                json!({"Put": {"key": "user:alice:balance", "value": 100}}),
                true,
            ),
            (
                "kv".to_string(),
                json!({"Get": {"key": "user:alice:balance"}}),
                false,
            ),
            (
                "kv".to_string(),
                json!({"Put": {"key": "user:alice:history", "value": "..."}}),
                true,
            ),
            (
                "kv".to_string(),
                json!({"Get": {"key": "user:alice:history"}}),
                false,
            ),
        ];
        learner.learn_from_transaction("transfer", &args1, &ops1);

        // Transaction 2: Put, Get, Put, Get (same pattern)
        let args2 = vec![json!({"user": "bob", "amount": 200})];
        let ops2 = vec![
            (
                "kv".to_string(),
                json!({"Put": {"key": "user:bob:balance", "value": 200}}),
                true,
            ),
            (
                "kv".to_string(),
                json!({"Get": {"key": "user:bob:balance"}}),
                false,
            ),
            (
                "kv".to_string(),
                json!({"Put": {"key": "user:bob:history", "value": "..."}}),
                true,
            ),
            (
                "kv".to_string(),
                json!({"Get": {"key": "user:bob:history"}}),
                false,
            ),
        ];
        learner.learn_from_transaction("transfer", &args2, &ops2);

        // Transaction 3: Put, Get only (diverges)
        let args3 = vec![json!({"user": "charlie", "amount": 300})];
        let ops3 = vec![
            (
                "kv".to_string(),
                json!({"Put": {"key": "user:charlie:balance", "value": 300}}),
                true,
            ),
            (
                "kv".to_string(),
                json!({"Get": {"key": "user:charlie:balance"}}),
                false,
            ),
        ];
        learner.learn_from_transaction("transfer", &args3, &ops3);

        // Should have learned common prefix of [Put, Get]
        let pattern = learner.get_pattern("transfer").unwrap();
        assert_eq!(pattern.operations.len(), 2);
        assert_eq!(pattern.total_occurrences, 3);
    }
}
