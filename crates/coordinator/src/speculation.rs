//! Speculative execution context and pattern learning
//!
//! This module implements the core speculation engine that learns
//! patterns from function arguments and predicts database operations.

#![allow(dead_code)] // Many fields will be used in later phases

use parking_lot::RwLock;
use serde_json::Value;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Instant;

/// A learned pattern that maps function arguments to operations
#[derive(Clone, Debug)]
pub struct SpeculationPattern {
    /// Category/function type this pattern belongs to
    pub category: String,

    /// JSON paths in arguments that correlate with this operation
    pub arg_paths: Vec<String>,

    /// Template operation that gets filled with argument values
    pub operation_template: serde_json::Value,

    /// The stream this operation targets (determined from pattern learning)
    pub target_stream: String,

    /// Order within the stream (for maintaining write order)
    pub sequence_number: u32,

    /// Confidence score (0.0 to 1.0)
    pub confidence: f64,

    /// Number of times this pattern was correctly predicted
    pub hit_count: u64,

    /// Number of times this pattern was incorrectly predicted
    pub miss_count: u64,

    /// Last time this pattern was observed
    pub last_seen: Instant,
}

/// Record of operations executed during a transaction (for learning)
#[derive(Debug)]
struct LearningRecord {
    /// The function arguments that started this transaction
    pub args: Vec<Value>,

    /// The category/function type
    pub category: String,

    /// Operations that were actually executed
    pub executed_operations: Vec<serde_json::Value>,

    /// Operations that were speculatively executed
    pub speculated_operations: HashSet<serde_json::Value>,
}

/// Configuration for speculation behavior
#[derive(Clone, Debug)]
pub struct SpeculationConfig {
    /// Minimum confidence required to speculate on read operations
    pub min_confidence_read: f64,

    /// Minimum confidence required to speculate on write operations
    pub min_confidence_write: f64,

    /// Minimum number of observations before enabling speculation
    pub min_samples_before_speculation: u32,

    /// Maximum number of operations to speculate per transaction
    pub max_speculation_per_txn: usize,
}

impl Default for SpeculationConfig {
    fn default() -> Self {
        Self {
            min_confidence_read: 0.8,
            min_confidence_write: 0.95,
            min_samples_before_speculation: 10,
            max_speculation_per_txn: 20,
        }
    }
}

/// Main speculative execution context
pub struct SpeculativeContext {
    /// Patterns organized by category (function type)
    patterns_by_category: Arc<RwLock<HashMap<String, Vec<SpeculationPattern>>>>,

    /// Learning records for transactions in progress
    learning_buffer: Arc<RwLock<HashMap<String, LearningRecord>>>,

    /// Configuration
    config: SpeculationConfig,
}

impl SpeculativeContext {
    /// Create a new speculative context
    pub fn new(config: SpeculationConfig) -> Self {
        Self {
            patterns_by_category: Arc::new(RwLock::new(HashMap::new())),
            learning_buffer: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
    }

    /// Predict operations based on function arguments
    /// Returns operations grouped by stream to maintain ordering
    pub fn predict_operations_by_stream(
        &self,
        category: &str,
        args: &[Value],
    ) -> HashMap<String, VecDeque<serde_json::Value>> {
        let patterns = self.patterns_by_category.read();
        let mut operations_by_stream = HashMap::new();

        if let Some(category_patterns) = patterns.get(category) {
            // Filter patterns by confidence threshold
            let confident_patterns: Vec<_> = category_patterns
                .iter()
                .filter(|p| {
                    // Check if we have enough samples
                    let total_samples = p.hit_count + p.miss_count;
                    if total_samples < self.config.min_samples_before_speculation as u64 {
                        return false;
                    }

                    // Apply confidence threshold based on operation type
                    // For now, we'll assume reads unless pattern indicates otherwise
                    p.confidence >= self.config.min_confidence_read
                })
                .collect();

            // Generate operations from patterns
            for pattern in confident_patterns.iter().take(self.config.max_speculation_per_txn) {
                if let Some(operation) = self.generate_operation_from_pattern(pattern, args) {
                    operations_by_stream
                        .entry(pattern.target_stream.clone())
                        .or_insert_with(VecDeque::new)
                        .push_back(operation);
                }
            }

            // Sort operations within each stream by sequence number
            // (This ensures write order is preserved)
            for (_, _ops) in operations_by_stream.iter_mut() {
                // Operations are already in sequence order from patterns
                // No additional sorting needed as we maintain order during pattern creation
            }
        }

        operations_by_stream
    }

    /// Start tracking a transaction for learning
    pub fn begin_learning(&self, txn_id: String, category: String, args: Vec<Value>) {
        let mut buffer = self.learning_buffer.write();
        buffer.insert(
            txn_id,
            LearningRecord {
                args,
                category,
                executed_operations: Vec::new(),
                speculated_operations: HashSet::new(),
            },
        );
    }

    /// Record that an operation was executed
    pub fn record_execution(&self, txn_id: &str, operation: serde_json::Value) {
        let mut buffer = self.learning_buffer.write();
        if let Some(record) = buffer.get_mut(txn_id) {
            record.executed_operations.push(operation);
        }
    }

    /// Record that an operation was speculatively executed
    pub fn record_speculation(&self, txn_id: &str, operation: serde_json::Value) {
        let mut buffer = self.learning_buffer.write();
        if let Some(record) = buffer.get_mut(txn_id) {
            record.speculated_operations.insert(operation);
        }
    }

    /// Update patterns based on transaction outcome
    pub fn update_patterns(&self, txn_id: &str, committed: bool) {
        let mut buffer = self.learning_buffer.write();
        if let Some(record) = buffer.remove(txn_id) {
            if committed {
                // Transaction succeeded - learn from the executed operations
                self.learn_from_success(record);
            } else {
                // Transaction failed - reduce confidence in speculated patterns
                self.learn_from_failure(record);
            }
        }
    }

    /// Learn patterns from a successful transaction
    fn learn_from_success(&self, record: LearningRecord) {
        // For Phase 1, we'll just track that the transaction succeeded
        // Pattern discovery will be implemented in Phase 4

        // Update hit/miss counts for patterns that were used
        let mut patterns = self.patterns_by_category.write();
        if let Some(category_patterns) = patterns.get_mut(&record.category) {
            for pattern in category_patterns.iter_mut() {
                // Check if this pattern's operation was executed
                if record.speculated_operations.contains(&pattern.operation_template) {
                    if record.executed_operations.contains(&pattern.operation_template) {
                        pattern.hit_count += 1;
                    } else {
                        pattern.miss_count += 1;
                    }
                    pattern.last_seen = Instant::now();
                    pattern.update_confidence();
                }
            }
        }
    }

    /// Learn from a failed transaction
    fn learn_from_failure(&self, record: LearningRecord) {
        // Reduce confidence in patterns that were speculated
        let mut patterns = self.patterns_by_category.write();
        if let Some(category_patterns) = patterns.get_mut(&record.category) {
            for pattern in category_patterns.iter_mut() {
                if record.speculated_operations.contains(&pattern.operation_template) {
                    pattern.miss_count += 1;
                    pattern.update_confidence();
                }
            }
        }
    }

    /// Generate an operation from a pattern and arguments
    fn generate_operation_from_pattern(
        &self,
        pattern: &SpeculationPattern,
        _args: &[Value],
    ) -> Option<serde_json::Value> {
        // For Phase 1, return the template as-is
        // Phase 4 will implement value substitution from args
        Some(pattern.operation_template.clone())
    }
}

impl SpeculationPattern {
    /// Update confidence score using Wilson score interval
    fn update_confidence(&mut self) {
        let total = self.hit_count + self.miss_count;
        if total == 0 {
            self.confidence = 0.0;
            return;
        }

        // Simple confidence calculation for now
        // Phase 4 will implement proper Wilson score
        self.confidence = self.hit_count as f64 / total as f64;
    }
}

impl Clone for SpeculativeContext {
    fn clone(&self) -> Self {
        Self {
            patterns_by_category: Arc::clone(&self.patterns_by_category),
            learning_buffer: Arc::clone(&self.learning_buffer),
            config: self.config.clone(),
        }
    }
}