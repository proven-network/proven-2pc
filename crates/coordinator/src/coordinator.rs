//! Core coordinator implementation

use crate::error::Result;
use crate::responses::ResponseCollector;
use crate::speculation::{SpeculativeContext, SpeculationConfig};
use crate::transaction::Transaction;
use proven_engine::MockClient;
use proven_hlc::{HlcClock, HlcTimestamp, NodeId};
use proven_runner::Runner;
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

/// Distributed transaction coordinator
#[derive(Clone)]
pub struct Coordinator {
    /// Coordinator ID
    coordinator_id: String,

    /// HLC clock for timestamps (thread-safe via atomics)
    hlc: Arc<HlcClock>,

    /// Response collector for all transactions
    response_collector: Arc<ResponseCollector>,

    /// Client for sending messages
    client: Arc<MockClient>,

    /// Runner for managing stream processors
    runner: Arc<Runner>,

    /// Speculative execution context for pattern learning and prediction
    speculative_context: SpeculativeContext,
}

impl Coordinator {
    /// Create a new coordinator with a runner for managing processors
    pub fn new(coordinator_id: String, client: Arc<MockClient>, runner: Arc<Runner>) -> Self {
        // Create HLC clock with node ID based on coordinator ID hash
        let seed = coordinator_id
            .bytes()
            .fold(0u8, |acc, b| acc.wrapping_add(b));
        let node_id = NodeId::from_seed(seed);
        let hlc = Arc::new(HlcClock::new(node_id));

        // Create and start response collector
        let response_collector = Arc::new(ResponseCollector::new(
            client.clone(),
            coordinator_id.clone(),
        ));
        response_collector.start();

        // Create speculative context with default config
        let speculative_context = SpeculativeContext::new(SpeculationConfig::default());

        Self {
            coordinator_id,
            hlc,
            response_collector,
            client,
            runner,
            speculative_context,
        }
    }

    /// Begin a new distributed transaction
    pub async fn begin(
        &self,
        timeout: Duration,
        transaction_args: Vec<Value>,
        speculative_category: String,
    ) -> Result<Transaction> {
        let timestamp = self.hlc.now();
        let txn_id = timestamp.to_string();

        // Calculate deadline
        let timeout_us = timeout.as_micros() as u64;
        let deadline = HlcTimestamp::new(
            timestamp.physical + timeout_us,
            timestamp.logical,
            timestamp.node_id,
        );

        // Start learning for this transaction
        self.speculative_context.begin_learning(
            txn_id.clone(),
            speculative_category.clone(),
            transaction_args.clone(),
        );

        // Predict operations to speculate
        let operations_by_stream = self.speculative_context.predict_operations_by_stream(
            &speculative_category,
            &transaction_args,
        );

        // Separate write operations by stream for ordering enforcement
        let mut pending_writes_by_stream = HashMap::new();
        let speculation_cache = HashMap::new();

        // Track speculated operations and identify writes
        for (stream, operations) in operations_by_stream {
            let mut write_queue = VecDeque::new();

            for op in &operations {
                // For Phase 1, we can't determine operation type from JSON
                // This will be properly implemented when we integrate with actual Operation types
                // For now, we'll track all operations as potential writes to be safe
                write_queue.push_back(op.clone());

                // Record that we're speculatively executing this
                self.speculative_context.record_speculation(&txn_id, op.clone());

                // Fire off speculative execution (non-blocking)
                // This would actually execute the operation speculatively
                // For Phase 1, we're just setting up the structure
                let _txn_id_clone = txn_id.clone();
                let _client = self.client.clone();
                let _stream_clone = stream.clone();
                let _op_clone = op.clone();

                // TODO: Actually execute speculatively in background task
                // tokio::spawn(async move {
                //     // Execute operation speculatively
                //     // Store result in shared cache
                // });
            }

            if !write_queue.is_empty() {
                pending_writes_by_stream.insert(stream, write_queue);
            }
        }

        Ok(Transaction::new(
            txn_id,
            timestamp,
            deadline,
            self.client.clone(),
            self.response_collector.clone(),
            self.coordinator_id.clone(),
            self.runner.clone(),
            pending_writes_by_stream,
            speculation_cache,
            self.speculative_context.clone(),
        ))
    }

    /// Stop the coordinator (cleanup)
    pub async fn stop(&self) {
        self.response_collector.stop().await;
    }
}
