//! Core coordinator implementation

use crate::error::Result;
use crate::executor::Executor;
use crate::responses::ResponseCollector;
use crate::speculation::{SpeculationConfig, SpeculationContext};
use crate::transaction::Transaction;
use proven_engine::MockClient;
use proven_hlc::{HlcClock, HlcTimestamp, NodeId};
use proven_runner::Runner;
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;

/// Distributed transaction coordinator
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
    speculative_context: SpeculationContext,
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
        let speculative_context = SpeculationContext::new(SpeculationConfig::default());

        Self {
            coordinator_id,
            hlc,
            response_collector,
            client,
            runner,
            speculative_context,
        }
    }

    /// Begin a new distributed transaction with speculation
    ///
    /// This will attempt to predict and pre-execute operations based on learned patterns.
    /// If speculation fails during execution, the client should retry with `begin_without_speculation`.
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

        // Create transaction-scoped executor with all context
        let executor = Arc::new(Executor::new(
            txn_id,
            timestamp,
            deadline,
            self.coordinator_id.clone(),
            self.client.clone(),
            self.response_collector.clone(),
            self.runner.clone(),
        ));

        // Create prediction context for this transaction
        let mut prediction_context = self
            .speculative_context
            .create_prediction_context(&speculative_category, &transaction_args);

        // Execute predictions speculatively using the same executor
        // This is best-effort - if it fails, we continue without predictions
        if let Err(e) = prediction_context
            .execute_predictions(executor.clone())
            .await
        {
            tracing::debug!("Failed to execute speculative predictions: {}", e);
        }

        // Create transaction with executor and predictions
        Ok(Transaction::new(executor, prediction_context))
    }

    /// Begin a new distributed transaction without speculation
    ///
    /// This is typically used for retrying after a speculation failure.
    /// Operations are still tracked for learning, but no predictions are made.
    ///
    /// ## Usage Pattern
    /// ```ignore
    /// // First attempt with speculation
    /// let txn = coordinator.begin(timeout, args.clone(), "transfer").await?;
    /// match txn.execute(stream, &op).await {
    ///     Err(CoordinatorError::SpeculationFailed(_)) => {
    ///         // Retry without speculation
    ///         let txn = coordinator.begin_without_speculation(timeout, args, "transfer").await?;
    ///         // Re-execute operations...
    ///     }
    ///     // ... handle other cases
    /// }
    /// ```
    pub async fn begin_without_speculation(
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

        // Create transaction-scoped executor with all context
        let executor = Arc::new(Executor::new(
            txn_id,
            timestamp,
            deadline,
            self.coordinator_id.clone(),
            self.client.clone(),
            self.response_collector.clone(),
            self.runner.clone(),
        ));

        // Create an empty prediction context for learning without speculation
        // This ensures we still track operations for improving patterns
        let prediction_context = self
            .speculative_context
            .create_empty_context(&speculative_category, &transaction_args);

        // Create transaction without any speculative execution
        Ok(Transaction::new(executor, prediction_context))
    }

    /// Stop the coordinator (cleanup)
    pub async fn stop(&self) {
        self.response_collector.stop().await;
    }
}
