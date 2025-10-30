//! Core coordinator implementation

use crate::error::Result;
use crate::executor::common::ExecutorInfra;
use crate::executor::{AdHocExecutor, ReadOnlyExecutor, ReadWriteExecutor};
use crate::responses::ResponseCollector;
use crate::speculation::{SpeculationConfig, SpeculationContext};
use proven_common::{Timestamp, TransactionId};
use proven_engine::MockClient;
use proven_runner::Runner;
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;

/// Distributed transaction coordinator
pub struct Coordinator {
    /// Coordinator ID
    coordinator_id: String,

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
            response_collector,
            client,
            runner,
            speculative_context,
        }
    }

    /// Create a read-write executor with full 2PC and speculation
    pub async fn begin_read_write(
        &self,
        timeout: Duration,
        transaction_args: Vec<Value>,
        speculative_category: String,
    ) -> Result<ReadWriteExecutor> {
        let timestamp = TransactionId::new();

        // Calculate deadline as absolute timestamp
        let deadline = Timestamp::now().add_micros(timeout.as_micros() as u64);

        // Create shared infrastructure
        let infra = Arc::new(ExecutorInfra::new(
            self.coordinator_id.clone(),
            self.client.clone(),
            self.response_collector.clone(),
            self.runner.clone(),
        ));

        // Create prediction context for this transaction
        let prediction_context = self
            .speculative_context
            .create_prediction_context(&speculative_category, &transaction_args);

        // Create the executor (predictions are executed in new())
        let executor = ReadWriteExecutor::new(timestamp, deadline, infra, prediction_context).await;

        Ok(executor)
    }

    /// Create a read-write executor without speculation (for retries after speculation failures)
    ///
    /// This is typically used for retrying after a speculation failure.
    /// Operations are still tracked for learning, but no predictions are made.
    pub async fn begin_read_write_without_speculation(
        &self,
        timeout: Duration,
        transaction_args: Vec<Value>,
        speculative_category: String,
    ) -> Result<ReadWriteExecutor> {
        let timestamp = TransactionId::new();

        // Calculate deadline as absolute timestamp
        let deadline = Timestamp::now().add_micros(timeout.as_micros() as u64);

        // Create shared infrastructure
        let infra = Arc::new(ExecutorInfra::new(
            self.coordinator_id.clone(),
            self.client.clone(),
            self.response_collector.clone(),
            self.runner.clone(),
        ));

        // Create an empty prediction context for learning without speculation
        let prediction_context = self
            .speculative_context
            .create_empty_context(&speculative_category, &transaction_args);

        // Create the executor without predictions
        let executor = ReadWriteExecutor::new(timestamp, deadline, infra, prediction_context).await;

        Ok(executor)
    }

    /// Create a read-only executor with snapshot isolation and speculation
    pub async fn begin_read_only(
        &self,
        transaction_args: Vec<Value>,
        speculative_category: String,
    ) -> Result<ReadOnlyExecutor> {
        let read_timestamp = TransactionId::new();

        // Create shared infrastructure
        let infra = Arc::new(ExecutorInfra::new(
            self.coordinator_id.clone(),
            self.client.clone(),
            self.response_collector.clone(),
            self.runner.clone(),
        ));

        // Create prediction context for learning
        let prediction_context = self
            .speculative_context
            .create_prediction_context(&speculative_category, &transaction_args);

        // Create the executor (predictions are executed in new())
        Ok(ReadOnlyExecutor::new(read_timestamp, infra, prediction_context).await)
    }

    /// Create an ad-hoc executor for auto-commit operations
    pub fn adhoc(&self) -> AdHocExecutor {
        // Create shared infrastructure
        let infra = Arc::new(ExecutorInfra::new(
            self.coordinator_id.clone(),
            self.client.clone(),
            self.response_collector.clone(),
            self.runner.clone(),
        ));

        AdHocExecutor::new(infra)
    }

    /// Stop the coordinator (cleanup)
    pub async fn stop(&self) {
        self.response_collector.stop().await;
    }
}
