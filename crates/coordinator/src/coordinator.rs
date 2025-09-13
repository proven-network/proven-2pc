//! Core coordinator implementation

use crate::error::Result;
use crate::responses::ResponseCollector;
use crate::transaction::Transaction;
use proven_engine::MockClient;
use proven_hlc::{HlcClock, HlcTimestamp, NodeId};
use proven_runner::Runner;
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

        Self {
            coordinator_id,
            hlc,
            response_collector,
            client,
            runner,
        }
    }

    /// Begin a new distributed transaction
    pub async fn begin(&self, timeout: Duration) -> Result<Transaction> {
        let timestamp = self.hlc.now();
        let txn_id = timestamp.to_string();

        // Calculate deadline
        let timeout_us = timeout.as_micros() as u64;
        let deadline = HlcTimestamp::new(
            timestamp.physical + timeout_us,
            timestamp.logical,
            timestamp.node_id,
        );

        Ok(Transaction::new(
            txn_id,
            timestamp,
            deadline,
            self.client.clone(),
            self.response_collector.clone(),
            self.coordinator_id.clone(),
            self.runner.clone(),
        ))
    }

    /// Stop the coordinator (cleanup)
    pub async fn stop(&self) {
        self.response_collector.stop().await;
    }
}
