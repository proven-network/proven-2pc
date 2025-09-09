//! Core coordinator implementation

use crate::error::Result;
use crate::responses::ResponseCollector;
use crate::transaction::Transaction;
use parking_lot::Mutex;
use proven_engine::MockClient;
use proven_hlc::{HlcClock, HlcTimestamp, NodeId};
use std::sync::Arc;
use std::time::Duration;

/// Distributed transaction coordinator
#[derive(Clone)]
pub struct Coordinator {
    /// Coordinator ID
    coordinator_id: String,

    /// HLC clock for timestamps
    hlc: Arc<Mutex<HlcClock>>,

    /// Response collector for all transactions
    response_collector: Arc<ResponseCollector>,

    /// Client for sending messages
    client: Arc<MockClient>,
}

impl Coordinator {
    /// Create a new coordinator
    pub fn new(coordinator_id: String, client: Arc<MockClient>) -> Self {
        // Create HLC clock with node ID based on coordinator ID hash
        let seed = coordinator_id
            .bytes()
            .fold(0u8, |acc, b| acc.wrapping_add(b));
        let node_id = NodeId::from_seed(seed);
        let hlc = Arc::new(Mutex::new(HlcClock::new(node_id)));

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
        }
    }

    /// Begin a new distributed transaction
    pub async fn begin(&self, timeout: Duration) -> Result<Transaction> {
        let timestamp = self.hlc.lock().now();
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
        ))
    }

    /// Stop the coordinator (cleanup)
    pub async fn stop(&self) {
        self.response_collector.stop().await;
    }
}
