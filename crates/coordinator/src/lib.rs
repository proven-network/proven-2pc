//! Mock coordinator for distributed transactions
//!
//! This module provides a mock coordinator that can orchestrate distributed
//! transactions across SQL and KV systems using two-phase commit.

use parking_lot::Mutex;
use proven_engine::{Message, MockClient, MockEngine};
use proven_hlc::{HlcClock, HlcTimestamp, NodeId};
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;

/// Coordinator errors
#[derive(Debug, Error)]
pub enum CoordinatorError {
    #[error("Transaction not found: {0}")]
    TransactionNotFound(String),

    #[error("Invalid transaction state: {0}")]
    InvalidState(String),

    #[error("Engine error: {0}")]
    EngineError(#[from] proven_engine::MockEngineError),
}

pub type Result<T> = std::result::Result<T, CoordinatorError>;

/// Transaction state in the coordinator
#[derive(Debug, Clone)]
pub enum TransactionState {
    Active,
    Preparing,
    Prepared,
    Committing,
    Committed,
    Aborting,
    Aborted,
}

/// Transaction metadata
#[derive(Debug, Clone)]
pub struct Transaction {
    pub id: String,
    pub state: TransactionState,
    pub participants: Vec<String>, // Stream names
    pub timestamp: HlcTimestamp,
}

/// Mock coordinator for distributed transactions
pub struct MockCoordinator {
    /// Coordinator ID
    coordinator_id: String,

    /// Client for engine communication
    client: MockClient,

    /// Active transactions
    transactions: Arc<Mutex<HashMap<String, Transaction>>>,

    /// HLC clock for timestamps
    hlc: Arc<Mutex<HlcClock>>,
}

impl MockCoordinator {
    /// Create a new mock coordinator
    pub fn new(coordinator_id: String, engine: Arc<MockEngine>) -> Self {
        let client = MockClient::new(format!("coordinator-{}", coordinator_id), engine);

        // Create HLC clock with node ID based on coordinator ID hash
        // Use wrapping arithmetic to avoid overflow
        let seed = coordinator_id
            .bytes()
            .fold(0u8, |acc, b| acc.wrapping_add(b));
        let node_id = NodeId::from_seed(seed);
        let hlc = Arc::new(Mutex::new(HlcClock::new(node_id)));

        Self {
            coordinator_id,
            client,
            transactions: Arc::new(Mutex::new(HashMap::new())),
            hlc,
        }
    }

    /// Begin a new distributed transaction
    pub async fn begin_transaction(&self, participants: Vec<String>) -> Result<String> {
        let timestamp = self.hlc.lock().now();
        let txn_id = format!("{}:{}", self.coordinator_id, timestamp);

        let transaction = Transaction {
            id: txn_id.clone(),
            state: TransactionState::Active,
            participants: participants.clone(),
            timestamp,
        };

        self.transactions.lock().insert(txn_id.clone(), transaction);

        // No need to send begin messages - transactions are created on first operation
        Ok(txn_id)
    }

    /// Execute an operation within a transaction
    pub async fn execute_operation(
        &self,
        txn_id: &str,
        stream: &str,
        operation: Vec<u8>,
    ) -> Result<()> {
        // Check transaction exists and is active
        let txns = self.transactions.lock();
        let txn = txns
            .get(txn_id)
            .ok_or_else(|| CoordinatorError::TransactionNotFound(txn_id.to_string()))?;

        if !matches!(txn.state, TransactionState::Active) {
            return Err(CoordinatorError::InvalidState(format!(
                "Transaction {} is not active",
                txn_id
            )));
        }
        drop(txns);

        // Create message with transaction headers
        let mut headers = HashMap::new();
        headers.insert("txn_id".to_string(), txn_id.to_string());
        headers.insert("coordinator_id".to_string(), self.coordinator_id.clone());

        let message = Message::new(operation, headers);

        // Send to the appropriate stream
        self.client
            .publish_to_stream(stream.to_string(), vec![message])
            .await?;

        Ok(())
    }

    /// Commit a distributed transaction using 2PC
    pub async fn commit_transaction(&self, txn_id: &str) -> Result<()> {
        // Phase 1: Prepare
        {
            let mut txns = self.transactions.lock();
            let txn = txns
                .get_mut(txn_id)
                .ok_or_else(|| CoordinatorError::TransactionNotFound(txn_id.to_string()))?;

            txn.state = TransactionState::Preparing;
        }

        // Send prepare messages to all participants
        let participants = {
            let txns = self.transactions.lock();
            txns.get(txn_id).unwrap().participants.clone()
        };

        for stream in &participants {
            let mut headers = HashMap::new();
            headers.insert("txn_id".to_string(), txn_id.to_string());
            headers.insert("txn_phase".to_string(), "prepare".to_string());

            let message = Message::new(Vec::new(), headers);
            self.client
                .publish_to_stream(stream.clone(), vec![message])
                .await?;
        }

        // In a real system, we'd wait for prepare responses
        // For the mock, we'll assume all prepared successfully
        {
            let mut txns = self.transactions.lock();
            let txn = txns.get_mut(txn_id).unwrap();
            txn.state = TransactionState::Prepared;
        }

        // Phase 2: Commit
        {
            let mut txns = self.transactions.lock();
            let txn = txns.get_mut(txn_id).unwrap();
            txn.state = TransactionState::Committing;
        }

        // Send commit messages to all participants
        for stream in &participants {
            let mut headers = HashMap::new();
            headers.insert("txn_id".to_string(), txn_id.to_string());
            headers.insert("txn_phase".to_string(), "commit".to_string());

            let message = Message::new(Vec::new(), headers);
            self.client
                .publish_to_stream(stream.clone(), vec![message])
                .await?;
        }

        // Mark as committed
        {
            let mut txns = self.transactions.lock();
            let txn = txns.get_mut(txn_id).unwrap();
            txn.state = TransactionState::Committed;
        }

        Ok(())
    }

    /// Abort a distributed transaction
    pub async fn abort_transaction(&self, txn_id: &str) -> Result<()> {
        // Update state
        let participants = {
            let mut txns = self.transactions.lock();
            let txn = txns
                .get_mut(txn_id)
                .ok_or_else(|| CoordinatorError::TransactionNotFound(txn_id.to_string()))?;

            txn.state = TransactionState::Aborting;
            txn.participants.clone()
        };

        // Send abort messages to all participants
        for stream in &participants {
            let mut headers = HashMap::new();
            headers.insert("txn_id".to_string(), txn_id.to_string());
            headers.insert("txn_phase".to_string(), "abort".to_string());

            let message = Message::new(Vec::new(), headers);
            self.client
                .publish_to_stream(stream.clone(), vec![message])
                .await?;
        }

        // Mark as aborted
        {
            let mut txns = self.transactions.lock();
            let txn = txns.get_mut(txn_id).unwrap();
            txn.state = TransactionState::Aborted;
        }

        Ok(())
    }

    /// Get transaction state
    pub fn get_transaction_state(&self, txn_id: &str) -> Option<TransactionState> {
        self.transactions
            .lock()
            .get(txn_id)
            .map(|t| t.state.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_coordinator_transaction_lifecycle() {
        let engine = Arc::new(MockEngine::new());

        // Create streams for SQL and KV
        engine.create_stream("sql-stream".to_string()).unwrap();
        engine.create_stream("kv-stream".to_string()).unwrap();

        let coordinator = MockCoordinator::new("coord-1".to_string(), engine.clone());

        // Begin a transaction across both systems
        let txn_id = coordinator
            .begin_transaction(vec!["sql-stream".to_string(), "kv-stream".to_string()])
            .await
            .unwrap();

        // Verify transaction is active
        assert!(matches!(
            coordinator.get_transaction_state(&txn_id),
            Some(TransactionState::Active)
        ));

        // Execute operations
        let sql_op = b"SELECT * FROM users".to_vec();
        coordinator
            .execute_operation(&txn_id, "sql-stream", sql_op)
            .await
            .unwrap();

        let kv_op = b"GET user:123".to_vec();
        coordinator
            .execute_operation(&txn_id, "kv-stream", kv_op)
            .await
            .unwrap();

        // Commit the transaction
        coordinator.commit_transaction(&txn_id).await.unwrap();

        // Verify transaction is committed
        assert!(matches!(
            coordinator.get_transaction_state(&txn_id),
            Some(TransactionState::Committed)
        ));
    }

    #[tokio::test]
    async fn test_coordinator_abort() {
        let engine = Arc::new(MockEngine::new());

        engine.create_stream("sql-stream".to_string()).unwrap();
        engine.create_stream("kv-stream".to_string()).unwrap();

        let coordinator = MockCoordinator::new("coord-1".to_string(), engine.clone());

        // Begin and then abort a transaction
        let txn_id = coordinator
            .begin_transaction(vec!["sql-stream".to_string(), "kv-stream".to_string()])
            .await
            .unwrap();

        // Abort the transaction
        coordinator.abort_transaction(&txn_id).await.unwrap();

        // Verify transaction is aborted
        assert!(matches!(
            coordinator.get_transaction_state(&txn_id),
            Some(TransactionState::Aborted)
        ));
    }
}
