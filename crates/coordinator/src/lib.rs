//! Mock coordinator for distributed transactions
//!
//! This module provides a mock coordinator that can orchestrate distributed
//! transactions across SQL and KV systems using two-phase commit.
//!
//! ## Current Implementation
//!
//! The coordinator manages distributed transactions with the following flow:
//! 1. Begin transaction with known participants
//! 2. Execute operations on participant streams
//! 3. Two-phase commit:
//!    - Phase 1: Send prepare to all participants, collect votes
//!    - Phase 2: Send commit if all prepared, abort otherwise
//!
//! ## Future Improvements
//!
//! The coordinator implementation is being enhanced in phases:
//!
//! ### Phase 2: Dynamic Participant Discovery
//! - Participants added automatically as operations are sent
//! - No need to declare participants upfront
//!
//! ### Phase 3: Single Participant Optimization  
//! - Skip prepare phase when only one participant
//! - Send combined "prepare_and_commit" message
//!
//! ### Phase 4: Progressive Participant Awareness
//! - Include participant list in operation messages
//! - Each participant learns about others over time
//! - Full awareness by prepare phase
//!
//! ### Phase 5: Recovery Protocol
//! - Include log offsets in prepare messages
//! - Allow participants to recover without coordinator
//! - Handle coordinator failures gracefully
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use proven_coordinator::MockCoordinator;
//! use proven_engine::MockEngine;
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() {
//!     let engine = Arc::new(MockEngine::new());
//!     let coordinator = MockCoordinator::new("coord-1".to_string(), engine);
//!     
//!     // Begin transaction (participants discovered dynamically)
//!     let txn_id = coordinator.begin_transaction().await.unwrap();
//!     
//!     // Execute operations
//!     coordinator.execute_operation(&txn_id, "sql-stream", b"INSERT...".to_vec()).await.unwrap();
//!     coordinator.execute_operation(&txn_id, "kv-stream", b"PUT...".to_vec()).await.unwrap();
//!     
//!     // Commit with 2PC
//!     coordinator.commit_transaction(&txn_id).await.unwrap();
//! }
//! ```

mod coordinator;
mod error;
mod transaction;

pub use coordinator::MockCoordinator;
pub use error::{CoordinatorError, Result};
pub use transaction::{PrepareVote, Transaction, TransactionState};

#[cfg(test)]
mod tests {
    use super::*;
    use proven_engine::MockEngine;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_coordinator_transaction_lifecycle() {
        let engine = Arc::new(MockEngine::new());

        // Create streams for SQL and KV
        engine.create_stream("sql-stream".to_string()).unwrap();
        engine.create_stream("kv-stream".to_string()).unwrap();

        let coordinator = MockCoordinator::new("coord-1".to_string(), engine.clone());

        // Begin a transaction (participants discovered dynamically)
        let txn_id = coordinator.begin_transaction().await.unwrap();

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
        let txn_id = coordinator.begin_transaction().await.unwrap();

        // Abort the transaction
        coordinator.abort_transaction(&txn_id).await.unwrap();

        // Verify transaction is aborted
        assert!(matches!(
            coordinator.get_transaction_state(&txn_id),
            Some(TransactionState::Aborted)
        ));
    }

    #[tokio::test]
    async fn test_dynamic_participant_discovery() {
        let engine = Arc::new(MockEngine::new());

        // Create streams
        engine.create_stream("stream-a".to_string()).unwrap();
        engine.create_stream("stream-b".to_string()).unwrap();
        engine.create_stream("stream-c".to_string()).unwrap();

        let coordinator = MockCoordinator::new("coord-1".to_string(), engine.clone());

        // Begin transaction with no participants
        let txn_id = coordinator.begin_transaction().await.unwrap();

        // Execute operations on different streams (participants added dynamically)
        coordinator
            .execute_operation(&txn_id, "stream-a", b"op1".to_vec())
            .await
            .unwrap();

        coordinator
            .execute_operation(&txn_id, "stream-b", b"op2".to_vec())
            .await
            .unwrap();

        // Same stream again (should not duplicate)
        coordinator
            .execute_operation(&txn_id, "stream-a", b"op3".to_vec())
            .await
            .unwrap();

        // Commit should send prepare to only stream-a and stream-b
        coordinator.commit_transaction(&txn_id).await.unwrap();

        // Verify the transaction committed successfully
        assert!(matches!(
            coordinator.get_transaction_state(&txn_id),
            Some(TransactionState::Committed)
        ));
    }

    #[tokio::test]
    async fn test_single_participant_optimization() {
        let engine = Arc::new(MockEngine::new());
        engine.create_stream("single-stream".to_string()).unwrap();

        let coordinator = MockCoordinator::new("coord-1".to_string(), engine.clone());

        // Begin transaction with no participants
        let txn_id = coordinator.begin_transaction().await.unwrap();

        // Execute operation on single stream
        coordinator
            .execute_operation(&txn_id, "single-stream", b"operation".to_vec())
            .await
            .unwrap();

        // Subscribe to stream to verify prepare_and_commit is sent
        let mut stream_consumer = engine.stream_messages("single-stream", None).unwrap();

        // Commit - should use prepare_and_commit optimization
        tokio::spawn(async move {
            coordinator.commit_transaction(&txn_id).await.unwrap();
        });

        // First message should be the operation
        let op_msg = stream_consumer.recv().await.unwrap();
        assert_eq!(op_msg.body, b"operation");

        // Second message should be prepare_and_commit
        let commit_msg = stream_consumer.recv().await.unwrap();
        assert_eq!(
            commit_msg.headers.get("txn_phase"),
            Some(&"prepare_and_commit".to_string())
        );
        assert!(commit_msg.body.is_empty());
    }

    #[tokio::test]
    async fn test_empty_transaction_commit() {
        let engine = Arc::new(MockEngine::new());
        let coordinator = MockCoordinator::new("coord-1".to_string(), engine.clone());

        // Begin transaction with no participants
        let txn_id = coordinator.begin_transaction().await.unwrap();

        // Commit with no operations (0 participants)
        coordinator.commit_transaction(&txn_id).await.unwrap();

        // Should be committed without any messages sent
        assert!(matches!(
            coordinator.get_transaction_state(&txn_id),
            Some(TransactionState::Committed)
        ));
    }

    #[tokio::test]
    async fn test_multiple_participants_uses_2pc() {
        let engine = Arc::new(MockEngine::new());
        engine.create_stream("stream-a".to_string()).unwrap();
        engine.create_stream("stream-b".to_string()).unwrap();

        let coordinator = MockCoordinator::new("coord-1".to_string(), engine.clone());

        // Begin transaction
        let txn_id = coordinator.begin_transaction().await.unwrap();

        // Execute operations on two streams
        coordinator
            .execute_operation(&txn_id, "stream-a", b"op1".to_vec())
            .await
            .unwrap();
        coordinator
            .execute_operation(&txn_id, "stream-b", b"op2".to_vec())
            .await
            .unwrap();

        // Subscribe to verify prepare messages
        let mut stream_a_consumer = engine.stream_messages("stream-a", None).unwrap();
        let mut stream_b_consumer = engine.stream_messages("stream-b", None).unwrap();

        // Commit - should use standard 2PC
        tokio::spawn(async move {
            coordinator.commit_transaction(&txn_id).await.unwrap();
        });

        // Skip operation messages
        let _ = stream_a_consumer.recv().await.unwrap();
        let _ = stream_b_consumer.recv().await.unwrap();

        // Both should receive prepare (not prepare_and_commit)
        let prepare_a = stream_a_consumer.recv().await.unwrap();
        assert_eq!(
            prepare_a.headers.get("txn_phase"),
            Some(&"prepare".to_string())
        );

        let prepare_b = stream_b_consumer.recv().await.unwrap();
        assert_eq!(
            prepare_b.headers.get("txn_phase"),
            Some(&"prepare".to_string())
        );

        // Then commit messages
        let commit_a = stream_a_consumer.recv().await.unwrap();
        assert_eq!(
            commit_a.headers.get("txn_phase"),
            Some(&"commit".to_string())
        );

        let commit_b = stream_b_consumer.recv().await.unwrap();
        assert_eq!(
            commit_b.headers.get("txn_phase"),
            Some(&"commit".to_string())
        );
    }
}
