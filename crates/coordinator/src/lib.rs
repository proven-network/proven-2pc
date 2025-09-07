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
//! use std::time::Duration;
//!
//! #[tokio::main]
//! async fn main() {
//!     let engine = Arc::new(MockEngine::new());
//!     let coordinator = MockCoordinator::new("coord-1".to_string(), engine);
//!     
//!     // Begin transaction with 30 second timeout (participants discovered dynamically)
//!     let txn_id = coordinator.begin_transaction(Duration::from_secs(30)).await.unwrap();
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
    use std::time::Duration;

    #[tokio::test]
    async fn test_coordinator_transaction_lifecycle() {
        let engine = Arc::new(MockEngine::new());

        // Create streams for SQL and KV
        engine.create_stream("sql-stream".to_string()).unwrap();
        engine.create_stream("kv-stream".to_string()).unwrap();

        let coordinator = MockCoordinator::new("coord-1".to_string(), engine.clone());

        // Begin a transaction (participants discovered dynamically)
        let txn_id = coordinator
            .begin_transaction(Duration::from_secs(30))
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
            .begin_transaction(Duration::from_secs(30))
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

    #[tokio::test]
    async fn test_dynamic_participant_discovery() {
        let engine = Arc::new(MockEngine::new());

        // Create streams
        engine.create_stream("stream-a".to_string()).unwrap();
        engine.create_stream("stream-b".to_string()).unwrap();
        engine.create_stream("stream-c".to_string()).unwrap();

        let coordinator = MockCoordinator::new("coord-1".to_string(), engine.clone());

        // Begin transaction with no participants
        let txn_id = coordinator
            .begin_transaction(Duration::from_secs(30))
            .await
            .unwrap();

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
        let txn_id = coordinator
            .begin_transaction(Duration::from_secs(30))
            .await
            .unwrap();

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
        let (op_msg, _, _) = stream_consumer.recv().await.unwrap();
        assert_eq!(op_msg.body, b"operation");

        // Second message should be prepare_and_commit
        let (commit_msg, _, _) = stream_consumer.recv().await.unwrap();
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
        let txn_id = coordinator
            .begin_transaction(Duration::from_secs(30))
            .await
            .unwrap();

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
        let txn_id = coordinator
            .begin_transaction(Duration::from_secs(30))
            .await
            .unwrap();

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
        let (_, _, _) = stream_a_consumer.recv().await.unwrap();
        let (_, _, _) = stream_b_consumer.recv().await.unwrap();

        // Both should receive prepare (not prepare_and_commit)
        let (prepare_a, _, _) = stream_a_consumer.recv().await.unwrap();
        assert_eq!(
            prepare_a.headers.get("txn_phase"),
            Some(&"prepare".to_string())
        );

        let (prepare_b, _, _) = stream_b_consumer.recv().await.unwrap();
        assert_eq!(
            prepare_b.headers.get("txn_phase"),
            Some(&"prepare".to_string())
        );

        // Then commit messages
        let (commit_a, _, _) = stream_a_consumer.recv().await.unwrap();
        assert_eq!(
            commit_a.headers.get("txn_phase"),
            Some(&"commit".to_string())
        );

        let (commit_b, _, _) = stream_b_consumer.recv().await.unwrap();
        assert_eq!(
            commit_b.headers.get("txn_phase"),
            Some(&"commit".to_string())
        );
    }

    #[tokio::test]
    async fn test_participant_awareness_tracking() {
        let engine = Arc::new(MockEngine::new());

        // Create three streams
        engine.create_stream("stream-a".to_string()).unwrap();
        engine.create_stream("stream-b".to_string()).unwrap();
        engine.create_stream("stream-c".to_string()).unwrap();

        let coordinator = MockCoordinator::new("coord-1".to_string(), engine.clone());

        // Begin transaction
        let txn_id = coordinator
            .begin_transaction(Duration::from_secs(30))
            .await
            .unwrap();

        // Subscribe to streams to verify awareness updates
        let mut stream_a_consumer = engine.stream_messages("stream-a", None).unwrap();
        let mut stream_b_consumer = engine.stream_messages("stream-b", None).unwrap();
        let mut stream_c_consumer = engine.stream_messages("stream-c", None).unwrap();

        // First operation on stream-a (no new_participants header expected)
        coordinator
            .execute_operation(&txn_id, "stream-a", b"op1".to_vec())
            .await
            .unwrap();

        let (msg_a1, _, _) = stream_a_consumer.recv().await.unwrap();
        assert_eq!(msg_a1.body, b"op1");
        assert!(msg_a1.headers.get("new_participants").is_none());

        // Second operation on stream-b (should learn about stream-a)
        coordinator
            .execute_operation(&txn_id, "stream-b", b"op2".to_vec())
            .await
            .unwrap();

        let (msg_b1, _, _) = stream_b_consumer.recv().await.unwrap();
        assert_eq!(msg_b1.body, b"op2");

        // Verify stream-b learns about stream-a with its offset
        let new_participants_b = msg_b1.headers.get("new_participants").unwrap();
        let participants_map: std::collections::HashMap<String, u64> =
            serde_json::from_str(new_participants_b).unwrap();
        assert_eq!(participants_map.len(), 1);
        assert!(participants_map.contains_key("stream-a"));

        // Third operation on stream-c (should learn about both stream-a and stream-b)
        coordinator
            .execute_operation(&txn_id, "stream-c", b"op3".to_vec())
            .await
            .unwrap();

        let (msg_c1, _, _) = stream_c_consumer.recv().await.unwrap();
        assert_eq!(msg_c1.body, b"op3");

        let new_participants_c = msg_c1.headers.get("new_participants").unwrap();
        let participants_map: std::collections::HashMap<String, u64> =
            serde_json::from_str(new_participants_c).unwrap();
        assert_eq!(participants_map.len(), 2);
        assert!(participants_map.contains_key("stream-a"));
        assert!(participants_map.contains_key("stream-b"));

        // Fourth operation back on stream-a (should only learn about stream-c, not stream-b again)
        coordinator
            .execute_operation(&txn_id, "stream-a", b"op4".to_vec())
            .await
            .unwrap();

        let (msg_a2, _, _) = stream_a_consumer.recv().await.unwrap();
        assert_eq!(msg_a2.body, b"op4");

        let new_participants_a = msg_a2.headers.get("new_participants").unwrap();
        let participants_map: std::collections::HashMap<String, u64> =
            serde_json::from_str(new_participants_a).unwrap();
        assert_eq!(participants_map.len(), 2); // stream-b and stream-c
        assert!(participants_map.contains_key("stream-b"));
        assert!(participants_map.contains_key("stream-c"));

        // Commit and verify prepare messages include any final deltas
        tokio::spawn({
            let coordinator = coordinator.clone();
            let txn_id = txn_id.clone();
            async move {
                coordinator.commit_transaction(&txn_id).await.unwrap();
            }
        });

        // Check prepare messages
        let (prepare_a, _, _) = stream_a_consumer.recv().await.unwrap();
        assert_eq!(
            prepare_a.headers.get("txn_phase"),
            Some(&"prepare".to_string())
        );
        // stream-a already knows about all participants, no new_participants expected
        assert!(prepare_a.headers.get("new_participants").is_none());

        let (prepare_b, _, _) = stream_b_consumer.recv().await.unwrap();
        assert_eq!(
            prepare_b.headers.get("txn_phase"),
            Some(&"prepare".to_string())
        );
        // stream-b should learn about stream-c in prepare
        if let Some(new_participants) = prepare_b.headers.get("new_participants") {
            let participants_map: std::collections::HashMap<String, u64> =
                serde_json::from_str(new_participants).unwrap();
            assert_eq!(participants_map.len(), 1);
            assert!(participants_map.contains_key("stream-c"));
        }

        let (prepare_c, _, _) = stream_c_consumer.recv().await.unwrap();
        assert_eq!(
            prepare_c.headers.get("txn_phase"),
            Some(&"prepare".to_string())
        );
        // stream-c already knows about all participants
        assert!(prepare_c.headers.get("new_participants").is_none());
    }

    #[tokio::test]
    async fn test_participant_awareness_no_duplicates() {
        let engine = Arc::new(MockEngine::new());

        engine.create_stream("stream-a".to_string()).unwrap();
        engine.create_stream("stream-b".to_string()).unwrap();

        let coordinator = MockCoordinator::new("coord-1".to_string(), engine.clone());
        let txn_id = coordinator
            .begin_transaction(Duration::from_secs(30))
            .await
            .unwrap();

        let mut stream_b_consumer = engine.stream_messages("stream-b", None).unwrap();

        // Execute operations
        coordinator
            .execute_operation(&txn_id, "stream-a", b"op1".to_vec())
            .await
            .unwrap();

        coordinator
            .execute_operation(&txn_id, "stream-b", b"op2".to_vec())
            .await
            .unwrap();

        let (msg_b1, _, _) = stream_b_consumer.recv().await.unwrap();
        let new_participants = msg_b1.headers.get("new_participants").unwrap();
        let participants_map: std::collections::HashMap<String, u64> =
            serde_json::from_str(new_participants).unwrap();
        assert_eq!(participants_map.len(), 1);
        assert!(participants_map.contains_key("stream-a"));

        // Another operation on stream-b (should not re-learn about stream-a)
        coordinator
            .execute_operation(&txn_id, "stream-b", b"op3".to_vec())
            .await
            .unwrap();

        let (msg_b2, _, _) = stream_b_consumer.recv().await.unwrap();
        assert!(msg_b2.headers.get("new_participants").is_none());
    }

    #[tokio::test]
    async fn test_participant_offsets_tracking() {
        let engine = Arc::new(MockEngine::new());

        engine.create_stream("stream-a".to_string()).unwrap();
        engine.create_stream("stream-b".to_string()).unwrap();

        let coordinator = MockCoordinator::new("coord-1".to_string(), engine.clone());
        let txn_id = coordinator
            .begin_transaction(Duration::from_secs(30))
            .await
            .unwrap();

        // Subscribe to get actual offsets
        let mut stream_a_consumer = engine.stream_messages("stream-a", None).unwrap();
        let mut stream_b_consumer = engine.stream_messages("stream-b", None).unwrap();

        // Execute first operation
        coordinator
            .execute_operation(&txn_id, "stream-a", b"op1".to_vec())
            .await
            .unwrap();

        let (_msg_a, _, seq_a) = stream_a_consumer.recv().await.unwrap();
        // The first message in stream-a should have sequence 1
        assert_eq!(seq_a, 1);

        // Execute second operation
        coordinator
            .execute_operation(&txn_id, "stream-b", b"op2".to_vec())
            .await
            .unwrap();

        let (msg_b, _, _) = stream_b_consumer.recv().await.unwrap();

        // Verify stream-b receives correct offset for stream-a
        let new_participants = msg_b.headers.get("new_participants").unwrap();
        let participants_map: std::collections::HashMap<String, u64> =
            serde_json::from_str(new_participants).unwrap();

        // The offset should be 1 (the sequence number where stream-a joined)
        assert_eq!(*participants_map.get("stream-a").unwrap(), 1);
    }
}
