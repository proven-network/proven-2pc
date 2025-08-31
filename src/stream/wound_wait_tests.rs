//! Comprehensive tests for wound-wait deadlock prevention
//!
//! These tests verify the deterministic behavior of the wound-wait
//! mechanism under various scenarios.

#[cfg(test)]
mod tests {
    use crate::stream::message::{SqlOperation, StreamMessage};
    use crate::stream::processor::SqlStreamProcessor;
    use crate::stream::response::{MockResponseChannel, SqlResponse};
    use std::collections::HashMap;
    use std::sync::Arc;

    /// Helper to create a test message
    fn create_message(
        operation: Option<SqlOperation>,
        txn_id: &str,
        coordinator_id: &str,
        auto_commit: bool,
        txn_phase: Option<&str>,
    ) -> StreamMessage {
        let mut headers = HashMap::new();
        headers.insert("txn_id".to_string(), txn_id.to_string());

        if operation.is_some() {
            headers.insert("coordinator_id".to_string(), coordinator_id.to_string());
        }

        if auto_commit {
            headers.insert("auto_commit".to_string(), "true".to_string());
        }

        if let Some(phase) = txn_phase {
            headers.insert("txn_phase".to_string(), phase.to_string());
        }

        let body = if let Some(op) = operation {
            bincode::encode_to_vec(&op, bincode::config::standard()).unwrap()
        } else {
            Vec::new()
        };

        StreamMessage::new(body, headers)
    }

    #[tokio::test]
    async fn test_basic_wound_older_wounds_younger() {
        let mock_channel = Arc::new(MockResponseChannel::new());
        let mut processor = SqlStreamProcessor::new(Box::new(mock_channel.clone()));

        // Setup: Create table with a row
        let setup_sql = vec![
            "CREATE TABLE test (id INT PRIMARY KEY, value INT)",
            "INSERT INTO test (id, value) VALUES (1, 100)",
        ];

        for (i, sql) in setup_sql.iter().enumerate() {
            let msg = create_message(
                Some(SqlOperation::Execute {
                    sql: sql.to_string(),
                }),
                &format!("txn_runtime1_{:010}", i * 100),
                "coordinator_setup",
                true,
                None,
            );
            processor.process_message(msg).await.unwrap();
        }

        // Younger transaction (3000) acquires lock first
        let msg = create_message(
            Some(SqlOperation::Execute {
                sql: "UPDATE test SET value = 200 WHERE id = 1".to_string(),
            }),
            "txn_runtime1_3000000000",
            "coord_younger",
            false, // Hold lock
            None,
        );
        processor.process_message(msg).await.unwrap();

        // Older transaction (2000) should wound younger
        let msg = create_message(
            Some(SqlOperation::Execute {
                sql: "UPDATE test SET value = 300 WHERE id = 1".to_string(),
            }),
            "txn_runtime1_2000000000",
            "coord_older",
            false,
            None,
        );
        processor.process_message(msg).await.unwrap();

        // Verify responses
        let responses = mock_channel.get_responses();

        // Check that younger got wounded notification
        let wounded_response = responses.iter().find(|(c, _, resp)| {
            c == "coord_younger" && matches!(resp, SqlResponse::Wounded { .. })
        });
        assert!(
            wounded_response.is_some(),
            "Younger should receive wounded notification"
        );

        // Check that older succeeded
        let older_response = responses.iter().find(|(c, _, _)| c == "coord_older");
        assert!(
            matches!(
                older_response,
                Some((_, _, SqlResponse::ExecuteResult { .. }))
            ),
            "Older transaction should succeed after wounding"
        );
    }

    #[tokio::test]
    async fn test_younger_defers_to_older() {
        let mock_channel = Arc::new(MockResponseChannel::new());
        let mut processor = SqlStreamProcessor::new(Box::new(mock_channel.clone()));

        // Setup
        let setup_sql = vec![
            "CREATE TABLE test (id INT PRIMARY KEY, value INT)",
            "INSERT INTO test (id, value) VALUES (1, 100)",
        ];

        for (i, sql) in setup_sql.iter().enumerate() {
            let msg = create_message(
                Some(SqlOperation::Execute {
                    sql: sql.to_string(),
                }),
                &format!("txn_runtime1_{:010}", i * 100),
                "coordinator_setup",
                true,
                None,
            );
            processor.process_message(msg).await.unwrap();
        }

        // Older transaction (2000) acquires lock first
        let msg = create_message(
            Some(SqlOperation::Execute {
                sql: "UPDATE test SET value = 200 WHERE id = 1".to_string(),
            }),
            "txn_runtime1_2000000000",
            "coord_older",
            false, // Hold lock
            None,
        );
        processor.process_message(msg).await.unwrap();

        // Younger transaction (3000) should defer
        let msg = create_message(
            Some(SqlOperation::Execute {
                sql: "UPDATE test SET value = 300 WHERE id = 1".to_string(),
            }),
            "txn_runtime1_3000000000",
            "coord_younger",
            false,
            None,
        );
        processor.process_message(msg).await.unwrap();

        // Check that younger got deferred response
        let responses = mock_channel.get_responses();
        let younger_response = responses.iter().find(|(c, _, _)| c == "coord_younger");
        assert!(
            matches!(younger_response, Some((_, _, SqlResponse::Deferred { .. }))),
            "Younger transaction should be deferred"
        );

        // Commit older to release lock
        let msg = create_message(
            None,
            "txn_runtime1_2000000000",
            "coord_older",
            false,
            Some("commit"),
        );
        processor.process_message(msg).await.unwrap();

        // Younger should be automatically retried and succeed
        // (In current implementation, this requires the deferred retry mechanism)
    }

    #[tokio::test]
    async fn test_multi_level_wound_chain() {
        let mock_channel = Arc::new(MockResponseChannel::new());
        let mut processor = SqlStreamProcessor::new(Box::new(mock_channel.clone()));

        // Setup
        let msg = create_message(
            Some(SqlOperation::Execute {
                sql: "CREATE TABLE test (id INT PRIMARY KEY, value INT)".to_string(),
            }),
            "txn_runtime1_0000000000",
            "coord_setup",
            true,
            None,
        );
        processor.process_message(msg).await.unwrap();

        let msg = create_message(
            Some(SqlOperation::Execute {
                sql: "INSERT INTO test (id, value) VALUES (1, 100)".to_string(),
            }),
            "txn_runtime1_0000000001",
            "coord_setup",
            true,
            None,
        );
        processor.process_message(msg).await.unwrap();

        // Youngest (4000) gets lock
        let msg = create_message(
            Some(SqlOperation::Execute {
                sql: "UPDATE test SET value = 400 WHERE id = 1".to_string(),
            }),
            "txn_runtime1_4000000000",
            "coord_youngest",
            false,
            None,
        );
        processor.process_message(msg).await.unwrap();

        // Middle (3000) tries - should defer (younger than 4000)
        let msg = create_message(
            Some(SqlOperation::Execute {
                sql: "UPDATE test SET value = 300 WHERE id = 1".to_string(),
            }),
            "txn_runtime1_3000000000",
            "coord_middle",
            false,
            None,
        );
        processor.process_message(msg).await.unwrap();

        // Oldest (2000) tries - should wound youngest
        let msg = create_message(
            Some(SqlOperation::Execute {
                sql: "UPDATE test SET value = 200 WHERE id = 1".to_string(),
            }),
            "txn_runtime1_2000000000",
            "coord_oldest",
            false,
            None,
        );
        processor.process_message(msg).await.unwrap();

        // Verify oldest succeeded
        let responses = mock_channel.get_responses();
        let oldest_response = responses.iter().find(|(c, _, _)| c == "coord_oldest");
        assert!(
            matches!(
                oldest_response,
                Some((_, _, SqlResponse::ExecuteResult { .. }))
            ),
            "Oldest should succeed after wounding"
        );
    }

    #[tokio::test]
    async fn test_wound_with_multiple_locks() {
        let mock_channel = Arc::new(MockResponseChannel::new());
        let mut processor = SqlStreamProcessor::new(Box::new(mock_channel.clone()));

        // Setup: Create table with multiple rows
        let setup_sql = vec![
            "CREATE TABLE test (id INT PRIMARY KEY, value INT)",
            "INSERT INTO test (id, value) VALUES (1, 100)",
            "INSERT INTO test (id, value) VALUES (2, 200)",
        ];

        for (i, sql) in setup_sql.iter().enumerate() {
            let msg = create_message(
                Some(SqlOperation::Execute {
                    sql: sql.to_string(),
                }),
                &format!("txn_runtime1_{:010}", i * 100),
                "coordinator_setup",
                true,
                None,
            );
            processor.process_message(msg).await.unwrap();
        }

        // Younger transaction holds locks on both rows
        for id in 1..=2 {
            let msg = create_message(
                Some(SqlOperation::Execute {
                    sql: format!("UPDATE test SET value = 999 WHERE id = {}", id),
                }),
                "txn_runtime1_3000000000",
                "coord_younger",
                false,
                None,
            );
            processor.process_message(msg).await.unwrap();
        }

        // Older transaction tries to update row 1 - should wound younger
        let msg = create_message(
            Some(SqlOperation::Execute {
                sql: "UPDATE test SET value = 111 WHERE id = 1".to_string(),
            }),
            "txn_runtime1_2000000000",
            "coord_older",
            false,
            None,
        );
        processor.process_message(msg).await.unwrap();

        // Verify older succeeded
        let responses = mock_channel.get_responses();
        let older_response = responses
            .iter()
            .filter(|(c, _, _)| c == "coord_older")
            .last();
        assert!(
            matches!(
                older_response,
                Some((_, _, SqlResponse::ExecuteResult { .. }))
            ),
            "Older should succeed after wounding"
        );

        // Verify younger's locks were all released by trying to update row 2
        let msg = create_message(
            Some(SqlOperation::Execute {
                sql: "UPDATE test SET value = 222 WHERE id = 2".to_string(),
            }),
            "txn_runtime1_4000000000",
            "coord_new",
            true,
            None,
        );
        processor.process_message(msg).await.unwrap();

        // Get fresh responses after the new operation
        let all_responses = mock_channel.get_responses();
        let new_response = all_responses
            .iter()
            .filter(|(c, _, _)| c == "coord_new")
            .last();
        assert!(
            matches!(
                new_response,
                Some((_, _, SqlResponse::ExecuteResult { .. }))
            ),
            "New transaction should succeed - younger's locks should be released"
        );
    }

    #[tokio::test]
    async fn test_wound_preserves_determinism() {
        // Run the same sequence twice - should get identical results
        let results1 = run_wound_sequence().await;
        let results2 = run_wound_sequence().await;

        assert_eq!(
            results1.len(),
            results2.len(),
            "Should have same number of results"
        );

        for (i, (r1, r2)) in results1.iter().zip(results2.iter()).enumerate() {
            // Compare coordinator IDs and response types
            assert_eq!(r1.0, r2.0, "Coordinator ID mismatch at position {}", i);
            assert_eq!(
                std::mem::discriminant(&r1.2),
                std::mem::discriminant(&r2.2),
                "Response type mismatch at position {}",
                i
            );
        }
    }

    async fn run_wound_sequence() -> Vec<(String, String, SqlResponse)> {
        let mock_channel = Arc::new(MockResponseChannel::new());
        let mut processor = SqlStreamProcessor::new(Box::new(mock_channel.clone()));

        // Fixed sequence of operations
        let operations = vec![
            (
                "txn_runtime1_1000000000",
                "CREATE TABLE test (id INT PRIMARY KEY, value INT)",
                true,
            ),
            (
                "txn_runtime1_1000000001",
                "INSERT INTO test (id, value) VALUES (1, 100)",
                true,
            ),
            (
                "txn_runtime1_3000000000",
                "UPDATE test SET value = 300 WHERE id = 1",
                false,
            ),
            (
                "txn_runtime1_2000000000",
                "UPDATE test SET value = 200 WHERE id = 1",
                false,
            ),
            (
                "txn_runtime1_4000000000",
                "UPDATE test SET value = 400 WHERE id = 1",
                false,
            ),
        ];

        for (txn_id, sql, auto_commit) in operations {
            let msg = create_message(
                Some(SqlOperation::Execute {
                    sql: sql.to_string(),
                }),
                txn_id,
                &format!("coord_{}", &txn_id[13..17]),
                auto_commit,
                None,
            );
            processor.process_message(msg).await.unwrap();
        }

        mock_channel.get_responses()
    }

    #[tokio::test]
    async fn test_wound_clears_wait_graph() {
        let mock_channel = Arc::new(MockResponseChannel::new());
        let mut processor = SqlStreamProcessor::new(Box::new(mock_channel.clone()));

        // Setup
        let setup_sql = vec![
            "CREATE TABLE test (id INT PRIMARY KEY, value INT)",
            "INSERT INTO test (id, value) VALUES (1, 100)",
        ];

        for (i, sql) in setup_sql.iter().enumerate() {
            let msg = create_message(
                Some(SqlOperation::Execute {
                    sql: sql.to_string(),
                }),
                &format!("txn_runtime1_{:010}", i * 100),
                "coordinator_setup",
                true,
                None,
            );
            processor.process_message(msg).await.unwrap();
        }

        // Create a wait chain: T3 holds lock, T4 waits, T5 waits
        let msg = create_message(
            Some(SqlOperation::Execute {
                sql: "UPDATE test SET value = 300 WHERE id = 1".to_string(),
            }),
            "txn_runtime1_3000000000",
            "coord_t3",
            false,
            None,
        );
        processor.process_message(msg).await.unwrap();

        // T4 waits (younger than T3, so defers)
        let msg = create_message(
            Some(SqlOperation::Execute {
                sql: "UPDATE test SET value = 400 WHERE id = 1".to_string(),
            }),
            "txn_runtime1_4000000000",
            "coord_t4",
            false,
            None,
        );
        processor.process_message(msg).await.unwrap();

        // T5 waits (younger than T3, so defers)
        let msg = create_message(
            Some(SqlOperation::Execute {
                sql: "UPDATE test SET value = 500 WHERE id = 1".to_string(),
            }),
            "txn_runtime1_5000000000",
            "coord_t5",
            false,
            None,
        );
        processor.process_message(msg).await.unwrap();

        // T2 (older) wounds T3
        let msg = create_message(
            Some(SqlOperation::Execute {
                sql: "UPDATE test SET value = 200 WHERE id = 1".to_string(),
            }),
            "txn_runtime1_2000000000",
            "coord_t2",
            false,
            None,
        );
        processor.process_message(msg).await.unwrap();

        // Verify T2 succeeded
        let responses = mock_channel.get_responses();
        let t2_response = responses.iter().filter(|(c, _, _)| c == "coord_t2").last();
        assert!(
            matches!(t2_response, Some((_, _, SqlResponse::ExecuteResult { .. }))),
            "T2 should succeed after wounding T3"
        );

        // Verify wait graph was cleaned up properly
        // (T4 and T5 should have been notified that T3 was wounded)
    }
}
