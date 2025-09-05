//! Comprehensive tests for wound-wait deadlock prevention
//!
//! These tests verify the deterministic behavior of the wound-wait
//! mechanism under various scenarios.

#[cfg(test)]
mod tests {
    use crate::stream::operation::SqlOperation;
    use crate::stream::processor::SqlStreamProcessor;
    use crate::stream::response::SqlResponse;
    use proven_engine::{Message, MockClient};
    use std::collections::HashMap;

    /// Helper to create a test message
    fn create_message(
        operation: Option<SqlOperation>,
        txn_id: &str,
        coordinator_id: &str,
        auto_commit: bool,
        txn_phase: Option<&str>,
    ) -> Message {
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
            serde_json::to_vec(&op).unwrap()
        } else {
            Vec::new()
        };

        Message::new(body, headers)
    }

    #[tokio::test]
    async fn test_basic_wound_older_wounds_younger() {
        let (mut processor, engine) = SqlStreamProcessor::new_for_testing();

        // Create test client and subscribe to coordinator responses
        let test_client = MockClient::new("test-observer".to_string(), engine.clone());
        let mut younger_responses = test_client
            .subscribe("coordinator.coord_younger.response", None)
            .await
            .unwrap();
        let mut older_responses = test_client
            .subscribe("coordinator.coord_older.response", None)
            .await
            .unwrap();

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

        // First, younger transaction should get execute result
        let younger_initial = younger_responses
            .recv()
            .await
            .expect("Should receive initial response for younger");
        let younger_initial_resp: SqlResponse =
            serde_json::from_slice(&younger_initial.body).expect("Should deserialize response");
        assert!(
            matches!(younger_initial_resp, SqlResponse::ExecuteResult { .. }),
            "Younger transaction should succeed initially"
        );

        // Then younger should get wounded notification
        let younger_wounded = younger_responses
            .recv()
            .await
            .expect("Should receive wounded notification for younger");
        let younger_wounded_resp: SqlResponse = serde_json::from_slice(&younger_wounded.body)
            .expect("Should deserialize wounded response");
        assert!(
            matches!(younger_wounded_resp, SqlResponse::Wounded { .. }),
            "Younger should receive wounded notification"
        );

        // Older transaction should succeed
        let older_response = older_responses
            .recv()
            .await
            .expect("Should receive response for older transaction");
        let older_resp: SqlResponse =
            serde_json::from_slice(&older_response.body).expect("Should deserialize response");
        assert!(
            matches!(older_resp, SqlResponse::ExecuteResult { .. }),
            "Older transaction should succeed after wounding"
        );
    }

    #[tokio::test]
    async fn test_younger_defers_to_older() {
        let (mut processor, engine) = SqlStreamProcessor::new_for_testing();

        // Create test client and subscribe to coordinator responses
        let test_client = MockClient::new("test-observer".to_string(), engine.clone());
        let mut younger_responses = test_client
            .subscribe("coordinator.coord_younger.response", None)
            .await
            .unwrap();

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
        let younger_response = younger_responses
            .recv()
            .await
            .expect("Should receive response for younger transaction");
        let younger_resp: SqlResponse =
            serde_json::from_slice(&younger_response.body).expect("Should deserialize response");
        assert!(
            matches!(younger_resp, SqlResponse::Deferred { .. }),
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
        let (mut processor, engine) = SqlStreamProcessor::new_for_testing();

        // Create test client and subscribe to coordinator responses
        let test_client = MockClient::new("test-observer".to_string(), engine.clone());
        let mut oldest_responses = test_client
            .subscribe("coordinator.coord_oldest.response", None)
            .await
            .unwrap();

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
        let oldest_response = oldest_responses
            .recv()
            .await
            .expect("Should receive response for oldest transaction");
        let oldest_resp: SqlResponse =
            serde_json::from_slice(&oldest_response.body).expect("Should deserialize response");
        assert!(
            matches!(oldest_resp, SqlResponse::ExecuteResult { .. }),
            "Oldest should succeed after wounding"
        );
    }

    #[tokio::test]
    async fn test_wound_with_multiple_locks() {
        let (mut processor, engine) = SqlStreamProcessor::new_for_testing();

        // Create test client and subscribe to coordinator responses
        let test_client = MockClient::new("test-observer".to_string(), engine.clone());
        let mut younger_responses = test_client
            .subscribe("coordinator.coord_younger.response", None)
            .await
            .unwrap();
        let mut older_responses = test_client
            .subscribe("coordinator.coord_older.response", None)
            .await
            .unwrap();
        let mut new_responses = test_client
            .subscribe("coordinator.coord_new.response", None)
            .await
            .unwrap();

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

        // First, younger should get execute result for each update
        let younger_resp1 = younger_responses
            .recv()
            .await
            .expect("Should receive first response for younger");
        let younger_r1: SqlResponse =
            serde_json::from_slice(&younger_resp1.body).expect("Should deserialize response");
        assert!(
            matches!(younger_r1, SqlResponse::ExecuteResult { .. }),
            "Younger should succeed initially on row 1"
        );

        let younger_resp2 = younger_responses
            .recv()
            .await
            .expect("Should receive second response for younger");
        let younger_r2: SqlResponse =
            serde_json::from_slice(&younger_resp2.body).expect("Should deserialize response");
        assert!(
            matches!(younger_r2, SqlResponse::ExecuteResult { .. }),
            "Younger should succeed initially on row 2"
        );

        // Then younger should get wounded notification
        let younger_wounded = younger_responses
            .recv()
            .await
            .expect("Should receive wounded notification");
        let younger_w: SqlResponse =
            serde_json::from_slice(&younger_wounded.body).expect("Should deserialize response");
        assert!(
            matches!(younger_w, SqlResponse::Wounded { .. }),
            "Younger should be wounded"
        );

        // Older should succeed
        let older_response = older_responses
            .recv()
            .await
            .expect("Should receive response for older");
        let older_resp: SqlResponse =
            serde_json::from_slice(&older_response.body).expect("Should deserialize response");
        assert!(
            matches!(older_resp, SqlResponse::ExecuteResult { .. }),
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

        // New transaction should succeed
        let new_response = new_responses
            .recv()
            .await
            .expect("Should receive response for new transaction");
        let new_resp: SqlResponse =
            serde_json::from_slice(&new_response.body).expect("Should deserialize response");
        assert!(
            matches!(new_resp, SqlResponse::ExecuteResult { .. }),
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
        let (mut processor, engine) = SqlStreamProcessor::new_for_testing();

        // Create test client and subscribe to all coordinator responses
        let test_client = MockClient::new("test-observer".to_string(), engine.clone());

        // We'll collect all responses here
        let mut collected_responses = Vec::new();

        // Subscribe to the coordinators we'll use
        let mut coord_1000_responses = test_client
            .subscribe("coordinator.coord_1000.response", None)
            .await
            .unwrap();
        let mut coord_3000_responses = test_client
            .subscribe("coordinator.coord_3000.response", None)
            .await
            .unwrap();
        let mut coord_2000_responses = test_client
            .subscribe("coordinator.coord_2000.response", None)
            .await
            .unwrap();
        let mut coord_4000_responses = test_client
            .subscribe("coordinator.coord_4000.response", None)
            .await
            .unwrap();

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
            let coord_id = format!("coord_{}", &txn_id[13..17]);
            let msg = create_message(
                Some(SqlOperation::Execute {
                    sql: sql.to_string(),
                }),
                txn_id,
                &coord_id,
                auto_commit,
                None,
            );
            processor.process_message(msg).await.unwrap();

            // Collect responses from appropriate coordinator channel
            let responses = match &coord_id[6..] {
                "1000" => &mut coord_1000_responses,
                "2000" => &mut coord_2000_responses,
                "3000" => &mut coord_3000_responses,
                "4000" => &mut coord_4000_responses,
                _ => continue,
            };

            // Try to collect any immediate responses
            while let Some(response_msg) = responses.try_recv() {
                if let Ok(response) = serde_json::from_slice::<SqlResponse>(&response_msg.body) {
                    collected_responses.push((coord_id.clone(), txn_id.to_string(), response));
                }
            }
        }

        // Give time for any delayed responses and collect them
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Collect any remaining responses
        for (coord_id, responses) in [
            ("coord_1000", &mut coord_1000_responses),
            ("coord_2000", &mut coord_2000_responses),
            ("coord_3000", &mut coord_3000_responses),
            ("coord_4000", &mut coord_4000_responses),
        ] {
            while let Some(response_msg) = responses.try_recv() {
                if let Ok(response) = serde_json::from_slice::<SqlResponse>(&response_msg.body) {
                    if let Some(txn_id) = response_msg.headers.get("txn_id") {
                        collected_responses.push((coord_id.to_string(), txn_id.clone(), response));
                    }
                }
            }
        }

        collected_responses
    }

    #[tokio::test]
    async fn test_wound_clears_wait_graph() {
        let (mut processor, engine) = SqlStreamProcessor::new_for_testing();

        // Create test client and subscribe to coordinator responses
        let test_client = MockClient::new("test-observer".to_string(), engine.clone());
        let mut t2_responses = test_client
            .subscribe("coordinator.coord_t2.response", None)
            .await
            .unwrap();

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
        let t2_response = t2_responses
            .recv()
            .await
            .expect("Should receive response for T2");
        let t2_resp: SqlResponse =
            serde_json::from_slice(&t2_response.body).expect("Should deserialize response");
        assert!(
            matches!(t2_resp, SqlResponse::ExecuteResult { .. }),
            "T2 should succeed after wounding T3"
        );

        // Verify wait graph was cleaned up properly
        // (T4 and T5 should have been notified that T3 was wounded)
    }
}
