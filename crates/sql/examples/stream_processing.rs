//! Example of stream-based SQL processing
//!
//! This demonstrates how the SQL engine consumes ordered messages from a stream,
//! handles transactions with PCC, and returns results through a response channel.

use proven_engine::{Message, MockClient, MockEngine};
use proven_sql::stream::{
    engine::SqlTransactionEngine, operation::SqlOperation, response::SqlResponse,
};
use proven_stream::StreamProcessor;
use std::collections::HashMap;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    println!("=== SQL Stream Processing Demo ===\n");

    // Create mock engine and client
    let engine = Arc::new(MockEngine::new());
    let client = Arc::new(MockClient::new("sql-example".to_string(), engine.clone()));

    // Create stream processor
    let sql_engine = SqlTransactionEngine::new();
    let mut processor = StreamProcessor::new(sql_engine, client.clone(), "sql-stream".to_string());

    // Create a test client to subscribe to coordinator responses
    let test_client = MockClient::new("demo-observer".to_string(), engine.clone());

    // Subscribe to coordinator response channel
    let mut coord_responses = test_client
        .subscribe("coordinator.coordinator_1.response", None)
        .await
        .unwrap();

    // Simulate a stream of messages
    let messages = vec![
        // 1. Create table (auto-commit)
        create_message(
            Some(SqlOperation::Execute {
                sql: "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR, balance INT)"
                    .to_string(),
            }),
            "txn_runtime1_1000000000",
            Some("coordinator_1"),
            true, // auto-commit
            None,
        ),
        // 2. Begin a transaction and insert data
        create_message(
            Some(SqlOperation::Execute {
                sql: "INSERT INTO users (id, name, balance) VALUES (1, 'Alice', 100)".to_string(),
            }),
            "txn_runtime1_2000000000",
            Some("coordinator_1"),
            false, // NOT auto-commit
            None,
        ),
        // 3. Update within same transaction
        create_message(
            Some(SqlOperation::Execute {
                sql: "UPDATE users SET balance = 150 WHERE id = 1".to_string(),
            }),
            "txn_runtime1_2000000000", // Same transaction
            Some("coordinator_1"),
            false,
            None,
        ),
        // 4. Commit the transaction
        create_message(None, "txn_runtime1_2000000000", None, false, Some("commit")),
        // 5. Query the data (auto-commit)
        create_message(
            Some(SqlOperation::Query {
                sql: "SELECT * FROM users WHERE id = 1".to_string(),
            }),
            "txn_runtime1_3000000000",
            Some("coordinator_1"),
            true,
            None,
        ),
        // 6. Start another transaction that will be aborted
        create_message(
            Some(SqlOperation::Execute {
                sql: "UPDATE users SET balance = 200 WHERE id = 1".to_string(),
            }),
            "txn_runtime1_4000000000",
            Some("coordinator_1"),
            false,
            None,
        ),
        // 7. Abort the transaction
        create_message(None, "txn_runtime1_4000000000", None, false, Some("abort")),
        // 8. Query again to verify abort worked
        create_message(
            Some(SqlOperation::Query {
                sql: "SELECT * FROM users".to_string(),
            }),
            "txn_runtime1_5000000000",
            Some("coordinator_1"),
            true,
            None,
        ),
    ];

    // Publish messages to the stream
    println!("Publishing {} messages to stream...\n", messages.len());
    for message in messages {
        client
            .publish_to_stream("sql-stream".to_string(), vec![message])
            .await
            .unwrap();
    }

    // Spawn processor to consume from stream
    tokio::spawn(async move { processor.run().await });

    // Give processor time to process messages
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Process the responses
    for _ in 0..5 {
        // Check for responses (non-blocking)
        if let Some(response_msg) = coord_responses.recv().await {
            if let Ok(response) = serde_json::from_slice::<SqlResponse>(&response_msg.body) {
                println!("  Response: {}", format_response(&response));
            }
        }
    }

    // Give time for final responses and collect them
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    while let Some(response_msg) = coord_responses.try_recv() {
        if let Ok(response) = serde_json::from_slice::<SqlResponse>(&response_msg.body) {
            println!("  Late response: {}", format_response(&response));
        }
    }

    println!("\n=== Processing Complete ===\n");
    println!("All messages have been processed and responses collected.");

    println!("\n=== Demo Complete ===");
}

fn format_response(response: &SqlResponse) -> String {
    match response {
        SqlResponse::ExecuteResult {
            result_type,
            rows_affected,
            message,
        } => {
            let rows_str = rows_affected
                .map(|n| format!("{} rows affected", n))
                .unwrap_or_else(|| "no rows affected".to_string());
            let msg_str = message
                .as_ref()
                .map(|m| format!(", {}", m))
                .unwrap_or_default();
            format!("{}: {}{}", result_type, rows_str, msg_str)
        }
        SqlResponse::QueryResult { rows, .. } => {
            if rows.is_empty() {
                "Query returned 0 rows".to_string()
            } else if rows.len() == 1 {
                format!("Query result: {:?}", rows[0])
            } else {
                format!("Query returned {} rows: {:?}", rows.len(), rows)
            }
        }
        SqlResponse::Error(e) => format!("Error: {}", e),
    }
}

fn create_message(
    operation: Option<SqlOperation>,
    txn_id: &str,
    coordinator_id: Option<&str>,
    auto_commit: bool,
    txn_phase: Option<&str>,
) -> Message {
    let mut headers = HashMap::new();
    headers.insert("txn_id".to_string(), txn_id.to_string());

    if let Some(coord) = coordinator_id {
        headers.insert("coordinator_id".to_string(), coord.to_string());
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
