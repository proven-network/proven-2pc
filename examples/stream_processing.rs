//! Example of stream-based SQL processing
//!
//! This demonstrates how the SQL engine consumes ordered messages from a stream,
//! handles transactions with PCC, and returns results through a response channel.

use proven_sql::sql::stream_processor::{
    MockResponseChannel, SqlOperation, SqlResponse, SqlStreamProcessor, StreamMessage,
};
use std::collections::HashMap;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    println!("=== SQL Stream Processing Demo ===\n");

    // Create response channel to capture results
    let mock_channel = Arc::new(MockResponseChannel::new());
    let response_channel: Box<dyn proven_sql::sql::stream_processor::ResponseChannel> =
        Box::new(mock_channel.clone());

    // Create stream processor with empty storage
    let mut processor = SqlStreamProcessor::new(response_channel);

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

    // Process the stream
    println!("Processing {} messages...\n", messages.len());
    for (i, message) in messages.into_iter().enumerate() {
        println!("Message {}: Processing...", i + 1);
        if let Err(e) = processor.process_message(message).await {
            eprintln!("  Error: {:?}", e);
        }
    }

    // Display responses
    println!("\n=== Responses Sent to Coordinators ===\n");
    let responses = mock_channel.get_responses();
    for (i, (coordinator, txn_id, response)) in responses.iter().enumerate() {
        println!("Response {}:", i + 1);
        println!("  Coordinator: {}", coordinator);
        println!("  Transaction: {}", txn_id);
        match response {
            SqlResponse::QueryResult { columns, rows } => {
                println!("  Type: Query Result");
                println!("  Columns: {:?}", columns);
                println!("  Rows: {} returned", rows.len());
                for row in rows {
                    println!("    {:?}", row);
                }
            }
            SqlResponse::ExecuteResult {
                result_type,
                rows_affected,
                message,
            } => {
                println!("  Type: Execute Result ({})", result_type);
                if let Some(count) = rows_affected {
                    println!("  Rows affected: {}", count);
                }
                if let Some(msg) = message {
                    println!("  Message: {}", msg);
                }
            }
            SqlResponse::Error(e) => {
                println!("  Type: Error");
                println!("  Message: {}", e);
            }
        }
        println!();
    }

    println!("=== Demo Complete ===");
}

fn create_message(
    operation: Option<SqlOperation>,
    txn_id: &str,
    coordinator_id: Option<&str>,
    auto_commit: bool,
    txn_phase: Option<&str>,
) -> StreamMessage {
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
        bincode::encode_to_vec(&op, bincode::config::standard()).unwrap()
    } else {
        Vec::new()
    };

    StreamMessage { body, headers }
}
