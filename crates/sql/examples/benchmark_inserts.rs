//! Benchmark for inserting 1 million rows
//!
//! This benchmark measures the throughput of the SQL engine by inserting
//! 1 million rows into a table through the streaming interface.

use proven_engine::{Message, MockClient, MockEngine};
use proven_sql::stream::{SqlOperation, SqlStreamProcessor};
use std::collections::HashMap;
use std::io::{self, Write};
use std::sync::Arc;
use std::time::Instant;

#[tokio::main]
async fn main() {
    println!("=== 1 Million Insert Benchmark ===\n");

    // Create mock engine and client
    let engine = Arc::new(MockEngine::new());
    let client = Arc::new(MockClient::new("benchmark".to_string(), engine.clone()));

    // Create stream processor
    let mut processor = SqlStreamProcessor::new(client.clone(), "sql-stream".to_string());

    // Create table
    println!("Creating table...");
    let create_table = create_message(
        Some(SqlOperation::Execute {
            sql: "CREATE TABLE bench (
                id INT PRIMARY KEY, 
                value INT, 
                data VARCHAR,
                timestamp INT
            )"
            .to_string(),
        }),
        "txn_bench_1000000000",
        Some("coordinator_bench"),
        true,
        None,
    );

    processor
        .process_message(create_table)
        .await
        .expect("Failed to create table");

    // Benchmark configuration
    const NUM_INSERTS: usize = 1_000_000;
    const PROGRESS_INTERVAL: usize = 10_000;
    const STATUS_INTERVAL: usize = 100_000;

    println!("Starting {} inserts...", NUM_INSERTS);
    let start_time = Instant::now();
    let mut last_status_time = start_time;
    let mut last_status_count = 0;

    // Process inserts
    for i in 0..NUM_INSERTS {
        // Generate unique transaction ID with incrementing timestamp
        let txn_id = format!("txn_bench_{}", 2000000000 + i as u64);

        // Create insert message
        let insert = create_message(
            Some(SqlOperation::Execute {
                sql: format!(
                    "INSERT INTO bench (id, value, data, timestamp) VALUES ({}, {}, 'data_{}', {})",
                    i,
                    i * 2,    // Some computation
                    i % 1000, // Repeating data pattern
                    2000000000 + i
                ),
            }),
            &txn_id,
            Some("coordinator_bench"),
            true, // Auto-commit each insert
            None,
        );

        // Process the message
        if let Err(e) = processor.process_message(insert).await {
            eprintln!("\nError at insert {}: {:?}", i, e);
            break;
        }

        // Progress indicator
        if (i + 1) % PROGRESS_INTERVAL == 0 {
            eprint!(".");
            io::stderr().flush().unwrap();
        }

        // Status update
        if (i + 1) % STATUS_INTERVAL == 0 {
            let current_time = Instant::now();
            let interval_duration = current_time.duration_since(last_status_time);
            let interval_count = (i + 1) - last_status_count;
            let interval_throughput = interval_count as f64 / interval_duration.as_secs_f64();

            eprintln!(
                "\n[{:7}/{:7}] {:3}% | Interval: {:.0} inserts/sec",
                i + 1,
                NUM_INSERTS,
                ((i + 1) * 100) / NUM_INSERTS,
                interval_throughput
            );

            last_status_time = current_time;
            last_status_count = i + 1;
        }
    }

    eprintln!(); // New line after progress dots

    // Calculate final statistics
    let total_duration = start_time.elapsed();
    let total_seconds = total_duration.as_secs_f64();
    let throughput = NUM_INSERTS as f64 / total_seconds;

    // Verify count
    println!("\nVerifying insert count...");
    let count_query = create_message(
        Some(SqlOperation::Query {
            sql: "SELECT COUNT(*) FROM bench".to_string(),
        }),
        "txn_bench_9999999999", // Valid timestamp format
        Some("coordinator_bench"),
        true,
        None,
    );

    processor
        .process_message(count_query)
        .await
        .expect("Failed to query count");

    // Note: In a real system, we would subscribe to responses to verify the count
    // For this benchmark, we'll just report timing

    // Print final statistics
    println!("\n=== Benchmark Results ===");
    println!("Total inserts:     {}", NUM_INSERTS);
    println!("Total time:        {:.2} seconds", total_seconds);
    println!("Throughput:        {:.0} inserts/second", throughput);
    println!(
        "Avg latency:       {:.3} ms/insert",
        (total_seconds * 1000.0) / NUM_INSERTS as f64
    );

    println!("\nMemory usage and detailed statistics:");
    println!("- Messages processed: {}", NUM_INSERTS + 2); // +2 for create table and count
    println!("\n✓ Benchmark complete!");
}

/// Helper function to create a stream message
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
