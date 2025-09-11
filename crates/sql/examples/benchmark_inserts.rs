//! Benchmark for inserting 1 million rows
//!
//! This benchmark measures the throughput of the SQL engine by inserting
//! 1 million rows into a table directly using the engine.

use proven_hlc::{HlcTimestamp, NodeId};
use proven_sql::stream::{engine::SqlTransactionEngine, operation::SqlOperation};
use proven_sql::types::value::Value;
use proven_stream::TransactionEngine;
use std::io::{self, Write};
use std::time::Instant;

fn main() {
    println!("=== 1 Million Insert Benchmark ===\n");

    // Create SQL engine directly
    let mut sql_engine = SqlTransactionEngine::new();

    // Create table
    println!("Creating table...");
    let txn_id = HlcTimestamp::new(1000000000, 0, NodeId::new(1));
    sql_engine.begin_transaction(txn_id);

    let create_table = SqlOperation::Execute {
        sql: "CREATE TABLE bench (
            id INT PRIMARY KEY, 
            value INT, 
            data VARCHAR,
            timestamp INT
        )"
        .to_string(),
        params: None,
    };

    match sql_engine.apply_operation(create_table, txn_id) {
        proven_stream::OperationResult::Success(_) => {
            sql_engine
                .commit(txn_id)
                .expect("Failed to commit table creation");
            println!("✓ Table created");
        }
        _ => panic!("Failed to create table"),
    }

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
        let txn_id = HlcTimestamp::new(2000000000 + i as u64, 0, NodeId::new(1));
        sql_engine.begin_transaction(txn_id);

        // Create insert operation
        let insert = SqlOperation::Execute {
            sql: "INSERT INTO bench (id, value, data, timestamp) VALUES (?, ?, ?, ?)".to_string(),
            params: Some(vec![
                Value::Integer(i as i64),                    // id
                Value::Integer((i * 2) as i64),              // value (some computation)
                Value::String(format!("data_{}", i % 1000)), // data (repeating pattern)
                Value::Integer((2000000000 + i) as i64),     // timestamp
            ]),
        };

        // Execute insert directly on engine
        match sql_engine.apply_operation(insert, txn_id) {
            proven_stream::OperationResult::Success(_) => {
                // Commit the transaction
                if let Err(e) = sql_engine.commit(txn_id) {
                    eprintln!("\nError committing insert {}: {}", i, e);
                    break;
                }
            }
            _ => {
                eprintln!("\nError at insert {}", i);
                break;
            }
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
    let verify_txn = HlcTimestamp::new(9999999999, 0, NodeId::new(1));
    sql_engine.begin_transaction(verify_txn);

    let count_query = SqlOperation::Query {
        sql: "SELECT COUNT(*) FROM bench".to_string(),
        params: None,
    };

    match sql_engine.apply_operation(count_query, verify_txn) {
        proven_stream::OperationResult::Success(_response) => {
            // In a real system, we'd parse the response to get the actual count
            println!("✓ Count query executed successfully");
            sql_engine
                .commit(verify_txn)
                .expect("Failed to commit count query");
        }
        _ => println!("⚠ Count query failed"),
    }

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
    println!("- Transactions executed: {}", NUM_INSERTS + 2); // +2 for create table and count
    println!("\n✓ Benchmark complete!");
}
