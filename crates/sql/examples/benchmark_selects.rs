//! Benchmark for sequential SELECT queries using read_at_timestamp
//!
//! This benchmark measures the throughput of the SQL engine by performing
//! snapshot reads on a populated table.

use fjall::{CompressionType, PersistMode};
use proven_common::TransactionId;
use proven_sql::{SqlOperation, SqlResponse, SqlStorageConfig, SqlTransactionEngine, Value};
use proven_stream::AutoBatchEngine;
use std::io::{self, Write};
use std::time::Instant;

fn main() {
    println!("=== Sequential SELECT Benchmark ===\n");

    let temp_dir_obj = tempfile::tempdir().expect("Failed to create temporary directory");
    let temp_dir = temp_dir_obj.path().to_path_buf();

    println!("Data directory: {}", temp_dir.display());

    let config = SqlStorageConfig {
        data_dir: temp_dir.clone(),
        block_cache_size: 1024 * 1024 * 1024,
        compression: CompressionType::Lz4,
        persist_mode: PersistMode::Buffer,
    };

    // Create SQL engine with auto-batch wrapper
    let mut sql_engine = AutoBatchEngine::new(SqlTransactionEngine::new(config.clone()));

    // Create table
    println!("Step 1: Creating table...");
    let txn_id = TransactionId::new();
    sql_engine.begin(txn_id);

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
        proven_stream::OperationResult::Complete(_) => {
            sql_engine.commit(txn_id);
            println!("✓ Table created\n");
        }
        _ => panic!("Failed to create table"),
    }

    // Populate table
    const NUM_ROWS: usize = 100_000;
    println!("Step 2: Populating table with {} rows...", NUM_ROWS);

    let populate_start = Instant::now();

    for i in 0..NUM_ROWS {
        let txn_id = TransactionId::new();
        sql_engine.begin(txn_id);

        let insert = SqlOperation::Execute {
            sql: "INSERT INTO bench (id, value, data, timestamp) VALUES (?, ?, ?, ?)".to_string(),
            params: Some(vec![
                Value::integer(i as i64),
                Value::integer((i * 2) as i64),
                Value::string(format!("data_{}", i % 1000)),
                Value::integer((2000000000 + i) as i64),
            ]),
        };

        match sql_engine.apply_operation(insert, txn_id) {
            proven_stream::OperationResult::Complete(_) => {
                sql_engine.commit(txn_id);
            }
            _ => {
                eprintln!("Error at insert {}", i);
                return;
            }
        }

        if (i + 1) % 10_000 == 0 {
            eprint!(".");
            io::stderr().flush().unwrap();
        }
    }

    let populate_duration = populate_start.elapsed();
    println!(
        "\n✓ Table populated in {:.2} seconds\n",
        populate_duration.as_secs_f64()
    );

    // Get a read timestamp for snapshot reads
    let read_timestamp = TransactionId::new();

    // First, run an EXPLAIN query to see the execution plan
    println!("Checking execution plan...");
    let explain_query = SqlOperation::Query {
        sql: "EXPLAIN SELECT id, value, data, timestamp FROM bench WHERE id = ?".to_string(),
        params: Some(vec![Value::integer(42)]),
    };

    match sql_engine.read_at_timestamp(explain_query, read_timestamp) {
        proven_stream::OperationResult::Complete(SqlResponse::ExplainPlan { plan }) => {
            println!("Execution Plan:\n{}\n", plan);
        }
        other => println!("EXPLAIN failed: {:?}\n", other),
    }

    // Benchmark configuration
    const NUM_QUERIES: usize = 100_000;
    const PROGRESS_INTERVAL: usize = 10_000;
    const STATUS_INTERVAL: usize = 20_000;

    println!(
        "Step 3: Running {} sequential SELECT queries using read_at_timestamp...",
        NUM_QUERIES
    );

    let start_time = Instant::now();
    let mut last_status_time = start_time;
    let mut last_status_count = 0;
    let mut successful_queries = 0;

    // Process queries
    for i in 0..NUM_QUERIES {
        // Query different rows to simulate realistic access patterns
        let row_id = i % NUM_ROWS;

        let query = SqlOperation::Query {
            sql: "SELECT id, value, data, timestamp FROM bench WHERE id = ?".to_string(),
            params: Some(vec![Value::I32(row_id as i32)]),
        };

        // Execute query using read_at_timestamp (snapshot read)
        match sql_engine.read_at_timestamp(query, read_timestamp) {
            proven_stream::OperationResult::Complete(SqlResponse::QueryResult {
                columns: _,
                rows,
            }) => {
                if rows.len() == 1 {
                    successful_queries += 1;
                }
            }
            _ => {
                eprintln!("\nError at query {}", i);
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
                "\n[{:7}/{:7}] {:3}% | Interval: {:.0} queries/sec",
                i + 1,
                NUM_QUERIES,
                ((i + 1) * 100) / NUM_QUERIES,
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
    let throughput = successful_queries as f64 / total_seconds;

    // Print final statistics
    println!("\n=== Benchmark Results ===");
    println!("Total queries:     {}", NUM_QUERIES);
    println!("Successful:        {}", successful_queries);
    println!("Total time:        {:.2} seconds", total_seconds);
    println!("Throughput:        {:.0} queries/second", throughput);
    println!(
        "Avg latency:       {:.3} ms/query",
        (total_seconds * 1000.0) / NUM_QUERIES as f64
    );

    println!("\nBenchmark details:");
    println!("- Query type: SELECT with WHERE clause (point lookups)");
    println!("- Read method: read_at_timestamp (snapshot reads)");
    println!("- Read timestamp: {:?}", read_timestamp);
    println!("- Rows in table: {}", NUM_ROWS);

    println!("\n✓ Benchmark complete!");
}
