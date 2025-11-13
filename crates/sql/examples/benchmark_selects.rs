//! Benchmark for concurrent SELECT queries using read_at_timestamp
//!
//! This benchmark measures the concurrent read throughput of the SQL engine
//! by spawning multiple threads that perform snapshot reads in parallel.

use fjall::{CompressionType, PersistMode};
use proven_common::TransactionId;
use proven_processor::AutoBatchEngine;
use proven_sql::{SqlOperation, SqlResponse, SqlStorageConfig, SqlTransactionEngine, Value};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

fn main() {
    println!("=== Concurrent SELECT Benchmark ===\n");

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
        proven_processor::OperationResult::Complete(_) => {
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
            proven_processor::OperationResult::Complete(_) => {
                sql_engine.commit(txn_id);
            }
            _ => {
                eprintln!("Error at insert {}", i);
                return;
            }
        }

        if (i + 1) % 10_000 == 0 {
            eprint!(".");
        }
    }

    let populate_duration = populate_start.elapsed();
    println!(
        "\n✓ Table populated in {:.2} seconds\n",
        populate_duration.as_secs_f64()
    );

    // Get a read timestamp for snapshot reads
    let read_timestamp = TransactionId::new();

    // Wrap engine in Arc for shared access across threads
    let engine = Arc::new(sql_engine);

    // Benchmark configuration
    const NUM_READERS: usize = 8;
    const QUERIES_PER_READER: usize = 50_000;

    // Barrier to synchronize thread starts
    let barrier = Arc::new(Barrier::new(NUM_READERS));

    println!(
        "Step 3: Starting {} concurrent readers ({} queries each)...",
        NUM_READERS, QUERIES_PER_READER
    );
    println!("Total queries: {}\n", NUM_READERS * QUERIES_PER_READER);

    // Spawn reader threads
    let mut handles = vec![];

    for thread_id in 0..NUM_READERS {
        let engine_clone = Arc::clone(&engine);
        let barrier_clone = Arc::clone(&barrier);

        let handle = thread::spawn(move || {
            // Wait for all threads to be ready
            barrier_clone.wait();

            let start = Instant::now();
            let mut successful_queries = 0;
            let mut failed_queries = 0;

            // Perform queries
            for i in 0..QUERIES_PER_READER {
                // Query different rows to simulate realistic access patterns
                // Use thread_id to ensure some overlap between threads
                let row_id = (thread_id * 1000 + i) % NUM_ROWS;

                let query = SqlOperation::Query {
                    sql: "SELECT id, value, data, timestamp FROM bench WHERE id = ?".to_string(),
                    params: Some(vec![Value::I32(row_id as i32)]),
                };

                match engine_clone.read_at_timestamp(query, read_timestamp) {
                    SqlResponse::QueryResult { columns: _, rows } if rows.len() == 1 => {
                        successful_queries += 1;
                    }
                    _ => {
                        failed_queries += 1;
                    }
                }
            }

            let duration = start.elapsed();

            (thread_id, successful_queries, failed_queries, duration)
        });

        handles.push(handle);
    }

    // Wait for all threads and collect results
    let mut total_successful = 0;
    let mut total_failed = 0;
    let mut max_duration = std::time::Duration::default();
    let mut min_duration = std::time::Duration::from_secs(u64::MAX);
    let benchmark_start = Instant::now();

    println!("Thread results:");
    for handle in handles {
        let (thread_id, successful, failed, duration) = handle.join().unwrap();
        total_successful += successful;
        total_failed += failed;
        max_duration = max_duration.max(duration);
        min_duration = min_duration.min(duration);

        let throughput = successful as f64 / duration.as_secs_f64();
        println!(
            "  Thread {}: {} queries in {:.2}s ({:.0} queries/sec)",
            thread_id,
            successful,
            duration.as_secs_f64(),
            throughput
        );
    }

    let total_duration = benchmark_start.elapsed();

    // Calculate statistics
    let total_queries = total_successful + total_failed;
    let overall_throughput = total_successful as f64 / total_duration.as_secs_f64();
    let avg_latency_ms = (total_duration.as_secs_f64() * 1000.0) / total_queries as f64;

    // Print final statistics
    println!("\n=== Benchmark Results ===");
    println!("Concurrent readers:    {}", NUM_READERS);
    println!("Queries per reader:    {}", QUERIES_PER_READER);
    println!("Total queries:         {}", total_queries);
    println!("Successful queries:    {}", total_successful);
    println!("Failed queries:       {}", total_failed);
    println!("\nTiming:");
    println!(
        "  Total time:          {:.2} seconds",
        total_duration.as_secs_f64()
    );
    println!(
        "  Min thread time:     {:.2} seconds",
        min_duration.as_secs_f64()
    );
    println!(
        "  Max thread time:     {:.2} seconds",
        max_duration.as_secs_f64()
    );
    println!("\nThroughput:");
    println!(
        "  Overall:             {:.0} queries/second",
        overall_throughput
    );
    println!("  Avg latency:         {:.3} ms/query", avg_latency_ms);

    println!("\nBenchmark details:");
    println!("- Query type: SELECT with WHERE clause (point lookups)");
    println!("- Read method: read_at_timestamp (concurrent snapshot reads)");
    println!("- Read timestamp: {:?}", read_timestamp);
    println!("- Rows in table: {}", NUM_ROWS);

    println!("\n✓ Benchmark complete!");
}
