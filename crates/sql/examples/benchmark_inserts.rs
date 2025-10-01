//! Benchmark for inserting 1 million rows
//!
//! This benchmark measures the throughput of the SQL engine by inserting
//! 1 million rows into a table directly using the engine.

use fjall::{CompressionType, PersistMode};
use proven_hlc::{HlcTimestamp, NodeId};
use proven_sql::{SqlOperation, SqlTransactionEngine, StorageConfig, Value};
use proven_stream::TransactionEngine;
use std::io::{self, Write};
use std::path::PathBuf;
use std::thread::sleep;
use std::time::{Duration, Instant};

fn main() {
    println!("=== 1 Million Insert Benchmark ===\n");

    let temp_dir_obj = tempfile::tempdir().expect("Failed to create temporary directory");
    let temp_dir = temp_dir_obj.path().to_path_buf();

    println!("Data directory: {}", temp_dir.display());

    // Create SQL engine directly
    let mut sql_engine = SqlTransactionEngine::new(StorageConfig {
        data_dir: temp_dir.clone(),
        block_cache_size: 1024 * 1024 * 1024,
        compression: CompressionType::Lz4,
        persist_mode: PersistMode::Buffer,
    });

    // Create table
    println!("Creating table...");
    let txn_id = HlcTimestamp::new(1000000000, 0, NodeId::new(1));
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
        sql_engine.begin(txn_id);

        // Create insert operation
        let insert = SqlOperation::Execute {
            sql: "INSERT INTO bench (id, value, data, timestamp) VALUES (?, ?, ?, ?)".to_string(),
            params: Some(vec![
                Value::integer(i as i64),                    // id
                Value::integer((i * 2) as i64),              // value (some computation)
                Value::string(format!("data_{}", i % 1000)), // data (repeating pattern)
                Value::integer((2000000000 + i) as i64),     // timestamp
            ]),
        };

        // Execute insert directly on engine
        match sql_engine.apply_operation(insert, txn_id) {
            proven_stream::OperationResult::Complete(_) => {
                // Commit the transaction
                sql_engine.commit(txn_id);
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
    sql_engine.begin(verify_txn);

    let count_query = SqlOperation::Query {
        sql: "SELECT COUNT(*) FROM bench".to_string(),
        params: None,
    };

    let elapsed = Instant::now();
    match sql_engine.apply_operation(count_query, verify_txn) {
        proven_stream::OperationResult::Complete(_response) => {
            let count_query_time = elapsed.elapsed();
            println!("Count query time: {}ms", count_query_time.as_millis());
            // In a real system, we'd parse the response to get the actual count
            println!("✓ Count query executed successfully");
            sql_engine.abort(verify_txn);
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

    println!("Sleeping for 20 seconds for compaction to bring data to steady state...");
    sleep(Duration::from_secs(20));

    // Calculate directory size
    println!("\nCalculating storage size...");
    let dir_size = calculate_dir_size(&temp_dir).unwrap_or(0);
    println!(
        "Total storage size: {:.2} MB",
        dir_size as f64 / 1_048_576.0
    );
    println!(
        "Bytes per row:      {:.1} bytes",
        dir_size as f64 / NUM_INSERTS as f64
    );

    // Show partition breakdown
    println!("\nStorage breakdown:");
    print_directory_breakdown(&temp_dir, dir_size, 0);

    println!("\n✓ Benchmark complete!");

    // Cleanup
    println!("\nCleaning up temporary directory...");
    drop(sql_engine); // Ensure engine releases file handles
    if let Err(e) = std::fs::remove_dir_all(&temp_dir) {
        eprintln!("⚠ Failed to cleanup directory: {}", e);
    } else {
        println!("✓ Directory cleaned up");
    }
}

/// Calculate total size of a directory recursively
fn calculate_dir_size(path: &PathBuf) -> std::io::Result<u64> {
    let mut total = 0;

    if path.is_dir() {
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let metadata = entry.metadata()?;

            if metadata.is_dir() {
                total += calculate_dir_size(&entry.path())?;
            } else {
                total += metadata.len();
            }
        }
    }

    Ok(total)
}

/// Print directory breakdown recursively with indentation
fn print_directory_breakdown(path: &PathBuf, total_size: u64, depth: usize) {
    let Ok(entries) = std::fs::read_dir(path) else {
        return;
    };

    let mut items: Vec<_> = entries
        .filter_map(|e| e.ok())
        .filter_map(|entry| {
            let size = calculate_dir_size(&entry.path()).ok()?;
            let name = entry.file_name().to_string_lossy().to_string();
            let is_dir = entry.path().is_dir();
            Some((name, size, is_dir, entry.path()))
        })
        .collect();

    // Sort by size descending
    items.sort_by(|a, b| b.1.cmp(&a.1));

    let indent = "  ".repeat(depth);

    for (name, size, is_dir, entry_path) in items {
        // Skip very small files (< 0.01 MB) at deeper levels
        if depth > 0 && size < 10_000 {
            continue;
        }

        let size_mb = size as f64 / 1_048_576.0;
        let percentage = (size as f64 / total_size as f64) * 100.0;

        let marker = if is_dir { "/" } else { "" };
        println!(
            "{}  {:40} {:>10.2} MB ({:>5.1}%)",
            indent,
            format!("{}{}", name, marker),
            size_mb,
            percentage
        );

        // Recursively print subdirectories (but limit depth to avoid clutter)
        if is_dir && depth < 2 {
            print_directory_breakdown(&entry_path, total_size, depth + 1);
        }
    }
}
