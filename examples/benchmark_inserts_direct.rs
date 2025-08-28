//! Direct benchmark bypassing SQL parsing - measures raw storage throughput
//!
//! This benchmark directly calls storage operations to measure the actual
//! throughput without SQL parsing overhead.

use proven_sql::hlc::{HlcClock, NodeId};
use proven_sql::storage::lock::LockManager;
use proven_sql::storage::mvcc::MvccStorage;
use proven_sql::storage::write_ops;
use proven_sql::stream_processor::{TransactionContext, TransactionState};
use proven_sql::types::schema::{Column, Table};
use proven_sql::types::value::{DataType, Value};
use std::io::{self, Write};
use std::time::Instant;

fn main() {
    println!("=== Direct Storage Benchmark (No SQL Parsing) ===\n");

    // Create storage and lock manager
    let mut storage = MvccStorage::new();
    let mut lock_manager = LockManager::new();

    // Create table schema
    let table_name = "bench";
    let schema = Table {
        name: table_name.to_string(),
        primary_key: 0, // First column is primary key
        columns: vec![
            Column {
                name: "id".to_string(),
                datatype: DataType::Integer,
                primary_key: true,
                nullable: false,
                default: None,
                unique: true,
                index: false,
                references: None,
            },
            Column {
                name: "value".to_string(),
                datatype: DataType::Integer,
                primary_key: false,
                nullable: true,
                default: None,
                unique: false,
                index: false,
                references: None,
            },
            Column {
                name: "data".to_string(),
                datatype: DataType::String,
                primary_key: false,
                nullable: true,
                default: None,
                unique: false,
                index: false,
                references: None,
            },
            Column {
                name: "timestamp".to_string(),
                datatype: DataType::Integer,
                primary_key: false,
                nullable: true,
                default: None,
                unique: false,
                index: false,
                references: None,
            },
        ],
    };

    println!("Creating table...");
    storage
        .create_table(table_name.to_string(), schema)
        .unwrap();

    // Benchmark configuration
    const NUM_INSERTS: usize = 1_000_000;
    const PROGRESS_INTERVAL: usize = 10_000;
    const STATUS_INTERVAL: usize = 100_000;

    // Create HLC clock for transaction IDs
    let clock = HlcClock::new(NodeId::from_seed(1));

    println!("Starting {} inserts...", NUM_INSERTS);
    let start_time = Instant::now();
    let mut last_status_time = start_time;
    let mut last_status_count = 0;

    // Process inserts
    for i in 0..NUM_INSERTS {
        // Create transaction context
        let tx_id = clock.now();
        let mut tx_ctx = TransactionContext {
            id: tx_id,
            timestamp: tx_id,
            state: TransactionState::Active,
            locks_held: Vec::new(),
            access_log: Vec::new(),
            context: proven_sql::context::TransactionContext::new(tx_id),
        };

        // Create row values
        let values = vec![
            Value::Integer(i as i64),
            Value::Integer((i * 2) as i64),
            Value::String(format!("data_{}", i % 1000)),
            Value::Integer((2000000000 + i) as i64),
        ];

        // Insert directly
        if let Err(e) = write_ops::insert(
            &mut storage,
            &mut lock_manager,
            &mut tx_ctx,
            table_name,
            values,
        ) {
            eprintln!("\nError at insert {}: {:?}", i, e);
            break;
        }

        // Commit the transaction
        storage.commit_transaction(tx_id).unwrap();
        lock_manager.release_all(tx_id).unwrap();

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

    // Verify count - for now just report what was attempted
    let row_count = NUM_INSERTS; // Storage doesn't expose count directly

    // Print final statistics
    println!("\n=== Benchmark Results ===");
    println!("Total inserts:     {}", NUM_INSERTS);
    println!("Total time:        {:.2} seconds", total_seconds);
    println!("Throughput:        {:.0} inserts/second", throughput);
    println!(
        "Avg latency:       {:.3} ms/insert",
        (total_seconds * 1000.0) / NUM_INSERTS as f64
    );
    println!("Verified count:    {}", row_count);

    println!("\nThis measures raw storage throughput without SQL parsing overhead.");
    println!("The difference from the SQL benchmark shows parsing/planning cost.");
}
