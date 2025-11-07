//! Benchmark for getting 1 million key-value pairs
//!
//! This benchmark measures the throughput of the KV engine by reading
//! 1 million key-value pairs using read_at_timestamp.

use proven_common::TransactionId;
use proven_kv::{KvOperation, KvTransactionEngine, Value};
use proven_stream::{AutoBatchEngine, OperationResult};
use std::io::{self, Write};
use std::time::Instant;

fn main() {
    println!("=== 1 Million Get Benchmark ===\n");

    // Create KV engine with auto-batch wrapper
    let mut kv_engine = AutoBatchEngine::new(KvTransactionEngine::new());

    // Benchmark configuration
    const NUM_KEYS: usize = 1_000_000;
    const PROGRESS_INTERVAL: usize = 10_000;
    const STATUS_INTERVAL: usize = 100_000;

    // Step 1: Populate the database
    println!("Step 1: Populating database with {} keys...", NUM_KEYS);
    let populate_start = Instant::now();

    for i in 0..NUM_KEYS {
        let txn_id = TransactionId::new();
        kv_engine.begin(txn_id);

        // Create put operation with various value types to simulate real usage
        let value = match i % 5 {
            0 => Value::I64(i as i64),
            1 => Value::Str(format!("value_{}", i)),
            2 => Value::Bool(i % 2 == 0),
            3 => Value::Bytea(format!("data_{}", i % 1000).into_bytes()),
            _ => Value::List(vec![
                Value::I64(i as i64),
                Value::Str(format!("item_{}", i % 100)),
            ]),
        };

        let put = KvOperation::Put {
            key: format!("key_{:08}", i), // Zero-padded keys for consistent ordering
            value,
        };

        match kv_engine.apply_operation(put, txn_id) {
            OperationResult::Complete(_) => {
                kv_engine.commit(txn_id);
            }
            _ => {
                eprintln!("\nError at put {}", i);
                return;
            }
        }

        if (i + 1) % PROGRESS_INTERVAL == 0 {
            eprint!(".");
            io::stderr().flush().unwrap();
        }
    }

    eprintln!(); // New line after progress dots
    let populate_duration = populate_start.elapsed();
    println!(
        "✓ Database populated in {:.2} seconds\n",
        populate_duration.as_secs_f64()
    );

    // Get a timestamp for snapshot reads
    let read_timestamp = TransactionId::new();

    // Step 2: Benchmark gets using read_at_timestamp
    println!(
        "Step 2: Starting {} gets using read_at_timestamp...",
        NUM_KEYS
    );
    let start_time = Instant::now();
    let mut last_status_time = start_time;
    let mut last_status_count = 0;
    let mut successful_gets = 0;

    // Process gets
    for i in 0..NUM_KEYS {
        let get = KvOperation::Get {
            key: format!("key_{:08}", i),
        };

        // Execute get using read_at_timestamp
        match kv_engine.read_at_timestamp(get, read_timestamp) {
            OperationResult::Complete(_) => {
                successful_gets += 1;
            }
            _ => {
                eprintln!("\nError at get {}", i);
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
                "\n[{:7}/{:7}] {:3}% | Interval: {:.0} gets/sec",
                i + 1,
                NUM_KEYS,
                ((i + 1) * 100) / NUM_KEYS,
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
    let throughput = successful_gets as f64 / total_seconds;

    // Print final statistics
    println!("\n=== Benchmark Results ===");
    println!("Total gets:        {}", NUM_KEYS);
    println!("Successful gets:   {}", successful_gets);
    println!("Total time:        {:.2} seconds", total_seconds);
    println!("Throughput:        {:.0} gets/second", throughput);
    println!(
        "Avg latency:       {:.3} ms/get",
        (total_seconds * 1000.0) / NUM_KEYS as f64
    );

    println!("\nBenchmark details:");
    println!("- Read method: read_at_timestamp (snapshot reads)");
    println!("- Read timestamp: {:?}", read_timestamp);
    println!("- Key pattern: key_00000000 to key_{:08}", NUM_KEYS - 1);
    println!("- Value types: Mixed (Integer, String, Boolean, Bytes, List)");
    println!("\n✓ Benchmark complete!");
}
