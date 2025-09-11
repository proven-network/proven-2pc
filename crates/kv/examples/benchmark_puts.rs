//! Benchmark for putting 1 million key-value pairs
//!
//! This benchmark measures the throughput of the KV engine by inserting
//! 1 million key-value pairs directly using the engine.

use proven_hlc::{HlcTimestamp, NodeId};
use proven_kv::stream::{engine::KvTransactionEngine, operation::KvOperation};
use proven_kv::types::Value;
use proven_stream::TransactionEngine;
use std::io::{self, Write};
use std::time::Instant;

fn main() {
    println!("=== 1 Million Put Benchmark ===\n");

    // Create KV engine directly
    let mut kv_engine = KvTransactionEngine::new();

    // Benchmark configuration
    const NUM_PUTS: usize = 1_000_000;
    const PROGRESS_INTERVAL: usize = 10_000;
    const STATUS_INTERVAL: usize = 100_000;

    println!("Starting {} puts...", NUM_PUTS);
    let start_time = Instant::now();
    let mut last_status_time = start_time;
    let mut last_status_count = 0;

    // Process puts
    for i in 0..NUM_PUTS {
        // Generate unique transaction ID with incrementing timestamp
        let txn_id = HlcTimestamp::new(2000000000 + i as u64, 0, NodeId::new(1));
        kv_engine.begin_transaction(txn_id);

        // Create put operation with various value types to simulate real usage
        let value = match i % 5 {
            0 => Value::Integer(i as i64),
            1 => Value::String(format!("value_{}", i)),
            2 => Value::Boolean(i % 2 == 0),
            3 => Value::Bytes(format!("data_{}", i % 1000).into_bytes()),
            _ => Value::List(vec![
                Value::Integer(i as i64),
                Value::String(format!("item_{}", i % 100)),
            ]),
        };

        let put = KvOperation::Put {
            key: format!("key_{:08}", i), // Zero-padded keys for consistent ordering
            value,
        };

        // Execute put directly on engine
        match kv_engine.apply_operation(put, txn_id) {
            proven_stream::OperationResult::Success(_) => {
                // Commit the transaction
                if let Err(e) = kv_engine.commit(txn_id) {
                    eprintln!("\nError committing put {}: {}", i, e);
                    break;
                }
            }
            _ => {
                eprintln!("\nError at put {}", i);
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
                "\n[{:7}/{:7}] {:3}% | Interval: {:.0} puts/sec",
                i + 1,
                NUM_PUTS,
                ((i + 1) * 100) / NUM_PUTS,
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
    let throughput = NUM_PUTS as f64 / total_seconds;

    // Verify a sample of keys
    println!("\nVerifying sample keys...");
    let verify_txn = HlcTimestamp::new(9999999999, 0, NodeId::new(1));
    kv_engine.begin_transaction(verify_txn);

    // Check a few keys to verify they were stored
    let sample_keys = [0, NUM_PUTS / 2, NUM_PUTS - 1];
    let mut verified = 0;

    for key_index in sample_keys {
        let get = KvOperation::Get {
            key: format!("key_{:08}", key_index),
        };

        match kv_engine.apply_operation(get, verify_txn) {
            proven_stream::OperationResult::Success(_) => {
                verified += 1;
            }
            _ => {
                println!("⚠ Failed to get key_{:08}", key_index);
            }
        }
    }

    kv_engine
        .commit(verify_txn)
        .expect("Failed to commit verification transaction");

    println!("✓ Verified {}/{} sample keys", verified, sample_keys.len());

    // Print final statistics
    println!("\n=== Benchmark Results ===");
    println!("Total puts:        {}", NUM_PUTS);
    println!("Total time:        {:.2} seconds", total_seconds);
    println!("Throughput:        {:.0} puts/second", throughput);
    println!(
        "Avg latency:       {:.3} ms/put",
        (total_seconds * 1000.0) / NUM_PUTS as f64
    );

    println!("\nMemory usage and detailed statistics:");
    println!("- Transactions executed: {}", NUM_PUTS + 1); // +1 for verification
    println!("- Key pattern: key_00000000 to key_{:08}", NUM_PUTS - 1);
    println!("- Value types: Mixed (Integer, String, Boolean, Bytes, List)");
    println!("\n✓ Benchmark complete!");
}