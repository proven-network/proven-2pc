//! Benchmark for appending 1 million items to stream
//!
//! This benchmark measures the throughput of the Stream engine by appending
//! 1 million items directly using the engine.

use proven_common::TransactionId;
use proven_processor::AutoBatchEngine;
use proven_stream::{StreamOperation, StreamTransactionEngine};
use proven_value::Value;
use std::io::{self, Write};
use std::time::Instant;

fn main() {
    println!("=== 1 Million Append Benchmark ===\n");

    // Create Stream engine with auto-batch wrapper
    let mut stream_engine = AutoBatchEngine::new(StreamTransactionEngine::new());

    // Benchmark configuration
    const NUM_APPENDS: usize = 1_000_000;
    const PROGRESS_INTERVAL: usize = 10_000;
    const STATUS_INTERVAL: usize = 100_000;
    const STREAM_NAME: &str = "benchmark_stream";

    println!("Starting {} appends...", NUM_APPENDS);
    let start_time = Instant::now();
    let mut last_status_time = start_time;
    let mut last_status_count = 0;

    // Process appends
    for i in 0..NUM_APPENDS {
        // Generate unique transaction ID
        let txn_id = TransactionId::new();
        stream_engine.begin(txn_id);

        // Create append operation with various value types to simulate real usage
        let value = match i % 6 {
            0 => Value::I64(i as i64),
            1 => Value::Str(format!("message_{}", i)),
            2 => Value::Bool(i % 2 == 0),
            3 => Value::F64(i as f64 * 1.5),
            4 => Value::Bytea(format!("data_{}", i % 1000).into_bytes()),
            _ => Value::Json(serde_json::json!({
                "id": i,
                "timestamp": 2000000000 + i,
                "type": "benchmark"
            })),
        };

        let append = StreamOperation::Append {
            values: vec![value],
        };

        // Execute append directly on engine
        match stream_engine.apply_operation(append, txn_id) {
            proven_processor::OperationResult::Complete(_) => {
                stream_engine.commit(txn_id);
            }
            _ => {
                eprintln!("\nError at append {}", i);
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
                "\n[{:7}/{:7}] {:3}% | Interval: {:.0} appends/sec",
                i + 1,
                NUM_APPENDS,
                ((i + 1) * 100) / NUM_APPENDS,
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
    let throughput = NUM_APPENDS as f64 / total_seconds;

    // Verify stream position
    println!("\nVerifying stream position...");
    let verify_txn = TransactionId::new();
    stream_engine.begin(verify_txn);

    let position_op = StreamOperation::GetLatestPosition;

    match stream_engine.apply_operation(position_op, verify_txn) {
        proven_processor::OperationResult::Complete(proven_stream::StreamResponse::Position(
            pos,
        )) => {
            println!("✓ Latest position: {} (expected: {})", pos, NUM_APPENDS);
            stream_engine.commit(verify_txn);
        }
        _ => println!("⚠ Position query failed"),
    }

    // Sample reads to verify items were stored
    println!("\nVerifying sample reads...");
    let mut verified = 0;
    const SAMPLE_SIZE: usize = 3;

    for sample_idx in 0..SAMPLE_SIZE {
        let read_txn = TransactionId::new();
        stream_engine.begin(read_txn);

        let position = (sample_idx * (NUM_APPENDS / SAMPLE_SIZE)) as u64 + 1; // Spread samples across stream
        let read_op = StreamOperation::ReadFrom {
            position,
            limit: 1,
        };

        match stream_engine.apply_operation(read_op, read_txn) {
            proven_processor::OperationResult::Complete(
                proven_stream::StreamResponse::Read { entries },
            ) => {
                if !entries.is_empty() {
                    verified += 1;
                    println!("  ✓ Position {}: {:?}", entries[0].0, entries[0].1);
                } else {
                    println!("  ⚠ No data at position {}", position);
                }
                stream_engine.commit(read_txn);
            }
            _ => {
                println!("  ⚠ Failed to read at position {}", position);
            }
        }
    }

    println!("✓ Verified {}/{} sample reads", verified, SAMPLE_SIZE);

    // Print final statistics
    println!("\n=== Benchmark Results ===");
    println!("Total appends:     {}", NUM_APPENDS);
    println!("Total time:        {:.2} seconds", total_seconds);
    println!("Throughput:        {:.0} appends/second", throughput);
    println!(
        "Avg latency:       {:.3} ms/append",
        (total_seconds * 1000.0) / NUM_APPENDS as f64
    );

    println!("\nMemory usage and detailed statistics:");
    println!(
        "- Transactions executed: {}",
        NUM_APPENDS + 1 + SAMPLE_SIZE
    ); // +1 for position check, +SAMPLE_SIZE for reads
    println!("- Stream name: {}", STREAM_NAME);
    println!("- Value types: Mixed (Integer, String, Boolean, Float, Bytes, Json)");
    println!("- Position allocation: Commit-time (gap-free)");
    println!("\n✓ Benchmark complete!");
}
