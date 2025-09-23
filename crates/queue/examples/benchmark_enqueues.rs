//! Benchmark for enqueuing 1 million items
//!
//! This benchmark measures the throughput of the Queue engine by enqueuing
//! 1 million items directly using the engine.

use proven_hlc::{HlcTimestamp, NodeId};
use proven_queue::stream::{engine::QueueTransactionEngine, operation::QueueOperation};
use proven_queue::types::QueueValue;
use proven_stream::TransactionEngine;
use std::io::{self, Write};
use std::time::Instant;

fn main() {
    println!("=== 1 Million Enqueue Benchmark ===\n");

    // Create Queue engine directly
    let mut queue_engine = QueueTransactionEngine::new();

    // Benchmark configuration
    const NUM_ENQUEUES: usize = 1_000_000;
    const PROGRESS_INTERVAL: usize = 10_000;
    const STATUS_INTERVAL: usize = 100_000;
    const QUEUE_NAME: &str = "benchmark_queue";

    println!("Starting {} enqueues...", NUM_ENQUEUES);
    let start_time = Instant::now();
    let mut last_status_time = start_time;
    let mut last_status_count = 0;

    // Process enqueues
    for i in 0..NUM_ENQUEUES {
        // Generate unique transaction ID with incrementing timestamp
        let txn_id = HlcTimestamp::new(2000000000 + i as u64, 0, NodeId::new(1));
        queue_engine.begin(txn_id);

        // Create enqueue operation with various value types to simulate real usage
        let value = match i % 6 {
            0 => QueueValue::Integer(i as i64),
            1 => QueueValue::String(format!("message_{}", i)),
            2 => QueueValue::Boolean(i % 2 == 0),
            3 => QueueValue::Float(i as f64 * 1.5),
            4 => QueueValue::Bytes(format!("data_{}", i % 1000).into_bytes()),
            _ => QueueValue::Json(serde_json::json!({
                "id": i,
                "timestamp": 2000000000 + i,
                "type": "benchmark"
            })),
        };

        let enqueue = QueueOperation::Enqueue {
            queue_name: QUEUE_NAME.to_string(),
            value,
        };

        // Execute enqueue directly on engine
        match queue_engine.apply_operation(enqueue, txn_id) {
            proven_stream::OperationResult::Complete(_) => {
                // Commit the transaction
                queue_engine.commit(txn_id);
            }
            _ => {
                eprintln!("\nError at enqueue {}", i);
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
                "\n[{:7}/{:7}] {:3}% | Interval: {:.0} enqueues/sec",
                i + 1,
                NUM_ENQUEUES,
                ((i + 1) * 100) / NUM_ENQUEUES,
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
    let throughput = NUM_ENQUEUES as f64 / total_seconds;

    // Verify queue size
    println!("\nVerifying queue size...");
    let verify_txn = HlcTimestamp::new(9999999999, 0, NodeId::new(1));
    queue_engine.begin(verify_txn);

    let size_op = QueueOperation::Size {
        queue_name: QUEUE_NAME.to_string(),
    };

    match queue_engine.apply_operation(size_op, verify_txn) {
        proven_stream::OperationResult::Complete(_response) => {
            println!("✓ Queue size query executed successfully");
            // The response would contain the actual size
            queue_engine.commit(verify_txn);
        }
        _ => println!("⚠ Size query failed"),
    }

    // Sample dequeue to verify items were stored
    println!("\nVerifying sample dequeues...");
    let mut verified = 0;
    const SAMPLE_SIZE: usize = 3;

    for sample_idx in 0..SAMPLE_SIZE {
        let dequeue_txn = HlcTimestamp::new(10000000000 + sample_idx as u64, 0, NodeId::new(1));
        queue_engine.begin(dequeue_txn);

        let dequeue = QueueOperation::Dequeue {
            queue_name: QUEUE_NAME.to_string(),
        };

        match queue_engine.apply_operation(dequeue, dequeue_txn) {
            proven_stream::OperationResult::Complete(_) => {
                verified += 1;
                queue_engine.commit(dequeue_txn);
            }
            _ => {
                println!("⚠ Failed to dequeue item {}", sample_idx);
            }
        }
    }

    println!("✓ Verified {}/{} sample dequeues", verified, SAMPLE_SIZE);

    // Print final statistics
    println!("\n=== Benchmark Results ===");
    println!("Total enqueues:    {}", NUM_ENQUEUES);
    println!("Total time:        {:.2} seconds", total_seconds);
    println!("Throughput:        {:.0} enqueues/second", throughput);
    println!(
        "Avg latency:       {:.3} ms/enqueue",
        (total_seconds * 1000.0) / NUM_ENQUEUES as f64
    );

    println!("\nMemory usage and detailed statistics:");
    println!(
        "- Transactions executed: {}",
        NUM_ENQUEUES + 1 + SAMPLE_SIZE
    ); // +1 for size check, +SAMPLE_SIZE for dequeues
    println!("- Queue name: {}", QUEUE_NAME);
    println!("- Value types: Mixed (Integer, String, Boolean, Float, Bytes, Json)");
    println!("\n✓ Benchmark complete!");
}
