//! Benchmark for concurrent gets using read_at_timestamp
//!
//! This benchmark measures the concurrent read throughput of the KV engine
//! by spawning multiple threads that perform snapshot reads in parallel.

use proven_common::TransactionId;
use proven_kv::{KvOperation, KvResponse, KvTransactionEngine, Value};
use proven_processor::{AutoBatchEngine, OperationResult};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

fn main() {
    println!("=== Concurrent Get Benchmark ===\n");

    // Benchmark configuration
    const NUM_KEYS: usize = 100_000;
    const NUM_READERS: usize = 8;
    const READS_PER_READER: usize = 100_000;

    // Create KV engine with auto-batch wrapper
    let mut kv_engine = AutoBatchEngine::new(KvTransactionEngine::new());

    // Step 1: Populate the database
    println!("Step 1: Populating database with {} keys...", NUM_KEYS);
    let populate_start = Instant::now();

    for i in 0..NUM_KEYS {
        let txn_id = TransactionId::new();
        kv_engine.begin(txn_id);

        // Create put operation with various value types
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
            key: format!("key_{:08}", i),
            value,
        };

        match kv_engine.apply_operation(put, txn_id) {
            OperationResult::Complete(_) => {
                kv_engine.commit(txn_id);
            }
            _ => {
                eprintln!("Error at put {}", i);
                return;
            }
        }

        if (i + 1) % 10_000 == 0 {
            eprint!(".");
        }
    }

    let populate_duration = populate_start.elapsed();
    println!(
        "\n✓ Database populated in {:.2} seconds\n",
        populate_duration.as_secs_f64()
    );

    // Get a timestamp for snapshot reads
    let read_timestamp = TransactionId::new();

    // Step 2: Wrap engine in Arc for shared access across threads
    let engine = Arc::new(kv_engine);

    // Barrier to synchronize thread starts
    let barrier = Arc::new(Barrier::new(NUM_READERS));

    println!(
        "Step 2: Starting {} concurrent readers ({} reads each)...",
        NUM_READERS, READS_PER_READER
    );
    println!("Total reads: {}\n", NUM_READERS * READS_PER_READER);

    // Spawn reader threads
    let mut handles = vec![];

    for thread_id in 0..NUM_READERS {
        let engine_clone = Arc::clone(&engine);
        let barrier_clone = Arc::clone(&barrier);

        let handle = thread::spawn(move || {
            // Wait for all threads to be ready
            barrier_clone.wait();

            let start = Instant::now();
            let mut successful_reads = 0;
            let mut failed_reads = 0;

            // Perform reads
            for i in 0..READS_PER_READER {
                // Read keys in a pattern to ensure some overlap between threads
                let key_index = (thread_id * 1000 + i) % NUM_KEYS;
                let get = KvOperation::Get {
                    key: format!("key_{:08}", key_index),
                };

                match engine_clone.read_at_timestamp(get, read_timestamp) {
                    KvResponse::GetResult { .. } => {
                        successful_reads += 1;
                    }
                    _ => {
                        failed_reads += 1;
                    }
                }
            }

            let duration = start.elapsed();

            (thread_id, successful_reads, failed_reads, duration)
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
            "  Thread {}: {} reads in {:.2}s ({:.0} reads/sec)",
            thread_id,
            successful,
            duration.as_secs_f64(),
            throughput
        );
    }

    let total_duration = benchmark_start.elapsed();

    // Calculate statistics
    let total_reads = total_successful + total_failed;
    let overall_throughput = total_successful as f64 / total_duration.as_secs_f64();
    let avg_latency_ms = (total_duration.as_secs_f64() * 1000.0) / total_reads as f64;

    // Print final statistics
    println!("\n=== Benchmark Results ===");
    println!("Concurrent readers:    {}", NUM_READERS);
    println!("Reads per reader:      {}", READS_PER_READER);
    println!("Total reads:           {}", total_reads);
    println!("Successful reads:      {}", total_successful);
    println!("Failed reads:         {}", total_failed);
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
        "  Overall:             {:.0} reads/second",
        overall_throughput
    );
    println!("  Avg latency:         {:.3} ms/read", avg_latency_ms);
    println!("\nConfiguration:");
    println!("  Keys in database:    {}", NUM_KEYS);
    println!("  Read timestamp:      {:?}", read_timestamp);
    println!("  Read method:         read_at_timestamp (concurrent snapshot reads)");
    println!("\n✓ Benchmark complete!");
}
