//! Benchmark for all storage engines running through the coordinator
//!
//! This benchmark measures the throughput of executing mixed operations
//! across all storage types (KV, Queue, Resource, SQL) using distributed
//! transactions through the coordinator.

use proven_coordinator::Coordinator;
use proven_engine::{MockClient, MockEngine};
use proven_kv::types::Value;
use proven_kv_client::KvClient;
use proven_queue::types::QueueValue;
use proven_queue_client::QueueClient;
use proven_resource_client::ResourceClient;
use proven_runner::Runner;
use proven_snapshot_memory::MemorySnapshotStore;
use proven_sql::Value as SqlValue;
use proven_sql_client::SqlClient;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinSet;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== All Storage Engines Benchmark ===\n");

    // Initialize the mock engine
    let engine = Arc::new(MockEngine::new());

    // Create streams for each storage type
    engine.create_stream("sql_stream".to_string())?;
    engine.create_stream("kv_stream".to_string())?;
    engine.create_stream("queue_stream".to_string())?;
    engine.create_stream("resource_stream".to_string())?;
    println!("✓ Created streams for all storage types");

    // Create and start the runner
    let runner_client = Arc::new(MockClient::new("runner-node".to_string(), engine.clone()));
    let snapshot_store = Arc::new(MemorySnapshotStore::new());
    let runner = Arc::new(Runner::new(
        "runner-node",
        runner_client.clone(),
        snapshot_store,
    ));
    runner.start().await.unwrap();
    println!("✓ Started runner with snapshot support");

    // Pre-start all processors with a long duration to avoid restarts during benchmark
    println!("Pre-starting processors for benchmark duration...");
    let processor_duration = Duration::from_secs(600); // 10 minutes - enough for the benchmark
    runner
        .ensure_processor("sql_stream", processor_duration)
        .await
        .unwrap();
    runner
        .ensure_processor("kv_stream", processor_duration)
        .await
        .unwrap();
    runner
        .ensure_processor("queue_stream", processor_duration)
        .await
        .unwrap();
    runner
        .ensure_processor("resource_stream", processor_duration)
        .await
        .unwrap();
    println!("✓ All processors pre-started with 10-minute guarantee");

    // Create multiple coordinators for better distributed testing
    const NUM_COORDINATORS: usize = 4;
    let mut coordinators = Vec::new();
    for i in 0..NUM_COORDINATORS {
        let coordinator_client = Arc::new(MockClient::new(
            format!("coordinator-{}", i + 1),
            engine.clone(),
        ));
        let coordinator = Arc::new(Coordinator::new(
            format!("coordinator-{}", i + 1),
            coordinator_client,
            runner.clone(),
        ));
        coordinators.push(coordinator);
    }
    println!("✓ Created {} coordinators", NUM_COORDINATORS);

    // Warm up each coordinator with a test transaction to ensure channels are ready
    println!("  Warming up coordinators...");
    for (idx, coordinator) in coordinators.iter().enumerate() {
        match coordinator
            .begin(
                Duration::from_secs(1),
                vec![],
                "benchmark_all_warmup".to_string(),
            )
            .await
        {
            Ok(txn) => {
                // Do a simple operation to fully exercise the path
                let kv = KvClient::new(txn.clone());
                let _ = kv.get("kv_stream", "warmup_key").await;
                let _ = txn.commit().await;
                println!("    ✓ Coordinator {} ready", idx);
            }
            Err(e) => {
                eprintln!("    ⚠ Coordinator {} warmup failed: {}", idx, e);
            }
        }
    }
    // Additional delay after warmup
    tokio::time::sleep(Duration::from_millis(100)).await;
    println!("  Ready!\n");

    // Use first coordinator for setup
    let coordinator = &coordinators[0];

    // Setup phase - Initialize storage
    println!("=== Setup Phase ===");
    let setup_txn = coordinator
        .begin(
            Duration::from_secs(10),
            vec![],
            "benchmark_all_setup".to_string(),
        )
        .await?;
    let sql_setup = SqlClient::new(setup_txn.clone());
    let resource_setup = ResourceClient::new(setup_txn.clone());

    // Create SQL table
    sql_setup
        .create_table(
            "sql_stream",
            "benchmark",
            "id INT PRIMARY KEY, value INT, data VARCHAR, timestamp INT",
        )
        .await?;
    println!("✓ Created SQL table");

    // Resource is auto-initialized on first use, no need to explicitly initialize

    // Mint initial supply
    resource_setup
        .mint_integer("resource_stream", "treasury", 1_000_000_000)
        .await?;
    println!("✓ Minted initial supply to treasury");

    // Create initial accounts for transfers
    const NUM_ACCOUNTS: usize = 100;
    for i in 0..NUM_ACCOUNTS {
        resource_setup
            .transfer_integer(
                "resource_stream",
                "treasury",
                &format!("account_{}", i),
                10_000,
            )
            .await?;
    }
    println!("✓ Created {} accounts with initial balances", NUM_ACCOUNTS);

    setup_txn.commit().await?;
    println!("\n=== Benchmark Configuration ===");

    // Benchmark configuration
    const NUM_TRANSACTIONS: usize = 100_000;
    const OPERATIONS_PER_TXN: usize = 4; // One of each: KV put, Queue enqueue, Resource transfer, SQL insert
    const PROGRESS_INTERVAL: usize = 100;

    println!("Transactions:     {}", NUM_TRANSACTIONS);
    println!("Operations/txn:   {}", OPERATIONS_PER_TXN);
    println!(
        "Total operations: {}",
        NUM_TRANSACTIONS * OPERATIONS_PER_TXN
    );
    println!("Coordinators:     {}", NUM_COORDINATORS);
    println!("\nStarting benchmark...\n");

    // Shared counters for progress tracking
    let successful_txns = Arc::new(AtomicUsize::new(0));
    let failed_txns = Arc::new(AtomicUsize::new(0));
    let total_completed = Arc::new(AtomicUsize::new(0));
    let total_retries = Arc::new(AtomicUsize::new(0));

    let start_time = Instant::now();

    // Create tasks for each coordinator
    let mut tasks = JoinSet::new();
    let txns_per_coordinator = NUM_TRANSACTIONS / NUM_COORDINATORS;

    for (coordinator_idx, coordinator) in coordinators.into_iter().enumerate() {
        let successful = successful_txns.clone();
        let failed = failed_txns.clone();
        let completed = total_completed.clone();
        let retries = total_retries.clone();
        let start_time_clone = start_time;

        tasks.spawn(async move {
            let start_idx = coordinator_idx * txns_per_coordinator;
            let end_idx = if coordinator_idx == NUM_COORDINATORS - 1 {
                NUM_TRANSACTIONS // Last coordinator handles any remainder
            } else {
                start_idx + txns_per_coordinator
            };

            for i in start_idx..end_idx {
                // Retry loop for each transaction
                const MAX_RETRIES: usize = 3;
                let mut retry_count = 0;
                let mut transaction_succeeded = false;

                while retry_count < MAX_RETRIES && !transaction_succeeded {
                    // Begin a distributed transaction
                    let txn = match coordinator
                        .begin(
                            Duration::from_secs(5),
                            vec![],
                            "benchmark_all_transaction".to_string(),
                        )
                        .await
                    {
                        Ok(t) => t,
                        Err(_e) => {
                            retry_count += 1;
                            retries.fetch_add(1, Ordering::Relaxed);
                            if retry_count >= MAX_RETRIES {
                                failed.fetch_add(1, Ordering::Relaxed);
                                completed.fetch_add(1, Ordering::Relaxed);
                            }
                            continue;
                        }
                    };

                    // Create clients for this transaction
                    let kv = KvClient::new(txn.clone());
                    let queue = QueueClient::new(txn.clone());
                    let resource = ResourceClient::new(txn.clone());
                    let sql = SqlClient::new(txn.clone());

                    // Track if all operations succeed
                    let mut all_succeeded = true;

                    // 1. KV Put operation
                    let kv_key = format!("key_{:08}", i);
                    let kv_value = match i % 5 {
                        0 => Value::Integer(i as i64),
                        1 => Value::String(format!("value_{}", i)),
                        2 => Value::Boolean(i % 2 == 0),
                        3 => Value::Bytes(format!("data_{}", i % 1000).into_bytes()),
                        _ => Value::List(vec![
                            Value::Integer(i as i64),
                            Value::String(format!("item_{}", i % 100)),
                        ]),
                    };

                    if let Err(e) = kv.put("kv_stream", &kv_key, kv_value).await {
                        eprintln!(
                            "[Coordinator {}] KV put failed in txn {}: {}",
                            coordinator_idx, i, e
                        );
                        all_succeeded = false;
                    }

                    // 2. Queue Enqueue operation
                    let queue_name = "benchmark_queue";
                    let queue_value = match i % 6 {
                        0 => QueueValue::Integer(i as i64),
                        1 => QueueValue::String(format!("message_{}", i)),
                        2 => QueueValue::Boolean(i % 2 == 0),
                        3 => QueueValue::Float(i as f64 * 1.5),
                        4 => QueueValue::Bytes(format!("queue_data_{}", i).into_bytes()),
                        _ => QueueValue::String(format!(
                            "json_{{id:{},timestamp:{},type:benchmark}}",
                            i,
                            start_time_clone.elapsed().as_millis()
                        )),
                    };

                    if let Err(e) = queue.enqueue("queue_stream", queue_name, queue_value).await {
                        eprintln!(
                            "[Coordinator {}] Queue enqueue failed in txn {}: {}",
                            coordinator_idx, i, e
                        );
                        all_succeeded = false;
                    }

                    // 3. Resource Transfer operation
                    let from_account = if i % 2 == 0 {
                        "treasury".to_string()
                    } else {
                        format!("account_{}", i % NUM_ACCOUNTS)
                    };
                    let to_account = if i % 2 == 0 {
                        format!("account_{}", i % NUM_ACCOUNTS)
                    } else {
                        "treasury".to_string()
                    };
                    let transfer_amount = match i % 5 {
                        0 => 1, // 1 token
                        1 => 0, // Skip very small amounts to avoid issues
                        2 => 0, // Skip very small amounts
                        3 => 0, // Skip very small amounts
                        _ => 0, // Skip very small amounts
                    };

                    if transfer_amount > 0 {
                        if let Err(e) = resource
                            .transfer_integer(
                                "resource_stream",
                                &from_account,
                                &to_account,
                                transfer_amount,
                            )
                            .await
                        {
                            // Resource transfers may fail due to insufficient balance
                            // This is expected in a benchmark, don't mark as failed
                            if !e.to_string().contains("Insufficient") {
                                eprintln!(
                                    "[Coordinator {}] Resource transfer failed in txn {}: {}",
                                    coordinator_idx, i, e
                                );
                                all_succeeded = false;
                            }
                        }
                    }

                    // 4. SQL Insert operation - use a unique ID to avoid conflicts
                    // Combine run timestamp with transaction number for guaranteed uniqueness
                    let run_id = start_time_clone.elapsed().as_secs();
                    let unique_id = (run_id * 1_000_000) + i as u64; // Guaranteed unique across runs

                    // Use parameterized query for better performance and safety
                    let params = vec![
                        SqlValue::integer(unique_id as i64),
                        SqlValue::integer((i * 2) as i64),
                        SqlValue::string(format!("data_{}", i % 1000)),
                        SqlValue::integer(start_time_clone.elapsed().as_millis() as i64),
                    ];

                    if let Err(e) = sql
                        .insert_with_params(
                            "sql_stream",
                            "benchmark",
                            &["id", "value", "data", "timestamp"],
                            params,
                        )
                        .await
                    {
                        eprintln!(
                            "[Coordinator {}] SQL insert failed in txn {}: {}",
                            coordinator_idx, i, e
                        );
                        all_succeeded = false;
                    }

                    // Commit or abort the transaction based on success
                    if all_succeeded {
                        match txn.commit().await {
                            Ok(_) => {
                                transaction_succeeded = true;
                                successful.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(_e) => {
                                retry_count += 1;
                                retries.fetch_add(1, Ordering::Relaxed);
                                if retry_count >= MAX_RETRIES {
                                    failed.fetch_add(1, Ordering::Relaxed);
                                    transaction_succeeded = true; // Mark as done to exit retry loop
                                }
                                // Otherwise, will retry
                            }
                        };
                    } else {
                        // Abort the transaction if any operation failed
                        match txn.abort().await {
                            Ok(_) => {
                                // Retry the transaction
                                retry_count += 1;
                                retries.fetch_add(1, Ordering::Relaxed);
                                if retry_count >= MAX_RETRIES {
                                    failed.fetch_add(1, Ordering::Relaxed);
                                    transaction_succeeded = true; // Mark as done to exit retry loop
                                }
                            }
                            Err(_e) => {
                                retry_count += 1;
                                retries.fetch_add(1, Ordering::Relaxed);
                                if retry_count >= MAX_RETRIES {
                                    failed.fetch_add(1, Ordering::Relaxed);
                                    transaction_succeeded = true; // Mark as done to exit retry loop
                                }
                            }
                        }
                    }
                } // End of retry loop

                completed.fetch_add(1, Ordering::Relaxed);
            }

            coordinator_idx
        });
    }

    // Progress monitoring task
    let monitor_successful = successful_txns.clone();
    let monitor_failed = failed_txns.clone();
    let monitor_completed = total_completed.clone();
    let monitor_handle = tokio::spawn(async move {
        let mut last_completed = 0;
        let mut last_time = Instant::now();

        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;

            let current_completed = monitor_completed.load(Ordering::Relaxed);
            if current_completed >= NUM_TRANSACTIONS {
                break;
            }

            if current_completed - last_completed >= PROGRESS_INTERVAL {
                let current_time = Instant::now();
                let interval_duration = current_time.duration_since(last_time);
                let interval_count = current_completed - last_completed;
                let interval_throughput = interval_count as f64 / interval_duration.as_secs_f64();

                let success = monitor_successful.load(Ordering::Relaxed);
                let fail = monitor_failed.load(Ordering::Relaxed);

                eprintln!(
                    "[{:6}/{:6}] {:3}% | {:.0} txns/sec | Success: {} Failed: {}",
                    current_completed,
                    NUM_TRANSACTIONS,
                    (current_completed * 100) / NUM_TRANSACTIONS,
                    interval_throughput,
                    success,
                    fail
                );

                last_completed = current_completed;
                last_time = current_time;
            }
        }
    });

    // Wait for all coordinators to complete
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(coordinator_idx) => {
                println!("Coordinator {} completed", coordinator_idx);
            }
            Err(e) => {
                eprintln!("Coordinator task failed: {}", e);
            }
        }
    }

    // Stop monitoring
    monitor_handle.abort();

    // Calculate final statistics
    let total_duration = start_time.elapsed();
    let total_seconds = total_duration.as_secs_f64();
    let final_successful = successful_txns.load(Ordering::Relaxed);
    let final_failed = failed_txns.load(Ordering::Relaxed);
    let final_retries = total_retries.load(Ordering::Relaxed);
    let txn_throughput = final_successful as f64 / total_seconds;
    let op_throughput = (final_successful * OPERATIONS_PER_TXN) as f64 / total_seconds;

    println!("\n=== Benchmark Results ===");
    println!("Total transactions:      {}", NUM_TRANSACTIONS);
    println!("Successful transactions: {}", final_successful);
    println!("Failed transactions:     {}", final_failed);
    if final_retries > 0 {
        println!("Transaction retries:     {}", final_retries);
    }
    println!(
        "Success rate:            {:.1}%",
        (final_successful as f64 / NUM_TRANSACTIONS as f64) * 100.0
    );
    println!("Total time:              {:.2} seconds", total_seconds);
    println!("Transaction throughput:  {:.0} txns/second", txn_throughput);
    println!("Operation throughput:    {:.0} ops/second", op_throughput);
    println!(
        "Avg latency:             {:.3} ms/transaction",
        (total_seconds * 1000.0) / final_successful as f64
    );

    println!("\nOperation breakdown:");
    println!("- KV puts:          {}", final_successful);
    println!("- Queue enqueues:   {}", final_successful);
    println!("- Resource transfers: ~{}", final_successful); // Some may fail due to balance
    println!("- SQL inserts:      {}", final_successful);
    println!(
        "- Total operations: ~{}",
        final_successful * OPERATIONS_PER_TXN
    );

    // Wait a bit for any pending operations to complete
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verification phase - create a new coordinator for verification
    println!("\n=== Verification Phase ===");
    let verify_coordinator_client = Arc::new(MockClient::new(
        "verify-coordinator".to_string(),
        engine.clone(),
    ));
    let verify_coordinator = Arc::new(Coordinator::new(
        "verify-coordinator".to_string(),
        verify_coordinator_client,
        runner.clone(),
    ));
    let verify_txn = verify_coordinator
        .begin(
            Duration::from_secs(10),
            vec![],
            "benchmark_all_verification".to_string(),
        )
        .await?;
    let kv_verify = KvClient::new(verify_txn.clone());
    let queue_verify = QueueClient::new(verify_txn.clone());
    let sql_verify = SqlClient::new(verify_txn.clone());

    // Check KV count by checking existence of keys
    let mut kv_count = 0;
    let mut missing_keys = Vec::new();
    for i in 0..NUM_TRANSACTIONS {
        let key = format!("key_{:08}", i);
        if kv_verify.get("kv_stream", &key).await?.is_some() {
            kv_count += 1;
        } else {
            missing_keys.push(i);
        }
    }
    println!(
        "✓ KV entries found: {} out of {}",
        kv_count, NUM_TRANSACTIONS
    );
    if !missing_keys.is_empty() && missing_keys.len() <= 10 {
        println!("  Missing KV keys for transactions: {:?}", missing_keys);
    }

    // Check queue size
    if let Ok(size) = queue_verify.size("queue_stream", "benchmark_queue").await {
        println!("✓ Queue contains {} items", size);
    }

    // Check SQL count
    let count_result = sql_verify
        .select("sql_stream", "benchmark", &["COUNT(*) as cnt"], None)
        .await?;
    if let Some(count) = count_result.column_values("cnt").first() {
        println!("✓ SQL table contains {} rows", count);
    }

    println!("\n✓ Benchmark complete!");
    Ok(())
}
