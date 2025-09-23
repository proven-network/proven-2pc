//! Benchmark for SQL inserts through the coordinator
//!
//! This benchmark measures the throughput of SQL inserts using distributed
//! transactions through the coordinator with parallel execution.

use proven_coordinator::{Coordinator, Transaction};
use proven_engine::{MockClient, MockEngine};
use proven_runner::Runner;
use proven_snapshot_memory::MemorySnapshotStore;
use proven_sql::Value as SqlValue;
use proven_sql_client::SqlClient;
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinSet;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("=== Coordinated SQL Insert Benchmark ===\n");

    // Initialize the mock engine
    let engine = Arc::new(MockEngine::new());

    // Create SQL stream
    engine.create_stream("sql_stream".to_string())?;
    println!("✓ Created SQL stream");

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

    // Pre-start SQL processor
    let processor_duration = Duration::from_secs(600);
    runner
        .ensure_processor("sql_stream", processor_duration)
        .await
        .unwrap();
    println!("✓ SQL processor pre-started");

    // Give processor time to initialize
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Create a single coordinator since we're parallelizing transactions
    let coordinator_client = Arc::new(MockClient::new("coordinator".to_string(), engine.clone()));
    let coordinator = Arc::new(Coordinator::new(
        "coordinator".to_string(),
        coordinator_client,
        runner.clone(),
    ));
    println!("✓ Created coordinator");

    // Setup phase - create the table
    println!("\n=== Setup Phase ===");
    let setup_txn = coordinator
        .begin(Duration::from_secs(10), vec![], "setup".to_string())
        .await?;

    let sql_setup = SqlClient::new(setup_txn.clone());

    // Create SQL table with simple schema
    sql_setup
        .create_table(
            "sql_stream",
            "benchmark_table",
            "id VARCHAR PRIMARY KEY, content VARCHAR, timestamp BIGINT",
        )
        .await?;
    println!("✓ Created SQL table");

    setup_txn.commit().await?;

    // Benchmark configuration
    println!("\n=== Benchmark Configuration ===");
    const WARMUP_TRANSACTIONS: usize = 100;
    const NUM_TRANSACTIONS: usize = 100_000;
    const MAX_CONCURRENT: Option<usize> = Some(1_000);

    println!("Warmup transactions:    {}", WARMUP_TRANSACTIONS);
    println!("Benchmark transactions: {}", NUM_TRANSACTIONS);
    println!("Operations per txn:     1 (SQL insert)");
    println!(
        "Parallelism:            1 coordinator, {} max concurrent tasks",
        MAX_CONCURRENT.unwrap_or(NUM_TRANSACTIONS)
    );

    // Warmup phase - establish patterns
    println!("\n=== Warmup Phase (Pattern Learning) ===");

    let warmup_successful = Arc::new(AtomicUsize::new(0));
    let warmup_failed = Arc::new(AtomicUsize::new(0));
    let warmup_start = Instant::now();

    let mut warmup_tasks = JoinSet::new();

    // Spawn all warmup tasks
    for i in 0..WARMUP_TRANSACTIONS {
        let coordinator = coordinator.clone();
        let successful = warmup_successful.clone();
        let failed = warmup_failed.clone();

        warmup_tasks.spawn(async move {
            let args = vec![json!({
                "id": format!("record_{:08}", i),
                "content": format!("Warmup content for record {}", i),
                "timestamp": i as i64
            })];

            let txn = match coordinator
                .begin(
                    Duration::from_secs(5),
                    args.clone(),
                    "sql_insert".to_string(),
                )
                .await
            {
                Ok(t) => t,
                Err(e) => {
                    eprintln!("Failed to begin warmup transaction {}: {:?}", i, e);
                    failed.fetch_add(1, Ordering::Relaxed);
                    return;
                }
            };

            if let Err(e) = execute_sql_insert(i, &args[0], txn.clone()).await {
                eprintln!("Failed to execute warmup transaction {}: {:?}", i, e);
                let _ = txn.abort().await;
                failed.fetch_add(1, Ordering::Relaxed);
            } else {
                successful.fetch_add(1, Ordering::Relaxed);
            }
        });
    }

    // Monitor warmup progress
    let warmup_monitor_successful = warmup_successful.clone();
    let warmup_monitor_failed = warmup_failed.clone();
    let warmup_monitor_handle = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(500)).await;

            let success = warmup_monitor_successful.load(Ordering::Relaxed);
            let fail = warmup_monitor_failed.load(Ordering::Relaxed);
            let total = success + fail;

            if total >= WARMUP_TRANSACTIONS {
                break;
            }

            if total > 0 && total.is_multiple_of(25) {
                println!("  Completed {} warmup transactions", total);
            }
        }
    });

    // Wait for all warmup tasks to complete
    while let Some(result) = warmup_tasks.join_next().await {
        match result {
            Ok(_) => {}
            Err(e) => eprintln!("Warmup task failed: {}", e),
        }
    }

    warmup_monitor_handle.abort();

    let warmup_duration = warmup_start.elapsed();
    let final_warmup_successful = warmup_successful.load(Ordering::Relaxed);
    let final_warmup_failed = warmup_failed.load(Ordering::Relaxed);

    println!(
        "✓ Warmup complete: {} successful, {} failed in {:.2}s",
        final_warmup_successful,
        final_warmup_failed,
        warmup_duration.as_secs_f64()
    );

    // Main benchmark phase
    println!("\n=== Benchmark Phase (With Speculation) ===");

    let successful_txns = Arc::new(AtomicUsize::new(0));
    let failed_txns = Arc::new(AtomicUsize::new(0));
    let total_retries = Arc::new(AtomicUsize::new(0));
    let total_latency_us = Arc::new(AtomicUsize::new(0));
    let start_time = Instant::now();

    let mut tasks = JoinSet::new();

    // Spawn tasks with optional concurrency limit
    for i in 0..NUM_TRANSACTIONS {
        // Wait if we've reached the concurrency limit
        if let Some(max) = MAX_CONCURRENT {
            while tasks.len() >= max {
                // Wait for at least one task to complete
                if let Some(result) = tasks.join_next().await {
                    match result {
                        Ok(_) => {}
                        Err(e) => eprintln!("Task failed: {}", e),
                    }
                }
            }
        }

        let coordinator = coordinator.clone();
        let successful = successful_txns.clone();
        let failed = failed_txns.clone();
        let retries = total_retries.clone();
        let latency_tracker = total_latency_us.clone();

        tasks.spawn(async move {
            let task_start = Instant::now();
            let args = vec![json!({
                "id": format!("record_{:08}", i + WARMUP_TRANSACTIONS),
                "content": format!("Benchmark content for record {}", i),
                "timestamp": (i + WARMUP_TRANSACTIONS) as i64
            })];

            // Retry loop with exponential backoff
            let mut retry_count = 0;
            let mut disable_speculation = false;
            const MAX_RETRIES: u32 = 10;

            loop {
                // Begin with speculation enabled unless we had a speculation failure
                let txn = match if disable_speculation {
                    coordinator
                        .begin_without_speculation(
                            Duration::from_secs(5),
                            args.clone(),
                            "sql_insert".to_string(),
                        )
                        .await
                } else {
                    coordinator
                        .begin(
                            Duration::from_secs(5),
                            args.clone(),
                            "sql_insert".to_string(),
                        )
                        .await
                } {
                    Ok(t) => t,
                    Err(e) => {
                        eprintln!("Failed to begin transaction {}: {:?}", i, e);
                        failed.fetch_add(1, Ordering::Relaxed);
                        return;
                    }
                };

                // Execute the transaction
                let result =
                    execute_sql_insert(i + WARMUP_TRANSACTIONS, &args[0], txn.clone()).await;

                match result {
                    Ok(_) => {
                        successful.fetch_add(1, Ordering::Relaxed);
                        let task_duration = task_start.elapsed();
                        latency_tracker
                            .fetch_add(task_duration.as_micros() as usize, Ordering::Relaxed);
                        return;
                    }
                    Err(e) => {
                        let error_str = e.to_string();

                        // Always abort the failed transaction
                        let _ = txn.abort().await;

                        // Don't retry unique constraint violations - they indicate the data was already inserted
                        if error_str.contains("Unique constraint violation") {
                            // This likely means a previous attempt succeeded but we didn't get confirmation
                            // Count it as successful since the data is there
                            successful.fetch_add(1, Ordering::Relaxed);
                            return;
                        }

                        // Check if it's a wound/abort that we should retry
                        let should_retry = error_str.contains("Transaction was aborted")
                            || error_str.contains("Transaction was wounded")
                            || error_str.contains("Speculation failed")
                            || error_str.contains("Transaction deadline exceeded")
                            || error_str.contains("Response timeout");

                        if should_retry && retry_count < MAX_RETRIES {
                            retry_count += 1;
                            retries.fetch_add(1, Ordering::Relaxed);

                            // Only disable speculation if it was a speculation failure
                            if error_str.contains("Speculation failed") {
                                disable_speculation = true;
                            }

                            // Exponential backoff
                            let backoff_ms = 1u64 << retry_count;
                            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                            continue;
                        }

                        // Max retries exceeded or non-retryable error
                        eprintln!(
                            "Transaction {} failed after {} retries: {}",
                            i, retry_count, error_str
                        );
                        failed.fetch_add(1, Ordering::Relaxed);
                        return;
                    }
                }
            }
        });
    }

    // Progress monitoring
    let monitor_successful = successful_txns.clone();
    let monitor_failed = failed_txns.clone();
    let monitor_handle = tokio::spawn(async move {
        let mut last_count = 0;
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;

            let success = monitor_successful.load(Ordering::Relaxed);
            let fail = monitor_failed.load(Ordering::Relaxed);
            let total = success + fail;

            if total >= NUM_TRANSACTIONS {
                break;
            }

            if total > last_count + 1000 {
                println!(
                    "[{:5}/{:5}] Success: {} Failed: {} ({:.1}%)",
                    total,
                    NUM_TRANSACTIONS,
                    success,
                    fail,
                    (success as f64 / total as f64) * 100.0
                );
                last_count = total;
            }
        }
    });

    // Wait for completion
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(_) => {}
            Err(e) => eprintln!("Task failed: {}", e),
        }
    }

    monitor_handle.abort();

    // Calculate results
    let total_duration = start_time.elapsed();
    let total_seconds = total_duration.as_secs_f64();
    let final_successful = successful_txns.load(Ordering::Relaxed);
    let final_failed = failed_txns.load(Ordering::Relaxed);
    let final_retries = total_retries.load(Ordering::Relaxed);
    let total_latency_micros = total_latency_us.load(Ordering::Relaxed);

    // Calculate actual average latency
    let avg_latency_ms = if final_successful > 0 {
        (total_latency_micros as f64 / final_successful as f64) / 1000.0
    } else {
        0.0
    };

    println!("\n=== Benchmark Results ===");
    println!("Total transactions:      {}", NUM_TRANSACTIONS);
    println!("Successful transactions: {}", final_successful);
    println!("Failed transactions:     {}", final_failed);
    println!("Total retries:           {}", final_retries);
    println!(
        "Success rate:            {:.1}%",
        (final_successful as f64 / NUM_TRANSACTIONS as f64) * 100.0
    );
    println!("Total time:              {:.2} seconds", total_seconds);
    println!(
        "Transaction throughput:  {:.0} txns/second",
        final_successful as f64 / total_seconds
    );
    println!(
        "SQL inserts/second:      {:.0}",
        final_successful as f64 / total_seconds
    );
    println!(
        "Avg latency:             {:.3} ms/transaction (actual)",
        avg_latency_ms
    );
    println!(
        "Amortized time:          {:.3} ms/transaction",
        (total_seconds * 1000.0) / final_successful as f64
    );
    if final_retries > 0 {
        println!(
            "Avg retries per txn:     {:.2}",
            final_retries as f64 / NUM_TRANSACTIONS as f64
        );
    }

    // Verification phase
    println!("\n=== Verification Phase ===");
    let verify_txn = coordinator
        .begin(Duration::from_secs(10), vec![], "verification".to_string())
        .await?;
    let sql_verify = SqlClient::new(verify_txn.clone());

    // Check SQL count
    let count_result = sql_verify
        .select("sql_stream", "benchmark_table", &["COUNT(*) as cnt"], None)
        .await?;
    if let Some(count) = count_result.column_values("cnt").first() {
        println!(
            "✓ SQL table contains {} rows (expected {})",
            count,
            WARMUP_TRANSACTIONS + final_successful
        );
    }

    verify_txn.commit().await?;

    println!("\n✓ Benchmark complete!");
    Ok(())
}

/// Execute a SQL insert transaction
async fn execute_sql_insert(
    _index: usize,
    args: &serde_json::Value,
    txn: Transaction,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let sql = SqlClient::new(txn.clone());

    // Extract values from args
    let id = args["id"].as_str().unwrap_or("default_id");
    let content = args["content"].as_str().unwrap_or("default_content");
    let timestamp = args["timestamp"].as_i64().unwrap_or(0);

    // Execute SQL insert (but don't commit yet)
    sql.insert_with_params(
        "sql_stream",
        "benchmark_table",
        &["id", "content", "timestamp"],
        vec![
            SqlValue::string(id.to_string()),
            SqlValue::string(content.to_string()),
            SqlValue::integer(timestamp),
        ],
    )
    .await?;

    // Commit separately so caller can handle commit failures properly
    txn.commit().await?;
    Ok(())
}
