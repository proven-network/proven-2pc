//! Simple benchmark to test speculative execution
//!
//! This benchmark executes transactions with a consistent pattern of 4 operations
//! across different storage types, using predictable values that can be speculated.

use proven_coordinator::{Coordinator, Executor};
use proven_engine::{MockClient, MockEngine};
use proven_kv::types::Value;
use proven_kv_client::KvClient;
use proven_queue_client::QueueClient;
use proven_resource_client::ResourceClient;
use proven_runner::Runner;
use proven_value::Vault;

use proven_sql::Value as SqlValue;
use proven_sql_client::SqlClient;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::task::JoinSet;
use uuid::Uuid;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("=== Simple Speculation Benchmark ===\n");

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
    let temp_dir = tempfile::tempdir()?.keep();
    let runner = Arc::new(Runner::new("runner-node", runner_client.clone(), temp_dir));
    runner.start().await.unwrap();
    println!("✓ Started runner");

    // Create a single coordinator since we're parallelizing transactions
    let coordinator_client = Arc::new(MockClient::new("coordinator".to_string(), engine.clone()));
    let coordinator = Arc::new(Coordinator::new(
        "coordinator".to_string(),
        coordinator_client,
        runner.clone(),
    ));
    println!("✓ Created coordinator");

    // Setup phase
    println!("\n=== Setup Phase ===");
    let setup_executor = Arc::new(
        coordinator
            .begin_read_write(Duration::from_secs(10), vec![], "setup".to_string())
            .await?,
    );

    let sql_setup = SqlClient::new(setup_executor.clone());
    let resource_setup = ResourceClient::new(setup_executor.clone());

    // Create SQL table with simple schema
    sql_setup
        .create_table(
            "sql_stream",
            "items",
            "id VARCHAR PRIMARY KEY, value VARCHAR",
        )
        .await?;
    println!("✓ Created SQL table");

    // Initialize resource system with treasury
    let treasury_vault = Vault::new(Uuid::new_v4());
    resource_setup
        .mint_integer("resource_stream", treasury_vault.clone(), 10_000_000_000)
        .await?;

    // Create initial accounts with much higher balance
    const NUM_ACCOUNTS: usize = 100;
    let mut account_vaults: HashMap<usize, Vault> = HashMap::new();
    for i in 0..NUM_ACCOUNTS {
        let vault = Vault::new(Uuid::new_v4());
        resource_setup
            .transfer_integer(
                "resource_stream",
                treasury_vault.clone(),
                vault.clone(),
                1_000_000, // 100x more than before
            )
            .await?;
        account_vaults.insert(i, vault);
    }
    println!("✓ Created {} accounts with initial balances", NUM_ACCOUNTS);

    // Share account vaults across all tasks
    let account_vaults = Arc::new(account_vaults);

    setup_executor.finish().await?;

    // Benchmark configuration
    println!("\n=== Benchmark Configuration ===");
    const WARMUP_TRANSACTIONS: usize = 100;
    const NUM_TRANSACTIONS: usize = 100_000;
    const MAX_CONCURRENT: Option<usize> = Some(500);

    println!("Warmup transactions:    {}", WARMUP_TRANSACTIONS);
    println!("Benchmark transactions: {}", NUM_TRANSACTIONS);
    println!("Operations per txn:     4 (KV, Queue, Resource, SQL)");
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
        let vaults = account_vaults.clone();

        warmup_tasks.spawn(async move {
            let from_idx = i % NUM_ACCOUNTS;
            let to_idx = (i + 1) % NUM_ACCOUNTS;

            // Create transaction arguments that directly map to operations
            let args = vec![json!({
                "key": format!("item_{}", i),
                "value": format!("value_{}", i),
                "message": format!("msg_{}", i),
                "from_vault_uuid": vaults[&from_idx].uuid().to_string(),
                "to_vault_uuid": vaults[&to_idx].uuid().to_string(),
                "amount": 10
            })];

            let executor = match coordinator
                .begin_read_write_without_speculation(
                    Duration::from_secs(5),
                    args.clone(),
                    "simple_transaction".to_string(),
                )
                .await
            {
                Ok(t) => Arc::new(t),
                Err(e) => {
                    eprintln!("Failed to begin warmup transaction {}: {:?}", i, e);
                    failed.fetch_add(1, Ordering::Relaxed);
                    return;
                }
            };

            if let Err(e) = execute_simple_transaction(i, &args[0], executor.clone()).await {
                println!("Failed to execute warmup transaction {}: {:?}", i, e);
                if let Err(cancel_err) = executor.cancel().await {
                    println!(
                        "Warmup transaction {} also failed to cancel: {:?}",
                        i, cancel_err
                    );
                }
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
        let vaults = account_vaults.clone();

        tasks.spawn(async move {
            let task_start = Instant::now();
            let from_idx = i % NUM_ACCOUNTS;
            let to_idx = (i + 1) % NUM_ACCOUNTS;

            // Same pattern of arguments as warmup
            let args = vec![json!({
                "key": format!("item_{}", i + WARMUP_TRANSACTIONS),
                "value": format!("value_{}", i + WARMUP_TRANSACTIONS),
                "message": format!("msg_{}", i + WARMUP_TRANSACTIONS),
                "from_vault_uuid": vaults[&from_idx].uuid().to_string(),
                "to_vault_uuid": vaults[&to_idx].uuid().to_string(),
                "amount": 10
            })];

            // Retry loop with exponential backoff
            let mut retry_count = 0;
            let mut speculation_failure_count = 0;
            const MAX_RETRIES: u32 = 10;
            const MAX_SPECULATION_FAILURES: u32 = 2;

            loop {
                // Decide whether to use speculation (disable after repeated failures)
                let use_speculation = speculation_failure_count < MAX_SPECULATION_FAILURES;

                // Calculate timeout (increase slightly on retries to reduce deadline thrashing)
                let timeout = Duration::from_secs(10 + (retry_count as u64 / 3));

                // Begin with speculation enabled unless we've had too many speculation failures
                let executor = match if use_speculation {
                    coordinator
                        .begin_read_write(
                            timeout,
                            args.clone(),
                            "simple_transaction".to_string(),
                        )
                        .await
                } else {
                    coordinator
                        .begin_read_write_without_speculation(
                            timeout,
                            args.clone(),
                            "simple_transaction".to_string(),
                        )
                        .await
                } {
                    Ok(t) => Arc::new(t),
                    Err(e) => {
                        let error_str = e.to_string();

                        // Check if begin failure is retryable
                        let should_retry = error_str.contains("Response timeout")
                            || error_str.contains("Transaction deadline exceeded")
                            || error_str.contains("Engine error")
                            || error_str.contains("Prepare phase timed out");

                        if should_retry && retry_count < MAX_RETRIES {
                            retry_count += 1;
                            retries.fetch_add(1, Ordering::Relaxed);

                            let backoff_ms = calculate_backoff(&error_str, retry_count);
                            eprintln!(
                                "Transaction {} failed to begin (attempt {}): {}, retrying in {}ms",
                                i, retry_count + 1, error_str, backoff_ms
                            );
                            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                            continue;
                        } else {
                            eprintln!("Transaction {} failed to begin after {} attempts: {}", i, retry_count + 1, error_str);
                            failed.fetch_add(1, Ordering::Relaxed);
                            return;
                        }
                    }
                };

                // Execute the transaction and convert error immediately
                let result =
                    execute_simple_transaction(i + WARMUP_TRANSACTIONS, &args[0], executor.clone())
                        .await;

                match result {
                    Ok(_) => {
                        successful.fetch_add(1, Ordering::Relaxed);
                        let task_duration = task_start.elapsed();
                        latency_tracker
                            .fetch_add(task_duration.as_micros() as usize, Ordering::Relaxed);
                        return;
                    }
                    Err(e) => {
                        // Convert to string immediately to avoid Send issues
                        let error_str = e.to_string();

                        // Always abort the failed transaction
                        if let Err(cancel_err) = executor.cancel().await {
                            eprintln!(
                                "Transaction {} failed AND cancel failed: original error: {}, cancel error: {:?}",
                                i, error_str, cancel_err
                            );
                            // Continue to handle the original error below
                        }

                        // Check if it's an error we should retry
                        let should_retry = error_str.contains("Transaction was aborted")
                            || error_str.contains("Transaction was wounded")
                            || error_str.contains("Speculation failed")
                            || error_str.contains("Response timeout")
                            || error_str.contains("Prepare phase timed out")
                            || error_str.contains("PrepareTimeout")
                            || error_str.contains("Transaction deadline exceeded")
                            || error_str.contains("Engine error");

                        if should_retry && retry_count < MAX_RETRIES {
                            retry_count += 1;
                            retries.fetch_add(1, Ordering::Relaxed);

                            // Track speculation failures separately
                            if error_str.contains("Speculation failed") {
                                speculation_failure_count += 1;
                            }

                            // Differentiated backoff based on error type
                            let backoff_ms = calculate_backoff(&error_str, retry_count);
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
        "Operations throughput:   {:.0} ops/second",
        (final_successful * 4) as f64 / total_seconds
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

    // Verification phase - use the same coordinator
    println!("\n=== Verification Phase ===");

    let verify_executor = Arc::new(
        coordinator
            .begin_read_write_without_speculation(
                Duration::from_secs(10),
                vec![],
                "verification_final".to_string(),
            )
            .await?,
    );
    let kv_verify = KvClient::new(verify_executor.clone());
    let queue_verify = QueueClient::new(verify_executor.clone());
    let sql_verify = SqlClient::new(verify_executor.clone());

    // Check KV count by checking a sample of keys
    let mut kv_count = 0;
    let mut missing_keys = Vec::new();
    let sample_size = std::cmp::min(100, NUM_TRANSACTIONS);
    for i in 0..sample_size {
        let key = format!("item_{}", i + WARMUP_TRANSACTIONS);
        if kv_verify.get("kv_stream", &key).await?.is_some() {
            kv_count += 1;
        } else {
            missing_keys.push(i);
        }
    }
    println!(
        "✓ KV entries found: {} out of {} sampled",
        kv_count, sample_size
    );
    if !missing_keys.is_empty() && missing_keys.len() <= 10 {
        println!("  Missing KV keys: {:?}", missing_keys);
    }

    // Check queue size
    let queue_size = if let Ok(size) = queue_verify.size("queue_stream").await {
        println!("✓ Queue contains {} items", size);
        size
    } else {
        0
    };

    // Check for holes: expected is warmup + benchmark = 100,100 items
    let expected_items = WARMUP_TRANSACTIONS + NUM_TRANSACTIONS;
    if queue_size < expected_items {
        let missing = expected_items - queue_size;
        println!("  ⚠ Missing {} items", missing);
    }

    // Check for missing rows in the SQL table
    let count_result = sql_verify
        .select("sql_stream", "items", &["COUNT(*) as cnt"], None)
        .await?;
    let expected_items = (WARMUP_TRANSACTIONS + NUM_TRANSACTIONS) as u64;
    if let Some(count_str) = count_result.column_values("cnt").first() {
        println!("✓ SQL table contains {} rows", count_str);
        let count = count_str.parse::<u64>().unwrap_or(0);
        if count < expected_items {
            let missing = expected_items - count;
            println!("  ⚠ Missing {} rows", missing);
        }
    }

    verify_executor.finish().await?;

    println!("\n✓ Benchmark complete!");
    Ok(())
}

/// Execute a simple transaction with 4 predictable operations
async fn execute_simple_transaction<E>(
    index: usize,
    args: &serde_json::Value,
    executor: Arc<E>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    E: proven_coordinator::Executor + Send + Sync + 'static,
{
    let kv = KvClient::new(executor.clone());
    let queue = QueueClient::new(executor.clone());
    let resource = ResourceClient::new(executor.clone());
    let sql = SqlClient::new(executor.clone());

    // Extract values from args - these should match what speculation predicts
    let key = args["key"].as_str().unwrap_or("default_key");
    let value = args["value"].as_str().unwrap_or("default_value");
    let message = args["message"].as_str().unwrap_or("default_message");
    let from_uuid_str = args["from_vault_uuid"]
        .as_str()
        .unwrap_or("00000000-0000-0000-0000-000000000000");
    let to_uuid_str = args["to_vault_uuid"]
        .as_str()
        .unwrap_or("00000000-0000-0000-0000-000000000001");
    let amount = args["amount"].as_u64().unwrap_or(10);

    // Parse UUIDs and create Vaults
    let from_vault = Vault::new(Uuid::parse_str(from_uuid_str).unwrap());
    let to_vault = Vault::new(Uuid::parse_str(to_uuid_str).unwrap());

    // 1. KV Put operation - using key and value from args
    if let Err(e) = kv
        .put("kv_stream", key, Value::Str(value.to_string()))
        .await
    {
        eprintln!("[{}] KV put failed for key '{}': {:?}", index, key, e);
        return Err(e.into());
    }

    // 2. Queue Enqueue operation - using message from args
    if let Err(e) = queue
        .enqueue("queue_stream", Value::Str(message.to_string()))
        .await
    {
        eprintln!(
            "[{}] Queue enqueue failed for message '{}': {:?}",
            index, message, e
        );
        return Err(e.into());
    }

    // 3. Resource Transfer operation - using accounts and amount from args
    if let Err(e) = resource
        .transfer_integer(
            "resource_stream",
            from_vault.clone(),
            to_vault.clone(),
            amount,
        )
        .await
    {
        eprintln!(
            "[{}] Resource transfer failed from {:?} to {:?} amount {}: {:?}",
            index, from_vault, to_vault, amount, e
        );
        return Err(e.into());
    }

    // 4. SQL Insert operation - using key and value from args
    if let Err(e) = sql
        .insert_with_params(
            "sql_stream",
            "items",
            &["id", "value"],
            vec![
                SqlValue::string(key.to_string()),
                SqlValue::string(value.to_string()),
            ],
        )
        .await
    {
        eprintln!("[{}] SQL insert failed for key '{}': {:?}", index, key, e);
        return Err(e.into());
    }

    if let Err(e) = executor.finish().await {
        eprintln!("[{}] Transaction finish failed: {:?}", index, e);
        return Err(e.into());
    }

    Ok(())
}

/// Calculate backoff duration based on error type and retry count
/// Different error types use different backoff strategies
fn calculate_backoff(error_str: &str, retry_count: u32) -> u64 {
    if error_str.contains("Transaction was wounded") {
        // Short backoff for wound-wait - other transaction should finish soon
        // 5ms, 10ms, 20ms, 40ms, then cap at 40ms
        5u64 << retry_count.min(3)
    } else if error_str.contains("Transaction deadline exceeded") {
        // Minimal backoff for deadline exceeded - we need a fresh start ASAP
        5
    } else if error_str.contains("Speculation failed") {
        // No backoff for speculation - try immediately without speculation
        1
    } else if error_str.contains("Response timeout")
        || error_str.contains("Prepare phase timed out")
    {
        // Medium backoff for timeouts
        // 10ms, 20ms, 40ms, 80ms, cap at 100ms
        (10u64 << retry_count.min(3)).min(100)
    } else {
        // Default exponential backoff with reasonable cap
        // 2ms, 4ms, 8ms, 16ms, 32ms, 64ms, cap at 100ms
        (2u64 << retry_count).min(100)
    }
}
