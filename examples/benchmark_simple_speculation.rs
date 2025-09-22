//! Simple benchmark to test speculative execution
//!
//! This benchmark executes transactions with a consistent pattern of 4 operations
//! across different storage types, using predictable values that can be speculated.

use proven_coordinator::{Coordinator, Transaction};
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
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinSet;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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
    let snapshot_store = Arc::new(MemorySnapshotStore::new());
    let runner = Arc::new(Runner::new(
        "runner-node",
        runner_client.clone(),
        snapshot_store,
    ));
    runner.start().await.unwrap();
    println!("✓ Started runner with snapshot support");

    // Pre-start all processors
    let processor_duration = Duration::from_secs(600);
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
    println!("✓ All processors pre-started");

    // Give processors time to initialize
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Create coordinators
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

    // Setup phase
    println!("\n=== Setup Phase ===");
    let setup_txn = coordinators[0]
        .begin(Duration::from_secs(10), vec![], "setup".to_string())
        .await?;

    let sql_setup = SqlClient::new(setup_txn.clone());
    let resource_setup = ResourceClient::new(setup_txn.clone());

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
    resource_setup
        .mint_integer("resource_stream", "treasury", 1_000_000_000)
        .await?;

    // Create initial accounts
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

    // Benchmark configuration
    println!("\n=== Benchmark Configuration ===");
    const WARMUP_TRANSACTIONS: usize = 100;
    const NUM_TRANSACTIONS: usize = 10_000;

    println!("Warmup transactions:    {}", WARMUP_TRANSACTIONS);
    println!("Benchmark transactions: {}", NUM_TRANSACTIONS);
    println!("Operations per txn:     4 (KV, Queue, Resource, SQL)");
    println!("Coordinators:           {}", NUM_COORDINATORS);

    // Warmup phase - establish patterns
    println!("\n=== Warmup Phase (Pattern Learning) ===");
    for i in 0..WARMUP_TRANSACTIONS {
        let coordinator = &coordinators[i % NUM_COORDINATORS];

        // Create transaction arguments that directly map to operations
        let args = vec![json!({
            "key": format!("item_{}", i),
            "value": format!("value_{}", i),
            "message": format!("msg_{}", i),
            "from_account": format!("account_{}", i % NUM_ACCOUNTS),
            "to_account": format!("account_{}", (i + 1) % NUM_ACCOUNTS),
            "amount": 10
        })];

        let txn = coordinator
            .begin_without_speculation(
                Duration::from_secs(5),
                args.clone(),
                "simple_transaction".to_string(),
            )
            .await?;

        execute_simple_transaction(i, &args[0], txn).await?;

        if (i + 1) % 25 == 0 {
            println!("  Completed {} warmup transactions", i + 1);
        }
    }
    println!("✓ Warmup complete");

    // Let the learning worker process patterns
    tokio::time::sleep(Duration::from_secs(6)).await;

    // Main benchmark phase
    println!("\n=== Benchmark Phase (With Speculation) ===");

    let successful_txns = Arc::new(AtomicUsize::new(0));
    let failed_txns = Arc::new(AtomicUsize::new(0));
    let start_time = Instant::now();

    let mut tasks = JoinSet::new();
    let txns_per_coordinator = NUM_TRANSACTIONS / NUM_COORDINATORS;

    for (coordinator_idx, coordinator) in coordinators.into_iter().enumerate() {
        let successful = successful_txns.clone();
        let failed = failed_txns.clone();

        tasks.spawn(async move {
            let start_idx = coordinator_idx * txns_per_coordinator;
            let end_idx = if coordinator_idx == NUM_COORDINATORS - 1 {
                NUM_TRANSACTIONS
            } else {
                start_idx + txns_per_coordinator
            };

            for i in start_idx..end_idx {
                // Same pattern of arguments as warmup
                let args = vec![json!({
                    "key": format!("item_{}", i + WARMUP_TRANSACTIONS),
                    "value": format!("value_{}", i + WARMUP_TRANSACTIONS),
                    "message": format!("msg_{}", i + WARMUP_TRANSACTIONS),
                    "from_account": format!("account_{}", i % NUM_ACCOUNTS),
                    "to_account": format!("account_{}", (i + 1) % NUM_ACCOUNTS),
                    "amount": 10
                })];

                // Begin with speculation enabled
                let txn = match coordinator
                    .begin(
                        Duration::from_secs(5),
                        args.clone(),
                        "simple_transaction".to_string(),
                    )
                    .await
                {
                    Ok(t) => t,
                    Err(e) => {
                        eprintln!("Failed to begin transaction: {:?}", e);
                        failed.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                };

                match execute_simple_transaction(i + WARMUP_TRANSACTIONS, &args[0], txn).await {
                    Ok(_) => {
                        successful.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        eprintln!("Transaction {} failed: {:?}", i, e);
                        failed.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }

            coordinator_idx
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
                eprintln!(
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

    println!("\n=== Benchmark Results ===");
    println!("Total transactions:      {}", NUM_TRANSACTIONS);
    println!("Successful transactions: {}", final_successful);
    println!("Failed transactions:     {}", final_failed);
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
        "Avg latency:             {:.3} ms/transaction",
        (total_seconds * 1000.0) / final_successful as f64
    );

    // Wait a bit for any pending operations to complete
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verification phase
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
        .begin(Duration::from_secs(10), vec![], "verification".to_string())
        .await?;
    let kv_verify = KvClient::new(verify_txn.clone());
    let queue_verify = QueueClient::new(verify_txn.clone());
    let sql_verify = SqlClient::new(verify_txn.clone());

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
    if let Ok(size) = queue_verify.size("queue_stream", "messages").await {
        println!("✓ Queue contains {} items", size);
    }

    // Check SQL count
    let count_result = sql_verify
        .select("sql_stream", "items", &["COUNT(*) as cnt"], None)
        .await?;
    if let Some(count) = count_result.column_values("cnt").first() {
        println!("✓ SQL table contains {} rows", count);
    }

    verify_txn.commit().await?;

    println!("\n✓ Benchmark complete!");
    Ok(())
}

/// Execute a simple transaction with 4 predictable operations
async fn execute_simple_transaction(
    _index: usize,
    args: &serde_json::Value,
    txn: Transaction,
) -> Result<(), Box<dyn std::error::Error>> {
    let kv = KvClient::new(txn.clone());
    let queue = QueueClient::new(txn.clone());
    let resource = ResourceClient::new(txn.clone());
    let sql = SqlClient::new(txn.clone());

    // Extract values from args - these should match what speculation predicts
    let key = args["key"].as_str().unwrap_or("default_key");
    let value = args["value"].as_str().unwrap_or("default_value");
    let message = args["message"].as_str().unwrap_or("default_message");
    let from_account = args["from_account"].as_str().unwrap_or("account_0");
    let to_account = args["to_account"].as_str().unwrap_or("account_1");
    let amount = args["amount"].as_u64().unwrap_or(10);

    // 1. KV Put operation - using key and value from args
    kv.put("kv_stream", key, Value::String(value.to_string()))
        .await?;

    // 2. Queue Enqueue operation - using message from args
    queue
        .enqueue(
            "queue_stream",
            "messages",
            QueueValue::String(message.to_string()),
        )
        .await?;

    // 3. Resource Transfer operation - using accounts and amount from args
    resource
        .transfer_integer("resource_stream", from_account, to_account, amount)
        .await?;

    // 4. SQL Insert operation - using key and value from args
    sql.insert_with_params(
        "sql_stream",
        "items",
        &["id", "value"],
        vec![
            SqlValue::string(key.to_string()),
            SqlValue::string(value.to_string()),
        ],
    )
    .await?;

    txn.commit().await?;
    Ok(())
}
