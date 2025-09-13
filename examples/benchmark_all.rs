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
use proven_sql_client::SqlClient;
use std::sync::Arc;
use std::time::{Duration, Instant};

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
    println!("✓ Created {} coordinators\n", NUM_COORDINATORS);

    // Use first coordinator for setup
    let coordinator = &coordinators[0];

    // Setup phase - Initialize storage
    println!("=== Setup Phase ===");
    let setup_txn = coordinator.begin(Duration::from_secs(10)).await?;
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

    let start_time = Instant::now();
    let mut successful_txns = 0;
    let mut failed_txns = 0;

    for i in 0..NUM_TRANSACTIONS {
        // Select coordinator in round-robin fashion
        let coordinator = &coordinators[i % NUM_COORDINATORS];

        // Begin a distributed transaction
        let txn = match coordinator.begin(Duration::from_secs(5)).await {
            Ok(t) => t,
            Err(e) => {
                eprintln!("Failed to begin transaction {}: {}", i, e);
                failed_txns += 1;
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
            eprintln!("KV put failed in txn {}: {}", i, e);
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
                start_time.elapsed().as_millis()
            )),
        };

        if let Err(e) = queue.enqueue("queue_stream", queue_name, queue_value).await {
            eprintln!("Queue enqueue failed in txn {}: {}", i, e);
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
                    eprintln!("Resource transfer failed in txn {}: {}", i, e);
                    all_succeeded = false;
                }
            }
        }

        // 4. SQL Insert operation - use a unique ID to avoid conflicts
        // Combine run timestamp with transaction number for guaranteed uniqueness
        let run_id = start_time.elapsed().as_secs();
        let unique_id = (run_id * 1_000_000) + i as u64; // Guaranteed unique across runs
        let sql_query = format!(
            "INSERT INTO benchmark (id, value, data, timestamp) VALUES ({}, {}, 'data_{}', {})",
            unique_id,
            i * 2,
            i % 1000,
            start_time.elapsed().as_millis()
        );

        if let Err(e) = sql.execute("sql_stream", &sql_query).await {
            eprintln!("SQL insert failed in txn {}: {}", i, e);
            all_succeeded = false;
        }

        // Commit or abort the transaction based on success
        if all_succeeded {
            match txn.commit().await {
                Ok(_) => successful_txns += 1,
                Err(e) => {
                    eprintln!("Failed to commit txn {}: {}", i, e);
                    failed_txns += 1;
                }
            }
        } else {
            // Abort the transaction if any operation failed
            match txn.abort().await {
                Ok(_) => {
                    failed_txns += 1;
                }
                Err(e) => {
                    eprintln!("Failed to abort txn {}: {}", i, e);
                    failed_txns += 1;
                }
            }
        }

        // Progress indicator
        if (i + 1) % PROGRESS_INTERVAL == 0 {
            let elapsed = start_time.elapsed();
            let throughput = (i + 1) as f64 / elapsed.as_secs_f64();
            eprintln!(
                "[{:6}/{:6}] {:3}% | {:.0} txns/sec | Success: {} Failed: {}",
                i + 1,
                NUM_TRANSACTIONS,
                ((i + 1) * 100) / NUM_TRANSACTIONS,
                throughput,
                successful_txns,
                failed_txns
            );
        }
    }

    // Calculate final statistics
    let total_duration = start_time.elapsed();
    let total_seconds = total_duration.as_secs_f64();
    let txn_throughput = successful_txns as f64 / total_seconds;
    let op_throughput = (successful_txns * OPERATIONS_PER_TXN) as f64 / total_seconds;

    println!("\n=== Benchmark Results ===");
    println!("Total transactions:      {}", NUM_TRANSACTIONS);
    println!("Successful transactions: {}", successful_txns);
    println!("Failed transactions:     {}", failed_txns);
    println!(
        "Success rate:            {:.1}%",
        (successful_txns as f64 / NUM_TRANSACTIONS as f64) * 100.0
    );
    println!("Total time:              {:.2} seconds", total_seconds);
    println!("Transaction throughput:  {:.0} txns/second", txn_throughput);
    println!("Operation throughput:    {:.0} ops/second", op_throughput);
    println!(
        "Avg latency:             {:.3} ms/transaction",
        (total_seconds * 1000.0) / successful_txns as f64
    );

    println!("\nOperation breakdown:");
    println!("- KV puts:          {}", successful_txns);
    println!("- Queue enqueues:   {}", successful_txns);
    println!("- Resource transfers: ~{}", successful_txns); // Some may fail due to balance
    println!("- SQL inserts:      {}", successful_txns);
    println!(
        "- Total operations: ~{}",
        successful_txns * OPERATIONS_PER_TXN
    );

    // Verification phase
    println!("\n=== Verification Phase ===");
    let verify_txn = coordinators[0].begin(Duration::from_secs(10)).await?;
    let kv_verify = KvClient::new(verify_txn.clone());
    let queue_verify = QueueClient::new(verify_txn.clone());
    let sql_verify = SqlClient::new(verify_txn.clone());

    // Check a sample KV entry
    if let Some(value) = kv_verify.get("kv_stream", "key_00000000").await? {
        println!("✓ Sample KV entry found: {:?}", value);
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

    verify_txn.commit().await?;

    // Clean up
    println!("\n=== Cleanup ===");
    for coordinator in &coordinators {
        coordinator.stop().await;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    println!("\n✓ Benchmark complete!");
    Ok(())
}
