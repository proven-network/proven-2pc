//! Benchmark for SQL inserts through the coordinator
//!
//! This benchmark measures the throughput of SQL inserts using distributed
//! transactions through the coordinator, comparable to benchmark_inserts.rs
//! but using the full coordinator stack.

use proven_coordinator::Coordinator;
use proven_engine::{MockClient, MockEngine};
use proven_runner::Runner;
use proven_snapshot_memory::MemorySnapshotStore;
use proven_sql::Value as SqlValue;
use proven_sql_client::SqlClient;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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

    // Pre-start SQL processor with a long duration to avoid restarts during benchmark
    println!("Pre-starting SQL processor for benchmark duration...");
    let processor_duration = Duration::from_secs(600); // 10 minutes - enough for the benchmark
    runner
        .ensure_processor("sql_stream", processor_duration)
        .await
        .unwrap();
    println!("✓ SQL processor pre-started with 10-minute guarantee");

    // Create coordinator
    let coordinator_client = Arc::new(MockClient::new("coordinator-1".to_string(), engine.clone()));
    let coordinator = Arc::new(Coordinator::new(
        "coordinator-1".to_string(),
        coordinator_client,
        runner.clone(),
    ));
    println!("✓ Created coordinator\n");

    // Setup phase - Create table
    println!("=== Setup Phase ===");
    let setup_txn = coordinator.begin(Duration::from_secs(10)).await?;
    let sql_setup = SqlClient::new(setup_txn.clone());

    // Create SQL table (same schema as benchmark_inserts.rs)
    sql_setup
        .create_table(
            "sql_stream",
            "bench",
            "id INT PRIMARY KEY, value INT, data VARCHAR, timestamp INT",
        )
        .await?;
    println!("✓ Created SQL table");

    setup_txn.commit().await?;
    println!("\n=== Benchmark Configuration ===");

    // Benchmark configuration (matching benchmark_inserts.rs)
    const NUM_INSERTS: usize = 1_000_000;
    const PROGRESS_INTERVAL: usize = 10_000;
    const STATUS_INTERVAL: usize = 100_000;

    println!("Total inserts:    {}", NUM_INSERTS);
    println!("Coordinator:      Single coordinator");
    println!("\nStarting {} inserts...\n", NUM_INSERTS);

    let start_time = Instant::now();

    // Spawn the work in a task for better async scheduling (matching benchmark_parallel_coordinators)
    let coordinator_clone = coordinator.clone();
    let insert_task = tokio::spawn(async move {
        let mut last_status_time = Instant::now();
        let mut last_status_count = 0;
        let mut successful_inserts = 0;
        let mut failed_inserts = 0;

        for i in 0..NUM_INSERTS {
            // Begin a transaction for each insert (matching benchmark_inserts.rs behavior)
            let txn = match coordinator_clone.begin(Duration::from_secs(5)).await {
                Ok(t) => t,
                Err(e) => {
                    eprintln!("\nError beginning transaction {}: {}", i, e);
                    failed_inserts += 1;
                    continue;
                }
            };

            let sql = SqlClient::new(txn.clone());

            // Use parameterized insert (matching the data from benchmark_inserts.rs)
            let params = vec![
                SqlValue::integer(i as i64),                    // id
                SqlValue::integer((i * 2) as i64),              // value (some computation)
                SqlValue::string(format!("data_{}", i % 1000)), // data (repeating pattern)
                SqlValue::integer((2000000000 + i) as i64),     // timestamp
            ];

            match sql
                .insert_with_params(
                    "sql_stream",
                    "bench",
                    &["id", "value", "data", "timestamp"],
                    params,
                )
                .await
            {
                Ok(_) => {
                    // Commit the transaction
                    match txn.commit().await {
                        Ok(_) => successful_inserts += 1,
                        Err(e) => {
                            eprintln!("\nError committing insert {}: {}", i, e);
                            failed_inserts += 1;
                        }
                    }
                }
                Err(e) => {
                    eprintln!("\nError executing insert {}: {}", i, e);
                    failed_inserts += 1;
                    // Try to abort the transaction
                    let _ = txn.abort().await;
                }
            }

            // Progress indicator
            if (i + 1) % PROGRESS_INTERVAL == 0 {
                eprint!(".");
                std::io::Write::flush(&mut std::io::stderr()).unwrap();
            }

            // Status update
            if (i + 1) % STATUS_INTERVAL == 0 {
                let current_time = Instant::now();
                let interval_duration = current_time.duration_since(last_status_time);
                let interval_count = (i + 1) - last_status_count;
                let interval_throughput = interval_count as f64 / interval_duration.as_secs_f64();

                eprintln!(
                    "\n[{:7}/{:7}] {:3}% | Interval: {:.0} inserts/sec | Success: {} Failed: {}",
                    i + 1,
                    NUM_INSERTS,
                    ((i + 1) * 100) / NUM_INSERTS,
                    interval_throughput,
                    successful_inserts,
                    failed_inserts
                );

                last_status_time = current_time;
                last_status_count = i + 1;
            }
        }

        eprintln!(); // New line after progress dots

        (successful_inserts, failed_inserts)
    });

    // Wait for the task to complete
    let (successful_inserts, failed_inserts) = match insert_task.await {
        Ok(result) => result,
        Err(e) => {
            eprintln!("Insert task failed: {}", e);
            (0, 0)
        }
    };

    // Calculate final statistics
    let total_duration = start_time.elapsed();
    let total_seconds = total_duration.as_secs_f64();
    let throughput = successful_inserts as f64 / total_seconds;

    // Verify count
    println!("\nVerifying insert count...");
    let verify_txn = coordinator.begin(Duration::from_secs(10)).await?;
    let sql_verify = SqlClient::new(verify_txn.clone());

    let count_result = sql_verify
        .query("sql_stream", "SELECT COUNT(*) as cnt FROM bench")
        .await?;

    let actual_count = count_result
        .column_values("cnt")
        .first()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);

    println!("✓ Actual count in table: {}", actual_count);
    verify_txn.commit().await?;

    // Print final statistics
    println!("\n=== Benchmark Results ===");
    println!("Total attempts:        {}", NUM_INSERTS);
    println!("Successful inserts:    {}", successful_inserts);
    println!("Failed inserts:        {}", failed_inserts);
    println!(
        "Success rate:          {:.1}%",
        (successful_inserts as f64 / NUM_INSERTS as f64) * 100.0
    );
    println!("Total time:            {:.2} seconds", total_seconds);
    println!("Throughput:            {:.0} inserts/second", throughput);
    println!(
        "Avg latency:           {:.3} ms/insert",
        (total_seconds * 1000.0) / successful_inserts as f64
    );

    println!("\nMemory usage and detailed statistics:");
    println!("- Transactions executed: {}", successful_inserts);
    println!("- Coordinator used:      1");
    println!("- Stream processors:     1 (SQL)");

    // Clean up
    println!("\n=== Cleanup ===");
    coordinator.stop().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    println!("\n✓ Benchmark complete!");
    Ok(())
}
