//! Benchmark for SQL inserts using multiple parallel coordinators
//!
//! This tests if multiple coordinators can better saturate a single stream
//! processor by parallelizing the coordination overhead.

use proven_coordinator::Coordinator;
use proven_engine::{MockClient, MockEngine};
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
    println!("=== Parallel Coordinators SQL Insert Benchmark ===\n");

    // Configuration
    const NUM_COORDINATORS: usize = 4;
    const NUM_INSERTS: usize = 1_000_000;
    const INSERTS_PER_COORDINATOR: usize = NUM_INSERTS / NUM_COORDINATORS;
    const STATUS_INTERVAL: usize = 10_000;

    println!("Configuration:");
    println!("  Coordinators:     {}", NUM_COORDINATORS);
    println!("  Total inserts:    {}", NUM_INSERTS);
    println!("  Per coordinator:  {}", INSERTS_PER_COORDINATOR);
    println!();

    // Initialize the mock engine
    let engine = Arc::new(MockEngine::new());

    // Create SQL stream
    engine.create_stream("sql_stream".to_string())?;
    println!("✓ Created SQL stream");

    // Create and start the runner (single instance)
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
    println!("Pre-starting SQL processor...");
    let processor_duration = Duration::from_secs(600);
    runner
        .ensure_processor("sql_stream", processor_duration)
        .await
        .unwrap();
    println!("✓ SQL processor pre-started\n");

    // Setup phase - Create table
    println!("=== Setup Phase ===");
    let setup_client = Arc::new(MockClient::new(
        "setup-coordinator".to_string(),
        engine.clone(),
    ));
    let setup_coordinator = Arc::new(Coordinator::new(
        "setup-coordinator".to_string(),
        setup_client,
        runner.clone(),
    ));

    let setup_txn = setup_coordinator.begin(Duration::from_secs(10)).await?;
    let sql_setup = SqlClient::new(setup_txn.clone());

    sql_setup
        .create_table(
            "sql_stream",
            "bench",
            "id INT PRIMARY KEY, value INT, data VARCHAR, timestamp INT",
        )
        .await?;
    println!("✓ Created SQL table");
    setup_txn.commit().await?;

    // Create multiple coordinators
    println!("\n=== Creating {} Coordinators ===", NUM_COORDINATORS);
    let mut coordinators = Vec::new();
    for i in 0..NUM_COORDINATORS {
        let coordinator_id = format!("coordinator-{}", i);
        let client = Arc::new(MockClient::new(coordinator_id.clone(), engine.clone()));
        let coordinator = Arc::new(Coordinator::new(
            coordinator_id.clone(),
            client,
            runner.clone(),
        ));
        coordinators.push(coordinator);
        println!("✓ Created {}", coordinator_id);
    }

    // Shared counters for progress tracking
    let successful_inserts = Arc::new(AtomicUsize::new(0));
    let failed_inserts = Arc::new(AtomicUsize::new(0));
    let total_completed = Arc::new(AtomicUsize::new(0));

    println!("\n=== Starting Parallel Benchmark ===\n");
    let start_time = Instant::now();

    // Create tasks for each coordinator
    let mut tasks = JoinSet::new();

    for (coordinator_idx, coordinator) in coordinators.into_iter().enumerate() {
        let successful = successful_inserts.clone();
        let failed = failed_inserts.clone();
        let completed = total_completed.clone();

        tasks.spawn(async move {
            let start_id = coordinator_idx * INSERTS_PER_COORDINATOR;
            let end_id = start_id + INSERTS_PER_COORDINATOR;

            for i in start_id..end_id {
                // Begin transaction
                let txn = match coordinator.begin(Duration::from_secs(5)).await {
                    Ok(t) => t,
                    Err(e) => {
                        eprintln!(
                            "\n[Coordinator {}] Error beginning transaction {}: {}",
                            coordinator_idx, i, e
                        );
                        failed.fetch_add(1, Ordering::Relaxed);
                        completed.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                };

                let sql = SqlClient::new(txn.clone());

                // Insert with parameterized values
                let params = vec![
                    SqlValue::integer(i as i64),
                    SqlValue::integer((i * 2) as i64),
                    SqlValue::string(format!("data_{}", i % 1000)),
                    SqlValue::integer((2000000000 + i) as i64),
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
                    Ok(_) => match txn.commit().await {
                        Ok(_) => {
                            successful.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(e) => {
                            eprintln!(
                                "\n[Coordinator {}] Error committing insert {}: {}",
                                coordinator_idx, i, e
                            );
                            failed.fetch_add(1, Ordering::Relaxed);
                        }
                    },
                    Err(e) => {
                        eprintln!(
                            "\n[Coordinator {}] Error executing insert {}: {}",
                            coordinator_idx, i, e
                        );
                        failed.fetch_add(1, Ordering::Relaxed);
                        let _ = txn.abort().await;
                    }
                }

                completed.fetch_add(1, Ordering::Relaxed);
            }

            coordinator_idx
        });
    }

    // Progress monitoring task
    let monitor_successful = successful_inserts.clone();
    let monitor_failed = failed_inserts.clone();
    let monitor_completed = total_completed.clone();
    let monitor_handle = tokio::spawn(async move {
        let mut last_completed = 0;
        let mut last_time = Instant::now();

        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;

            let current_completed = monitor_completed.load(Ordering::Relaxed);
            if current_completed >= NUM_INSERTS {
                break;
            }

            if current_completed - last_completed >= STATUS_INTERVAL {
                let current_time = Instant::now();
                let interval_duration = current_time.duration_since(last_time);
                let interval_count = current_completed - last_completed;
                let interval_throughput = interval_count as f64 / interval_duration.as_secs_f64();

                let success = monitor_successful.load(Ordering::Relaxed);
                let fail = monitor_failed.load(Ordering::Relaxed);

                eprintln!(
                    "[{:7}/{:7}] {:3}% | Rate: {:.0} inserts/sec | Success: {} Failed: {}",
                    current_completed,
                    NUM_INSERTS,
                    (current_completed * 100) / NUM_INSERTS,
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
    let final_successful = successful_inserts.load(Ordering::Relaxed);
    let final_failed = failed_inserts.load(Ordering::Relaxed);
    let throughput = final_successful as f64 / total_seconds;

    // Verify count
    println!("\nVerifying insert count...");
    let verify_txn = setup_coordinator.begin(Duration::from_secs(10)).await?;
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

    // Print final results
    println!("\n=== Benchmark Results ===");
    println!("Coordinators used:     {}", NUM_COORDINATORS);
    println!("Total attempts:        {}", NUM_INSERTS);
    println!("Successful inserts:    {}", final_successful);
    println!("Failed inserts:        {}", final_failed);
    println!(
        "Success rate:          {:.1}%",
        (final_successful as f64 / NUM_INSERTS as f64) * 100.0
    );
    println!("Total time:            {:.2} seconds", total_seconds);
    println!("Throughput:            {:.0} inserts/second", throughput);
    println!(
        "Avg latency:           {:.3} ms/insert",
        (total_seconds * 1000.0) / final_successful as f64
    );

    // Clean up
    println!("\n=== Cleanup ===");
    setup_coordinator.stop().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    println!("\n✓ Benchmark complete!");
    Ok(())
}
