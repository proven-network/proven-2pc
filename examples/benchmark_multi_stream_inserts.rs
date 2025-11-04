//! Benchmark for SQL inserts through the coordinator with multiple streams
//!
//! This benchmark measures the throughput of SQL inserts using distributed
//! transactions across multiple independent streams, allowing true parallelism.

use proven_coordinator::{Coordinator, Executor};
use proven_engine::{MockClient, MockEngine};
use proven_runner::Runner;

use proven_sql::Value as SqlValue;
use proven_sql_client::SqlClient;
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::task::JoinSet;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("=== Multi-Stream SQL Insert Benchmark ===\n");

    // Parse command line arguments
    let args: Vec<String> = std::env::args().collect();
    let num_streams = if args.len() > 1 {
        args[1].parse::<usize>().unwrap_or(4)
    } else {
        4
    };

    // Configuration
    const WARMUP_TRANSACTIONS: usize = 100;
    const NUM_TRANSACTIONS: usize = 100_000;
    const MAX_CONCURRENT: Option<usize> = Some(1_000);

    println!("Configuration:");
    println!("  Streams:                {}", num_streams);
    println!("  Warmup transactions:    {}", WARMUP_TRANSACTIONS);
    println!("  Benchmark transactions: {}", NUM_TRANSACTIONS);
    println!("  Max concurrent:         {:?}", MAX_CONCURRENT);
    println!();

    // Initialize the mock engine
    let engine = Arc::new(MockEngine::new());

    // Create SQL streams
    let stream_names: Vec<String> = (0..num_streams)
        .map(|i| format!("sql_stream_{}", i))
        .collect();

    for stream_name in &stream_names {
        engine.create_stream(stream_name.clone())?;
        println!("✓ Created stream: {}", stream_name);
    }

    // Create and start the runner
    let runner_client = Arc::new(MockClient::new("runner-node".to_string(), engine.clone()));
    let temp_dir = tempfile::tempdir()?;
    let runner = Arc::new(Runner::new(
        "runner-node",
        runner_client.clone(),
        temp_dir.path(),
    ));
    runner.start().await.unwrap();
    println!("✓ Started runner with snapshot support");

    // Pre-start SQL processors for all streams
    let processor_duration = Duration::from_secs(600);
    for stream_name in &stream_names {
        runner
            .ensure_processor(stream_name, processor_duration)
            .await
            .unwrap();
        println!("✓ SQL processor pre-started for {}", stream_name);
    }

    // Give processors time to initialize
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Create one coordinator per stream for better parallelism
    let coordinators: Vec<Arc<Coordinator>> = stream_names
        .iter()
        .enumerate()
        .map(|(i, _)| {
            let coord_name = format!("coordinator_{}", i);
            let coordinator_client = Arc::new(MockClient::new(coord_name.clone(), engine.clone()));
            Arc::new(Coordinator::new(
                coord_name.clone(),
                coordinator_client,
                runner.clone(),
            ))
        })
        .collect();
    println!(
        "✓ Created {} coordinators (one per stream)",
        coordinators.len()
    );

    // Setup phase - create tables in each stream
    println!("\n=== Setup Phase ===");
    let mut setup_tasks = JoinSet::new();

    for (i, stream_name) in stream_names.iter().enumerate() {
        let stream_name = stream_name.clone();
        let coordinator = coordinators[i].clone();

        setup_tasks.spawn(async move {
            let setup_executor = Arc::new(
                coordinator
                    .begin_read_write(Duration::from_secs(10), vec![], format!("setup_{}", i))
                    .await?,
            );

            let sql_setup = SqlClient::new(setup_executor.clone());

            // Create SQL table with simple schema
            sql_setup
                .create_table(
                    &stream_name,
                    &format!("benchmark_table_{}", i),
                    "id VARCHAR PRIMARY KEY, content VARCHAR, timestamp BIGINT, stream_id INT",
                )
                .await?;

            setup_executor.finish().await?;

            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        });
    }

    // Wait for all setup tasks
    while let Some(result) = setup_tasks.join_next().await {
        result??;
    }
    println!("✓ Created tables in all streams");

    // Warmup phase
    println!("\n=== Warmup Phase ===");
    let warmup_successful = Arc::new(AtomicUsize::new(0));
    let warmup_failed = Arc::new(AtomicUsize::new(0));
    let warmup_start = Instant::now();

    let mut warmup_tasks = JoinSet::new();

    for i in 0..WARMUP_TRANSACTIONS {
        let stream_idx = i % num_streams;
        let stream_name = stream_names[stream_idx].clone();
        let coordinator = coordinators[stream_idx].clone();
        let successful = warmup_successful.clone();
        let failed = warmup_failed.clone();

        warmup_tasks.spawn(async move {
            let args = vec![json!({
                "id": format!("record_{:08}", i),
                "content": format!("Warmup content for record {}", i),
                "timestamp": i as i64,
                "stream_id": stream_idx as i64
            })];

            let executor = match coordinator
                .begin_read_write_without_speculation(
                    Duration::from_secs(5),
                    args.clone(),
                    format!("warmup_{}", stream_idx),
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

            if let Err(e) = execute_sql_insert(
                i,
                &args[0],
                executor.clone(),
                &stream_name,
                &format!("benchmark_table_{}", stream_idx),
            )
            .await
            {
                eprintln!("Failed to execute warmup transaction {}: {:?}", i, e);
                let _ = executor.cancel().await;
                failed.fetch_add(1, Ordering::Relaxed);
            } else {
                successful.fetch_add(1, Ordering::Relaxed);
            }
        });
    }

    // Wait for warmup
    while let Some(result) = warmup_tasks.join_next().await {
        match result {
            Ok(_) => {}
            Err(e) => eprintln!("Warmup task failed: {}", e),
        }
    }

    let warmup_duration = warmup_start.elapsed();
    println!(
        "✓ Warmup complete: {} successful, {} failed in {:.2}s",
        warmup_successful.load(Ordering::Relaxed),
        warmup_failed.load(Ordering::Relaxed),
        warmup_duration.as_secs_f64()
    );

    // Main benchmark phase
    println!("\n=== Benchmark Phase ===");
    println!(
        "Distributing {} transactions across {} streams",
        NUM_TRANSACTIONS, num_streams
    );
    println!(
        "Each stream will handle ~{} transactions",
        NUM_TRANSACTIONS / num_streams
    );

    let successful_txns = Arc::new(AtomicUsize::new(0));
    let failed_txns = Arc::new(AtomicUsize::new(0));
    let total_retries = Arc::new(AtomicUsize::new(0));
    let total_latency_us = Arc::new(AtomicUsize::new(0));
    let start_time = Instant::now();

    // Track per-stream statistics
    let stream_successful: Vec<Arc<AtomicUsize>> = (0..num_streams)
        .map(|_| Arc::new(AtomicUsize::new(0)))
        .collect();

    let mut tasks = JoinSet::new();

    // Spawn tasks with optional concurrency limit
    for i in 0..NUM_TRANSACTIONS {
        // Round-robin distribution across streams
        let stream_idx = i % num_streams;
        let stream_name = stream_names[stream_idx].clone();
        let coordinator = coordinators[stream_idx].clone();

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

        let successful = successful_txns.clone();
        let stream_success = stream_successful[stream_idx].clone();
        let failed = failed_txns.clone();
        let retries = total_retries.clone();
        let latency_tracker = total_latency_us.clone();

        tasks.spawn(async move {
            let task_start = Instant::now();
            let args = vec![json!({
                "id": format!("record_{:08}", i + WARMUP_TRANSACTIONS),
                "content": format!("Benchmark content for record {}", i),
                "timestamp": (i + WARMUP_TRANSACTIONS) as i64,
                "stream_id": stream_idx as i64
            })];

            // Retry loop with exponential backoff
            let mut retry_count = 0;
            const MAX_RETRIES: u32 = 10;

            loop {
                let executor = match coordinator
                    .begin_read_write(
                        Duration::from_secs(5),
                        args.clone(),
                        format!("sql_insert_{}", stream_idx),
                    )
                    .await
                {
                    Ok(t) => Arc::new(t),
                    Err(e) => {
                        eprintln!("Failed to begin transaction {}: {:?}", i, e);
                        failed.fetch_add(1, Ordering::Relaxed);
                        return;
                    }
                };

                // Execute the transaction
                let result = execute_sql_insert(
                    i + WARMUP_TRANSACTIONS,
                    &args[0],
                    executor.clone(),
                    &stream_name,
                    &format!("benchmark_table_{}", stream_idx),
                )
                .await;

                match result {
                    Ok(_) => {
                        successful.fetch_add(1, Ordering::Relaxed);
                        stream_success.fetch_add(1, Ordering::Relaxed);
                        let task_duration = task_start.elapsed();
                        latency_tracker
                            .fetch_add(task_duration.as_micros() as usize, Ordering::Relaxed);
                        return;
                    }
                    Err(e) => {
                        let error_str = e.to_string();

                        // Always abort the failed transaction
                        let _ = executor.cancel().await;

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
    let monitor_stream_successful = stream_successful.clone();
    let monitor_handle = tokio::spawn(async move {
        let mut last_count = 0;
        loop {
            tokio::time::sleep(Duration::from_secs(2)).await;

            let success = monitor_successful.load(Ordering::Relaxed);
            let fail = monitor_failed.load(Ordering::Relaxed);
            let total = success + fail;

            if total >= NUM_TRANSACTIONS {
                break;
            }

            if total > last_count + 5000 {
                // Show per-stream progress
                let stream_counts: Vec<usize> = monitor_stream_successful
                    .iter()
                    .map(|s| s.load(Ordering::Relaxed))
                    .collect();

                println!(
                    "[{:6}/{:6}] Success: {} Failed: {} | Per-stream: {:?}",
                    total, NUM_TRANSACTIONS, success, fail, stream_counts
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

    // Get per-stream statistics
    let stream_counts: Vec<usize> = stream_successful
        .iter()
        .map(|s| s.load(Ordering::Relaxed))
        .collect();

    println!("\n=== Benchmark Results ===");
    println!("Streams used:            {}", num_streams);
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
        "Per-stream throughput:   {:.0} txns/second",
        (final_successful as f64 / total_seconds) / num_streams as f64
    );
    println!(
        "Avg latency:             {:.3} ms/transaction (actual)",
        avg_latency_ms
    );
    println!(
        "Amortized time:          {:.3} ms/transaction",
        (total_seconds * 1000.0) / final_successful as f64
    );

    println!("\n=== Per-Stream Statistics ===");
    for (i, count) in stream_counts.iter().enumerate() {
        println!(
            "Stream {}: {} transactions ({:.1}%)",
            i,
            count,
            (*count as f64 / final_successful as f64) * 100.0
        );
    }

    if final_retries > 0 {
        println!(
            "\nAvg retries per txn:     {:.2}",
            final_retries as f64 / NUM_TRANSACTIONS as f64
        );
    }

    // Verification phase - check each stream
    println!("\n=== Verification Phase ===");
    for (i, stream_name) in stream_names.iter().enumerate() {
        let verify_executor = Arc::new(
            coordinators[i]
                .begin_read_only(vec![], format!("verification_{}", i))
                .await?,
        );

        let sql_verify = SqlClient::new(verify_executor.clone());

        // Check SQL count
        let count_result = sql_verify
            .select(
                stream_name,
                &format!("benchmark_table_{}", i),
                &["COUNT(*) as cnt"],
                None,
            )
            .await?;

        if let Some(count) = count_result.column_values("cnt").first() {
            println!("✓ Stream {} contains {} rows", i, count);
        }

        verify_executor.finish().await?;
    }

    println!("\n✓ Multi-stream benchmark complete!");
    Ok(())
}

/// Execute a SQL insert transaction
async fn execute_sql_insert<E>(
    _index: usize,
    args: &serde_json::Value,
    executor: Arc<E>,
    stream_name: &str,
    table_name: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    E: proven_coordinator::Executor + Send + Sync + 'static,
{
    let sql = SqlClient::new(executor.clone());

    // Extract values from args
    let id = args["id"].as_str().unwrap_or("default_id");
    let content = args["content"].as_str().unwrap_or("default_content");
    let timestamp = args["timestamp"].as_i64().unwrap_or(0);
    let stream_id = args["stream_id"].as_i64().unwrap_or(0);

    // Execute SQL insert
    sql.insert_with_params(
        stream_name,
        table_name,
        &["id", "content", "timestamp", "stream_id"],
        vec![
            SqlValue::string(id.to_string()),
            SqlValue::string(content.to_string()),
            SqlValue::integer(timestamp),
            SqlValue::integer(stream_id),
        ],
    )
    .await?;

    executor.finish().await?;
    Ok(())
}
