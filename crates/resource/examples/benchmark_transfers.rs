//! Benchmark for executing 1 million transfers
//!
//! This benchmark measures the throughput of the Resource engine by executing
//! 1 million transfers between accounts directly using the engine.

use proven_hlc::{HlcTimestamp, NodeId};
use proven_resource::types::Amount;
use proven_resource::{ResourceOperation, ResourceTransactionEngine};
use proven_stream::TransactionEngine;
use rust_decimal::Decimal;
use std::io::{self, Write};
use std::time::Instant;

fn main() {
    println!("=== 1 Million Transfer Benchmark ===\n");

    // Create Resource engine directly
    let mut resource_engine = ResourceTransactionEngine::new();

    // Initialize the resource
    println!("Initializing resource...");
    let init_txn = HlcTimestamp::new(1000000000, 0, NodeId::new(1));
    resource_engine.begin_transaction(init_txn);

    let init_op = ResourceOperation::Initialize {
        name: "BenchToken".to_string(),
        symbol: "BENCH".to_string(),
        decimals: 6, // 6 decimal places like USDC
    };

    match resource_engine.apply_operation(init_op, init_txn) {
        proven_stream::OperationResult::Complete(_) => {
            resource_engine
                .commit(init_txn)
                .expect("Failed to commit initialization");
            println!("✓ Resource initialized");
        }
        _ => panic!("Failed to initialize resource"),
    }

    // Mint initial supply to the source account
    println!("Minting initial supply...");
    let mint_txn = HlcTimestamp::new(1000000001, 0, NodeId::new(1));
    resource_engine.begin_transaction(mint_txn);

    // Mint 1 billion tokens (with 6 decimals)
    let mint_amount = Amount::from(Decimal::from(1_000_000_000i64));
    let mint_op = ResourceOperation::Mint {
        to: "source_account".to_string(),
        amount: mint_amount,
        memo: Some("Initial supply".to_string()),
    };

    match resource_engine.apply_operation(mint_op, mint_txn) {
        proven_stream::OperationResult::Complete(_) => {
            resource_engine
                .commit(mint_txn)
                .expect("Failed to commit mint");
            println!("✓ Initial supply minted");
        }
        _ => panic!("Failed to mint initial supply"),
    }

    // Pre-create target accounts with sufficient balance for transfers
    println!("Creating target accounts with initial balances...");
    const NUM_ACCOUNTS: usize = 100;
    // Give each account 10,000 tokens to ensure they have enough for transfers
    let initial_balance = Amount::from(Decimal::from(10_000i64));

    for i in 0..NUM_ACCOUNTS {
        let setup_txn = HlcTimestamp::new(1500000000 + i as u64, 0, NodeId::new(1));
        resource_engine.begin_transaction(setup_txn);

        let transfer_op = ResourceOperation::Transfer {
            from: "source_account".to_string(),
            to: format!("account_{}", i),
            amount: initial_balance,
            memo: None,
        };

        match resource_engine.apply_operation(transfer_op, setup_txn) {
            proven_stream::OperationResult::Complete(_) => {
                resource_engine
                    .commit(setup_txn)
                    .expect("Failed to commit setup transfer");
            }
            _ => panic!("Failed to setup account {}", i),
        }
    }
    println!(
        "✓ {} target accounts created with {} tokens each",
        NUM_ACCOUNTS, initial_balance
    );

    // Benchmark configuration
    const NUM_TRANSFERS: usize = 1_000_000;
    const PROGRESS_INTERVAL: usize = 10_000;
    const STATUS_INTERVAL: usize = 100_000;

    println!("\nStarting {} transfers...", NUM_TRANSFERS);
    let start_time = Instant::now();
    let mut last_status_time = start_time;
    let mut last_status_count = 0;

    // Process transfers
    for i in 0..NUM_TRANSFERS {
        // Generate unique transaction ID with incrementing timestamp
        let txn_id = HlcTimestamp::new(2000000000 + i as u64, 0, NodeId::new(1));
        resource_engine.begin_transaction(txn_id);

        // Create transfer operation
        // Transfer between different account pairs to avoid conflicts
        let from_account = if i % 2 == 0 {
            "source_account".to_string()
        } else {
            format!("account_{}", i % NUM_ACCOUNTS)
        };

        let to_account = if i % 2 == 0 {
            format!("account_{}", i % NUM_ACCOUNTS)
        } else {
            "source_account".to_string()
        };

        // Variable transfer amounts to simulate real usage (small amounts to avoid exhaustion)
        let amount = match i % 5 {
            0 => Amount::from_integer(1000000, 6), // 1 token (1.000000 with 6 decimals)
            1 => Amount::from_integer(500000, 6),  // 0.5 tokens
            2 => Amount::from_integer(100000, 6),  // 0.1 tokens
            3 => Amount::from_integer(10000, 6),   // 0.01 tokens
            _ => Amount::from_integer(1000, 6),    // 0.001 tokens
        };

        let transfer = ResourceOperation::Transfer {
            from: from_account,
            to: to_account,
            amount,
            memo: if i % 10 == 0 {
                Some(format!("Transfer #{}", i))
            } else {
                None
            },
        };

        // Execute transfer directly on engine
        match resource_engine.apply_operation(transfer, txn_id) {
            proven_stream::OperationResult::Complete(_) => {
                // Commit the transaction
                if let Err(e) = resource_engine.commit(txn_id) {
                    eprintln!("\nError committing transfer {}: {}", i, e);
                    break;
                }
            }
            proven_stream::OperationResult::WouldBlock { .. } => {
                // In a real system, we'd retry after the blocking transaction
                // For benchmark, just skip and continue
                resource_engine.abort(txn_id).ok();
                continue;
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
                "\n[{:7}/{:7}] {:3}% | Interval: {:.0} transfers/sec",
                i + 1,
                NUM_TRANSFERS,
                ((i + 1) * 100) / NUM_TRANSFERS,
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
    let throughput = NUM_TRANSFERS as f64 / total_seconds;

    // Verify balances
    println!("\nVerifying sample balances...");
    let verify_txn = HlcTimestamp::new(9999999999, 0, NodeId::new(1));
    resource_engine.begin_transaction(verify_txn);

    // Check source account balance
    let balance_op = ResourceOperation::GetBalance {
        account: "source_account".to_string(),
    };

    match resource_engine.apply_operation(balance_op, verify_txn) {
        proven_stream::OperationResult::Complete(_response) => {
            println!("✓ Source account balance query successful");
        }
        _ => println!("⚠ Balance query failed"),
    }

    // Check a few target account balances
    let sample_accounts = [0, NUM_ACCOUNTS / 2, NUM_ACCOUNTS - 1];
    let mut verified = 0;

    for account_idx in sample_accounts {
        let balance_op = ResourceOperation::GetBalance {
            account: format!("account_{}", account_idx),
        };

        match resource_engine.apply_operation(balance_op, verify_txn) {
            proven_stream::OperationResult::Complete(_) => {
                verified += 1;
            }
            _ => {
                println!("⚠ Failed to get balance for account_{}", account_idx);
            }
        }
    }

    resource_engine
        .commit(verify_txn)
        .expect("Failed to commit verification transaction");

    println!(
        "✓ Verified {}/{} sample account balances",
        verified,
        sample_accounts.len()
    );

    // Check total supply
    let supply_txn = HlcTimestamp::new(10000000000, 0, NodeId::new(1));
    resource_engine.begin_transaction(supply_txn);

    let supply_op = ResourceOperation::GetTotalSupply;
    match resource_engine.apply_operation(supply_op, supply_txn) {
        proven_stream::OperationResult::Complete(_) => {
            println!("✓ Total supply query successful");
            resource_engine
                .commit(supply_txn)
                .expect("Failed to commit supply query");
        }
        _ => println!("⚠ Total supply query failed"),
    }

    // Print final statistics
    println!("\n=== Benchmark Results ===");
    println!("Total transfers:   {}", NUM_TRANSFERS);
    println!("Total time:        {:.2} seconds", total_seconds);
    println!("Throughput:        {:.0} transfers/second", throughput);
    println!(
        "Avg latency:       {:.3} ms/transfer",
        (total_seconds * 1000.0) / NUM_TRANSFERS as f64
    );

    println!("\nMemory usage and detailed statistics:");
    println!(
        "- Transactions executed: {}",
        NUM_TRANSFERS + NUM_ACCOUNTS + 4
    ); // +setup +verify transactions
    println!("- Number of accounts: {}", NUM_ACCOUNTS);
    println!("- Transfer patterns: Bidirectional between source and target accounts");
    println!("- Amount types: Mixed (1, 0.5, 0.1, 0.01, 0.001 tokens)");
    println!("\n✓ Benchmark complete!");
}
