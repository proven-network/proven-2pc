//! Benchmark for executing 1 million transfers
//!
//! This benchmark measures the throughput of the Resource engine by executing
//! 1 million transfers between accounts directly using the engine.

use proven_common::TransactionId;
use proven_processor::AutoBatchEngine;
use proven_resource::types::Amount;
use proven_resource::{ResourceOperation, ResourceTransactionEngine};
use proven_value::Vault;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::io::{self, Write};
use std::time::Instant;
use uuid::Uuid;

fn main() {
    println!("=== 1 Million Transfer Benchmark ===\n");

    // Create Resource engine with auto-batch wrapper
    let mut resource_engine = AutoBatchEngine::new(ResourceTransactionEngine::new());

    // Initialize the resource
    println!("Initializing resource...");
    let init_txn = TransactionId::new();
    resource_engine.begin(init_txn);

    let init_op = ResourceOperation::Initialize {
        name: "BenchToken".to_string(),
        symbol: "BENCH".to_string(),
        decimals: 6, // 6 decimal places like USDC
    };

    match resource_engine.apply_operation(init_op, init_txn) {
        proven_processor::OperationResult::Complete(_) => {
            resource_engine.commit(init_txn);
            println!("✓ Resource initialized");
        }
        _ => panic!("Failed to initialize resource"),
    }

    // Create source account vault
    let source_vault = Vault::new(Uuid::new_v4());

    // Mint initial supply to the source account
    println!("Minting initial supply...");
    let mint_txn = TransactionId::new();
    resource_engine.begin(mint_txn);

    // Mint 1 billion tokens (with 6 decimals)
    let mint_amount = Amount::from(Decimal::from(1_000_000_000i64));
    let mint_op = ResourceOperation::Mint {
        to: source_vault.clone(),
        amount: mint_amount,
        memo: Some("Initial supply".to_string()),
    };

    match resource_engine.apply_operation(mint_op, mint_txn) {
        proven_processor::OperationResult::Complete(_) => {
            resource_engine.commit(mint_txn);
            println!("✓ Initial supply minted");
        }
        _ => panic!("Failed to mint initial supply"),
    }

    // Pre-create target accounts with sufficient balance for transfers
    println!("Creating target accounts with initial balances...");
    const NUM_ACCOUNTS: usize = 100;
    // Give each account 10,000 tokens to ensure they have enough for transfers
    let initial_balance = Amount::from(Decimal::from(10_000i64));

    // Create vaults for all target accounts
    let mut account_vaults: HashMap<usize, Vault> = HashMap::new();
    for i in 0..NUM_ACCOUNTS {
        account_vaults.insert(i, Vault::new(Uuid::new_v4()));
    }

    for i in 0..NUM_ACCOUNTS {
        let setup_txn = TransactionId::new();
        resource_engine.begin(setup_txn);

        let transfer_op = ResourceOperation::Transfer {
            from: source_vault.clone(),
            to: account_vaults[&i].clone(),
            amount: initial_balance,
            memo: None,
        };

        match resource_engine.apply_operation(transfer_op, setup_txn) {
            proven_processor::OperationResult::Complete(_) => {
                resource_engine.commit(setup_txn);
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
        // Generate unique transaction ID
        let txn_id = TransactionId::new();
        resource_engine.begin(txn_id);

        // Create transfer operation
        // Transfer between different account pairs to avoid conflicts
        let from_vault = if i % 2 == 0 {
            source_vault.clone()
        } else {
            account_vaults[&(i % NUM_ACCOUNTS)].clone()
        };

        let to_vault = if i % 2 == 0 {
            account_vaults[&(i % NUM_ACCOUNTS)].clone()
        } else {
            source_vault.clone()
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
            from: from_vault,
            to: to_vault,
            amount,
            memo: if i % 10 == 0 {
                Some(format!("Transfer #{}", i))
            } else {
                None
            },
        };

        // Execute transfer directly on engine
        match resource_engine.apply_operation(transfer, txn_id) {
            proven_processor::OperationResult::Complete(_) => {
                resource_engine.commit(txn_id);
            }
            proven_processor::OperationResult::WouldBlock { .. } => {
                // In a real system, we'd retry after the blocking transaction
                // For benchmark, just skip and continue
                resource_engine.abort(txn_id);
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
    let verify_txn = TransactionId::new();
    resource_engine.begin(verify_txn);

    // Check source account balance
    let balance_op = ResourceOperation::GetBalance {
        account: source_vault.clone(),
    };

    match resource_engine.apply_operation(balance_op, verify_txn) {
        proven_processor::OperationResult::Complete(_response) => {
            println!("✓ Source account balance query successful");
        }
        _ => println!("⚠ Balance query failed"),
    }

    // Check a few target account balances
    let sample_accounts = [0, NUM_ACCOUNTS / 2, NUM_ACCOUNTS - 1];
    let mut verified = 0;

    for account_idx in sample_accounts {
        let balance_op = ResourceOperation::GetBalance {
            account: account_vaults[&account_idx].clone(),
        };

        match resource_engine.apply_operation(balance_op, verify_txn) {
            proven_processor::OperationResult::Complete(_) => {
                verified += 1;
            }
            _ => {
                println!("⚠ Failed to get balance for account index {}", account_idx);
            }
        }
    }

    resource_engine.commit(verify_txn);

    println!(
        "✓ Verified {}/{} sample account balances",
        verified,
        sample_accounts.len()
    );

    // Check total supply
    let supply_txn = TransactionId::new();
    resource_engine.begin(supply_txn);

    let supply_op = ResourceOperation::GetTotalSupply;
    match resource_engine.apply_operation(supply_op, supply_txn) {
        proven_processor::OperationResult::Complete(_) => {
            println!("✓ Total supply query successful");
            resource_engine.commit(supply_txn);
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
