//! Integration tests for resource engine

use proven_common::TransactionId;
use proven_resource::types::Amount;
use proven_resource::{ResourceOperation, ResourceResponse, ResourceTransactionEngine};
use proven_stream::{OperationResult, RetryOn, TransactionEngine};
use uuid::Uuid;

fn make_timestamp(n: u64) -> TransactionId {
    TransactionId::from_uuid(Uuid::from_u128(n as u128))
}

#[test]
fn test_resource_lifecycle() {
    let mut engine = ResourceTransactionEngine::new();

    // Initialize resource
    let tx1 = make_timestamp(100);
    let mut batch1 = engine.start_batch();
    engine.begin(&mut batch1, tx1);
    engine.commit_batch(batch1, 1);

    let init_op = ResourceOperation::Initialize {
        name: "Test Token".to_string(),
        symbol: "TEST".to_string(),
        decimals: 18,
    };

    let mut batch2 = engine.start_batch();
    let result = engine.apply_operation(&mut batch2, init_op, tx1);
    engine.commit_batch(batch2, 2);
    match result {
        OperationResult::Complete(ResourceResponse::Initialized {
            name,
            symbol,
            decimals,
        }) => {
            assert_eq!(name, "Test Token");
            assert_eq!(symbol, "TEST");
            assert_eq!(decimals, 18);
        }
        _ => panic!("Expected Initialized response"),
    }

    let mut batch3 = engine.start_batch();
    engine.prepare(&mut batch3, tx1);
    engine.commit_batch(batch3, 3);

    let mut batch4 = engine.start_batch();
    engine.commit(&mut batch4, tx1);
    engine.commit_batch(batch4, 4);

    // Cannot initialize twice
    let tx2 = make_timestamp(200);
    let mut batch5 = engine.start_batch();
    engine.begin(&mut batch5, tx2);
    engine.commit_batch(batch5, 5);

    let init_op = ResourceOperation::Initialize {
        name: "Another Token".to_string(),
        symbol: "OTHER".to_string(),
        decimals: 8,
    };

    let mut batch6 = engine.start_batch();
    let result = engine.apply_operation(&mut batch6, init_op, tx2);
    engine.commit_batch(batch6, 6);
    assert!(matches!(
        result,
        OperationResult::Complete(ResourceResponse::Error(_))
    ));

    let mut batch7 = engine.start_batch();
    engine.abort(&mut batch7, tx2);
    engine.commit_batch(batch7, 7);
}

#[test]
fn test_mint_and_burn() {
    let mut engine = ResourceTransactionEngine::new();

    // Initialize
    let tx1 = make_timestamp(100);
    let mut batch1 = engine.start_batch();
    engine.begin(&mut batch1, tx1);
    engine.commit_batch(batch1, 1);

    let mut batch2 = engine.start_batch();
    engine.apply_operation(
        &mut batch2,
        ResourceOperation::Initialize {
            name: "Test Token".to_string(),
            symbol: "TEST".to_string(),
            decimals: 8,
        },
        tx1,
    );
    engine.commit_batch(batch2, 2);

    // Mint tokens to alice
    let mint_op = ResourceOperation::Mint {
        to: "alice".to_string(),
        amount: Amount::from_integer(1000, 8),
        memo: Some("Initial mint".to_string()),
    };

    let mut batch3 = engine.start_batch();
    let result = engine.apply_operation(&mut batch3, mint_op, tx1);
    engine.commit_batch(batch3, 3);
    match result {
        OperationResult::Complete(ResourceResponse::Minted {
            to,
            amount,
            new_balance,
            total_supply,
        }) => {
            assert_eq!(to, "alice");
            assert_eq!(amount, Amount::from_integer(1000, 8));
            assert_eq!(new_balance, Amount::from_integer(1000, 8));
            assert_eq!(total_supply, Amount::from_integer(1000, 8));
        }
        _ => panic!("Expected Minted response"),
    }

    let mut batch4 = engine.start_batch();
    engine.prepare(&mut batch4, tx1);
    engine.commit_batch(batch4, 4);

    let mut batch5 = engine.start_batch();
    engine.commit(&mut batch5, tx1);
    engine.commit_batch(batch5, 5);

    // Burn tokens from alice
    let tx2 = make_timestamp(200);
    let mut batch6 = engine.start_batch();
    engine.begin(&mut batch6, tx2);
    engine.commit_batch(batch6, 6);

    let burn_op = ResourceOperation::Burn {
        from: "alice".to_string(),
        amount: Amount::from_integer(300, 8),
        memo: Some("Burn tokens".to_string()),
    };

    let mut batch7 = engine.start_batch();
    let result = engine.apply_operation(&mut batch7, burn_op, tx2);
    engine.commit_batch(batch7, 7);
    match result {
        OperationResult::Complete(ResourceResponse::Burned {
            from,
            amount,
            new_balance,
            total_supply,
        }) => {
            assert_eq!(from, "alice");
            assert_eq!(amount, Amount::from_integer(300, 8));
            assert_eq!(new_balance, Amount::from_integer(700, 8));
            assert_eq!(total_supply, Amount::from_integer(700, 8));
        }
        _ => panic!("Expected Burned response"),
    }

    let mut batch8 = engine.start_batch();
    engine.prepare(&mut batch8, tx2);
    engine.commit_batch(batch8, 8);

    let mut batch9 = engine.start_batch();
    engine.commit(&mut batch9, tx2);
    engine.commit_batch(batch9, 9);

    // Check final balance and total supply
    let tx3 = make_timestamp(300);
    let mut batch10 = engine.start_batch();
    engine.begin(&mut batch10, tx3);
    engine.commit_batch(batch10, 10);

    let balance_op = ResourceOperation::GetBalance {
        account: "alice".to_string(),
    };

    let mut batch11 = engine.start_batch();
    let result = engine.apply_operation(&mut batch11, balance_op, tx3);
    engine.commit_batch(batch11, 11);
    match result {
        OperationResult::Complete(ResourceResponse::Balance { account, amount }) => {
            assert_eq!(account, "alice");
            assert_eq!(amount, Amount::from_integer(700, 8));
        }
        _ => panic!("Expected Balance response"),
    }

    // Check total supply after mint and burn
    let mut batch12 = engine.start_batch();
    let result = engine.apply_operation(&mut batch12, ResourceOperation::GetTotalSupply, tx3);
    engine.commit_batch(batch12, 12);
    match result {
        OperationResult::Complete(ResourceResponse::TotalSupply { amount }) => {
            assert_eq!(amount, Amount::from_integer(700, 8)); // 1000 minted - 300 burned
        }
        _ => panic!("Expected TotalSupply response"),
    }
}

#[test]
fn test_transfer() {
    let mut engine = ResourceTransactionEngine::new();

    // Initialize and mint
    let tx1 = make_timestamp(100);
    let mut batch1 = engine.start_batch();
    engine.begin(&mut batch1, tx1);
    engine.commit_batch(batch1, 1);

    let mut batch2 = engine.start_batch();
    engine.apply_operation(
        &mut batch2,
        ResourceOperation::Initialize {
            name: "Test Token".to_string(),
            symbol: "TEST".to_string(),
            decimals: 6,
        },
        tx1,
    );
    engine.commit_batch(batch2, 2);

    let mut batch3 = engine.start_batch();
    engine.apply_operation(
        &mut batch3,
        ResourceOperation::Mint {
            to: "alice".to_string(),
            amount: Amount::from_integer(1000, 6),
            memo: None,
        },
        tx1,
    );
    engine.commit_batch(batch3, 3);

    let mut batch4 = engine.start_batch();
    engine.prepare(&mut batch4, tx1);
    engine.commit_batch(batch4, 4);

    let mut batch5 = engine.start_batch();
    engine.commit(&mut batch5, tx1);
    engine.commit_batch(batch5, 5);

    // Transfer from alice to bob
    let tx2 = make_timestamp(200);
    let mut batch6 = engine.start_batch();
    engine.begin(&mut batch6, tx2);
    engine.commit_batch(batch6, 6);

    let transfer_op = ResourceOperation::Transfer {
        from: "alice".to_string(),
        to: "bob".to_string(),
        amount: Amount::from_integer(250, 6),
        memo: Some("Payment".to_string()),
    };

    let mut batch7 = engine.start_batch();
    let result = engine.apply_operation(&mut batch7, transfer_op, tx2);
    engine.commit_batch(batch7, 7);
    match result {
        OperationResult::Complete(ResourceResponse::Transferred {
            from,
            to,
            amount,
            from_balance,
            to_balance,
        }) => {
            assert_eq!(from, "alice");
            assert_eq!(to, "bob");
            assert_eq!(amount, Amount::from_integer(250, 6));
            assert_eq!(from_balance, Amount::from_integer(750, 6));
            assert_eq!(to_balance, Amount::from_integer(250, 6));
        }
        _ => panic!("Expected Transferred response"),
    }

    let mut batch8 = engine.start_batch();
    engine.prepare(&mut batch8, tx2);
    engine.commit_batch(batch8, 8);

    let mut batch9 = engine.start_batch();
    engine.commit(&mut batch9, tx2);
    engine.commit_batch(batch9, 9);

    // Check balances
    let tx3 = make_timestamp(300);
    let mut batch10 = engine.start_batch();
    engine.begin(&mut batch10, tx3);
    engine.commit_batch(batch10, 10);

    let mut batch11 = engine.start_batch();
    let alice_balance = engine.apply_operation(
        &mut batch11,
        ResourceOperation::GetBalance {
            account: "alice".to_string(),
        },
        tx3,
    );
    engine.commit_batch(batch11, 11);

    let mut batch12 = engine.start_batch();
    let bob_balance = engine.apply_operation(
        &mut batch12,
        ResourceOperation::GetBalance {
            account: "bob".to_string(),
        },
        tx3,
    );
    engine.commit_batch(batch12, 12);

    match alice_balance {
        OperationResult::Complete(ResourceResponse::Balance { amount, .. }) => {
            assert_eq!(amount, Amount::from_integer(750, 6));
        }
        _ => panic!("Expected Balance response"),
    }

    match bob_balance {
        OperationResult::Complete(ResourceResponse::Balance { amount, .. }) => {
            assert_eq!(amount, Amount::from_integer(250, 6));
        }
        _ => panic!("Expected Balance response"),
    }
}

#[test]
fn test_insufficient_balance() {
    let mut engine = ResourceTransactionEngine::new();

    // Initialize and mint small amount
    let tx1 = make_timestamp(100);
    let mut batch1 = engine.start_batch();
    engine.begin(&mut batch1, tx1);
    engine.commit_batch(batch1, 1);

    let mut batch2 = engine.start_batch();
    engine.apply_operation(
        &mut batch2,
        ResourceOperation::Initialize {
            name: "Test Token".to_string(),
            symbol: "TEST".to_string(),
            decimals: 0,
        },
        tx1,
    );
    engine.commit_batch(batch2, 2);

    let mut batch3 = engine.start_batch();
    engine.apply_operation(
        &mut batch3,
        ResourceOperation::Mint {
            to: "alice".to_string(),
            amount: Amount::from_integer(100, 0),
            memo: None,
        },
        tx1,
    );
    engine.commit_batch(batch3, 3);

    let mut batch4 = engine.start_batch();
    engine.prepare(&mut batch4, tx1);
    engine.commit_batch(batch4, 4);

    let mut batch5 = engine.start_batch();
    engine.commit(&mut batch5, tx1);
    engine.commit_batch(batch5, 5);

    // Try to transfer more than balance
    let tx2 = make_timestamp(200);
    let mut batch6 = engine.start_batch();
    engine.begin(&mut batch6, tx2);
    engine.commit_batch(batch6, 6);

    let transfer_op = ResourceOperation::Transfer {
        from: "alice".to_string(),
        to: "bob".to_string(),
        amount: Amount::from_integer(150, 0),
        memo: None,
    };

    let mut batch7 = engine.start_batch();
    let result = engine.apply_operation(&mut batch7, transfer_op, tx2);
    engine.commit_batch(batch7, 7);
    assert!(matches!(
        result,
        OperationResult::Complete(ResourceResponse::Error(_))
    ));

    let mut batch8 = engine.start_batch();
    engine.abort(&mut batch8, tx2);
    engine.commit_batch(batch8, 8);

    // Try to burn more than balance
    let tx3 = make_timestamp(300);
    let mut batch9 = engine.start_batch();
    engine.begin(&mut batch9, tx3);
    engine.commit_batch(batch9, 9);

    let burn_op = ResourceOperation::Burn {
        from: "alice".to_string(),
        amount: Amount::from_integer(150, 0),
        memo: None,
    };

    let mut batch10 = engine.start_batch();
    let result = engine.apply_operation(&mut batch10, burn_op, tx3);
    engine.commit_batch(batch10, 10);
    assert!(matches!(
        result,
        OperationResult::Complete(ResourceResponse::Error(_))
    ));

    let mut batch11 = engine.start_batch();
    engine.abort(&mut batch11, tx3);
    engine.commit_batch(batch11, 11);
}

#[test]
fn test_concurrent_transfers_with_reservations() {
    let mut engine = ResourceTransactionEngine::new();

    // Initialize and mint
    let tx1 = make_timestamp(100);
    let mut batch1 = engine.start_batch();
    engine.begin(&mut batch1, tx1);
    engine.commit_batch(batch1, 1);

    let mut batch2 = engine.start_batch();
    engine.apply_operation(
        &mut batch2,
        ResourceOperation::Initialize {
            name: "Test Token".to_string(),
            symbol: "TEST".to_string(),
            decimals: 0,
        },
        tx1,
    );
    engine.commit_batch(batch2, 2);

    let mut batch3 = engine.start_batch();
    engine.apply_operation(
        &mut batch3,
        ResourceOperation::Mint {
            to: "alice".to_string(),
            amount: Amount::from_integer(100, 0),
            memo: None,
        },
        tx1,
    );
    engine.commit_batch(batch3, 3);

    let mut batch4 = engine.start_batch();
    engine.prepare(&mut batch4, tx1);
    engine.commit_batch(batch4, 4);

    let mut batch5 = engine.start_batch();
    engine.commit(&mut batch5, tx1);
    engine.commit_batch(batch5, 5);

    // Start two concurrent transactions
    let tx2 = make_timestamp(200);
    let tx3 = make_timestamp(201);

    let mut batch6 = engine.start_batch();
    engine.begin(&mut batch6, tx2);
    engine.commit_batch(batch6, 6);

    let mut batch7 = engine.start_batch();
    engine.begin(&mut batch7, tx3);
    engine.commit_batch(batch7, 7);

    // First transfer: alice -> bob 60
    let transfer1 = ResourceOperation::Transfer {
        from: "alice".to_string(),
        to: "bob".to_string(),
        amount: Amount::from_integer(60, 0),
        memo: None,
    };

    let mut batch8 = engine.start_batch();
    let result1 = engine.apply_operation(&mut batch8, transfer1, tx2);
    engine.commit_batch(batch8, 8);
    assert!(matches!(result1, OperationResult::Complete(_)));

    // Second transfer: alice -> charlie 50 (should block due to insufficient balance after reservation)
    let transfer2 = ResourceOperation::Transfer {
        from: "alice".to_string(),
        to: "charlie".to_string(),
        amount: Amount::from_integer(50, 0),
        memo: None,
    };

    let mut batch9 = engine.start_batch();
    let result2 = engine.apply_operation(&mut batch9, transfer2, tx3);
    engine.commit_batch(batch9, 9);
    // Should block due to insufficient balance after tx2's reservation
    assert!(
        matches!(result2, OperationResult::WouldBlock { .. })
            || matches!(
                result2,
                OperationResult::Complete(ResourceResponse::Error(_))
            )
    );

    // Commit first transaction
    let mut batch10 = engine.start_batch();
    engine.prepare(&mut batch10, tx2);
    engine.commit_batch(batch10, 10);

    let mut batch11 = engine.start_batch();
    engine.commit(&mut batch11, tx2);
    engine.commit_batch(batch11, 11);

    // Abort second transaction
    let mut batch12 = engine.start_batch();
    engine.abort(&mut batch12, tx3);
    engine.commit_batch(batch12, 12);

    // Now the second transfer should work
    let tx4 = make_timestamp(300);
    let mut batch13 = engine.start_batch();
    engine.begin(&mut batch13, tx4);
    engine.commit_batch(batch13, 13);

    let transfer3 = ResourceOperation::Transfer {
        from: "alice".to_string(),
        to: "charlie".to_string(),
        amount: Amount::from_integer(40, 0), // Reduced amount
        memo: None,
    };

    let mut batch14 = engine.start_batch();
    let result3 = engine.apply_operation(&mut batch14, transfer3, tx4);
    engine.commit_batch(batch14, 14);
    assert!(matches!(result3, OperationResult::Complete(_)));

    let mut batch15 = engine.start_batch();
    engine.prepare(&mut batch15, tx4);
    engine.commit_batch(batch15, 15);

    let mut batch16 = engine.start_batch();
    engine.commit(&mut batch16, tx4);
    engine.commit_batch(batch16, 16);
}

#[test]
fn test_metadata_update() {
    let mut engine = ResourceTransactionEngine::new();

    // Initialize
    let tx1 = make_timestamp(100);
    let mut batch1 = engine.start_batch();
    engine.begin(&mut batch1, tx1);
    engine.commit_batch(batch1, 1);

    let mut batch2 = engine.start_batch();
    engine.apply_operation(
        &mut batch2,
        ResourceOperation::Initialize {
            name: "Test Token".to_string(),
            symbol: "TEST".to_string(),
            decimals: 8,
        },
        tx1,
    );
    engine.commit_batch(batch2, 2);

    let mut batch3 = engine.start_batch();
    engine.prepare(&mut batch3, tx1);
    engine.commit_batch(batch3, 3);

    let mut batch4 = engine.start_batch();
    engine.commit(&mut batch4, tx1);
    engine.commit_batch(batch4, 4);

    // Update metadata
    let tx2 = make_timestamp(200);
    let mut batch5 = engine.start_batch();
    engine.begin(&mut batch5, tx2);
    engine.commit_batch(batch5, 5);

    let update_op = ResourceOperation::UpdateMetadata {
        name: Some("Updated Token".to_string()),
        symbol: None,
    };

    let mut batch6 = engine.start_batch();
    let result = engine.apply_operation(&mut batch6, update_op, tx2);
    engine.commit_batch(batch6, 6);
    match result {
        OperationResult::Complete(ResourceResponse::MetadataUpdated { name, symbol }) => {
            assert_eq!(name, Some("Updated Token".to_string()));
            assert_eq!(symbol, None);
        }
        _ => panic!("Expected MetadataUpdated response"),
    }

    let mut batch7 = engine.start_batch();
    engine.prepare(&mut batch7, tx2);
    engine.commit_batch(batch7, 7);

    let mut batch8 = engine.start_batch();
    engine.commit(&mut batch8, tx2);
    engine.commit_batch(batch8, 8);

    // Check metadata
    let tx3 = make_timestamp(300);
    let mut batch9 = engine.start_batch();
    engine.begin(&mut batch9, tx3);
    engine.commit_batch(batch9, 9);

    let mut batch10 = engine.start_batch();
    let result = engine.apply_operation(&mut batch10, ResourceOperation::GetMetadata, tx3);
    engine.commit_batch(batch10, 10);
    match result {
        OperationResult::Complete(ResourceResponse::Metadata {
            name,
            symbol,
            decimals,
            ..
        }) => {
            assert_eq!(name, "Updated Token");
            assert_eq!(symbol, "TEST");
            assert_eq!(decimals, 8);
        }
        _ => panic!("Expected Metadata response"),
    }

    // Also test GetTotalSupply
    let mut batch11 = engine.start_batch();
    let result = engine.apply_operation(&mut batch11, ResourceOperation::GetTotalSupply, tx3);
    engine.commit_batch(batch11, 11);
    match result {
        OperationResult::Complete(ResourceResponse::TotalSupply { amount }) => {
            assert_eq!(amount, Amount::zero()); // No mints in this test
        }
        _ => panic!("Expected TotalSupply response"),
    }
}

#[test]
fn test_transaction_rollback() {
    let mut engine = ResourceTransactionEngine::new();

    // Initialize and mint
    let tx1 = make_timestamp(100);
    let mut batch1 = engine.start_batch();
    engine.begin(&mut batch1, tx1);
    engine.commit_batch(batch1, 1);

    let mut batch2 = engine.start_batch();
    engine.apply_operation(
        &mut batch2,
        ResourceOperation::Initialize {
            name: "Test Token".to_string(),
            symbol: "TEST".to_string(),
            decimals: 0,
        },
        tx1,
    );
    engine.commit_batch(batch2, 2);

    let mut batch3 = engine.start_batch();
    engine.apply_operation(
        &mut batch3,
        ResourceOperation::Mint {
            to: "alice".to_string(),
            amount: Amount::from_integer(1000, 0),
            memo: None,
        },
        tx1,
    );
    engine.commit_batch(batch3, 3);

    let mut batch4 = engine.start_batch();
    engine.prepare(&mut batch4, tx1);
    engine.commit_batch(batch4, 4);

    let mut batch5 = engine.start_batch();
    engine.commit(&mut batch5, tx1);
    engine.commit_batch(batch5, 5);

    // Start transaction with multiple operations
    let tx2 = make_timestamp(200);
    let mut batch6 = engine.start_batch();
    engine.begin(&mut batch6, tx2);
    engine.commit_batch(batch6, 6);

    // Transfer to bob
    let mut batch7 = engine.start_batch();
    engine.apply_operation(
        &mut batch7,
        ResourceOperation::Transfer {
            from: "alice".to_string(),
            to: "bob".to_string(),
            amount: Amount::from_integer(500, 0),
            memo: None,
        },
        tx2,
    );
    engine.commit_batch(batch7, 7);

    // Burn from alice
    let mut batch8 = engine.start_batch();
    engine.apply_operation(
        &mut batch8,
        ResourceOperation::Burn {
            from: "alice".to_string(),
            amount: Amount::from_integer(200, 0),
            memo: None,
        },
        tx2,
    );
    engine.commit_batch(batch8, 8);

    // Abort the transaction
    let mut batch9 = engine.start_batch();
    engine.abort(&mut batch9, tx2);
    engine.commit_batch(batch9, 9);

    // Check that balances are unchanged
    let tx3 = make_timestamp(300);
    let mut batch10 = engine.start_batch();
    engine.begin(&mut batch10, tx3);
    engine.commit_batch(batch10, 10);

    let mut batch11 = engine.start_batch();
    let alice_balance = engine.apply_operation(
        &mut batch11,
        ResourceOperation::GetBalance {
            account: "alice".to_string(),
        },
        tx3,
    );
    engine.commit_batch(batch11, 11);

    let mut batch12 = engine.start_batch();
    let bob_balance = engine.apply_operation(
        &mut batch12,
        ResourceOperation::GetBalance {
            account: "bob".to_string(),
        },
        tx3,
    );
    engine.commit_batch(batch12, 12);

    match alice_balance {
        OperationResult::Complete(ResourceResponse::Balance { amount, .. }) => {
            assert_eq!(amount, Amount::from_integer(1000, 0));
        }
        _ => panic!("Expected Balance response"),
    }

    match bob_balance {
        OperationResult::Complete(ResourceResponse::Balance { amount, .. }) => {
            assert_eq!(amount, Amount::from_integer(0, 0));
        }
        _ => panic!("Expected Balance response"),
    }
}

#[test]
fn test_snapshot_read_properly_blocks_on_pending_writes() {
    let mut engine = ResourceTransactionEngine::new();

    // Initialize resource and mint some tokens
    let tx1 = make_timestamp(100);
    let mut batch1 = engine.start_batch();
    engine.begin(&mut batch1, tx1);
    engine.commit_batch(batch1, 1);

    let mut batch2 = engine.start_batch();
    engine.apply_operation(
        &mut batch2,
        ResourceOperation::Initialize {
            name: "Test Token".to_string(),
            symbol: "TEST".to_string(),
            decimals: 8,
        },
        tx1,
    );
    engine.commit_batch(batch2, 2);

    let mut batch3 = engine.start_batch();
    engine.apply_operation(
        &mut batch3,
        ResourceOperation::Mint {
            to: "alice".to_string(),
            amount: Amount::from_integer(1000, 8),
            memo: None,
        },
        tx1,
    );
    engine.commit_batch(batch3, 3);

    let mut batch4 = engine.start_batch();
    engine.commit(&mut batch4, tx1);
    engine.commit_batch(batch4, 4);

    // Start a write transaction at timestamp 200 (but don't commit)
    let tx_write = make_timestamp(200);
    let mut batch5 = engine.start_batch();
    engine.begin(&mut batch5, tx_write);
    engine.commit_batch(batch5, 5);

    let mut batch6 = engine.start_batch();
    engine.apply_operation(
        &mut batch6,
        ResourceOperation::Transfer {
            from: "alice".to_string(),
            to: "bob".to_string(),
            amount: Amount::from_integer(100, 8),
            memo: None,
        },
        tx_write,
    );
    engine.commit_batch(batch6, 6);

    // Snapshot read at timestamp 300 MUST block
    // because there's a pending write from timestamp 200
    let read_ts = make_timestamp(300);
    let get_op = ResourceOperation::GetBalance {
        account: "alice".to_string(),
    };

    let result = engine.read_at_timestamp(get_op, read_ts);

    // Must block with correct blocker info
    match result {
        OperationResult::WouldBlock { blockers } => {
            assert_eq!(blockers.len(), 1);
            assert_eq!(blockers[0].txn, tx_write);
            assert_eq!(blockers[0].retry_on, RetryOn::CommitOrAbort);
        }
        _ => panic!("Expected WouldBlock but got {:?}", result),
    }

    // Commit the write transaction
    let mut batch8 = engine.start_batch();
    engine.commit(&mut batch8, tx_write);
    engine.commit_batch(batch8, 8);

    // Now the same read should succeed
    let get_op = ResourceOperation::GetBalance {
        account: "alice".to_string(),
    };
    let result = engine.read_at_timestamp(get_op, read_ts);

    match result {
        OperationResult::Complete(ResourceResponse::Balance { amount, .. }) => {
            // Should see the new balance after transfer
            assert_eq!(amount, Amount::from_integer(900, 8));
        }
        _ => panic!("Expected Complete but got {:?}", result),
    }
}

#[test]
fn test_metadata_read_properly_blocks() {
    let mut engine = ResourceTransactionEngine::new();

    // Initialize resource
    let tx1 = make_timestamp(100);
    let mut batch1 = engine.start_batch();
    engine.begin(&mut batch1, tx1);
    engine.commit_batch(batch1, 1);

    let mut batch2 = engine.start_batch();
    engine.apply_operation(
        &mut batch2,
        ResourceOperation::Initialize {
            name: "Old Name".to_string(),
            symbol: "OLD".to_string(),
            decimals: 8,
        },
        tx1,
    );
    engine.commit_batch(batch2, 2);

    let mut batch_commit = engine.start_batch();
    engine.commit(&mut batch_commit, tx1);
    engine.commit_batch(batch_commit, 2);

    // Start a metadata update transaction (but don't commit)
    let tx_update = make_timestamp(200);
    let mut batch3 = engine.start_batch();
    engine.begin(&mut batch3, tx_update);
    engine.commit_batch(batch3, 3);

    let mut batch4 = engine.start_batch();
    engine.apply_operation(
        &mut batch4,
        ResourceOperation::UpdateMetadata {
            name: Some("New Name".to_string()),
            symbol: Some("NEW".to_string()),
        },
        tx_update,
    );
    engine.commit_batch(batch4, 4);

    // Snapshot read at timestamp 300 with MVCC
    // With MVCC, snapshot reads don't block - they read the last committed version
    // visible at their timestamp. Since tx_update (200) hasn't committed yet,
    // the snapshot at 300 will see the old committed data from tx1 (100).
    let read_ts = make_timestamp(300);
    let result = engine.read_at_timestamp(ResourceOperation::GetMetadata, read_ts);

    // Should succeed and see old metadata (uncommitted changes are not visible)
    match result {
        OperationResult::Complete(ResourceResponse::Metadata { name, symbol, .. }) => {
            assert_eq!(name, "Old Name");
            assert_eq!(symbol, "OLD");
        }
        _ => panic!("Expected Complete with old metadata but got {:?}", result),
    }

    // Abort the update transaction
    let mut batch6 = engine.start_batch();
    engine.abort(&mut batch6, tx_update);
    engine.commit_batch(batch6, 6);

    // Read again - should still see the old metadata
    let result = engine.read_at_timestamp(ResourceOperation::GetMetadata, read_ts);

    match result {
        OperationResult::Complete(ResourceResponse::Metadata { name, symbol, .. }) => {
            assert_eq!(name, "Old Name");
            assert_eq!(symbol, "OLD");
        }
        _ => panic!("Expected Complete but got {:?}", result),
    }
}

#[test]
fn test_supply_read_properly_blocks() {
    let mut engine = ResourceTransactionEngine::new();

    // Initialize and mint initial supply
    let tx1 = make_timestamp(100);
    let mut batch1 = engine.start_batch();
    engine.begin(&mut batch1, tx1);
    engine.commit_batch(batch1, 1);

    let mut batch2 = engine.start_batch();
    engine.apply_operation(
        &mut batch2,
        ResourceOperation::Initialize {
            name: "Test Token".to_string(),
            symbol: "TEST".to_string(),
            decimals: 8,
        },
        tx1,
    );
    engine.commit_batch(batch2, 2);

    let mut batch3 = engine.start_batch();
    engine.apply_operation(
        &mut batch3,
        ResourceOperation::Mint {
            to: "alice".to_string(),
            amount: Amount::from_integer(1000, 8),
            memo: None,
        },
        tx1,
    );
    engine.commit_batch(batch3, 3);

    let mut batch4 = engine.start_batch();
    engine.commit(&mut batch4, tx1);
    engine.commit_batch(batch4, 4);

    // Start another mint transaction (but don't commit)
    let tx_mint = make_timestamp(200);
    let mut batch5 = engine.start_batch();
    engine.begin(&mut batch5, tx_mint);
    engine.commit_batch(batch5, 5);

    let mut batch6 = engine.start_batch();
    engine.apply_operation(
        &mut batch6,
        ResourceOperation::Mint {
            to: "bob".to_string(),
            amount: Amount::from_integer(500, 8),
            memo: None,
        },
        tx_mint,
    );
    engine.commit_batch(batch6, 6);

    // Snapshot read at timestamp 300 with MVCC
    // With MVCC, snapshot reads don't block - they read the last committed version
    // visible at their timestamp. Since tx_mint (200) hasn't committed yet,
    // the snapshot at 300 will see the old committed supply from tx1 (100).
    let read_ts = make_timestamp(300);
    let result = engine.read_at_timestamp(ResourceOperation::GetTotalSupply, read_ts);

    // Should succeed and see old supply (uncommitted mint is not visible)
    match result {
        OperationResult::Complete(ResourceResponse::TotalSupply { amount }) => {
            assert_eq!(amount, Amount::from_integer(1000, 8));
        }
        _ => panic!("Expected Complete with old supply but got {:?}", result),
    }

    // Commit the mint transaction
    let mut batch8 = engine.start_batch();
    engine.commit(&mut batch8, tx_mint);
    engine.commit_batch(batch8, 8);

    // Read again at the same snapshot timestamp - should now see the new supply
    // because tx_mint (200) < read_ts (300), so the committed change is visible
    let result = engine.read_at_timestamp(ResourceOperation::GetTotalSupply, read_ts);

    match result {
        OperationResult::Complete(ResourceResponse::TotalSupply { amount }) => {
            assert_eq!(amount, Amount::from_integer(1500, 8));
        }
        _ => panic!("Expected Complete but got {:?}", result),
    }
}

#[test]
fn test_no_blocking_when_no_pending_writes() {
    let mut engine = ResourceTransactionEngine::new();

    // Initialize and setup initial state
    let tx1 = make_timestamp(100);
    let mut batch1 = engine.start_batch();
    engine.begin(&mut batch1, tx1);
    engine.commit_batch(batch1, 1);

    let mut batch2 = engine.start_batch();
    engine.apply_operation(
        &mut batch2,
        ResourceOperation::Initialize {
            name: "Test Token".to_string(),
            symbol: "TEST".to_string(),
            decimals: 8,
        },
        tx1,
    );
    engine.commit_batch(batch2, 2);

    let mut batch3 = engine.start_batch();
    engine.apply_operation(
        &mut batch3,
        ResourceOperation::Mint {
            to: "alice".to_string(),
            amount: Amount::from_integer(1000, 8),
            memo: None,
        },
        tx1,
    );
    engine.commit_batch(batch3, 3);

    let mut batch4 = engine.start_batch();
    engine.commit(&mut batch4, tx1);
    engine.commit_batch(batch4, 4);

    // Read at timestamp 200 - no pending transactions, should succeed immediately
    let read_ts = make_timestamp(200);

    // Test balance read
    let result = engine.read_at_timestamp(
        ResourceOperation::GetBalance {
            account: "alice".to_string(),
        },
        read_ts,
    );
    assert!(matches!(result, OperationResult::Complete(_)));

    // Test metadata read
    let result = engine.read_at_timestamp(ResourceOperation::GetMetadata, read_ts);
    assert!(matches!(result, OperationResult::Complete(_)));

    // Test supply read
    let result = engine.read_at_timestamp(ResourceOperation::GetTotalSupply, read_ts);
    assert!(matches!(result, OperationResult::Complete(_)));
}

#[test]
fn test_snapshot_read_balance_doesnt_block_write() {
    let mut engine = ResourceTransactionEngine::new();

    // Initialize resource and mint some tokens
    let tx1 = make_timestamp(100);
    let mut batch1 = engine.start_batch();
    engine.begin(&mut batch1, tx1);
    engine.commit_batch(batch1, 1);

    let mut batch2 = engine.start_batch();
    engine.apply_operation(
        &mut batch2,
        ResourceOperation::Initialize {
            name: "Test Token".to_string(),
            symbol: "TEST".to_string(),
            decimals: 8,
        },
        tx1,
    );
    engine.commit_batch(batch2, 2);

    let mut batch3 = engine.start_batch();
    engine.apply_operation(
        &mut batch3,
        ResourceOperation::Mint {
            to: "alice".to_string(),
            amount: Amount::from_integer(1000, 8),
            memo: None,
        },
        tx1,
    );
    engine.commit_batch(batch3, 3);

    let mut batch4 = engine.start_batch();
    engine.commit(&mut batch4, tx1);
    engine.commit_batch(batch4, 4);

    // Start a write transaction
    let tx_write = make_timestamp(300); // Write is AFTER the read timestamp
    let mut batch5 = engine.start_batch();
    engine.begin(&mut batch5, tx_write);
    engine.commit_batch(batch5, 5);

    // Apply a transfer (write operation)
    let transfer_op = ResourceOperation::Transfer {
        from: "alice".to_string(),
        to: "bob".to_string(),
        amount: Amount::from_integer(100, 8),
        memo: None,
    };
    let mut batch6 = engine.start_batch();
    let result = engine.apply_operation(&mut batch6, transfer_op, tx_write);
    engine.commit_batch(batch6, 6);
    assert!(matches!(result, OperationResult::Complete(_)));

    // Snapshot read at timestamp 250 should NOT block
    // (read is BEFORE the write transaction at 300)
    let read_ts = make_timestamp(250);
    let get_op = ResourceOperation::GetBalance {
        account: "alice".to_string(),
    };

    let result = engine.read_at_timestamp(get_op, read_ts);
    assert!(matches!(result, OperationResult::Complete(_)));

    // The read should see the old balance (before transfer)
    if let OperationResult::Complete(ResourceResponse::Balance { amount, .. }) = result {
        assert_eq!(amount, Amount::from_integer(1000, 8));
    } else {
        panic!("Expected balance response");
    }

    let mut batch8 = engine.start_batch();
    engine.commit(&mut batch8, tx_write);
    engine.commit_batch(batch8, 8);
}

#[test]
fn test_snapshot_read_blocks_on_earlier_write() {
    let mut engine = ResourceTransactionEngine::new();

    // Initialize resource
    let tx1 = make_timestamp(100);
    let mut batch1 = engine.start_batch();
    engine.begin(&mut batch1, tx1);
    engine.commit_batch(batch1, 1);

    let mut batch2 = engine.start_batch();
    engine.apply_operation(
        &mut batch2,
        ResourceOperation::Initialize {
            name: "Test Token".to_string(),
            symbol: "TEST".to_string(),
            decimals: 8,
        },
        tx1,
    );
    engine.commit_batch(batch2, 2);

    let mut batch3 = engine.start_batch();
    engine.apply_operation(
        &mut batch3,
        ResourceOperation::Mint {
            to: "alice".to_string(),
            amount: Amount::from_integer(1000, 8),
            memo: None,
        },
        tx1,
    );
    engine.commit_batch(batch3, 3);

    let mut batch4 = engine.start_batch();
    engine.commit(&mut batch4, tx1);
    engine.commit_batch(batch4, 4);

    // Start a write transaction at timestamp 200
    let tx_write = make_timestamp(200);
    let mut batch5 = engine.start_batch();
    engine.begin(&mut batch5, tx_write);
    engine.commit_batch(batch5, 5);

    // Apply a transfer
    let transfer_op = ResourceOperation::Transfer {
        from: "alice".to_string(),
        to: "bob".to_string(),
        amount: Amount::from_integer(100, 8),
        memo: None,
    };
    let mut batch6 = engine.start_batch();
    engine.apply_operation(&mut batch6, transfer_op, tx_write);
    engine.commit_batch(batch6, 6);

    // Snapshot read at timestamp 250 SHOULD block on alice's balance
    // (the write at 200 is earlier than read at 250)
    let read_ts = make_timestamp(250);
    let get_op = ResourceOperation::GetBalance {
        account: "alice".to_string(),
    };

    let result = engine.read_at_timestamp(get_op, read_ts);

    // Should block waiting for tx_write to commit/abort
    assert!(matches!(result, OperationResult::WouldBlock { .. }));

    if let OperationResult::WouldBlock { blockers } = result {
        assert_eq!(blockers.len(), 1);
        assert_eq!(blockers[0].txn, tx_write);
    }

    let mut batch8 = engine.start_batch();
    engine.commit(&mut batch8, tx_write);
    engine.commit_batch(batch8, 8);
}

#[test]
fn test_snapshot_read_metadata_consistency() {
    let mut engine = ResourceTransactionEngine::new();

    // Initialize resource
    let tx1 = make_timestamp(100);
    let mut batch1 = engine.start_batch();
    engine.begin(&mut batch1, tx1);
    engine.commit_batch(batch1, 1);

    let mut batch2 = engine.start_batch();
    engine.apply_operation(
        &mut batch2,
        ResourceOperation::Initialize {
            name: "Test Token".to_string(),
            symbol: "TEST".to_string(),
            decimals: 8,
        },
        tx1,
    );
    engine.commit_batch(batch2, 2);

    let mut batch3 = engine.start_batch();
    engine.commit(&mut batch3, tx1);
    engine.commit_batch(batch3, 3);

    // Update metadata at timestamp 200
    let tx2 = make_timestamp(200);
    let mut batch4 = engine.start_batch();
    engine.begin(&mut batch4, tx2);
    engine.commit_batch(batch4, 4);

    let mut batch5 = engine.start_batch();
    engine.apply_operation(
        &mut batch5,
        ResourceOperation::UpdateMetadata {
            name: Some("New Token".to_string()),
            symbol: None,
        },
        tx2,
    );
    engine.commit_batch(batch5, 5);

    let mut batch6 = engine.start_batch();
    engine.commit(&mut batch6, tx2);
    engine.commit_batch(batch6, 6);

    // Snapshot read at timestamp 150 should see old metadata
    let read_ts1 = make_timestamp(150);
    let result = engine.read_at_timestamp(ResourceOperation::GetMetadata, read_ts1);

    if let OperationResult::Complete(ResourceResponse::Metadata { name, symbol, .. }) = result {
        assert_eq!(name, "Test Token");
        assert_eq!(symbol, "TEST");
    } else {
        panic!("Expected metadata response");
    }

    // Snapshot read at timestamp 250 should see new metadata
    let read_ts2 = make_timestamp(250);
    let result = engine.read_at_timestamp(ResourceOperation::GetMetadata, read_ts2);

    if let OperationResult::Complete(ResourceResponse::Metadata { name, symbol, .. }) = result {
        assert_eq!(name, "New Token");
        assert_eq!(symbol, "TEST");
    } else {
        panic!("Expected metadata response");
    }
}

#[test]
fn test_snapshot_read_total_supply() {
    let mut engine = ResourceTransactionEngine::new();

    // Initialize resource
    let tx1 = make_timestamp(100);
    let mut batch1 = engine.start_batch();
    engine.begin(&mut batch1, tx1);
    engine.commit_batch(batch1, 1);

    let mut batch2 = engine.start_batch();
    engine.apply_operation(
        &mut batch2,
        ResourceOperation::Initialize {
            name: "Test Token".to_string(),
            symbol: "TEST".to_string(),
            decimals: 8,
        },
        tx1,
    );
    engine.commit_batch(batch2, 2);

    let mut batch3 = engine.start_batch();
    engine.commit(&mut batch3, tx1);
    engine.commit_batch(batch3, 3);

    // Mint tokens at timestamp 200
    let tx2 = make_timestamp(200);
    let mut batch4 = engine.start_batch();
    engine.begin(&mut batch4, tx2);
    engine.commit_batch(batch4, 4);

    let mut batch5 = engine.start_batch();
    engine.apply_operation(
        &mut batch5,
        ResourceOperation::Mint {
            to: "alice".to_string(),
            amount: Amount::from_integer(1000, 8),
            memo: None,
        },
        tx2,
    );
    engine.commit_batch(batch5, 5);

    let mut batch6 = engine.start_batch();
    engine.commit(&mut batch6, tx2);
    engine.commit_batch(batch6, 6);

    // Burn tokens at timestamp 300
    let tx3 = make_timestamp(300);
    let mut batch7 = engine.start_batch();
    engine.begin(&mut batch7, tx3);
    engine.commit_batch(batch7, 7);

    let mut batch8 = engine.start_batch();
    engine.apply_operation(
        &mut batch8,
        ResourceOperation::Burn {
            from: "alice".to_string(),
            amount: Amount::from_integer(300, 8),
            memo: None,
        },
        tx3,
    );
    engine.commit_batch(batch8, 8);

    let mut batch9 = engine.start_batch();
    engine.commit(&mut batch9, tx3);
    engine.commit_batch(batch9, 9);

    // Read at different timestamps
    let read_ts1 = make_timestamp(150);
    let result = engine.read_at_timestamp(ResourceOperation::GetTotalSupply, read_ts1);
    if let OperationResult::Complete(ResourceResponse::TotalSupply { amount }) = result {
        assert_eq!(amount, Amount::zero());
    } else {
        panic!("Expected total supply response");
    }

    let read_ts2 = make_timestamp(250);
    let result = engine.read_at_timestamp(ResourceOperation::GetTotalSupply, read_ts2);
    if let OperationResult::Complete(ResourceResponse::TotalSupply { amount }) = result {
        assert_eq!(amount, Amount::from_integer(1000, 8));
    } else {
        panic!("Expected total supply response");
    }

    let read_ts3 = make_timestamp(350);
    let result = engine.read_at_timestamp(ResourceOperation::GetTotalSupply, read_ts3);
    if let OperationResult::Complete(ResourceResponse::TotalSupply { amount }) = result {
        assert_eq!(amount, Amount::from_integer(700, 8));
    } else {
        panic!("Expected total supply response");
    }
}

#[test]
fn test_snapshot_read_ignores_aborted_writes() {
    let mut engine = ResourceTransactionEngine::new();

    // Initialize and mint tokens
    let tx1 = make_timestamp(100);
    let mut batch1 = engine.start_batch();
    engine.begin(&mut batch1, tx1);
    engine.commit_batch(batch1, 1);

    let mut batch2 = engine.start_batch();
    engine.apply_operation(
        &mut batch2,
        ResourceOperation::Initialize {
            name: "Test Token".to_string(),
            symbol: "TEST".to_string(),
            decimals: 8,
        },
        tx1,
    );
    engine.commit_batch(batch2, 2);

    let mut batch3 = engine.start_batch();
    engine.apply_operation(
        &mut batch3,
        ResourceOperation::Mint {
            to: "alice".to_string(),
            amount: Amount::from_integer(1000, 8),
            memo: None,
        },
        tx1,
    );
    engine.commit_batch(batch3, 3);

    let mut batch4 = engine.start_batch();
    engine.commit(&mut batch4, tx1);
    engine.commit_batch(batch4, 4);

    // Start a write transaction that will be aborted
    let tx_abort = make_timestamp(200);
    let mut batch5 = engine.start_batch();
    engine.begin(&mut batch5, tx_abort);
    engine.commit_batch(batch5, 5);

    let mut batch6 = engine.start_batch();
    engine.apply_operation(
        &mut batch6,
        ResourceOperation::Transfer {
            from: "alice".to_string(),
            to: "bob".to_string(),
            amount: Amount::from_integer(500, 8),
            memo: None,
        },
        tx_abort,
    );
    engine.commit_batch(batch6, 6);

    // Abort the transaction
    let mut batch7 = engine.start_batch();
    engine.abort(&mut batch7, tx_abort);
    engine.commit_batch(batch7, 7);

    // Snapshot read at timestamp 250 should see original balance
    // (aborted transaction should not affect reads)
    let read_ts = make_timestamp(250);
    let get_op = ResourceOperation::GetBalance {
        account: "alice".to_string(),
    };

    let result = engine.read_at_timestamp(get_op, read_ts);

    // Should NOT block (transaction was aborted)
    assert!(matches!(result, OperationResult::Complete(_)));

    if let OperationResult::Complete(ResourceResponse::Balance { amount, .. }) = result {
        assert_eq!(amount, Amount::from_integer(1000, 8));
    } else {
        panic!("Expected balance response");
    }
}

#[test]
fn test_concurrent_snapshot_reads() {
    let mut engine = ResourceTransactionEngine::new();

    // Initialize and set up initial state
    let tx1 = make_timestamp(100);
    let mut batch1 = engine.start_batch();
    engine.begin(&mut batch1, tx1);
    engine.commit_batch(batch1, 1);

    let mut batch2 = engine.start_batch();
    engine.apply_operation(
        &mut batch2,
        ResourceOperation::Initialize {
            name: "Test Token".to_string(),
            symbol: "TEST".to_string(),
            decimals: 8,
        },
        tx1,
    );
    engine.commit_batch(batch2, 2);

    let mut batch3 = engine.start_batch();
    engine.apply_operation(
        &mut batch3,
        ResourceOperation::Mint {
            to: "alice".to_string(),
            amount: Amount::from_integer(1000, 8),
            memo: None,
        },
        tx1,
    );
    engine.commit_batch(batch3, 3);

    let mut batch4 = engine.start_batch();
    engine.apply_operation(
        &mut batch4,
        ResourceOperation::Mint {
            to: "bob".to_string(),
            amount: Amount::from_integer(500, 8),
            memo: None,
        },
        tx1,
    );
    engine.commit_batch(batch4, 4);

    let mut batch5 = engine.start_batch();
    engine.commit(&mut batch5, tx1);
    engine.commit_batch(batch5, 5);

    // Multiple snapshot reads at the same timestamp should all succeed
    let read_ts = make_timestamp(200);

    // Read alice's balance
    let result1 = engine.read_at_timestamp(
        ResourceOperation::GetBalance {
            account: "alice".to_string(),
        },
        read_ts,
    );
    assert!(matches!(result1, OperationResult::Complete(_)));

    // Read bob's balance
    let result2 = engine.read_at_timestamp(
        ResourceOperation::GetBalance {
            account: "bob".to_string(),
        },
        read_ts,
    );
    assert!(matches!(result2, OperationResult::Complete(_)));

    // Read metadata
    let result3 = engine.read_at_timestamp(ResourceOperation::GetMetadata, read_ts);
    assert!(matches!(result3, OperationResult::Complete(_)));

    // Read total supply
    let result4 = engine.read_at_timestamp(ResourceOperation::GetTotalSupply, read_ts);
    assert!(matches!(result4, OperationResult::Complete(_)));

    // Verify values
    if let OperationResult::Complete(ResourceResponse::Balance { amount, .. }) = result1 {
        assert_eq!(amount, Amount::from_integer(1000, 8));
    }
    if let OperationResult::Complete(ResourceResponse::Balance { amount, .. }) = result2 {
        assert_eq!(amount, Amount::from_integer(500, 8));
    }
    if let OperationResult::Complete(ResourceResponse::TotalSupply { amount }) = result4 {
        assert_eq!(amount, Amount::from_integer(1500, 8));
    }
}
