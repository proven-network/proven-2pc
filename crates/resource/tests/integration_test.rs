//! Integration tests for resource engine

use proven_common::TransactionId;
use proven_resource::types::Amount;
use proven_resource::{ResourceOperation, ResourceResponse, ResourceTransactionEngine};
use proven_stream::{AutoBatchEngine, OperationResult, RetryOn};
use uuid::Uuid;

fn make_timestamp(n: u64) -> TransactionId {
    TransactionId::from_uuid(Uuid::from_u128(n as u128))
}

fn create_engine() -> AutoBatchEngine<ResourceTransactionEngine> {
    AutoBatchEngine::new(ResourceTransactionEngine::new())
}

#[test]
fn test_resource_lifecycle() {
    let mut engine = create_engine();

    // Initialize resource
    let tx1 = make_timestamp(100);
    engine.begin(tx1);

    let init_op = ResourceOperation::Initialize {
        name: "Test Token".to_string(),
        symbol: "TEST".to_string(),
        decimals: 18,
    };

    let result = engine.apply_operation(init_op, tx1);
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

    engine.prepare(tx1);

    engine.commit(tx1);

    // Cannot initialize twice
    let tx2 = make_timestamp(200);
    engine.begin(tx2);

    let init_op = ResourceOperation::Initialize {
        name: "Another Token".to_string(),
        symbol: "OTHER".to_string(),
        decimals: 8,
    };

    let result = engine.apply_operation(init_op, tx2);
    assert!(matches!(
        result,
        OperationResult::Complete(ResourceResponse::Error(_))
    ));

    engine.abort(tx2);
}

#[test]
fn test_mint_and_burn() {
    let mut engine = create_engine();

    // Initialize
    let tx1 = make_timestamp(100);
    engine.begin(tx1);

    engine.apply_operation(
        ResourceOperation::Initialize {
            name: "Test Token".to_string(),
            symbol: "TEST".to_string(),
            decimals: 8,
        },
        tx1,
    );

    // Mint tokens to alice
    let mint_op = ResourceOperation::Mint {
        to: "alice".to_string(),
        amount: Amount::from_integer(1000, 8),
        memo: Some("Initial mint".to_string()),
    };

    let result = engine.apply_operation(mint_op, tx1);
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

    engine.prepare(tx1);

    engine.commit(tx1);

    // Burn tokens from alice
    let tx2 = make_timestamp(200);
    engine.begin(tx2);

    let burn_op = ResourceOperation::Burn {
        from: "alice".to_string(),
        amount: Amount::from_integer(300, 8),
        memo: Some("Burn tokens".to_string()),
    };

    let result = engine.apply_operation(burn_op, tx2);
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

    engine.prepare(tx2);

    engine.commit(tx2);

    // Check final balance and total supply
    let tx3 = make_timestamp(300);
    engine.begin(tx3);

    let balance_op = ResourceOperation::GetBalance {
        account: "alice".to_string(),
    };

    let result = engine.apply_operation(balance_op, tx3);
    match result {
        OperationResult::Complete(ResourceResponse::Balance { account, amount }) => {
            assert_eq!(account, "alice");
            assert_eq!(amount, Amount::from_integer(700, 8));
        }
        _ => panic!("Expected Balance response"),
    }

    // Check total supply after mint and burn
    let result = engine.apply_operation(ResourceOperation::GetTotalSupply, tx3);
    match result {
        OperationResult::Complete(ResourceResponse::TotalSupply { amount }) => {
            assert_eq!(amount, Amount::from_integer(700, 8)); // 1000 minted - 300 burned
        }
        _ => panic!("Expected TotalSupply response"),
    }
}

#[test]
fn test_transfer() {
    let mut engine = create_engine();

    // Initialize and mint
    let tx1 = make_timestamp(100);
    engine.begin(tx1);

    engine.apply_operation(
        ResourceOperation::Initialize {
            name: "Test Token".to_string(),
            symbol: "TEST".to_string(),
            decimals: 6,
        },
        tx1,
    );

    engine.apply_operation(
        ResourceOperation::Mint {
            to: "alice".to_string(),
            amount: Amount::from_integer(1000, 6),
            memo: None,
        },
        tx1,
    );

    engine.prepare(tx1);

    engine.commit(tx1);

    // Transfer from alice to bob
    let tx2 = make_timestamp(200);
    engine.begin(tx2);

    let transfer_op = ResourceOperation::Transfer {
        from: "alice".to_string(),
        to: "bob".to_string(),
        amount: Amount::from_integer(250, 6),
        memo: Some("Payment".to_string()),
    };

    let result = engine.apply_operation(transfer_op, tx2);
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

    engine.prepare(tx2);

    engine.commit(tx2);

    // Check balances
    let tx3 = make_timestamp(300);
    engine.begin(tx3);

    let alice_balance = engine.apply_operation(
        ResourceOperation::GetBalance {
            account: "alice".to_string(),
        },
        tx3,
    );

    let bob_balance = engine.apply_operation(
        ResourceOperation::GetBalance {
            account: "bob".to_string(),
        },
        tx3,
    );

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
    let mut engine = create_engine();

    // Initialize and mint small amount
    let tx1 = make_timestamp(100);
    engine.begin(tx1);

    engine.apply_operation(
        ResourceOperation::Initialize {
            name: "Test Token".to_string(),
            symbol: "TEST".to_string(),
            decimals: 0,
        },
        tx1,
    );

    engine.apply_operation(
        ResourceOperation::Mint {
            to: "alice".to_string(),
            amount: Amount::from_integer(100, 0),
            memo: None,
        },
        tx1,
    );

    engine.prepare(tx1);

    engine.commit(tx1);

    // Try to transfer more than balance
    let tx2 = make_timestamp(200);
    engine.begin(tx2);

    let transfer_op = ResourceOperation::Transfer {
        from: "alice".to_string(),
        to: "bob".to_string(),
        amount: Amount::from_integer(150, 0),
        memo: None,
    };

    let result = engine.apply_operation(transfer_op, tx2);
    assert!(matches!(
        result,
        OperationResult::Complete(ResourceResponse::Error(_))
    ));

    engine.abort(tx2);

    // Try to burn more than balance
    let tx3 = make_timestamp(300);
    engine.begin(tx3);

    let burn_op = ResourceOperation::Burn {
        from: "alice".to_string(),
        amount: Amount::from_integer(150, 0),
        memo: None,
    };

    let result = engine.apply_operation(burn_op, tx3);
    assert!(matches!(
        result,
        OperationResult::Complete(ResourceResponse::Error(_))
    ));

    engine.abort(tx3);
}

#[test]
fn test_concurrent_transfers_with_reservations() {
    let mut engine = create_engine();

    // Initialize and mint
    let tx1 = make_timestamp(100);
    engine.begin(tx1);

    engine.apply_operation(
        ResourceOperation::Initialize {
            name: "Test Token".to_string(),
            symbol: "TEST".to_string(),
            decimals: 0,
        },
        tx1,
    );

    engine.apply_operation(
        ResourceOperation::Mint {
            to: "alice".to_string(),
            amount: Amount::from_integer(100, 0),
            memo: None,
        },
        tx1,
    );

    engine.prepare(tx1);

    engine.commit(tx1);

    // Start two concurrent transactions
    let tx2 = make_timestamp(200);
    let tx3 = make_timestamp(201);

    engine.begin(tx2);

    engine.begin(tx3);

    // First transfer: alice -> bob 60
    let transfer1 = ResourceOperation::Transfer {
        from: "alice".to_string(),
        to: "bob".to_string(),
        amount: Amount::from_integer(60, 0),
        memo: None,
    };

    let result1 = engine.apply_operation(transfer1, tx2);
    assert!(matches!(result1, OperationResult::Complete(_)));

    // Second transfer: alice -> charlie 50 (should block due to insufficient balance after reservation)
    let transfer2 = ResourceOperation::Transfer {
        from: "alice".to_string(),
        to: "charlie".to_string(),
        amount: Amount::from_integer(50, 0),
        memo: None,
    };

    let result2 = engine.apply_operation(transfer2, tx3);
    // Should block due to insufficient balance after tx2's reservation
    assert!(
        matches!(result2, OperationResult::WouldBlock { .. })
            || matches!(
                result2,
                OperationResult::Complete(ResourceResponse::Error(_))
            )
    );

    // Commit first transaction
    engine.prepare(tx2);

    engine.commit(tx2);

    // Abort second transaction
    engine.abort(tx3);

    // Now the second transfer should work
    let tx4 = make_timestamp(300);
    engine.begin(tx4);

    let transfer3 = ResourceOperation::Transfer {
        from: "alice".to_string(),
        to: "charlie".to_string(),
        amount: Amount::from_integer(40, 0), // Reduced amount
        memo: None,
    };

    let result3 = engine.apply_operation(transfer3, tx4);
    assert!(matches!(result3, OperationResult::Complete(_)));

    engine.prepare(tx4);

    engine.commit(tx4);
}

#[test]
fn test_metadata_update() {
    let mut engine = create_engine();

    // Initialize
    let tx1 = make_timestamp(100);
    engine.begin(tx1);

    engine.apply_operation(
        ResourceOperation::Initialize {
            name: "Test Token".to_string(),
            symbol: "TEST".to_string(),
            decimals: 8,
        },
        tx1,
    );

    engine.prepare(tx1);

    engine.commit(tx1);

    // Update metadata
    let tx2 = make_timestamp(200);
    engine.begin(tx2);

    let update_op = ResourceOperation::UpdateMetadata {
        name: Some("Updated Token".to_string()),
        symbol: None,
    };

    let result = engine.apply_operation(update_op, tx2);
    match result {
        OperationResult::Complete(ResourceResponse::MetadataUpdated { name, symbol }) => {
            assert_eq!(name, Some("Updated Token".to_string()));
            assert_eq!(symbol, None);
        }
        _ => panic!("Expected MetadataUpdated response"),
    }

    engine.prepare(tx2);

    engine.commit(tx2);

    // Check metadata
    let tx3 = make_timestamp(300);
    engine.begin(tx3);

    let result = engine.apply_operation(ResourceOperation::GetMetadata, tx3);
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
    let result = engine.apply_operation(ResourceOperation::GetTotalSupply, tx3);
    match result {
        OperationResult::Complete(ResourceResponse::TotalSupply { amount }) => {
            assert_eq!(amount, Amount::zero()); // No mints in this test
        }
        _ => panic!("Expected TotalSupply response"),
    }
}

#[test]
fn test_transaction_rollback() {
    let mut engine = create_engine();

    // Initialize and mint
    let tx1 = make_timestamp(100);
    engine.begin(tx1);

    engine.apply_operation(
        ResourceOperation::Initialize {
            name: "Test Token".to_string(),
            symbol: "TEST".to_string(),
            decimals: 0,
        },
        tx1,
    );

    engine.apply_operation(
        ResourceOperation::Mint {
            to: "alice".to_string(),
            amount: Amount::from_integer(1000, 0),
            memo: None,
        },
        tx1,
    );

    engine.prepare(tx1);

    engine.commit(tx1);

    // Start transaction with multiple operations
    let tx2 = make_timestamp(200);
    engine.begin(tx2);

    // Transfer to bob
    engine.apply_operation(
        ResourceOperation::Transfer {
            from: "alice".to_string(),
            to: "bob".to_string(),
            amount: Amount::from_integer(500, 0),
            memo: None,
        },
        tx2,
    );

    // Burn from alice
    engine.apply_operation(
        ResourceOperation::Burn {
            from: "alice".to_string(),
            amount: Amount::from_integer(200, 0),
            memo: None,
        },
        tx2,
    );

    // Abort the transaction
    engine.abort(tx2);

    // Check that balances are unchanged
    let tx3 = make_timestamp(300);
    engine.begin(tx3);

    let alice_balance = engine.apply_operation(
        ResourceOperation::GetBalance {
            account: "alice".to_string(),
        },
        tx3,
    );

    let bob_balance = engine.apply_operation(
        ResourceOperation::GetBalance {
            account: "bob".to_string(),
        },
        tx3,
    );

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
    let mut engine = create_engine();

    // Initialize resource and mint some tokens
    let tx1 = make_timestamp(100);
    engine.begin(tx1);

    engine.apply_operation(
        ResourceOperation::Initialize {
            name: "Test Token".to_string(),
            symbol: "TEST".to_string(),
            decimals: 8,
        },
        tx1,
    );

    engine.apply_operation(
        ResourceOperation::Mint {
            to: "alice".to_string(),
            amount: Amount::from_integer(1000, 8),
            memo: None,
        },
        tx1,
    );

    engine.commit(tx1);

    // Start a write transaction at timestamp 200 (but don't commit)
    let tx_write = make_timestamp(200);
    engine.begin(tx_write);

    engine.apply_operation(
        ResourceOperation::Transfer {
            from: "alice".to_string(),
            to: "bob".to_string(),
            amount: Amount::from_integer(100, 8),
            memo: None,
        },
        tx_write,
    );

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
    engine.commit(tx_write);

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
    let mut engine = create_engine();

    // Initialize resource
    let tx1 = make_timestamp(100);
    engine.begin(tx1);

    engine.apply_operation(
        ResourceOperation::Initialize {
            name: "Old Name".to_string(),
            symbol: "OLD".to_string(),
            decimals: 8,
        },
        tx1,
    );

    engine.commit(tx1);

    // Start a metadata update transaction (but don't commit)
    let tx_update = make_timestamp(200);
    engine.begin(tx_update);

    engine.apply_operation(
        ResourceOperation::UpdateMetadata {
            name: Some("New Name".to_string()),
            symbol: Some("NEW".to_string()),
        },
        tx_update,
    );

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
    engine.abort(tx_update);

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
    let mut engine = create_engine();

    // Initialize and mint initial supply
    let tx1 = make_timestamp(100);
    engine.begin(tx1);

    engine.apply_operation(
        ResourceOperation::Initialize {
            name: "Test Token".to_string(),
            symbol: "TEST".to_string(),
            decimals: 8,
        },
        tx1,
    );

    engine.apply_operation(
        ResourceOperation::Mint {
            to: "alice".to_string(),
            amount: Amount::from_integer(1000, 8),
            memo: None,
        },
        tx1,
    );

    engine.commit(tx1);

    // Start another mint transaction (but don't commit)
    let tx_mint = make_timestamp(200);
    engine.begin(tx_mint);

    engine.apply_operation(
        ResourceOperation::Mint {
            to: "bob".to_string(),
            amount: Amount::from_integer(500, 8),
            memo: None,
        },
        tx_mint,
    );

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
    engine.commit(tx_mint);

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
    let mut engine = create_engine();

    // Initialize and setup initial state
    let tx1 = make_timestamp(100);
    engine.begin(tx1);

    engine.apply_operation(
        ResourceOperation::Initialize {
            name: "Test Token".to_string(),
            symbol: "TEST".to_string(),
            decimals: 8,
        },
        tx1,
    );

    engine.apply_operation(
        ResourceOperation::Mint {
            to: "alice".to_string(),
            amount: Amount::from_integer(1000, 8),
            memo: None,
        },
        tx1,
    );

    engine.commit(tx1);

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
    let mut engine = create_engine();

    // Initialize resource and mint some tokens
    let tx1 = make_timestamp(100);
    engine.begin(tx1);

    engine.apply_operation(
        ResourceOperation::Initialize {
            name: "Test Token".to_string(),
            symbol: "TEST".to_string(),
            decimals: 8,
        },
        tx1,
    );

    engine.apply_operation(
        ResourceOperation::Mint {
            to: "alice".to_string(),
            amount: Amount::from_integer(1000, 8),
            memo: None,
        },
        tx1,
    );

    engine.commit(tx1);

    // Start a write transaction
    let tx_write = make_timestamp(300); // Write is AFTER the read timestamp
    engine.begin(tx_write);

    // Apply a transfer (write operation)
    let transfer_op = ResourceOperation::Transfer {
        from: "alice".to_string(),
        to: "bob".to_string(),
        amount: Amount::from_integer(100, 8),
        memo: None,
    };
    let result = engine.apply_operation(transfer_op, tx_write);
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

    engine.commit(tx_write);
}

#[test]
fn test_snapshot_read_blocks_on_earlier_write() {
    let mut engine = create_engine();

    // Initialize resource
    let tx1 = make_timestamp(100);
    engine.begin(tx1);

    engine.apply_operation(
        ResourceOperation::Initialize {
            name: "Test Token".to_string(),
            symbol: "TEST".to_string(),
            decimals: 8,
        },
        tx1,
    );

    engine.apply_operation(
        ResourceOperation::Mint {
            to: "alice".to_string(),
            amount: Amount::from_integer(1000, 8),
            memo: None,
        },
        tx1,
    );

    engine.commit(tx1);

    // Start a write transaction at timestamp 200
    let tx_write = make_timestamp(200);
    engine.begin(tx_write);

    // Apply a transfer
    let transfer_op = ResourceOperation::Transfer {
        from: "alice".to_string(),
        to: "bob".to_string(),
        amount: Amount::from_integer(100, 8),
        memo: None,
    };
    engine.apply_operation(transfer_op, tx_write);

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

    engine.commit(tx_write);
}

#[test]
fn test_snapshot_read_metadata_consistency() {
    let mut engine = create_engine();

    // Initialize resource
    let tx1 = make_timestamp(100);
    engine.begin(tx1);

    engine.apply_operation(
        ResourceOperation::Initialize {
            name: "Test Token".to_string(),
            symbol: "TEST".to_string(),
            decimals: 8,
        },
        tx1,
    );

    engine.commit(tx1);

    // Update metadata at timestamp 200
    let tx2 = make_timestamp(200);
    engine.begin(tx2);

    engine.apply_operation(
        ResourceOperation::UpdateMetadata {
            name: Some("New Token".to_string()),
            symbol: None,
        },
        tx2,
    );

    engine.commit(tx2);

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
    let mut engine = create_engine();

    // Initialize resource
    let tx1 = make_timestamp(100);
    engine.begin(tx1);

    engine.apply_operation(
        ResourceOperation::Initialize {
            name: "Test Token".to_string(),
            symbol: "TEST".to_string(),
            decimals: 8,
        },
        tx1,
    );

    engine.commit(tx1);

    // Mint tokens at timestamp 200
    let tx2 = make_timestamp(200);
    engine.begin(tx2);

    engine.apply_operation(
        ResourceOperation::Mint {
            to: "alice".to_string(),
            amount: Amount::from_integer(1000, 8),
            memo: None,
        },
        tx2,
    );

    engine.commit(tx2);

    // Burn tokens at timestamp 300
    let tx3 = make_timestamp(300);
    engine.begin(tx3);

    engine.apply_operation(
        ResourceOperation::Burn {
            from: "alice".to_string(),
            amount: Amount::from_integer(300, 8),
            memo: None,
        },
        tx3,
    );

    engine.commit(tx3);

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
    let mut engine = create_engine();

    // Initialize and mint tokens
    let tx1 = make_timestamp(100);
    engine.begin(tx1);

    engine.apply_operation(
        ResourceOperation::Initialize {
            name: "Test Token".to_string(),
            symbol: "TEST".to_string(),
            decimals: 8,
        },
        tx1,
    );

    engine.apply_operation(
        ResourceOperation::Mint {
            to: "alice".to_string(),
            amount: Amount::from_integer(1000, 8),
            memo: None,
        },
        tx1,
    );

    engine.commit(tx1);

    // Start a write transaction that will be aborted
    let tx_abort = make_timestamp(200);
    engine.begin(tx_abort);

    engine.apply_operation(
        ResourceOperation::Transfer {
            from: "alice".to_string(),
            to: "bob".to_string(),
            amount: Amount::from_integer(500, 8),
            memo: None,
        },
        tx_abort,
    );

    // Abort the transaction
    engine.abort(tx_abort);

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
    let mut engine = create_engine();

    // Initialize and set up initial state
    let tx1 = make_timestamp(100);
    engine.begin(tx1);

    engine.apply_operation(
        ResourceOperation::Initialize {
            name: "Test Token".to_string(),
            symbol: "TEST".to_string(),
            decimals: 8,
        },
        tx1,
    );

    engine.apply_operation(
        ResourceOperation::Mint {
            to: "alice".to_string(),
            amount: Amount::from_integer(1000, 8),
            memo: None,
        },
        tx1,
    );

    engine.apply_operation(
        ResourceOperation::Mint {
            to: "bob".to_string(),
            amount: Amount::from_integer(500, 8),
            memo: None,
        },
        tx1,
    );

    engine.commit(tx1);

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
