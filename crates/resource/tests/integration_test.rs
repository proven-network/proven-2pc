//! Integration tests for resource engine

use proven_hlc::{HlcTimestamp, NodeId};
use proven_resource::stream::{ResourceOperation, ResourceResponse, ResourceTransactionEngine};
use proven_resource::types::Amount;
use proven_stream::{OperationResult, TransactionEngine};

fn make_timestamp(n: u64) -> HlcTimestamp {
    HlcTimestamp::new(n, 0, NodeId::new(0))
}

#[test]
fn test_resource_lifecycle() {
    let mut engine = ResourceTransactionEngine::new();

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
    let mut engine = ResourceTransactionEngine::new();

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
    let mut engine = ResourceTransactionEngine::new();

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
    let mut engine = ResourceTransactionEngine::new();

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
    let mut engine = ResourceTransactionEngine::new();

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
    let mut engine = ResourceTransactionEngine::new();

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
    let mut engine = ResourceTransactionEngine::new();

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
