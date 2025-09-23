//! Tests for Resource engine snapshot functionality

use proven_hlc::{HlcTimestamp, NodeId};
use proven_resource::types::Amount;
use proven_resource::{ResourceOperation, ResourceResponse, ResourceTransactionEngine};
use proven_stream::{OperationResult, TransactionEngine};

#[test]
fn test_resource_snapshot_and_restore() {
    // Create engine and initialize resource
    let mut engine1 = ResourceTransactionEngine::new();

    // Initialize the resource
    let txn1 = HlcTimestamp::new(1, 0, NodeId::new(1));
    engine1.begin(txn1);

    let init_op = ResourceOperation::Initialize {
        name: "Test Token".to_string(),
        symbol: "TEST".to_string(),
        decimals: 8,
    };
    let result = engine1.apply_operation(init_op, txn1);
    assert!(matches!(result, OperationResult::Complete(_)));

    // Mint some tokens to different accounts
    let mint1 = ResourceOperation::Mint {
        to: "alice".to_string(),
        amount: Amount::from_integer(1000, 8),
        memo: Some("Initial mint".to_string()),
    };
    let result = engine1.apply_operation(mint1, txn1);
    assert!(matches!(result, OperationResult::Complete(_)));

    let mint2 = ResourceOperation::Mint {
        to: "bob".to_string(),
        amount: Amount::from_integer(500, 8),
        memo: None,
    };
    let result = engine1.apply_operation(mint2, txn1);
    assert!(matches!(result, OperationResult::Complete(_)));

    // Commit transaction
    engine1.prepare(txn1);
    engine1.commit(txn1);

    // Do a transfer in a new transaction
    let txn2 = HlcTimestamp::new(2, 0, NodeId::new(1));
    engine1.begin(txn2);

    let transfer_op = ResourceOperation::Transfer {
        from: "alice".to_string(),
        to: "charlie".to_string(),
        amount: Amount::from_integer(100, 8),
        memo: None,
    };
    let result = engine1.apply_operation(transfer_op, txn2);
    assert!(matches!(result, OperationResult::Complete(_)));

    engine1.prepare(txn2);
    engine1.commit(txn2);

    // Take a snapshot
    let snapshot = engine1.snapshot().unwrap();
    assert!(!snapshot.is_empty());

    // Create a new engine and restore
    let mut engine2 = ResourceTransactionEngine::new();
    engine2.restore_from_snapshot(&snapshot).unwrap();

    // Verify balances were restored
    let txn3 = HlcTimestamp::new(3, 0, NodeId::new(1));
    engine2.begin(txn3);

    // Check Alice's balance (1000 - 100 = 900)
    let balance_op = ResourceOperation::GetBalance {
        account: "alice".to_string(),
    };
    let result = engine2.apply_operation(balance_op, txn3);
    if let OperationResult::Complete(response) = result {
        if let ResourceResponse::Balance { amount, .. } = response {
            assert_eq!(amount, Amount::from_integer(900, 8));
        } else {
            panic!("Expected Balance response");
        }
    } else {
        panic!("Expected success");
    }

    // Check Bob's balance (500)
    let balance_op = ResourceOperation::GetBalance {
        account: "bob".to_string(),
    };
    let result = engine2.apply_operation(balance_op, txn3);
    if let OperationResult::Complete(response) = result {
        if let ResourceResponse::Balance { amount, .. } = response {
            assert_eq!(amount, Amount::from_integer(500, 8));
        } else {
            panic!("Expected Balance response");
        }
    }

    // Check Charlie's balance (100)
    let balance_op = ResourceOperation::GetBalance {
        account: "charlie".to_string(),
    };
    let result = engine2.apply_operation(balance_op, txn3);
    if let OperationResult::Complete(response) = result {
        if let ResourceResponse::Balance { amount, .. } = response {
            assert_eq!(amount, Amount::from_integer(100, 8));
        } else {
            panic!("Expected Balance response");
        }
    }

    // Check total supply (1500)
    let supply_op = ResourceOperation::GetTotalSupply;
    let result = engine2.apply_operation(supply_op, txn3);
    if let OperationResult::Complete(ResourceResponse::TotalSupply { amount }) = result {
        assert_eq!(amount, Amount::from_integer(1500, 8));
    }
}

#[test]
fn test_snapshot_with_active_transaction_fails() {
    let mut engine = ResourceTransactionEngine::new();

    // Begin a transaction
    let txn = HlcTimestamp::new(1, 0, NodeId::new(1));
    engine.begin(txn);

    // Try to snapshot with active transaction
    let result = engine.snapshot();
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("active transactions"));

    // Commit the transaction
    engine.commit(txn);

    // Now snapshot should succeed
    let result = engine.snapshot();
    assert!(result.is_ok());
}

#[test]
fn test_snapshot_with_burns() {
    let mut engine = ResourceTransactionEngine::new();

    // Initialize and mint
    let txn1 = HlcTimestamp::new(1, 0, NodeId::new(1));
    engine.begin(txn1);

    let init_op = ResourceOperation::Initialize {
        name: "Burn Token".to_string(),
        symbol: "BURN".to_string(),
        decimals: 6,
    };
    engine.apply_operation(init_op, txn1);

    let mint_op = ResourceOperation::Mint {
        to: "treasury".to_string(),
        amount: Amount::from_integer(10000, 6),
        memo: None,
    };
    engine.apply_operation(mint_op, txn1);

    engine.prepare(txn1);
    engine.commit(txn1);

    // Burn some tokens
    let txn2 = HlcTimestamp::new(2, 0, NodeId::new(1));
    engine.begin(txn2);

    let burn_op = ResourceOperation::Burn {
        from: "treasury".to_string(),
        amount: Amount::from_integer(2000, 6),
        memo: Some("Burning tokens".to_string()),
    };
    engine.apply_operation(burn_op, txn2);

    engine.prepare(txn2);
    engine.commit(txn2);

    // Take snapshot
    let snapshot = engine.snapshot().unwrap();

    // Restore to new engine
    let mut engine2 = ResourceTransactionEngine::new();
    engine2.restore_from_snapshot(&snapshot).unwrap();

    // Verify remaining balance and supply
    let txn3 = HlcTimestamp::new(3, 0, NodeId::new(1));
    engine2.begin(txn3);

    let balance_op = ResourceOperation::GetBalance {
        account: "treasury".to_string(),
    };
    let result = engine2.apply_operation(balance_op, txn3);
    if let OperationResult::Complete(ResourceResponse::Balance { amount, .. }) = result {
        assert_eq!(amount, Amount::from_integer(8000, 6));
    }

    let supply_op = ResourceOperation::GetTotalSupply;
    let result = engine2.apply_operation(supply_op, txn3);
    if let OperationResult::Complete(ResourceResponse::TotalSupply { amount }) = result {
        assert_eq!(amount, Amount::from_integer(8000, 6));
    }
}

#[test]
fn test_snapshot_compression() {
    let mut engine = ResourceTransactionEngine::new();

    // Initialize resource
    let txn1 = HlcTimestamp::new(1, 0, NodeId::new(1));
    engine.begin(txn1);

    let init_op = ResourceOperation::Initialize {
        name: "Compression Test Token".to_string(),
        symbol: "COMPRESS".to_string(),
        decimals: 18,
    };
    engine.apply_operation(init_op, txn1);

    // Create many accounts with the same balance (good for compression)
    let standard_amount = Amount::from_integer(1000000, 18);
    for i in 0..1000 {
        let mint_op = ResourceOperation::Mint {
            to: format!("account_{}", i),
            amount: standard_amount,
            memo: None,
        };
        engine.apply_operation(mint_op, txn1);
    }

    engine.prepare(txn1);
    engine.commit(txn1);

    // Take snapshot
    let snapshot = engine.snapshot().unwrap();

    // The compressed snapshot should be much smaller than the raw data
    // 1000 accounts * ~100 bytes per account = ~100KB uncompressed
    // With compression (many identical values), should be much smaller
    println!("Snapshot size: {} bytes", snapshot.len());
    assert!(snapshot.len() < 50000, "Snapshot should be compressed");

    // Verify it can be restored
    let mut engine2 = ResourceTransactionEngine::new();
    engine2.restore_from_snapshot(&snapshot).unwrap();

    // Spot check some accounts
    let txn2 = HlcTimestamp::new(2, 0, NodeId::new(1));
    engine2.begin(txn2);

    for i in [0, 100, 500, 999] {
        let balance_op = ResourceOperation::GetBalance {
            account: format!("account_{}", i),
        };
        let result = engine2.apply_operation(balance_op, txn2);
        if let OperationResult::Complete(ResourceResponse::Balance { amount, .. }) = result {
            assert_eq!(amount, standard_amount);
        }
    }
}

#[test]
fn test_metadata_preserved_in_snapshot() {
    let mut engine = ResourceTransactionEngine::new();

    // Initialize with specific metadata
    let txn1 = HlcTimestamp::new(1, 0, NodeId::new(1));
    engine.begin(txn1);

    let init_op = ResourceOperation::Initialize {
        name: "My Special Token".to_string(),
        symbol: "MST".to_string(),
        decimals: 12,
    };
    engine.apply_operation(init_op, txn1);

    engine.prepare(txn1);
    engine.commit(txn1);

    // Update metadata
    let txn2 = HlcTimestamp::new(2, 0, NodeId::new(1));
    engine.begin(txn2);

    let update_op = ResourceOperation::UpdateMetadata {
        name: Some("My Updated Token".to_string()),
        symbol: Some("MUT".to_string()),
    };
    engine.apply_operation(update_op, txn2);

    engine.prepare(txn2);
    engine.commit(txn2);

    // Snapshot and restore
    let snapshot = engine.snapshot().unwrap();
    let mut engine2 = ResourceTransactionEngine::new();
    engine2.restore_from_snapshot(&snapshot).unwrap();

    // Verify metadata was preserved
    let txn3 = HlcTimestamp::new(3, 0, NodeId::new(1));
    engine2.begin(txn3);

    let metadata_op = ResourceOperation::GetMetadata;
    let result = engine2.apply_operation(metadata_op, txn3);
    if let OperationResult::Complete(response) = result {
        if let ResourceResponse::Metadata {
            name,
            symbol,
            decimals,
            ..
        } = response
        {
            assert_eq!(name, "My Updated Token");
            assert_eq!(symbol, "MUT");
            assert_eq!(decimals, 12);
        } else {
            panic!("Expected Metadata response");
        }
    }

    println!("Metadata preserved across snapshot/restore");
}
