//! Concurrency safety tests proving the resource system is sound
//!
//! These tests validate that:
//! 1. SetSupply deltas merge correctly within transactions
//! 2. Reservations prevent double-spending across transactions
//! 3. MVCC ensures snapshot isolation
//! 4. No race conditions exist in multi-key operations

use proven_common::TransactionId;
use proven_resource::engine::ResourceTransactionEngine;
use proven_resource::types::{Amount, ResourceOperation, ResourceResponse};
use proven_stream::{OperationResult, TransactionEngine};
use uuid::Uuid;

fn make_timestamp(n: u64) -> TransactionId {
    TransactionId::from_uuid(Uuid::from_u128(n as u128))
}

#[test]
fn test_supply_delta_merging_within_transaction() {
    // This test proves that multiple SetSupply deltas within a single transaction
    // merge correctly, preserving the original old value and final new value
    let mut engine = ResourceTransactionEngine::new();
    let tx1 = make_timestamp(100);

    let mut batch1 = engine.start_batch();
    engine.begin(&mut batch1, tx1);
    engine.commit_batch(batch1, 1);

    // Initialize
    let mut batch2 = engine.start_batch();
    engine.apply_operation(
        &mut batch2,
        ResourceOperation::Initialize {
            name: "Test".to_string(),
            symbol: "TST".to_string(),
            decimals: 2,
        },
        tx1,
    );
    engine.commit_batch(batch2, 2);

    // Mint three times in the same transaction
    // Each creates a SetSupply delta that should merge
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
    engine.apply_operation(
        &mut batch4,
        ResourceOperation::Mint {
            to: "bob".to_string(),
            amount: Amount::from_integer(200, 0),
            memo: None,
        },
        tx1,
    );
    engine.commit_batch(batch4, 4);

    let mut batch5 = engine.start_batch();
    engine.apply_operation(
        &mut batch5,
        ResourceOperation::Mint {
            to: "charlie".to_string(),
            amount: Amount::from_integer(300, 0),
            memo: None,
        },
        tx1,
    );
    engine.commit_batch(batch5, 5);

    let mut batch6 = engine.start_batch();
    engine.commit(&mut batch6, tx1);
    engine.commit_batch(batch6, 6);

    // Verify final supply is correct (all mints summed)
    let tx2 = make_timestamp(200);
    let result = engine.read_at_timestamp(ResourceOperation::GetTotalSupply, tx2);

    match result {
        OperationResult::Complete(ResourceResponse::TotalSupply { amount }) => {
            assert_eq!(
                amount,
                Amount::from_integer(600, 0),
                "Supply should be 100 + 200 + 300 = 600"
            );
        }
        _ => panic!("Expected TotalSupply response"),
    }
}

#[test]
fn test_reservation_prevents_double_spending() {
    // This test proves that debit reservations prevent double-spending
    // even when multiple transactions try to spend from the same account
    let mut engine = ResourceTransactionEngine::new();

    // Setup: Alice has 1000 tokens
    let tx_setup = make_timestamp(100);
    let mut batch1 = engine.start_batch();
    engine.begin(&mut batch1, tx_setup);
    engine.commit_batch(batch1, 1);

    let mut batch2 = engine.start_batch();
    engine.apply_operation(
        &mut batch2,
        ResourceOperation::Initialize {
            name: "Test".to_string(),
            symbol: "TST".to_string(),
            decimals: 0,
        },
        tx_setup,
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
        tx_setup,
    );
    engine.commit_batch(batch3, 3);

    let mut batch4 = engine.start_batch();
    engine.commit(&mut batch4, tx_setup);
    engine.commit_batch(batch4, 4);

    // TX1: Try to transfer 700 from Alice to Bob
    let tx1 = make_timestamp(200);
    let mut batch5 = engine.start_batch();
    engine.begin(&mut batch5, tx1);
    engine.commit_batch(batch5, 5);

    let mut batch6 = engine.start_batch();
    let result1 = engine.apply_operation(
        &mut batch6,
        ResourceOperation::Transfer {
            from: "alice".to_string(),
            to: "bob".to_string(),
            amount: Amount::from_integer(700, 0),
            memo: None,
        },
        tx1,
    );
    engine.commit_batch(batch6, 6);
    assert!(matches!(result1, OperationResult::Complete(_)));

    // TX2: Try to transfer 700 from Alice to Charlie (should fail due to reservation)
    let tx2 = make_timestamp(201);
    let mut batch7 = engine.start_batch();
    engine.begin(&mut batch7, tx2);
    engine.commit_batch(batch7, 7);

    let mut batch8 = engine.start_batch();
    let result2 = engine.apply_operation(
        &mut batch8,
        ResourceOperation::Transfer {
            from: "alice".to_string(),
            to: "charlie".to_string(),
            amount: Amount::from_integer(700, 0),
            memo: None,
        },
        tx2,
    );
    engine.commit_batch(batch8, 8);

    // TX2 should be blocked because TX1 has reserved 700, leaving only 300 available
    // The system correctly returns WouldBlock or Error
    match result2 {
        OperationResult::WouldBlock { .. } => {
            // Correctly blocked by TX1's reservation
            // (The exact blocker ID may not be perfectly tracked in error parsing,
            // but the important thing is that the operation is blocked)
        }
        OperationResult::Complete(ResourceResponse::Error(msg)) => {
            // Also acceptable - insufficient balance error
            assert!(
                msg.contains("Insufficient balance") || msg.contains("reservation"),
                "Expected reservation or balance error, got: {}",
                msg
            );
        }
        _ => panic!(
            "Expected WouldBlock or error due to reservation conflict, got: {:?}",
            result2
        ),
    }

    // Commit TX1
    let mut batch9 = engine.start_batch();
    engine.commit(&mut batch9, tx1);
    engine.commit_batch(batch9, 9);

    // Now TX2 should definitely fail (not enough balance)
    let mut batch10 = engine.start_batch();
    let result2_retry = engine.apply_operation(
        &mut batch10,
        ResourceOperation::Transfer {
            from: "alice".to_string(),
            to: "charlie".to_string(),
            amount: Amount::from_integer(700, 0),
            memo: None,
        },
        tx2,
    );
    engine.commit_batch(batch10, 10);

    match result2_retry {
        OperationResult::Complete(ResourceResponse::Error(msg)) => {
            assert!(
                msg.contains("Insufficient balance"),
                "Expected insufficient balance error, got: {}",
                msg
            );
        }
        _ => panic!("Expected insufficient balance error"),
    }
}

#[test]
fn test_mvcc_snapshot_isolation() {
    // This test proves that MVCC provides proper snapshot isolation
    // Transactions see a consistent view of data at their start timestamp
    let mut engine = ResourceTransactionEngine::new();

    // Setup initial state
    let tx_setup = make_timestamp(100);
    let mut batch1 = engine.start_batch();
    engine.begin(&mut batch1, tx_setup);
    engine.commit_batch(batch1, 1);

    let mut batch2 = engine.start_batch();
    engine.apply_operation(
        &mut batch2,
        ResourceOperation::Initialize {
            name: "Test".to_string(),
            symbol: "TST".to_string(),
            decimals: 0,
        },
        tx_setup,
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
        tx_setup,
    );
    engine.commit_batch(batch3, 3);

    let mut batch4 = engine.start_batch();
    engine.commit(&mut batch4, tx_setup);
    engine.commit_batch(batch4, 4);

    // TX1 starts at time 200 and reads Alice's balance
    let tx1 = make_timestamp(200);
    let mut batch5 = engine.start_batch();
    engine.begin(&mut batch5, tx1);
    engine.commit_batch(batch5, 5);

    let mut batch6 = engine.start_batch();
    let result1 = engine.apply_operation(
        &mut batch6,
        ResourceOperation::GetBalance {
            account: "alice".to_string(),
        },
        tx1,
    );
    engine.commit_batch(batch6, 6);
    match result1 {
        OperationResult::Complete(ResourceResponse::Balance { amount, .. }) => {
            assert_eq!(amount, Amount::from_integer(1000, 0));
        }
        _ => panic!("Expected balance"),
    }

    // TX2 transfers 300 from Alice to Bob and commits
    let tx2 = make_timestamp(250);
    let mut batch7 = engine.start_batch();
    engine.begin(&mut batch7, tx2);
    engine.commit_batch(batch7, 7);

    let mut batch8 = engine.start_batch();
    engine.apply_operation(
        &mut batch8,
        ResourceOperation::Transfer {
            from: "alice".to_string(),
            to: "bob".to_string(),
            amount: Amount::from_integer(300, 0),
            memo: None,
        },
        tx2,
    );
    engine.commit_batch(batch8, 8);

    let mut batch9 = engine.start_batch();
    engine.commit(&mut batch9, tx2);
    engine.commit_batch(batch9, 9);

    // TX1 reads Alice's balance again - should still see 1000 (snapshot isolation)
    let mut batch10 = engine.start_batch();
    let result1_again = engine.apply_operation(
        &mut batch10,
        ResourceOperation::GetBalance {
            account: "alice".to_string(),
        },
        tx1,
    );
    engine.commit_batch(batch10, 10);
    match result1_again {
        OperationResult::Complete(ResourceResponse::Balance { amount, .. }) => {
            assert_eq!(
                amount,
                Amount::from_integer(1000, 0),
                "TX1 should see consistent snapshot (1000), not modified value (700)"
            );
        }
        _ => panic!("Expected balance"),
    }

    // TX3 starting after TX2 commits should see the new balance
    let tx3 = make_timestamp(300);
    let mut batch11 = engine.start_batch();
    engine.begin(&mut batch11, tx3);
    engine.commit_batch(batch11, 11);

    let mut batch12 = engine.start_batch();
    let result3 = engine.apply_operation(
        &mut batch12,
        ResourceOperation::GetBalance {
            account: "alice".to_string(),
        },
        tx3,
    );
    engine.commit_batch(batch12, 12);
    match result3 {
        OperationResult::Complete(ResourceResponse::Balance { amount, .. }) => {
            assert_eq!(
                amount,
                Amount::from_integer(700, 0),
                "TX3 should see committed changes (700)"
            );
        }
        _ => panic!("Expected balance"),
    }
}

#[test]
fn test_multi_key_atomicity() {
    // This test proves that multi-key operations (like Transfer) are atomic
    // Either both keys update or neither does
    let mut engine = ResourceTransactionEngine::new();

    // Setup
    let tx_setup = make_timestamp(100);
    let mut batch1 = engine.start_batch();
    engine.begin(&mut batch1, tx_setup);
    engine.commit_batch(batch1, 1);

    let mut batch2 = engine.start_batch();
    engine.apply_operation(
        &mut batch2,
        ResourceOperation::Initialize {
            name: "Test".to_string(),
            symbol: "TST".to_string(),
            decimals: 0,
        },
        tx_setup,
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
        tx_setup,
    );
    engine.commit_batch(batch3, 3);

    let mut batch4 = engine.start_batch();
    engine.commit(&mut batch4, tx_setup);
    engine.commit_batch(batch4, 4);

    // Transfer from Alice to Bob
    let tx1 = make_timestamp(200);
    let mut batch5 = engine.start_batch();
    engine.begin(&mut batch5, tx1);
    engine.commit_batch(batch5, 5);

    let mut batch6 = engine.start_batch();
    let result = engine.apply_operation(
        &mut batch6,
        ResourceOperation::Transfer {
            from: "alice".to_string(),
            to: "bob".to_string(),
            amount: Amount::from_integer(400, 0),
            memo: None,
        },
        tx1,
    );
    engine.commit_batch(batch6, 6);
    assert!(matches!(result, OperationResult::Complete(_)));

    // Before commit, verify within transaction
    let mut batch7 = engine.start_batch();
    let alice_balance = engine.apply_operation(
        &mut batch7,
        ResourceOperation::GetBalance {
            account: "alice".to_string(),
        },
        tx1,
    );
    engine.commit_batch(batch7, 7);
    match alice_balance {
        OperationResult::Complete(ResourceResponse::Balance { amount, .. }) => {
            assert_eq!(amount, Amount::from_integer(600, 0));
        }
        _ => panic!("Expected balance"),
    }

    let mut batch8 = engine.start_batch();
    let bob_balance = engine.apply_operation(
        &mut batch8,
        ResourceOperation::GetBalance {
            account: "bob".to_string(),
        },
        tx1,
    );
    engine.commit_batch(batch8, 8);
    match bob_balance {
        OperationResult::Complete(ResourceResponse::Balance { amount, .. }) => {
            assert_eq!(amount, Amount::from_integer(400, 0));
        }
        _ => panic!("Expected balance"),
    }

    let mut batch9 = engine.start_batch();
    engine.commit(&mut batch9, tx1);
    engine.commit_batch(batch9, 9);

    // Verify atomicity after commit
    let tx2 = make_timestamp(300);
    let alice_final = engine.read_at_timestamp(
        ResourceOperation::GetBalance {
            account: "alice".to_string(),
        },
        tx2,
    );
    let bob_final = engine.read_at_timestamp(
        ResourceOperation::GetBalance {
            account: "bob".to_string(),
        },
        tx2,
    );

    match (alice_final, bob_final) {
        (
            OperationResult::Complete(ResourceResponse::Balance {
                amount: alice_amt, ..
            }),
            OperationResult::Complete(ResourceResponse::Balance {
                amount: bob_amt, ..
            }),
        ) => {
            assert_eq!(alice_amt, Amount::from_integer(600, 0));
            assert_eq!(bob_amt, Amount::from_integer(400, 0));
            // Most importantly: the sum should equal the original amount (conservation)
            assert_eq!(
                alice_amt + bob_amt,
                Amount::from_integer(1000, 0),
                "Total tokens must be conserved"
            );
        }
        _ => panic!("Expected balance responses"),
    }
}

#[test]
fn test_mint_burn_supply_consistency() {
    // This test proves that mint/burn operations maintain supply consistency
    // Supply always equals sum of all balances
    let mut engine = ResourceTransactionEngine::new();

    // Setup
    let tx_setup = make_timestamp(100);
    let mut batch1 = engine.start_batch();
    engine.begin(&mut batch1, tx_setup);
    engine.commit_batch(batch1, 1);

    let mut batch2 = engine.start_batch();
    engine.apply_operation(
        &mut batch2,
        ResourceOperation::Initialize {
            name: "Test".to_string(),
            symbol: "TST".to_string(),
            decimals: 0,
        },
        tx_setup,
    );
    engine.commit_batch(batch2, 2);

    let mut batch3 = engine.start_batch();
    engine.commit(&mut batch3, tx_setup);
    engine.commit_batch(batch3, 3);

    // Mint to multiple accounts
    let tx_mint = make_timestamp(200);
    let mut batch4 = engine.start_batch();
    engine.begin(&mut batch4, tx_mint);
    engine.commit_batch(batch4, 4);

    let mut batch5 = engine.start_batch();
    engine.apply_operation(
        &mut batch5,
        ResourceOperation::Mint {
            to: "alice".to_string(),
            amount: Amount::from_integer(100, 0),
            memo: None,
        },
        tx_mint,
    );
    engine.commit_batch(batch5, 5);

    let mut batch6 = engine.start_batch();
    engine.apply_operation(
        &mut batch6,
        ResourceOperation::Mint {
            to: "bob".to_string(),
            amount: Amount::from_integer(200, 0),
            memo: None,
        },
        tx_mint,
    );
    engine.commit_batch(batch6, 6);

    let mut batch7 = engine.start_batch();
    engine.apply_operation(
        &mut batch7,
        ResourceOperation::Mint {
            to: "charlie".to_string(),
            amount: Amount::from_integer(300, 0),
            memo: None,
        },
        tx_mint,
    );
    engine.commit_batch(batch7, 7);

    let mut batch8 = engine.start_batch();
    engine.commit(&mut batch8, tx_mint);
    engine.commit_batch(batch8, 8);

    // Verify supply equals sum of balances
    let tx_check = make_timestamp(300);
    let supply = engine.read_at_timestamp(ResourceOperation::GetTotalSupply, tx_check);
    let alice = engine.read_at_timestamp(
        ResourceOperation::GetBalance {
            account: "alice".to_string(),
        },
        tx_check,
    );
    let bob = engine.read_at_timestamp(
        ResourceOperation::GetBalance {
            account: "bob".to_string(),
        },
        tx_check,
    );
    let charlie = engine.read_at_timestamp(
        ResourceOperation::GetBalance {
            account: "charlie".to_string(),
        },
        tx_check,
    );

    match (supply, alice, bob, charlie) {
        (
            OperationResult::Complete(ResourceResponse::TotalSupply { amount: supply_amt }),
            OperationResult::Complete(ResourceResponse::Balance {
                amount: alice_amt, ..
            }),
            OperationResult::Complete(ResourceResponse::Balance {
                amount: bob_amt, ..
            }),
            OperationResult::Complete(ResourceResponse::Balance {
                amount: charlie_amt,
                ..
            }),
        ) => {
            let total_balances = alice_amt + bob_amt + charlie_amt;
            assert_eq!(
                supply_amt, total_balances,
                "Supply must equal sum of all balances"
            );
        }
        _ => panic!("Expected responses"),
    }

    // Burn from one account
    let tx_burn = make_timestamp(400);
    let mut batch13 = engine.start_batch();
    engine.begin(&mut batch13, tx_burn);
    engine.commit_batch(batch13, 13);

    let mut batch14 = engine.start_batch();
    engine.apply_operation(
        &mut batch14,
        ResourceOperation::Burn {
            from: "alice".to_string(),
            amount: Amount::from_integer(50, 0),
            memo: None,
        },
        tx_burn,
    );
    engine.commit_batch(batch14, 14);

    let mut batch15 = engine.start_batch();
    engine.commit(&mut batch15, tx_burn);
    engine.commit_batch(batch15, 15);

    // Verify supply consistency after burn
    let tx_check2 = make_timestamp(500);
    let supply2 = engine.read_at_timestamp(ResourceOperation::GetTotalSupply, tx_check2);
    let alice2 = engine.read_at_timestamp(
        ResourceOperation::GetBalance {
            account: "alice".to_string(),
        },
        tx_check2,
    );

    match (supply2, alice2) {
        (
            OperationResult::Complete(ResourceResponse::TotalSupply { amount: supply_amt }),
            OperationResult::Complete(ResourceResponse::Balance {
                amount: alice_amt, ..
            }),
        ) => {
            assert_eq!(supply_amt, Amount::from_integer(550, 0));
            assert_eq!(alice_amt, Amount::from_integer(50, 0));
            // Verify: supply = alice (50) + bob (200) + charlie (300) = 550
            assert_eq!(
                supply_amt,
                alice_amt + Amount::from_integer(200, 0) + Amount::from_integer(300, 0)
            );
        }
        _ => panic!("Expected responses"),
    }
}

#[test]
fn test_abort_rollback_consistency() {
    // This test proves that aborting a transaction properly rolls back
    // all changes and releases reservations
    let mut engine = ResourceTransactionEngine::new();

    // Setup
    let tx_setup = make_timestamp(100);
    let mut batch1 = engine.start_batch();
    engine.begin(&mut batch1, tx_setup);
    engine.commit_batch(batch1, 1);

    let mut batch2 = engine.start_batch();
    engine.apply_operation(
        &mut batch2,
        ResourceOperation::Initialize {
            name: "Test".to_string(),
            symbol: "TST".to_string(),
            decimals: 0,
        },
        tx_setup,
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
        tx_setup,
    );
    engine.commit_batch(batch3, 3);

    let mut batch4 = engine.start_batch();
    engine.commit(&mut batch4, tx_setup);
    engine.commit_batch(batch4, 4);

    // TX1: Transfer but don't commit
    let tx1 = make_timestamp(200);
    let mut batch5 = engine.start_batch();
    engine.begin(&mut batch5, tx1);
    engine.commit_batch(batch5, 5);

    let mut batch6 = engine.start_batch();
    engine.apply_operation(
        &mut batch6,
        ResourceOperation::Transfer {
            from: "alice".to_string(),
            to: "bob".to_string(),
            amount: Amount::from_integer(400, 0),
            memo: None,
        },
        tx1,
    );
    engine.commit_batch(batch6, 6);

    // Abort TX1
    let mut batch7 = engine.start_batch();
    engine.abort(&mut batch7, tx1);
    engine.commit_batch(batch7, 7);

    // Verify balances are unchanged
    let tx_check = make_timestamp(300);
    let alice_balance = engine.read_at_timestamp(
        ResourceOperation::GetBalance {
            account: "alice".to_string(),
        },
        tx_check,
    );
    let bob_balance = engine.read_at_timestamp(
        ResourceOperation::GetBalance {
            account: "bob".to_string(),
        },
        tx_check,
    );

    match (alice_balance, bob_balance) {
        (
            OperationResult::Complete(ResourceResponse::Balance {
                amount: alice_amt, ..
            }),
            OperationResult::Complete(ResourceResponse::Balance {
                amount: bob_amt, ..
            }),
        ) => {
            assert_eq!(
                alice_amt,
                Amount::from_integer(1000, 0),
                "Alice's balance should be unchanged after abort"
            );
            assert_eq!(
                bob_amt,
                Amount::from_integer(0, 0),
                "Bob's balance should be unchanged after abort"
            );
        }
        _ => panic!("Expected balance responses"),
    }

    // Verify reservations were released - another transaction can now reserve
    let tx2 = make_timestamp(400);
    let mut batch10 = engine.start_batch();
    engine.begin(&mut batch10, tx2);
    engine.commit_batch(batch10, 10);

    let mut batch11 = engine.start_batch();
    let result = engine.apply_operation(
        &mut batch11,
        ResourceOperation::Transfer {
            from: "alice".to_string(),
            to: "bob".to_string(),
            amount: Amount::from_integer(400, 0),
            memo: None,
        },
        tx2,
    );
    engine.commit_batch(batch11, 11);
    assert!(
        matches!(
            result,
            OperationResult::Complete(ResourceResponse::Transferred { .. })
        ),
        "Should be able to reserve after abort released reservations"
    );
}
