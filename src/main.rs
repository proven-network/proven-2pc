//! Proof of concept for PCC-based SQL engine

use proven_sql::{
    error::Result,
    lock::{LockKey, LockManager, LockMode},
    storage::{Column, Schema, Storage},
    transaction::TransactionManager,
    types::{DataType, Value},
};
use std::sync::Arc;

fn main() -> Result<()> {
    println!("=== Proven SQL: PCC-Based SQL Engine Demo ===\n");

    // Demo 1: Basic storage operations
    demo_storage()?;

    // Demo 2: Lock manager with wound-wait
    demo_locks()?;

    // Demo 3: Transactions with concurrency control
    demo_transactions()?;

    println!("\n=== All demos completed successfully! ===");
    Ok(())
}

fn demo_storage() -> Result<()> {
    println!("--- Demo 1: Storage Operations ---");

    let storage = Storage::new();

    // Create a users table
    let schema = Schema::new(vec![
        Column::new("id".into(), DataType::Integer).primary_key(),
        Column::new("name".into(), DataType::String),
        Column::new("age".into(), DataType::Integer),
        Column::new("email".into(), DataType::String).unique(),
    ])?;

    storage.create_table("users".into(), schema)?;

    // Insert some data
    storage.with_table_mut("users", |table| {
        table.insert(vec![
            Value::Integer(1),
            Value::String("Alice".into()),
            Value::Integer(30),
            Value::String("alice@example.com".into()),
        ])?;

        table.insert(vec![
            Value::Integer(2),
            Value::String("Bob".into()),
            Value::Integer(25),
            Value::String("bob@example.com".into()),
        ])?;

        println!("  ✓ Inserted 2 users");

        // Create an index on email
        table.create_index("email".into())?;
        println!("  ✓ Created index on email");

        // Look up by index
        let ids = table.index_lookup("email", &Value::String("alice@example.com".into()))?;
        println!("  ✓ Index lookup found user with ID: {:?}", ids);

        Ok(())
    })?;

    // Scan the table
    let table = storage.get_table("users")?;
    let count = table.scan().count();
    println!("  ✓ Table scan found {} users", count);

    Ok(())
}

fn demo_locks() -> Result<()> {
    println!("\n--- Demo 2: Lock Manager ---");

    let lock_manager = LockManager::new();

    let key = LockKey::Row {
        table: "accounts".into(),
        row_id: 100,
    };

    // Transaction 1 acquires exclusive lock
    let result = lock_manager.try_acquire(1, key.clone(), LockMode::Exclusive)?;
    println!("  ✓ Tx1 acquired exclusive lock: {:?}", result);

    // Transaction 2 tries to acquire - gets conflict info
    let result = lock_manager.try_acquire(2, key.clone(), LockMode::Exclusive)?;
    println!("  ✓ Tx2 conflict detected: {:?}", result);

    // Multiple shared locks are compatible
    let shared_key = LockKey::Row {
        table: "products".into(),
        row_id: 200,
    };

    lock_manager.try_acquire(3, shared_key.clone(), LockMode::Shared)?;
    lock_manager.try_acquire(4, shared_key.clone(), LockMode::Shared)?;
    println!("  ✓ Multiple shared locks acquired successfully");

    Ok(())
}

fn demo_transactions() -> Result<()> {
    println!("\n--- Demo 3: Transactions with Wound-Wait ---");

    let lock_manager = Arc::new(LockManager::new());
    let storage = Arc::new(Storage::new());

    // Create accounts table
    let schema = Schema::new(vec![
        Column::new("id".into(), DataType::Integer).primary_key(),
        Column::new("balance".into(), DataType::Integer),
    ])?;

    storage.create_table("accounts".into(), schema)?;

    // Insert initial data
    storage.with_table_mut("accounts", |table| {
        table.insert(vec![Value::Integer(1), Value::Integer(1000)])?;
        table.insert(vec![Value::Integer(2), Value::Integer(2000)])?;
        Ok(())
    })?;

    let tx_manager = TransactionManager::new(lock_manager, storage.clone());

    // Start transaction with priority 100 (older)
    let tx1 = tx_manager.begin_with_priority(Some(100))?;
    println!("  ✓ Transaction 1 started (ID: {}, priority: {})", tx1.id, tx1.priority);

    // Directly update balance
    tx1.write(
        "accounts",
        1,
        vec![
            Value::Integer(1),
            Value::Integer(900), // Deduct 100
        ],
    )?;
    println!("  ✓ Tx1 updated account 1 balance to 900");

    // Start younger transaction (priority 200)
    let tx2 = tx_manager.begin_with_priority(Some(200))?;
    println!("  ✓ Transaction 2 started (ID: {}, priority: {})", tx2.id, tx2.priority);

    // Tx2 tries to write same account - should be blocked (younger waits)
    match tx2.write("accounts", 1, vec![Value::Integer(1), Value::Integer(800)]) {
        Err(proven_sql::error::Error::LockConflict { .. }) => {
            println!("  ✓ Tx2 blocked by Tx1's lock (younger waits for older)");
        }
        _ => println!("  ✗ Unexpected result"),
    }

    // Start older transaction (priority 50)
    let tx3 = tx_manager.begin_with_priority(Some(50))?;
    println!("  ✓ Transaction 3 started (ID: {}, priority: {})", tx3.id, tx3.priority);
    
    // Tx3 tries to write - should wound tx1 using the manager's method
    let key = LockKey::Row {
        table: "accounts".to_string(),
        row_id: 1,
    };
    
    match tx_manager.acquire_lock_with_wound_wait(&tx3, key, LockMode::Exclusive) {
        Ok(()) => println!("  ✓ Tx3 wounded Tx1 and acquired lock (older wounds younger)"),
        Err(e) => println!("  ✗ Error: {:?}", e),
    }

    // Commit Tx3
    tx3.commit()?;
    println!("  ✓ Tx3 committed");

    Ok(())
}
