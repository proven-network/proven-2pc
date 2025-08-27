//! Proof of concept for PCC-based SQL engine

use proven_sql::{
    error::Result,
    hlc::{HlcClock, HlcTimestamp, NodeId},
    sql,
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
    
    // Demo 4: SQL parser
    demo_sql_parser()?;

    println!("\n=== All demos completed successfully! ==="  );
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

    // Create HLC timestamps for transactions
    let clock = HlcClock::new(NodeId::new(1));
    let tx1 = clock.now();
    let tx2 = clock.now();
    let tx3 = clock.now();
    let tx4 = clock.now();

    let key = LockKey::Row {
        table: "accounts".into(),
        row_id: 100,
    };

    // Transaction 1 acquires exclusive lock
    let result = lock_manager.try_acquire(tx1, key.clone(), LockMode::Exclusive)?;
    println!("  ✓ Tx1 acquired exclusive lock: {:?}", result);

    // Transaction 2 tries to acquire - gets conflict info
    let result = lock_manager.try_acquire(tx2, key.clone(), LockMode::Exclusive)?;
    println!("  ✓ Tx2 conflict detected: {:?}", result);

    // Multiple shared locks are compatible
    let shared_key = LockKey::Row {
        table: "products".into(),
        row_id: 200,
    };

    lock_manager.try_acquire(tx3, shared_key.clone(), LockMode::Shared)?;
    lock_manager.try_acquire(tx4, shared_key.clone(), LockMode::Shared)?;
    println!("  ✓ Multiple shared locks acquired successfully");

    Ok(())
}

fn demo_transactions() -> Result<()> {
    println!("\n--- Demo 3: Transactions with HLC-based Wound-Wait ---");

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

    // Use a single transaction manager with one clock
    // In a real distributed system, each node would have its own clock
    // but they'd share transaction state through consensus
    let clock = Arc::new(HlcClock::new(NodeId::new(1)));
    let tx_manager = TransactionManager::with_clock(
        lock_manager.clone(),
        storage.clone(),
        clock,
    );

    // Start first transaction
    let tx1 = tx_manager.begin()?;
    println!("  ✓ Transaction 1 started (ID: {})", tx1.id);

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

    // Sleep to ensure tx2 gets a later timestamp
    std::thread::sleep(std::time::Duration::from_millis(10));

    // Start younger transaction
    let tx2 = tx_manager.begin()?;
    println!("  ✓ Transaction 2 started (ID: {})", tx2.id);
    println!("  ✓ Tx2 is younger than Tx1: {}", tx1.id.is_older_than(&tx2.id));

    // Tx2 tries to write same account - should be blocked (younger waits)
    match tx2.write("accounts", 1, vec![Value::Integer(1), Value::Integer(800)]) {
        Err(proven_sql::error::Error::LockConflict { .. }) => {
            println!("  ✓ Tx2 blocked by Tx1's lock (younger waits for older)");
        }
        _ => println!("  ✗ Unexpected result"),
    }

    // Create an artificially older transaction for demo
    // In real system, this would be a transaction that started much earlier
    let older_timestamp = HlcTimestamp::new(
        tx1.id.physical - 1000000, // Much earlier physical time
        0,
        NodeId::new(3),
    );
    let tx3 = tx_manager.begin_with_timestamp(older_timestamp)?;
    println!("  ✓ Transaction 3 started (ID: {})", tx3.id);
    println!("  ✓ Tx3 is older than Tx1: {}", tx3.id.is_older_than(&tx1.id));
    
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

fn demo_sql_parser() -> Result<()> {
    println!("\n--- Demo 4: SQL Parser ---");
    
    // Test various SQL statements
    let statements = vec![
        "SELECT * FROM users WHERE age > 25",
        "INSERT INTO accounts (id, balance) VALUES (1, 1000), (2, 2000)",
        "UPDATE users SET age = 31 WHERE id = 1",
        "DELETE FROM users WHERE name = 'Bob'",
        "CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR NOT NULL, price INT)",
        "BEGIN READ ONLY",
        "COMMIT",
    ];
    
    for stmt_str in statements {
        match sql::parse_sql(stmt_str) {
            Ok(_stmt) => println!("  ✓ Parsed: {}", stmt_str.split_whitespace().next().unwrap_or("")),
            Err(e) => println!("  ✗ Failed to parse '{}': {:?}", stmt_str, e),
        }
    }
    
    Ok(())
}
