use proven_sql::execution::ExecutionResult;
use proven_sql::execution::Executor;
use proven_sql::hlc::{HlcTimestamp, NodeId};
use proven_sql::parsing::Parser;
use proven_sql::planning::planner::Planner;
use proven_sql::storage::lock::LockManager;
use proven_sql::storage::mvcc::MvccStorage;
use proven_sql::stream::TransactionContext;
use proven_sql::types::schema::{Column, Table};
use proven_sql::types::value::{DataType, Value};
use std::collections::HashMap;

fn setup_test_env() -> (MvccStorage, LockManager, HashMap<String, Table>) {
    let mut storage = MvccStorage::new();
    let lock_manager = LockManager::new();
    let mut schemas = HashMap::new();

    // Create users table
    let users_schema = Table::new(
        "users".to_string(),
        vec![
            Column::new("id".to_string(), DataType::Integer).primary_key(),
            Column::new("name".to_string(), DataType::String),
            Column::new("age".to_string(), DataType::Integer),
        ],
    )
    .unwrap();

    // Create orders table
    let orders_schema = Table::new(
        "orders".to_string(),
        vec![
            Column::new("id".to_string(), DataType::Integer).primary_key(),
            Column::new("user_id".to_string(), DataType::Integer),
            Column::new("amount".to_string(), DataType::Integer),
            Column::new("status".to_string(), DataType::String),
        ],
    )
    .unwrap();

    storage
        .create_table("users".to_string(), users_schema.clone())
        .unwrap();
    storage
        .create_table("orders".to_string(), orders_schema.clone())
        .unwrap();

    schemas.insert("users".to_string(), users_schema);
    schemas.insert("orders".to_string(), orders_schema);

    (storage, lock_manager, schemas)
}

fn execute_sql(
    sql: &str,
    storage: &mut MvccStorage,
    lock_manager: &mut LockManager,
    schemas: &HashMap<String, Table>,
) -> proven_sql::Result<ExecutionResult> {
    let ast = Parser::parse(sql)?;

    let planner = Planner::new(schemas.clone());
    let plan = planner.plan(ast)?;

    let executor = Executor::new();

    // Create a transaction context
    let tx_id = HlcTimestamp::new(1000, 0, NodeId::new(1));
    storage.register_transaction(tx_id, tx_id);

    let mut tx_context = TransactionContext::new(tx_id);

    let result = executor.execute(plan, storage, lock_manager, &mut tx_context)?;

    // Commit the transaction
    storage.commit_transaction(tx_id)?;

    Ok(result)
}

fn insert_test_data(
    storage: &mut MvccStorage,
    lock_manager: &mut LockManager,
    schemas: &HashMap<String, Table>,
) -> proven_sql::Result<()> {
    // Insert users
    execute_sql(
        "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)",
        storage,
        lock_manager,
        schemas,
    )?;
    execute_sql(
        "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)",
        storage,
        lock_manager,
        schemas,
    )?;
    execute_sql(
        "INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 35)",
        storage,
        lock_manager,
        schemas,
    )?;

    // Insert orders
    execute_sql(
        "INSERT INTO orders (id, user_id, amount, status) VALUES (1, 1, 100, 'pending')",
        storage,
        lock_manager,
        schemas,
    )?;
    execute_sql(
        "INSERT INTO orders (id, user_id, amount, status) VALUES (2, 1, 200, 'completed')",
        storage,
        lock_manager,
        schemas,
    )?;
    execute_sql(
        "INSERT INTO orders (id, user_id, amount, status) VALUES (3, 2, 150, 'pending')",
        storage,
        lock_manager,
        schemas,
    )?;

    Ok(())
}

#[test]
fn test_inner_join() -> proven_sql::Result<()> {
    let (mut storage, mut lock_manager, schemas) = setup_test_env();
    insert_test_data(&mut storage, &mut lock_manager, &schemas)?;

    // Test INNER JOIN
    let result = execute_sql(
        "SELECT users.name, orders.amount 
         FROM users 
         INNER JOIN orders ON users.id = orders.user_id",
        &mut storage,
        &mut lock_manager,
        &schemas,
    )?;

    match result {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 3); // Alice has 2 orders, Bob has 1

            // Check that we have the expected data
            let mut found_alice_100 = false;
            let mut found_alice_200 = false;
            let mut found_bob_150 = false;

            for row in &rows {
                if let (Value::String(name), Value::Integer(amount)) = (&row[0], &row[1]) {
                    if name == "Alice" && *amount == 100 {
                        found_alice_100 = true;
                    } else if name == "Alice" && *amount == 200 {
                        found_alice_200 = true;
                    } else if name == "Bob" && *amount == 150 {
                        found_bob_150 = true;
                    }
                }
            }

            assert!(found_alice_100, "Should find Alice's order for 100");
            assert!(found_alice_200, "Should find Alice's order for 200");
            assert!(found_bob_150, "Should find Bob's order for 150");
        }
        _ => panic!("Expected Select result"),
    }

    Ok(())
}

#[test]
fn test_left_join() -> proven_sql::Result<()> {
    let (mut storage, mut lock_manager, schemas) = setup_test_env();
    insert_test_data(&mut storage, &mut lock_manager, &schemas)?;

    // Test LEFT JOIN - should include Charlie even though he has no orders
    let result = execute_sql(
        "SELECT users.name, orders.amount 
         FROM users 
         LEFT JOIN orders ON users.id = orders.user_id",
        &mut storage,
        &mut lock_manager,
        &schemas,
    )?;

    match result {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 4); // Alice (2), Bob (1), Charlie (1 with NULL)

            // Check that Charlie appears with NULL amount
            let mut found_charlie_null = false;
            for row in &rows {
                if let Value::String(name) = &row[0]
                    && name == "Charlie"
                {
                    assert_eq!(row[1], Value::Null, "Charlie should have NULL amount");
                    found_charlie_null = true;
                }
            }

            assert!(found_charlie_null, "Should find Charlie with NULL order");
        }
        _ => panic!("Expected Select result"),
    }

    Ok(())
}

#[test]
fn test_cross_join() -> proven_sql::Result<()> {
    let (mut storage, mut lock_manager, schemas) = setup_test_env();
    insert_test_data(&mut storage, &mut lock_manager, &schemas)?;

    // Test cross join (implicit)
    let result = execute_sql(
        "SELECT users.name, orders.id FROM users, orders",
        &mut storage,
        &mut lock_manager,
        &schemas,
    )?;

    match result {
        ExecutionResult::Select { rows, .. } => {
            // Cross join should produce 3 users * 3 orders = 9 rows
            assert_eq!(rows.len(), 9);
        }
        _ => panic!("Expected Select result"),
    }

    Ok(())
}

#[test]
fn test_join_with_where_clause() -> proven_sql::Result<()> {
    let (mut storage, mut lock_manager, schemas) = setup_test_env();
    insert_test_data(&mut storage, &mut lock_manager, &schemas)?;

    // Test JOIN with additional WHERE conditions
    let result = execute_sql(
        "SELECT users.name, orders.amount 
         FROM users 
         INNER JOIN orders ON users.id = orders.user_id
         WHERE orders.amount > 150",
        &mut storage,
        &mut lock_manager,
        &schemas,
    )?;

    match result {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 1); // Only Alice's 200 order

            if let (Value::String(name), Value::Integer(amount)) = (&rows[0][0], &rows[0][1]) {
                assert_eq!(name, "Alice");
                assert_eq!(*amount, 200);
            } else {
                panic!("Unexpected row format");
            }
        }
        _ => panic!("Expected Select result"),
    }

    Ok(())
}

#[test]
fn test_self_join() -> proven_sql::Result<()> {
    let (mut storage, mut lock_manager, schemas) = setup_test_env();
    insert_test_data(&mut storage, &mut lock_manager, &schemas)?;

    // Test self-join to find users with same age
    execute_sql(
        "INSERT INTO users (id, name, age) VALUES (4, 'Diana', 30)",
        &mut storage,
        &mut lock_manager,
        &schemas,
    )?;

    let result = execute_sql(
        "SELECT u1.name, u2.name 
         FROM users u1 
         INNER JOIN users u2 ON u1.age = u2.age 
         WHERE u1.id < u2.id",
        &mut storage,
        &mut lock_manager,
        &schemas,
    )?;

    match result {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 1); // Alice and Diana both have age 30

            if let (Value::String(name1), Value::String(name2)) = (&rows[0][0], &rows[0][1]) {
                assert_eq!(name1, "Alice");
                assert_eq!(name2, "Diana");
            } else {
                panic!("Unexpected row format");
            }
        }
        _ => panic!("Expected Select result"),
    }

    Ok(())
}

#[test]
fn test_right_join() -> proven_sql::Result<()> {
    let (mut storage, mut lock_manager, schemas) = setup_test_env();
    insert_test_data(&mut storage, &mut lock_manager, &schemas)?;

    // Test RIGHT JOIN - should include all orders, even if user doesn't exist
    // First add an orphan order without a user
    execute_sql(
        "INSERT INTO orders (id, user_id, amount, status) VALUES (4, 999, 300, 'orphan')",
        &mut storage,
        &mut lock_manager,
        &schemas,
    )?;

    let result = execute_sql(
        "SELECT users.name, orders.amount 
         FROM users 
         RIGHT JOIN orders ON users.id = orders.user_id",
        &mut storage,
        &mut lock_manager,
        &schemas,
    )?;

    match result {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 4); // All 4 orders should appear

            // Check that the orphan order appears with NULL user name
            let mut found_orphan = false;
            for row in &rows {
                if let Value::Integer(amount) = &row[1]
                    && *amount == 300
                {
                    assert_eq!(row[0], Value::Null, "Orphan order should have NULL user");
                    found_orphan = true;
                }
            }

            assert!(found_orphan, "Should find orphan order with NULL user");
        }
        _ => panic!("Expected Select result"),
    }

    Ok(())
}

#[test]
fn test_full_join() -> proven_sql::Result<()> {
    let (mut storage, mut lock_manager, schemas) = setup_test_env();
    insert_test_data(&mut storage, &mut lock_manager, &schemas)?;

    // Add an orphan order (order without user)
    execute_sql(
        "INSERT INTO orders (id, user_id, amount, status) VALUES (4, 999, 300, 'orphan')",
        &mut storage,
        &mut lock_manager,
        &schemas,
    )?;

    // Test FULL OUTER JOIN - should include:
    // - All matched rows (Alice's 2 orders, Bob's 1 order)
    // - Unmatched left rows (Charlie with no orders)
    // - Unmatched right rows (orphan order)
    let result = execute_sql(
        "SELECT users.name, orders.amount 
         FROM users 
         FULL JOIN orders ON users.id = orders.user_id",
        &mut storage,
        &mut lock_manager,
        &schemas,
    )?;

    match result {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 5); // 3 matched + 1 unmatched left (Charlie) + 1 unmatched right (orphan)

            let mut found_charlie_null = false;
            let mut found_orphan_null = false;
            let mut matched_count = 0;

            for row in &rows {
                match (&row[0], &row[1]) {
                    (Value::String(name), Value::Null) if name == "Charlie" => {
                        found_charlie_null = true;
                    }
                    (Value::Null, Value::Integer(300)) => {
                        found_orphan_null = true;
                    }
                    (Value::String(_), Value::Integer(_)) => {
                        matched_count += 1;
                    }
                    _ => {}
                }
            }

            assert_eq!(matched_count, 3, "Should have 3 matched rows");
            assert!(found_charlie_null, "Should find Charlie with NULL order");
            assert!(found_orphan_null, "Should find orphan order with NULL user");
        }
        _ => panic!("Expected Select result"),
    }

    Ok(())
}
