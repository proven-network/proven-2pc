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

    // Create events table with composite index potential
    let events_schema = Table::new(
        "events".to_string(),
        vec![
            Column::new("id".to_string(), DataType::Integer).primary_key(),
            Column::new("user_id".to_string(), DataType::Integer),
            Column::new("timestamp".to_string(), DataType::Integer),
            Column::new("event_type".to_string(), DataType::String),
            Column::new("data".to_string(), DataType::String),
        ],
    )
    .unwrap();

    storage
        .create_table("events".to_string(), events_schema.clone())
        .unwrap();
    schemas.insert("events".to_string(), events_schema);

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

    let tx_id = HlcTimestamp::new(1000, 0, NodeId::new(1));
    storage.register_transaction(tx_id, tx_id);
    let mut tx_context = TransactionContext::new(tx_id);

    let result = executor.execute(plan, storage, lock_manager, &mut tx_context)?;
    storage.commit_transaction(tx_id)?;

    Ok(result)
}

#[test]
fn test_composite_index_range_scan() -> proven_sql::Result<()> {
    let (mut storage, mut lock_manager, schemas) = setup_test_env();

    // Insert test data
    execute_sql(
        "INSERT INTO events VALUES (1, 100, 1000, 'login', 'data1')",
        &mut storage,
        &mut lock_manager,
        &schemas,
    )?;
    execute_sql(
        "INSERT INTO events VALUES (2, 100, 1100, 'click', 'data2')",
        &mut storage,
        &mut lock_manager,
        &schemas,
    )?;
    execute_sql(
        "INSERT INTO events VALUES (3, 100, 1200, 'logout', 'data3')",
        &mut storage,
        &mut lock_manager,
        &schemas,
    )?;
    execute_sql(
        "INSERT INTO events VALUES (4, 101, 1050, 'login', 'data4')",
        &mut storage,
        &mut lock_manager,
        &schemas,
    )?;
    execute_sql(
        "INSERT INTO events VALUES (5, 101, 1150, 'click', 'data5')",
        &mut storage,
        &mut lock_manager,
        &schemas,
    )?;

    // Create composite index on (user_id, timestamp)
    execute_sql(
        "CREATE INDEX idx_user_time ON events(user_id, timestamp)",
        &mut storage,
        &mut lock_manager,
        &schemas,
    )?;

    // Range query: Find events for user 100 between timestamps 1050 and 1150
    let result = execute_sql(
        "SELECT * FROM events WHERE user_id = 100 AND timestamp >= 1050 AND timestamp <= 1150",
        &mut storage,
        &mut lock_manager,
        &schemas,
    )?;

    match result {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].first(), Some(&Value::Integer(2))); // id = 2
            assert_eq!(rows[0].get(2), Some(&Value::Integer(1100))); // timestamp = 1100
        }
        _ => panic!("Expected Select result"),
    }

    Ok(())
}

#[test]
fn test_composite_index_with_nulls() -> proven_sql::Result<()> {
    let (mut storage, mut lock_manager, schemas) = setup_test_env();

    // Insert data with NULL values
    execute_sql(
        "INSERT INTO events VALUES (1, 100, 1000, 'login', 'data1')",
        &mut storage,
        &mut lock_manager,
        &schemas,
    )?;
    execute_sql(
        "INSERT INTO events VALUES (2, NULL, 1100, 'click', 'data2')",
        &mut storage,
        &mut lock_manager,
        &schemas,
    )?;
    execute_sql(
        "INSERT INTO events VALUES (3, 100, NULL, 'logout', 'data3')",
        &mut storage,
        &mut lock_manager,
        &schemas,
    )?;

    // Create composite index - should skip NULL entries
    execute_sql(
        "CREATE INDEX idx_user_time ON events(user_id, timestamp)",
        &mut storage,
        &mut lock_manager,
        &schemas,
    )?;

    // Query should only find non-NULL entries
    let result = execute_sql(
        "SELECT * FROM events WHERE user_id = 100 AND timestamp = 1000",
        &mut storage,
        &mut lock_manager,
        &schemas,
    )?;

    match result {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].first(), Some(&Value::Integer(1)));
        }
        _ => panic!("Expected Select result"),
    }

    Ok(())
}

#[test]
fn test_composite_index_ordering() -> proven_sql::Result<()> {
    let (mut storage, mut lock_manager, schemas) = setup_test_env();

    // Insert data to test lexicographic ordering
    execute_sql(
        "INSERT INTO events VALUES (1, 100, 1000, 'a', 'data1')",
        &mut storage,
        &mut lock_manager,
        &schemas,
    )?;
    execute_sql(
        "INSERT INTO events VALUES (2, 100, 2000, 'b', 'data2')",
        &mut storage,
        &mut lock_manager,
        &schemas,
    )?;
    execute_sql(
        "INSERT INTO events VALUES (3, 101, 1000, 'c', 'data3')",
        &mut storage,
        &mut lock_manager,
        &schemas,
    )?;
    execute_sql(
        "INSERT INTO events VALUES (4, 101, 2000, 'd', 'data4')",
        &mut storage,
        &mut lock_manager,
        &schemas,
    )?;

    // Create composite index
    execute_sql(
        "CREATE INDEX idx_user_time ON events(user_id, timestamp)",
        &mut storage,
        &mut lock_manager,
        &schemas,
    )?;

    // Query with ORDER BY matching index order
    let result = execute_sql(
        "SELECT id FROM events ORDER BY user_id, timestamp",
        &mut storage,
        &mut lock_manager,
        &schemas,
    )?;

    match result {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 4);
            // Should be ordered: (100,1000), (100,2000), (101,1000), (101,2000)
            assert_eq!(rows[0].first(), Some(&Value::Integer(1)));
            assert_eq!(rows[1].first(), Some(&Value::Integer(2)));
            assert_eq!(rows[2].first(), Some(&Value::Integer(3)));
            assert_eq!(rows[3].first(), Some(&Value::Integer(4)));
        }
        _ => panic!("Expected Select result"),
    }

    Ok(())
}

#[test]
fn test_mixed_type_composite_index() -> proven_sql::Result<()> {
    let (mut storage, mut lock_manager, schemas) = setup_test_env();

    // Insert data with mixed types in composite index
    execute_sql(
        "INSERT INTO events VALUES (1, 100, 1000, 'alpha', 'data1')",
        &mut storage,
        &mut lock_manager,
        &schemas,
    )?;
    execute_sql(
        "INSERT INTO events VALUES (2, 100, 1000, 'beta', 'data2')",
        &mut storage,
        &mut lock_manager,
        &schemas,
    )?;
    execute_sql(
        "INSERT INTO events VALUES (3, 101, 1000, 'alpha', 'data3')",
        &mut storage,
        &mut lock_manager,
        &schemas,
    )?;

    // Create composite index on integer and string columns
    execute_sql(
        "CREATE INDEX idx_user_type ON events(user_id, event_type)",
        &mut storage,
        &mut lock_manager,
        &schemas,
    )?;

    // Query using both columns
    let result = execute_sql(
        "SELECT * FROM events WHERE user_id = 100 AND event_type = 'beta'",
        &mut storage,
        &mut lock_manager,
        &schemas,
    )?;

    match result {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].first(), Some(&Value::Integer(2)));
            assert_eq!(rows[0].get(3), Some(&Value::String("beta".to_string())));
        }
        _ => panic!("Expected Select result"),
    }

    Ok(())
}
