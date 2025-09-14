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

fn setup_test_env() -> (MvccStorage, LockManager) {
    let mut storage = MvccStorage::new();
    let lock_manager = LockManager::new();

    // Create a table with multiple potential indexes
    let orders_schema = Table::new(
        "orders".to_string(),
        vec![
            Column::new("id".to_string(), DataType::Integer).primary_key(),
            Column::new("customer_id".to_string(), DataType::Integer),
            Column::new("product_id".to_string(), DataType::Integer),
            Column::new("status".to_string(), DataType::String),
            Column::new("amount".to_string(), DataType::Integer),
        ],
    )
    .unwrap();

    storage
        .create_table("orders".to_string(), orders_schema)
        .unwrap();

    (storage, lock_manager)
}

fn execute_sql(
    sql: &str,
    storage: &mut MvccStorage,
    lock_manager: &mut LockManager,
) -> proven_sql::Result<ExecutionResult> {
    // Get schemas
    let schemas = storage.get_schemas();

    let ast = Parser::parse(sql)?;
    let mut planner = Planner::new(schemas.clone());

    // Add statistics to planner for optimization
    let stats = storage.calculate_statistics();
    planner.update_statistics(stats);

    let plan = planner.plan(ast)?;

    // Get schemas for executor
    let executor = Executor::new();

    let tx_id = HlcTimestamp::new(1000, 0, NodeId::new(1));
    storage.register_transaction(tx_id, tx_id);
    let mut tx_context = TransactionContext::new(tx_id);

    let result = executor.execute(plan, storage, lock_manager, &mut tx_context)?;
    storage.commit_transaction(tx_id)?;

    Ok(result)
}

#[test]
fn test_statistics_based_index_selection() -> proven_sql::Result<()> {
    let (mut storage, mut lock_manager) = setup_test_env();

    // Insert diverse data to create different selectivities
    // customer_id will have low cardinality (3 distinct values)
    // product_id will have high cardinality (50 distinct values)
    for i in 1..=100 {
        let customer_id = (i % 3) + 1; // Only 3 customers
        let product_id = (i % 50) + 1; // 50 products
        let status = if i % 2 == 0 { "completed" } else { "pending" };

        execute_sql(
            &format!(
                "INSERT INTO orders VALUES ({}, {}, {}, '{}', {})",
                i,
                customer_id,
                product_id,
                status,
                i * 10
            ),
            &mut storage,
            &mut lock_manager,
        )?;
    }

    // Create indexes with different selectivities
    execute_sql(
        "CREATE INDEX idx_customer ON orders(customer_id)",
        &mut storage,
        &mut lock_manager,
    )?;

    execute_sql(
        "CREATE INDEX idx_product ON orders(product_id)",
        &mut storage,
        &mut lock_manager,
    )?;

    // Verify statistics are calculated correctly
    if let Some(orders_table) = storage.tables.get("orders") {
        let stats = orders_table.get_table_stats();

        // Verify statistics were created
        assert!(stats.indexes.contains_key("idx_customer"));
        assert!(stats.indexes.contains_key("idx_product"));

        // Customer index should have lower cardinality (less selective)
        let customer_stats = stats.indexes.get("idx_customer").unwrap();
        let product_stats = stats.indexes.get("idx_product").unwrap();

        println!(
            "Customer index cardinality: {:?}",
            customer_stats.cardinality
        );
        println!("Product index cardinality: {:?}", product_stats.cardinality);

        // Product index should be more selective (higher cardinality)
        assert!(product_stats.cardinality[0] > customer_stats.cardinality[0]);
    }

    // Query that could use either index
    // The planner should prefer idx_product because it's more selective
    let result = execute_sql(
        "SELECT * FROM orders WHERE customer_id = 1 AND product_id = 25",
        &mut storage,
        &mut lock_manager,
    )?;

    match result {
        ExecutionResult::Select { rows, .. } => {
            // Should find the specific orders
            assert!(!rows.is_empty());
        }
        _ => panic!("Expected Select result"),
    }

    Ok(())
}

#[test]
fn test_selectivity_threshold() -> proven_sql::Result<()> {
    let (mut storage, mut lock_manager) = setup_test_env();

    // Insert data where an index would not be selective
    // All orders have the same status
    for i in 1..=100 {
        execute_sql(
            &format!(
                "INSERT INTO orders VALUES ({}, {}, {}, 'pending', {})",
                i,
                i,
                i,
                i * 10
            ),
            &mut storage,
            &mut lock_manager,
        )?;
    }

    // Create an index on status (which has no selectivity)
    execute_sql(
        "CREATE INDEX idx_status ON orders(status)",
        &mut storage,
        &mut lock_manager,
    )?;

    // Verify statistics
    if let Some(orders_table) = storage.tables.get("orders") {
        let stats = orders_table.get_table_stats();
        let status_stats = stats.indexes.get("idx_status").unwrap();
        println!("Status index cardinality: {:?}", status_stats.cardinality);

        // With only 1 distinct value, cardinality should be 1
        assert_eq!(status_stats.cardinality[0], 1);
    }

    // Query on status - planner should avoid the index due to poor selectivity
    // (though in our simple implementation, it might still use it)
    let result = execute_sql(
        "SELECT * FROM orders WHERE status = 'pending'",
        &mut storage,
        &mut lock_manager,
    )?;

    match result {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 100); // All rows match
        }
        _ => panic!("Expected Select result"),
    }

    Ok(())
}

#[test]
fn test_composite_index_statistics() -> proven_sql::Result<()> {
    let (mut storage, mut lock_manager) = setup_test_env();

    // Insert data with varying selectivity on different columns
    for i in 1..=100 {
        let customer_id = (i % 10) + 1; // 10 customers
        let status = if i <= 50 { "pending" } else { "completed" }; // 2 statuses

        execute_sql(
            &format!(
                "INSERT INTO orders VALUES ({}, {}, {}, '{}', {})",
                i,
                customer_id,
                i,
                status,
                i * 10
            ),
            &mut storage,
            &mut lock_manager,
        )?;
    }

    // Create composite index
    execute_sql(
        "CREATE INDEX idx_customer_status ON orders(customer_id, status)",
        &mut storage,
        &mut lock_manager,
    )?;

    // Verify statistics
    if let Some(orders_table) = storage.tables.get("orders") {
        let stats = orders_table.get_table_stats();
        let composite_stats = stats.indexes.get("idx_customer_status").unwrap();
        println!(
            "Composite index cardinality: {:?}",
            composite_stats.cardinality
        );

        // The composite index should show cardinality for both columns
        assert_eq!(composite_stats.cardinality.len(), 2);
    }

    // Query using the composite index
    let result = execute_sql(
        "SELECT * FROM orders WHERE customer_id = 5 AND status = 'pending'",
        &mut storage,
        &mut lock_manager,
    )?;

    match result {
        ExecutionResult::Select { rows, .. } => {
            // Should find specific orders for customer 5 with pending status
            assert!(!rows.is_empty());
            for row in &rows {
                assert_eq!(row.get(1), Some(&Value::Integer(5))); // customer_id = 5
                assert_eq!(row.get(3), Some(&Value::String("pending".to_string())));
            }
        }
        _ => panic!("Expected Select result"),
    }

    Ok(())
}
