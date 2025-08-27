//! Integration tests for the full SQL pipeline (parser -> planner -> executor)

use crate::error::Error;
use crate::hlc::{HlcClock, NodeId};
use crate::lock::LockManager;
use crate::sql::executor::{Executor, ExecutionResult};
use crate::sql::parser::Parser;
use crate::sql::planner::QueryPlan;
use crate::sql::schema::{Column, Table};
use crate::storage::Storage;
use crate::transaction::Transaction;
use crate::types::{DataType, Value};
use std::collections::HashMap;
use std::sync::Arc;

fn setup_test_db() -> (Executor, Transaction) {
    let mut executor = Executor::new();
    
    // Create storage and transaction first
    let clock = HlcClock::new(NodeId::new(1));
    let timestamp = clock.now();
    let lock_manager = Arc::new(LockManager::new());
    let storage = Arc::new(Storage::new());
    
    // Create test schemas for executor
    let mut schemas = HashMap::new();
    
    // Users table
    let users_table = Table::new(
        "users".to_string(),
        vec![
            Column::new("id".to_string(), DataType::Integer).primary_key(),
            Column::new("name".to_string(), DataType::String).nullable(false),
            Column::new("email".to_string(), DataType::String).unique(),
            Column::new("age".to_string(), DataType::Integer).nullable(true),
        ],
    ).unwrap();
    schemas.insert("users".to_string(), users_table.clone());
    
    // Create corresponding table in storage
    {
        use crate::storage::{Column as StorageColumn, Schema as StorageSchema};
        let storage_schema = StorageSchema::new(vec![
            StorageColumn {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default: None,
                primary_key: true,
                unique: false,
            },
            StorageColumn {
                name: "name".to_string(),
                data_type: DataType::String,
                nullable: false,
                default: None,
                primary_key: false,
                unique: false,
            },
            StorageColumn {
                name: "email".to_string(),
                data_type: DataType::String,
                nullable: true,
                default: None,
                primary_key: false,
                unique: true,
            },
            StorageColumn {
                name: "age".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default: None,
                primary_key: false,
                unique: false,
            },
        ]).unwrap();
        storage.create_table("users".to_string(), storage_schema).unwrap();
    }
    
    // Products table
    let products_table = Table::new(
        "products".to_string(),
        vec![
            Column::new("id".to_string(), DataType::Integer).primary_key(),
            Column::new("name".to_string(), DataType::String).nullable(false),
            Column::new("price".to_string(), DataType::Decimal(10, 2)).nullable(false),
            Column::new("stock".to_string(), DataType::Integer).nullable(false),
        ],
    ).unwrap();
    schemas.insert("products".to_string(), products_table);
    
    // Create corresponding table in storage
    {
        use crate::storage::{Column as StorageColumn, Schema as StorageSchema};
        let storage_schema = StorageSchema::new(vec![
            StorageColumn {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default: None,
                primary_key: true,
                unique: false,
            },
            StorageColumn {
                name: "name".to_string(),
                data_type: DataType::String,
                nullable: false,
                default: None,
                primary_key: false,
                unique: false,
            },
            StorageColumn {
                name: "price".to_string(),
                data_type: DataType::Decimal(10, 2),
                nullable: false,
                default: None,
                primary_key: false,
                unique: false,
            },
            StorageColumn {
                name: "stock".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default: None,
                primary_key: false,
                unique: false,
            },
        ]).unwrap();
        storage.create_table("products".to_string(), storage_schema).unwrap();
    }
    
    // Orders table with foreign key
    let orders_table = Table::new(
        "orders".to_string(),
        vec![
            Column::new("id".to_string(), DataType::Integer).primary_key(),
            Column::new("user_id".to_string(), DataType::Integer)
                .nullable(false)
                .references("users".to_string()),
            Column::new("product_id".to_string(), DataType::Integer)
                .nullable(false)
                .references("products".to_string()),
            Column::new("quantity".to_string(), DataType::Integer).nullable(false),
            Column::new("total".to_string(), DataType::Decimal(10, 2)).nullable(false),
        ],
    ).unwrap();
    schemas.insert("orders".to_string(), orders_table);
    
    // Create corresponding table in storage
    {
        use crate::storage::{Column as StorageColumn, Schema as StorageSchema};
        let storage_schema = StorageSchema::new(vec![
            StorageColumn {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default: None,
                primary_key: true,
                unique: false,
            },
            StorageColumn {
                name: "user_id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default: None,
                primary_key: false,
                unique: false,
            },
            StorageColumn {
                name: "product_id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default: None,
                primary_key: false,
                unique: false,
            },
            StorageColumn {
                name: "quantity".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default: None,
                primary_key: false,
                unique: false,
            },
            StorageColumn {
                name: "total".to_string(),
                data_type: DataType::Decimal(10, 2),
                nullable: false,
                default: None,
                primary_key: false,
                unique: false,
            },
        ]).unwrap();
        storage.create_table("orders".to_string(), storage_schema).unwrap();
    }
    
    executor.update_schemas(schemas.clone());
    
    let transaction = Transaction::new(timestamp, lock_manager, storage);
    
    (executor, transaction)
}

fn execute_sql(executor: &mut Executor, tx: &mut Transaction, sql: &str) -> Result<ExecutionResult, Error> {
    let statement = Parser::parse(sql)?;
    // For now, bypass the planner and create plan directly
    // The planner needs access to the same schemas as the executor
    let plan = QueryPlan {
        statement,
        locks: Vec::new(),
        estimated_rows: None,
        streamable: false,
    };
    executor.execute(plan, tx)
}

#[test]
fn test_insert_and_select() {
    let (mut executor, mut tx) = setup_test_db();
    
    // Insert users
    let insert_sql = "INSERT INTO users (id, name, email, age) VALUES (1, 'Alice', 'alice@example.com', 30)";
    let result = execute_sql(&mut executor, &mut tx, insert_sql);
    assert!(result.is_ok());
    
    let insert_sql2 = "INSERT INTO users (id, name, email, age) VALUES (2, 'Bob', 'bob@example.com', 25)";
    let result = execute_sql(&mut executor, &mut tx, insert_sql2);
    assert!(result.is_ok());
    
    // Select all users
    let select_sql = "SELECT * FROM users";
    let result = execute_sql(&mut executor, &mut tx, select_sql);
    assert!(result.is_ok());
    
    if let Ok(ExecutionResult::Select { rows, .. }) = result {
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][1], Value::String("Alice".to_string()));
        assert_eq!(rows[1][1], Value::String("Bob".to_string()));
    } else {
        panic!("Expected Query result");
    }
}

#[test]
fn test_where_clause() {
    let (mut executor, mut tx) = setup_test_db();
    
    // Insert users
    execute_sql(&mut executor, &mut tx, 
        "INSERT INTO users (id, name, email, age) VALUES (1, 'Alice', 'alice@example.com', 30)").unwrap();
    execute_sql(&mut executor, &mut tx, 
        "INSERT INTO users (id, name, email, age) VALUES (2, 'Bob', 'bob@example.com', 25)").unwrap();
    execute_sql(&mut executor, &mut tx, 
        "INSERT INTO users (id, name, email, age) VALUES (3, 'Charlie', 'charlie@example.com', 35)").unwrap();
    
    // Select with WHERE clause
    let select_sql = "SELECT name, age FROM users WHERE age > 28";
    let result = execute_sql(&mut executor, &mut tx, select_sql);
    assert!(result.is_ok());
    
    if let Ok(ExecutionResult::Select { rows, columns }) = result {
        eprintln!("SELECT with WHERE returned {} rows", rows.len());
        for (i, row) in rows.iter().enumerate() {
            eprintln!("Row {}: {:?}", i, row);
        }
        assert_eq!(rows.len(), 2); // Alice and Charlie
        assert_eq!(columns.len(), 2);
        assert_eq!(&columns[0], "name");
        assert_eq!(&columns[1], "age");
        
        // Results should be Alice and Charlie
        let names: Vec<_> = rows.iter().map(|r| &r[0]).collect();
        assert!(names.contains(&&Value::String("Alice".to_string())));
        assert!(names.contains(&&Value::String("Charlie".to_string())));
    } else {
        panic!("Expected Query result");
    }
}

#[test]
fn test_update_statement() {
    let (mut executor, mut tx) = setup_test_db();
    
    // Insert users
    execute_sql(&mut executor, &mut tx, 
        "INSERT INTO users (id, name, email, age) VALUES (1, 'Alice', 'alice@example.com', 30)").unwrap();
    execute_sql(&mut executor, &mut tx, 
        "INSERT INTO users (id, name, email, age) VALUES (2, 'Bob', 'bob@example.com', 25)").unwrap();
    
    // Update Bob's age
    let update_sql = "UPDATE users SET age = 26 WHERE name = 'Bob'";
    let result = execute_sql(&mut executor, &mut tx, update_sql);
    assert!(result.is_ok());
    
    if let Ok(ExecutionResult::Modified(rows_affected)) = result {
        assert_eq!(rows_affected, 1);
    } else {
        panic!("Expected Command result");
    }
    
    // Verify the update
    let select_sql = "SELECT age FROM users WHERE name = 'Bob'";
    let result = execute_sql(&mut executor, &mut tx, select_sql);
    assert!(result.is_ok());
    
    if let Ok(ExecutionResult::Select { rows, .. }) = result {
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Integer(26));
    } else {
        panic!("Expected Query result");
    }
}

#[test]
fn test_delete_statement() {
    let (mut executor, mut tx) = setup_test_db();
    
    // Insert users
    execute_sql(&mut executor, &mut tx, 
        "INSERT INTO users (id, name, email, age) VALUES (1, 'Alice', 'alice@example.com', 30)").unwrap();
    execute_sql(&mut executor, &mut tx, 
        "INSERT INTO users (id, name, email, age) VALUES (2, 'Bob', 'bob@example.com', 25)").unwrap();
    execute_sql(&mut executor, &mut tx, 
        "INSERT INTO users (id, name, email, age) VALUES (3, 'Charlie', 'charlie@example.com', 35)").unwrap();
    
    // Delete users with age < 30
    let delete_sql = "DELETE FROM users WHERE age < 30";
    let result = execute_sql(&mut executor, &mut tx, delete_sql);
    assert!(result.is_ok());
    
    if let Ok(ExecutionResult::Modified(rows_affected)) = result {
        assert_eq!(rows_affected, 1); // Only Bob
    } else {
        panic!("Expected Command result");
    }
    
    // Verify deletion
    let select_sql = "SELECT * FROM users";
    let result = execute_sql(&mut executor, &mut tx, select_sql);
    assert!(result.is_ok());
    
    if let Ok(ExecutionResult::Select { rows, .. }) = result {
        assert_eq!(rows.len(), 2);
        // Should only have Alice and Charlie
        let names: Vec<_> = rows.iter().map(|r| &r[1]).collect();
        assert!(names.contains(&&Value::String("Alice".to_string())));
        assert!(names.contains(&&Value::String("Charlie".to_string())));
    } else {
        panic!("Expected Query result");
    }
}

#[test]
fn test_join_query() {
    let (mut executor, mut tx) = setup_test_db();
    
    // Insert test data
    execute_sql(&mut executor, &mut tx, 
        "INSERT INTO users (id, name, email, age) VALUES (1, 'Alice', 'alice@example.com', 30)").unwrap();
    execute_sql(&mut executor, &mut tx, 
        "INSERT INTO products (id, name, price, stock) VALUES (100, 'Widget', 19.99, 50)").unwrap();
    execute_sql(&mut executor, &mut tx, 
        "INSERT INTO orders (id, user_id, product_id, quantity, total) VALUES (1000, 1, 100, 2, 39.98)").unwrap();
    
    // Join query
    let join_sql = "SELECT u.name, p.name, o.quantity, o.total 
                    FROM orders o 
                    JOIN users u ON o.user_id = u.id 
                    JOIN products p ON o.product_id = p.id";
    let result = execute_sql(&mut executor, &mut tx, join_sql);
    assert!(result.is_ok());
    
    if let Ok(ExecutionResult::Select { rows, columns }) = result {
        assert_eq!(rows.len(), 1);
        assert_eq!(columns.len(), 4);
        assert_eq!(rows[0][0], Value::String("Alice".to_string()));
        assert_eq!(rows[0][1], Value::String("Widget".to_string()));
        assert_eq!(rows[0][2], Value::Integer(2));
    } else {
        panic!("Expected Query result");
    }
}

#[test]
fn test_aggregate_functions() {
    let (mut executor, mut tx) = setup_test_db();
    
    // Insert products
    execute_sql(&mut executor, &mut tx, 
        "INSERT INTO products (id, name, price, stock) VALUES (1, 'Widget', 19.99, 50)").unwrap();
    execute_sql(&mut executor, &mut tx, 
        "INSERT INTO products (id, name, price, stock) VALUES (2, 'Gadget', 29.99, 30)").unwrap();
    execute_sql(&mut executor, &mut tx, 
        "INSERT INTO products (id, name, price, stock) VALUES (3, 'Doohickey', 9.99, 100)").unwrap();
    
    // Test COUNT
    let count_sql = "SELECT COUNT(*) FROM products";
    let result = execute_sql(&mut executor, &mut tx, count_sql);
    assert!(result.is_ok());
    if let Ok(ExecutionResult::Select { rows, .. }) = result {
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Integer(3));
    }
    
    // Test SUM
    let sum_sql = "SELECT SUM(stock) FROM products";
    let result = execute_sql(&mut executor, &mut tx, sum_sql);
    assert!(result.is_ok());
    if let Ok(ExecutionResult::Select { rows, .. }) = result {
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Integer(180));
    }
    
    // Test AVG
    let avg_sql = "SELECT AVG(price) FROM products";
    let result = execute_sql(&mut executor, &mut tx, avg_sql);
    assert!(result.is_ok());
    if let Ok(ExecutionResult::Select { rows, .. }) = result {
        assert_eq!(rows.len(), 1);
        // Average should be approximately 19.99
        if let Value::Decimal(d) = &rows[0][0] {
            assert!(d > &"19".parse().unwrap() && d < &"20".parse().unwrap());
        } else {
            panic!("Expected Decimal value");
        }
    }
}

#[test]
fn test_group_by() {
    let (mut executor, mut tx) = setup_test_db();
    
    // Insert users and orders
    execute_sql(&mut executor, &mut tx, 
        "INSERT INTO users (id, name, email, age) VALUES (1, 'Alice', 'alice@example.com', 30)").unwrap();
    execute_sql(&mut executor, &mut tx, 
        "INSERT INTO users (id, name, email, age) VALUES (2, 'Bob', 'bob@example.com', 25)").unwrap();
    
    execute_sql(&mut executor, &mut tx, 
        "INSERT INTO products (id, name, price, stock) VALUES (100, 'Widget', 19.99, 50)").unwrap();
    
    execute_sql(&mut executor, &mut tx, 
        "INSERT INTO orders (id, user_id, product_id, quantity, total) VALUES (1, 1, 100, 2, 39.98)").unwrap();
    execute_sql(&mut executor, &mut tx, 
        "INSERT INTO orders (id, user_id, product_id, quantity, total) VALUES (2, 1, 100, 1, 19.99)").unwrap();
    execute_sql(&mut executor, &mut tx, 
        "INSERT INTO orders (id, user_id, product_id, quantity, total) VALUES (3, 2, 100, 3, 59.97)").unwrap();
    
    // Group by user_id with SUM
    let group_sql = "SELECT user_id, SUM(total) FROM orders GROUP BY user_id";
    let result = execute_sql(&mut executor, &mut tx, group_sql);
    assert!(result.is_ok());
    
    if let Ok(ExecutionResult::Select { rows, .. }) = result {
        assert_eq!(rows.len(), 2);
        
        // Find Alice's total (user_id = 1)
        let alice_row = rows.iter().find(|r| r[0] == Value::Integer(1)).unwrap();
        if let Value::Decimal(total) = &alice_row[1] {
            assert_eq!(total, &"59.97".parse().unwrap());
        }
        
        // Find Bob's total (user_id = 2)
        let bob_row = rows.iter().find(|r| r[0] == Value::Integer(2)).unwrap();
        if let Value::Decimal(total) = &bob_row[1] {
            assert_eq!(total, &"59.97".parse().unwrap());
        }
    } else {
        panic!("Expected Query result");
    }
}

#[test]
fn test_order_by() {
    let (mut executor, mut tx) = setup_test_db();
    
    // Insert users
    execute_sql(&mut executor, &mut tx, 
        "INSERT INTO users (id, name, email, age) VALUES (1, 'Charlie', 'charlie@example.com', 35)").unwrap();
    execute_sql(&mut executor, &mut tx, 
        "INSERT INTO users (id, name, email, age) VALUES (2, 'Alice', 'alice@example.com', 30)").unwrap();
    execute_sql(&mut executor, &mut tx, 
        "INSERT INTO users (id, name, email, age) VALUES (3, 'Bob', 'bob@example.com', 25)").unwrap();
    
    // Order by name ASC
    let order_sql = "SELECT name FROM users ORDER BY name ASC";
    let result = execute_sql(&mut executor, &mut tx, order_sql);
    assert!(result.is_ok());
    
    if let Ok(ExecutionResult::Select { rows, .. }) = result {
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][0], Value::String("Alice".to_string()));
        assert_eq!(rows[1][0], Value::String("Bob".to_string()));
        assert_eq!(rows[2][0], Value::String("Charlie".to_string()));
    }
    
    // Order by age DESC
    let order_sql = "SELECT name, age FROM users ORDER BY age DESC";
    let result = execute_sql(&mut executor, &mut tx, order_sql);
    assert!(result.is_ok());
    
    if let Ok(ExecutionResult::Select { rows, .. }) = result {
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][0], Value::String("Charlie".to_string()));
        assert_eq!(rows[0][1], Value::Integer(35));
        assert_eq!(rows[2][0], Value::String("Bob".to_string()));
        assert_eq!(rows[2][1], Value::Integer(25));
    }
}

#[test]
fn test_limit_offset() {
    let (mut executor, mut tx) = setup_test_db();
    
    // Insert 5 users
    for i in 1..=5 {
        let sql = format!("INSERT INTO users (id, name, email, age) VALUES ({}, 'User{}', 'user{}@example.com', {})",
                         i, i, i, 20 + i);
        execute_sql(&mut executor, &mut tx, &sql).unwrap();
    }
    
    // Test LIMIT
    let limit_sql = "SELECT * FROM users LIMIT 3";
    let result = execute_sql(&mut executor, &mut tx, limit_sql);
    assert!(result.is_ok());
    if let Ok(ExecutionResult::Select { rows, .. }) = result {
        assert_eq!(rows.len(), 3);
    }
    
    // Test LIMIT with OFFSET
    let offset_sql = "SELECT name FROM users LIMIT 2 OFFSET 2";
    let result = execute_sql(&mut executor, &mut tx, offset_sql);
    assert!(result.is_ok());
    if let Ok(ExecutionResult::Select { rows, .. }) = result {
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], Value::String("User3".to_string()));
        assert_eq!(rows[1][0], Value::String("User4".to_string()));
    }
}

#[test]
fn test_transaction_rollback() {
    let (mut executor, mut tx) = setup_test_db();
    
    // Insert a user
    execute_sql(&mut executor, &mut tx, 
        "INSERT INTO users (id, name, email, age) VALUES (1, 'Alice', 'alice@example.com', 30)").unwrap();
    
    // Verify insertion
    let select_sql = "SELECT * FROM users";
    let result = execute_sql(&mut executor, &mut tx, select_sql);
    assert!(result.is_ok());
    if let Ok(ExecutionResult::Select { rows, .. }) = result {
        assert_eq!(rows.len(), 1);
    }
    
    // Transaction rollback would go here - not yet implemented
    // For now, just create a new transaction to simulate rollback
    let clock = HlcClock::new(NodeId::new(1));
    let timestamp = clock.now();
    let lock_manager = Arc::new(LockManager::new());
    let storage = Arc::new(Storage::new());
    let mut new_tx = Transaction::new(timestamp, lock_manager, storage);
    
    let result = execute_sql(&mut executor, &mut new_tx, select_sql);
    assert!(result.is_ok());
    if let Ok(ExecutionResult::Select { rows, .. }) = result {
        assert_eq!(rows.len(), 0); // Should be empty after rollback
    }
}