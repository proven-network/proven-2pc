//! Tests for the MVCC SQL executor

#[cfg(test)]
mod tests {
    use crate::error::Result;
    use crate::hlc::{HlcClock, HlcTimestamp, NodeId};
    use crate::storage::lock::LockManager;
    use crate::storage::mvcc::MvccStorage;
    use crate::sql::execution::{Executor, ExecutionResult};
    use crate::sql::parser::Parser;
    use crate::sql::planner::planner::Planner;
    use crate::sql::types::schema::{Column as SchemaColumn, Table};
    use crate::sql::types::value::{DataType, Value};
    use crate::storage::transaction::MvccTransactionManager;
    use crate::context::TransactionContext;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn setup_test() -> (Arc<MvccStorage>, Arc<LockManager>, HlcClock) {
        let storage = Arc::new(MvccStorage::new());
        let lock_manager = Arc::new(LockManager::new());
        let clock = HlcClock::new(NodeId::new(1));
        (storage, lock_manager, clock)
    }

    #[test]
    fn test_select_scan() -> Result<()> {
        let (storage, lock_manager, clock) = setup_test();
        
        // Create a test table schema
        let mut schemas = HashMap::new();
        schemas.insert(
            "users".to_string(),
            Table {
                name: "users".to_string(),
                columns: vec![
                    SchemaColumn {
                        name: "id".to_string(),
                        data_type: DataType::Integer,
                        nullable: false,
                        default: None,
                        unique: false,
                    },
                    SchemaColumn {
                        name: "name".to_string(),
                        data_type: DataType::String,
                        nullable: false,
                        default: None,
                        unique: false,
                    },
                ],
                primary_key: Some(vec![0]),
            },
        );

        // Create executor with schemas
        let executor = Executor::new(schemas.clone());
        
        // Begin a transaction
        let tx_manager = MvccTransactionManager::new(lock_manager.clone(), storage.clone());
        let tx = tx_manager.begin(clock.now());
        
        // Insert some test data
        {
            let mut tx_guard = tx.lock().unwrap();
            tx_guard.insert("users", vec![Value::Integer(1), Value::String("Alice".to_string())])?;
            tx_guard.insert("users", vec![Value::Integer(2), Value::String("Bob".to_string())])?;
            tx_guard.commit()?;
        }
        
        // Start a new transaction for the query
        let query_tx = tx_manager.begin(clock.now());
        let context = TransactionContext::new(query_tx.lock().unwrap().id);
        
        // Parse and plan a simple SELECT query
        let sql = "SELECT * FROM users";
        let statement = Parser::parse(sql)?;
        let planner = Planner::new(schemas.clone());
        let plan = planner.plan(statement)?;
        
        // Execute the query
        let result = executor.execute(plan, &query_tx, &context)?;
        
        match result {
            ExecutionResult::Select { columns, rows } => {
                assert_eq!(columns, vec!["id", "name"]);
                assert_eq!(rows.len(), 2);
                assert_eq!(*rows[0], vec![Value::Integer(1), Value::String("Alice".to_string())]);
                assert_eq!(*rows[1], vec![Value::Integer(2), Value::String("Bob".to_string())]);
            }
            _ => panic!("Expected Select result"),
        }
        
        Ok(())
    }

    #[test]
    fn test_select_with_filter() -> Result<()> {
        let (storage, lock_manager, clock) = setup_test();
        
        // Create a test table schema
        let mut schemas = HashMap::new();
        schemas.insert(
            "products".to_string(),
            Table {
                name: "products".to_string(),
                columns: vec![
                    SchemaColumn {
                        name: "id".to_string(),
                        data_type: DataType::Integer,
                        nullable: false,
                        default: None,
                        unique: false,
                    },
                    SchemaColumn {
                        name: "name".to_string(),
                        data_type: DataType::String,
                        nullable: false,
                        default: None,
                        unique: false,
                    },
                    SchemaColumn {
                        name: "price".to_string(),
                        data_type: DataType::Integer,
                        nullable: false,
                        default: None,
                        unique: false,
                    },
                ],
                primary_key: Some(vec![0]),
            },
        );

        let executor = Executor::new(schemas.clone());
        let tx_manager = MvccTransactionManager::new(lock_manager.clone(), storage.clone());
        let tx = tx_manager.begin(clock.now());
        
        // Insert test data
        {
            let mut tx_guard = tx.lock().unwrap();
            tx_guard.insert("products", vec![Value::Integer(1), Value::String("Widget".to_string()), Value::Integer(100)])?;
            tx_guard.insert("products", vec![Value::Integer(2), Value::String("Gadget".to_string()), Value::Integer(200)])?;
            tx_guard.insert("products", vec![Value::Integer(3), Value::String("Thing".to_string()), Value::Integer(50)])?;
            tx_guard.commit()?;
        }
        
        // Query with WHERE clause
        let query_tx = tx_manager.begin(clock.now());
        let context = TransactionContext::new(query_tx.lock().unwrap().id);
        
        let sql = "SELECT name, price FROM products WHERE price > 75";
        let statement = Parser::parse(sql)?;
        let planner = Planner::new(schemas.clone());
        let plan = planner.plan(statement)?;
        
        let result = executor.execute(plan, &query_tx, &context)?;
        
        match result {
            ExecutionResult::Select { columns, rows } => {
                assert_eq!(columns, vec!["name", "price"]);
                assert_eq!(rows.len(), 2); // Only Widget and Gadget have price > 75
            }
            _ => panic!("Expected Select result"),
        }
        
        Ok(())
    }

    #[test]
    fn test_aggregate_count() -> Result<()> {
        let (storage, lock_manager, clock) = setup_test();
        
        let mut schemas = HashMap::new();
        schemas.insert(
            "orders".to_string(),
            Table {
                name: "orders".to_string(),
                columns: vec![
                    SchemaColumn {
                        name: "id".to_string(),
                        data_type: DataType::Integer,
                        nullable: false,
                        default: None,
                        unique: false,
                    },
                    SchemaColumn {
                        name: "customer".to_string(),
                        data_type: DataType::String,
                        nullable: false,
                        default: None,
                        unique: false,
                    },
                ],
                primary_key: Some(vec![0]),
            },
        );

        let executor = Executor::new(schemas.clone());
        let tx_manager = MvccTransactionManager::new(lock_manager.clone(), storage.clone());
        let tx = tx_manager.begin(clock.now());
        
        // Insert test data
        {
            let mut tx_guard = tx.lock().unwrap();
            tx_guard.insert("orders", vec![Value::Integer(1), Value::String("Alice".to_string())])?;
            tx_guard.insert("orders", vec![Value::Integer(2), Value::String("Bob".to_string())])?;
            tx_guard.insert("orders", vec![Value::Integer(3), Value::String("Alice".to_string())])?;
            tx_guard.commit()?;
        }
        
        // Test COUNT(*)
        let query_tx = tx_manager.begin(clock.now());
        let context = TransactionContext::new(query_tx.lock().unwrap().id);
        
        let sql = "SELECT COUNT(*) FROM orders";
        let statement = Parser::parse(sql)?;
        let planner = Planner::new(schemas.clone());
        let plan = planner.plan(statement)?;
        
        let result = executor.execute(plan, &query_tx, &context)?;
        
        match result {
            ExecutionResult::Select { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(*rows[0], vec![Value::Integer(3)]);
            }
            _ => panic!("Expected Select result"),
        }
        
        Ok(())
    }
}