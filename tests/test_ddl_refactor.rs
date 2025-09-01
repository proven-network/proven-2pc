// Test to verify DDL refactoring - DDL operations are now handled by executor

use proven_sql::hlc::{HlcTimestamp, NodeId};
use proven_sql::stream::TransactionContext;
use proven_sql::{Executor, LockManager, MvccStorage, Parser, Planner, Result};
use std::collections::HashMap;

fn main() -> Result<()> {
    println!("Testing DDL refactoring...\n");

    // Create storage and lock manager
    let mut storage = MvccStorage::new();
    let mut lock_manager = LockManager::new();

    // Create transaction context
    let tx_id = HlcTimestamp::new(1000, 0, NodeId::new(1));
    storage.register_transaction(tx_id, tx_id);
    let mut tx_ctx = TransactionContext::new(tx_id);

    // Test CREATE TABLE through executor
    {
        println!("1. Testing CREATE TABLE through executor:");
        let sql = "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR)";

        // Parse and plan
        let ast = Parser::parse(sql)?;
        let planner = Planner::new(HashMap::new());
        let plan = planner.plan(ast)?;

        // Execute through executor (DDL is now handled here!)
        let executor = Executor::new();
        let result = executor.execute(plan, &mut storage, &mut lock_manager, &mut tx_ctx)?;

        match result {
            proven_sql::ExecutionResult::DDL(msg) => {
                println!("   ✓ {}", msg);
            }
            _ => panic!("Expected DDL result"),
        }

        // Verify table was created in storage
        let schemas = storage.get_schemas();
        assert!(schemas.contains_key("users"));
        println!("   ✓ Table exists in storage");
    }

    // Test INSERT to verify table works
    {
        println!("\n2. Testing INSERT into created table:");
        let sql = "INSERT INTO users VALUES (1, 'Alice')";

        // Need updated schemas for planner
        let schemas = storage.get_schemas();

        let ast = Parser::parse(sql)?;
        let planner = Planner::new(schemas.clone());
        let plan = planner.plan(ast)?;

        let executor = Executor::new();
        let result = executor.execute(plan, &mut storage, &mut lock_manager, &mut tx_ctx)?;

        match result {
            proven_sql::ExecutionResult::Modified(n) => {
                println!("   ✓ Inserted {} row(s)", n);
            }
            _ => panic!("Expected Modified result"),
        }
    }

    // Test CREATE INDEX through executor
    {
        println!("\n3. Testing CREATE INDEX through executor:");
        let sql = "CREATE INDEX idx_name ON users(name)";

        let schemas = storage.get_schemas();
        let ast = Parser::parse(sql)?;
        let planner = Planner::new(schemas.clone());
        let plan = planner.plan(ast)?;

        let executor = Executor::new();
        let result = executor.execute(plan, &mut storage, &mut lock_manager, &mut tx_ctx)?;

        match result {
            proven_sql::ExecutionResult::DDL(msg) => {
                println!("   ✓ {}", msg);
            }
            _ => panic!("Expected DDL result"),
        }
    }

    // Test DROP TABLE through executor
    {
        println!("\n4. Testing DROP TABLE through executor:");
        let sql = "DROP TABLE users";

        let schemas = storage.get_schemas();
        let ast = Parser::parse(sql)?;
        let planner = Planner::new(schemas.clone());
        let plan = planner.plan(ast)?;

        let executor = Executor::new();
        let result = executor.execute(plan, &mut storage, &mut lock_manager, &mut tx_ctx)?;

        match result {
            proven_sql::ExecutionResult::DDL(msg) => {
                println!("   ✓ {}", msg);
            }
            _ => panic!("Expected DDL result"),
        }

        // Verify table was dropped from storage
        let schemas = storage.get_schemas();
        assert!(!schemas.contains_key("users"));
        println!("   ✓ Table removed from storage");
    }

    // Test DROP TABLE IF EXISTS
    {
        println!("\n5. Testing DROP TABLE IF EXISTS (non-existent table):");
        let sql = "DROP TABLE IF EXISTS nonexistent";

        let schemas = storage.get_schemas();
        let ast = Parser::parse(sql)?;
        let planner = Planner::new(schemas.clone());
        let plan = planner.plan(ast)?;

        let executor = Executor::new();
        let result = executor.execute(plan, &mut storage, &mut lock_manager, &mut tx_ctx)?;

        match result {
            proven_sql::ExecutionResult::DDL(msg) => {
                println!("   ✓ {}", msg);
                assert!(msg.contains("does not exist") || msg.contains("dropped"));
            }
            _ => panic!("Expected DDL result"),
        }
    }

    // Commit transaction
    storage.commit_transaction(tx_id)?;

    println!("\n✅ All DDL refactoring tests passed!");
    println!("\nKey improvements:");
    println!("- DDL operations are now handled by the executor (not processor)");
    println!("- No duplicate schema tracking in processor");
    println!("- Consistent execution path for all SQL operations");
    println!("- Proper IF EXISTS handling for DROP TABLE");

    Ok(())
}
