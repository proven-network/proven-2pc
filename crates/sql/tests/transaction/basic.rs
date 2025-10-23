//! Basic transaction tests
//! Based on gluesql/test-suite/src/transaction/basic.rs

use crate::common::TestContext;
use proven_value::Value;

#[test]
fn test_insert_with_rollback() {
    let mut ctx = TestContext::new();

    // Setup: Create table and insert initial data
    ctx.begin();
    ctx.exec("CREATE TABLE TxTest (id INTEGER, name TEXT)");
    ctx.exec("INSERT INTO TxTest VALUES (1, 'Friday'), (2, 'Phone')");
    ctx.commit();

    // Test: Insert within transaction, then rollback
    ctx.begin();
    ctx.exec("INSERT INTO TxTest VALUES (3, 'New one')");

    // Verify data visible within transaction
    ctx.assert_row_count("SELECT id, name FROM TxTest", 3);

    // Rollback
    ctx.abort();

    // Verify data not persisted after rollback (start new transaction to query)
    ctx.begin();
    ctx.assert_row_count("SELECT id, name FROM TxTest", 2);
    let results = ctx.query("SELECT id, name FROM TxTest ORDER BY id");
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(
        results[0].get("name").unwrap(),
        &Value::Str("Friday".to_string())
    );
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(2));
    assert_eq!(
        results[1].get("name").unwrap(),
        &Value::Str("Phone".to_string())
    );
}

#[test]
fn test_insert_with_commit() {
    let mut ctx = TestContext::new();

    // Setup: Create table and insert initial data
    ctx.begin();
    ctx.exec("CREATE TABLE TxTest (id INTEGER, name TEXT)");
    ctx.exec("INSERT INTO TxTest VALUES (1, 'Friday'), (2, 'Phone')");
    ctx.commit();

    // Test: Insert within transaction, then commit
    ctx.begin();
    ctx.exec("INSERT INTO TxTest VALUES (3, 'Vienna')");

    // Verify data visible within transaction
    ctx.assert_row_count("SELECT id, name FROM TxTest", 3);
    let results = ctx.query("SELECT id, name FROM TxTest ORDER BY id");
    assert_eq!(results[2].get("id").unwrap(), &Value::I32(3));
    assert_eq!(
        results[2].get("name").unwrap(),
        &Value::Str("Vienna".to_string())
    );

    // Commit
    ctx.commit();

    // Verify data persisted after commit (start new transaction to query)
    ctx.begin();
    ctx.assert_row_count("SELECT id, name FROM TxTest", 3);
    let results = ctx.query("SELECT id, name FROM TxTest ORDER BY id");
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(
        results[0].get("name").unwrap(),
        &Value::Str("Friday".to_string())
    );
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(2));
    assert_eq!(
        results[1].get("name").unwrap(),
        &Value::Str("Phone".to_string())
    );
    assert_eq!(results[2].get("id").unwrap(), &Value::I32(3));
    assert_eq!(
        results[2].get("name").unwrap(),
        &Value::Str("Vienna".to_string())
    );
}

#[test]
fn test_delete_with_rollback() {
    let mut ctx = TestContext::new();

    // Setup: Create table and insert data
    ctx.begin();
    ctx.exec("CREATE TABLE TxTest (id INTEGER, name TEXT)");
    ctx.exec("INSERT INTO TxTest VALUES (1, 'Friday'), (2, 'Phone'), (3, 'Vienna')");
    ctx.commit();

    // Test: Delete within transaction, then rollback
    ctx.begin();
    ctx.exec("DELETE FROM TxTest WHERE id = 3");

    // Verify data deleted within transaction
    ctx.assert_row_count("SELECT id, name FROM TxTest", 2);

    // Rollback
    ctx.abort();

    // Verify data restored after rollback (start new transaction to query)
    ctx.begin();
    ctx.assert_row_count("SELECT id, name FROM TxTest", 3);
    let results = ctx.query("SELECT id, name FROM TxTest ORDER BY id");
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(
        results[0].get("name").unwrap(),
        &Value::Str("Friday".to_string())
    );
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(2));
    assert_eq!(
        results[1].get("name").unwrap(),
        &Value::Str("Phone".to_string())
    );
    assert_eq!(results[2].get("id").unwrap(), &Value::I32(3));
    assert_eq!(
        results[2].get("name").unwrap(),
        &Value::Str("Vienna".to_string())
    );
}

#[test]
fn test_delete_with_commit() {
    let mut ctx = TestContext::new();

    // Setup: Create table and insert data
    ctx.begin();
    ctx.exec("CREATE TABLE TxTest (id INTEGER, name TEXT)");
    ctx.exec("INSERT INTO TxTest VALUES (1, 'Friday'), (2, 'Phone'), (3, 'Vienna')");
    ctx.commit();

    // Test: Delete within transaction, then commit
    ctx.begin();
    ctx.exec("DELETE FROM TxTest WHERE id = 3");

    // Verify data deleted within transaction
    ctx.assert_row_count("SELECT id, name FROM TxTest", 2);

    // Commit
    ctx.commit();

    // Verify data permanently deleted after commit (start new transaction to query)
    ctx.begin();
    ctx.assert_row_count("SELECT id, name FROM TxTest", 2);
    let results = ctx.query("SELECT id, name FROM TxTest ORDER BY id");
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(
        results[0].get("name").unwrap(),
        &Value::Str("Friday".to_string())
    );
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(2));
    assert_eq!(
        results[1].get("name").unwrap(),
        &Value::Str("Phone".to_string())
    );
}

#[test]
fn test_update_with_rollback() {
    let mut ctx = TestContext::new();

    // Setup: Create table and insert data
    ctx.begin();
    ctx.exec("CREATE TABLE TxTest (id INTEGER, name TEXT)");
    ctx.exec("INSERT INTO TxTest VALUES (1, 'Friday'), (2, 'Phone')");
    ctx.commit();

    // Test: Update within transaction, then rollback
    ctx.begin();
    ctx.exec("UPDATE TxTest SET name = 'Sunday' WHERE id = 1");

    // Verify data updated within transaction
    let results = ctx.query("SELECT id, name FROM TxTest ORDER BY id");
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(
        results[0].get("name").unwrap(),
        &Value::Str("Sunday".to_string())
    );
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(2));
    assert_eq!(
        results[1].get("name").unwrap(),
        &Value::Str("Phone".to_string())
    );

    // Rollback
    ctx.abort();

    // Verify original data restored after rollback (start new transaction to query)
    ctx.begin();
    let results = ctx.query("SELECT id, name FROM TxTest ORDER BY id");
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(
        results[0].get("name").unwrap(),
        &Value::Str("Friday".to_string())
    );
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(2));
    assert_eq!(
        results[1].get("name").unwrap(),
        &Value::Str("Phone".to_string())
    );
}

#[test]
fn test_update_with_commit() {
    let mut ctx = TestContext::new();

    // Setup: Create table and insert data
    ctx.begin();
    ctx.exec("CREATE TABLE TxTest (id INTEGER, name TEXT)");
    ctx.exec("INSERT INTO TxTest VALUES (1, 'Friday'), (2, 'Phone')");
    ctx.commit();

    // Test: Update within transaction, then commit
    ctx.begin();
    ctx.exec("UPDATE TxTest SET name = 'Sunday' WHERE id = 1");

    // Verify data updated within transaction
    let results = ctx.query("SELECT id, name FROM TxTest ORDER BY id");
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(
        results[0].get("name").unwrap(),
        &Value::Str("Sunday".to_string())
    );

    // Commit
    ctx.commit();

    // Verify data permanently updated after commit (start new transaction to query)
    ctx.begin();
    let results = ctx.query("SELECT id, name FROM TxTest ORDER BY id");
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(
        results[0].get("name").unwrap(),
        &Value::Str("Sunday".to_string())
    );
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(2));
    assert_eq!(
        results[1].get("name").unwrap(),
        &Value::Str("Phone".to_string())
    );
}

#[test]
fn test_read_only_transactions() {
    let mut ctx = TestContext::new();

    // Setup: Create table and insert data
    ctx.begin();
    ctx.exec("CREATE TABLE TxTest (id INTEGER, name TEXT)");
    ctx.exec("INSERT INTO TxTest VALUES (1, 'Friday'), (2, 'Phone')");
    ctx.commit();

    // Test: Read-only transaction with rollback
    ctx.begin();
    ctx.assert_row_count("SELECT * FROM TxTest", 2);
    ctx.abort();

    // Test: Read-only transaction with commit
    ctx.begin();
    ctx.assert_row_count("SELECT * FROM TxTest", 2);
    ctx.commit();

    // Verify data unchanged (start new transaction to query)
    ctx.begin();
    ctx.assert_row_count("SELECT * FROM TxTest", 2);
}

#[test]
fn test_empty_transaction() {
    let mut ctx = TestContext::new();

    // Setup: Create table for reference
    ctx.begin();
    ctx.exec("CREATE TABLE TxTest (id INTEGER, name TEXT)");
    ctx.commit();

    // Test: Empty transaction (begin immediately followed by commit)
    ctx.begin();
    ctx.commit();

    // Verify system still works
    ctx.begin();
    ctx.exec("INSERT INTO TxTest VALUES (1, 'test')");
    ctx.commit();

    ctx.begin();
    ctx.assert_row_count("SELECT * FROM TxTest", 1);
}
