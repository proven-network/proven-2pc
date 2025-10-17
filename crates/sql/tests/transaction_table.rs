//! Transaction table DDL tests
//! Based on gluesql/test-suite/src/transaction/table.rs

mod common;

use common::TestContext;
use proven_value::Value;

#[test]
fn test_create_table_with_rollback() {
    let mut ctx = TestContext::new();

    // Test: Create table within transaction, then rollback
    ctx.begin();
    ctx.exec("CREATE TABLE Test (id INTEGER)");
    ctx.exec("INSERT INTO Test VALUES (1)");

    // Verify table exists and has data within transaction
    ctx.assert_row_count("SELECT * FROM Test", 1);
    let results = ctx.query("SELECT * FROM Test");
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));

    // Rollback
    ctx.abort();

    // Verify table no longer exists after rollback
    ctx.begin();
    ctx.assert_error_contains("SELECT * FROM Test", "TableNotFound");
}

#[test]
fn test_create_table_with_commit() {
    let mut ctx = TestContext::new();

    // Test: Create table within transaction, then commit
    ctx.begin();
    ctx.exec("CREATE TABLE Test (id INTEGER)");
    ctx.exec("INSERT INTO Test VALUES (3)");
    ctx.commit();

    // Verify table persisted after commit
    ctx.begin();
    ctx.assert_row_count("SELECT * FROM Test", 1);
    let results = ctx.query("SELECT * FROM Test");
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(3));
}

#[test]
fn test_drop_table_with_rollback() {
    let mut ctx = TestContext::new();

    // Setup: Create table with data
    ctx.begin();
    ctx.exec("CREATE TABLE Test (id INTEGER)");
    ctx.exec("INSERT INTO Test VALUES (3)");
    ctx.commit();

    // Test: Drop table within transaction, then rollback
    ctx.begin();
    ctx.exec("DROP TABLE Test");

    // Verify table not found within transaction
    ctx.assert_error_contains("SELECT * FROM Test", "TableNotFound");

    // Rollback
    ctx.abort();

    // Verify table restored after rollback
    ctx.begin();
    ctx.assert_row_count("SELECT * FROM Test", 1);
    let results = ctx.query("SELECT * FROM Test");
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(3));
}

#[test]
fn test_drop_table_with_commit() {
    let mut ctx = TestContext::new();

    // Setup: Create table with data
    ctx.begin();
    ctx.exec("CREATE TABLE Test (id INTEGER)");
    ctx.exec("INSERT INTO Test VALUES (3)");
    ctx.commit();

    // Test: Drop table within transaction, then commit
    ctx.begin();
    ctx.exec("DROP TABLE Test");
    ctx.commit();

    // Verify table permanently dropped after commit
    ctx.begin();
    ctx.assert_error_contains("SELECT * FROM Test", "TableNotFound");
}

#[test]
fn test_alter_table_rename_table_with_rollback() {
    let mut ctx = TestContext::new();

    // Setup: Create table and insert data
    ctx.begin();
    ctx.exec("CREATE TABLE RenameTable (id INTEGER)");
    ctx.exec("INSERT INTO RenameTable VALUES (1)");
    ctx.commit();

    // Test: Rename table within transaction, then rollback
    ctx.begin();
    ctx.exec("ALTER TABLE RenameTable RENAME TO NewName");

    // Verify new name works within transaction
    ctx.assert_error_contains("SELECT * FROM RenameTable", "TableNotFound");
    ctx.assert_row_count("SELECT * FROM NewName", 1);
    let results = ctx.query("SELECT * FROM NewName");
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));

    // Rollback
    ctx.abort();

    // Verify original name restored after rollback
    ctx.begin();
    ctx.assert_error_contains("SELECT * FROM NewName", "TableNotFound");
    ctx.assert_row_count("SELECT * FROM RenameTable", 1);
    let results = ctx.query("SELECT * FROM RenameTable");
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
}

#[test]
fn test_alter_table_rename_table_with_commit() {
    let mut ctx = TestContext::new();

    // Setup: Create table and insert data
    ctx.begin();
    ctx.exec("CREATE TABLE RenameTable (id INTEGER)");
    ctx.exec("INSERT INTO RenameTable VALUES (1)");
    ctx.commit();

    // Test: Rename table within transaction, then commit
    ctx.begin();
    ctx.exec("ALTER TABLE RenameTable RENAME TO NewName");

    // Verify new name works within transaction
    ctx.assert_error_contains("SELECT * FROM RenameTable", "TableNotFound");
    ctx.assert_row_count("SELECT * FROM NewName", 1);

    // Commit
    ctx.commit();

    // Verify new name persisted after commit
    ctx.begin();
    ctx.assert_error_contains("SELECT * FROM RenameTable", "TableNotFound");
    ctx.assert_row_count("SELECT * FROM NewName", 1);
    let results = ctx.query("SELECT * FROM NewName");
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
}

#[test]
fn test_alter_table_rename_column_with_rollback() {
    let mut ctx = TestContext::new();

    // Setup: Create table and insert data
    ctx.begin();
    ctx.exec("CREATE TABLE RenameCol (id INTEGER)");
    ctx.exec("INSERT INTO RenameCol VALUES (1)");
    ctx.commit();

    // Test: Rename column within transaction, then rollback
    ctx.begin();
    ctx.exec("ALTER TABLE RenameCol RENAME COLUMN id TO new_id");

    // Verify new column name works within transaction
    let results = ctx.query("SELECT * FROM RenameCol");
    assert_eq!(results[0].get("new_id").unwrap(), &Value::I32(1));

    // Rollback
    ctx.abort();

    // Verify original column name restored after rollback
    ctx.begin();
    let results = ctx.query("SELECT * FROM RenameCol");
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
}

#[test]
fn test_alter_table_rename_column_with_commit() {
    let mut ctx = TestContext::new();

    // Setup: Create table and insert data
    ctx.begin();
    ctx.exec("CREATE TABLE RenameCol (id INTEGER)");
    ctx.exec("INSERT INTO RenameCol VALUES (1)");
    ctx.commit();

    // Test: Rename column within transaction, then commit
    ctx.begin();
    ctx.exec("ALTER TABLE RenameCol RENAME COLUMN id TO new_id");

    // Verify new column name works within transaction
    let results = ctx.query("SELECT * FROM RenameCol");
    assert_eq!(results[0].get("new_id").unwrap(), &Value::I32(1));

    // Commit
    ctx.commit();

    // Verify new column name persisted after commit
    ctx.begin();
    let results = ctx.query("SELECT * FROM RenameCol");
    assert_eq!(results[0].get("new_id").unwrap(), &Value::I32(1));
}

#[test]
fn test_alter_table_add_column_with_rollback() {
    let mut ctx = TestContext::new();

    // Setup: Create table and insert data
    ctx.begin();
    ctx.exec("CREATE TABLE AddCol (id INTEGER)");
    ctx.exec("INSERT INTO AddCol VALUES (1)");
    ctx.commit();

    // Test: Add column within transaction, then rollback
    ctx.begin();
    ctx.exec("ALTER TABLE AddCol ADD COLUMN new_col INTEGER DEFAULT 3");

    // Verify new column exists within transaction
    let results = ctx.query("SELECT * FROM AddCol");
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[0].get("new_col").unwrap(), &Value::I32(3));

    // Rollback
    ctx.abort();

    // Verify column not added after rollback
    ctx.begin();
    let results = ctx.query("SELECT * FROM AddCol");
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[0].get("new_col"), None);
}

#[test]
fn test_alter_table_add_column_with_commit() {
    let mut ctx = TestContext::new();

    // Setup: Create table and insert data
    ctx.begin();
    ctx.exec("CREATE TABLE AddCol (id INTEGER)");
    ctx.exec("INSERT INTO AddCol VALUES (1)");
    ctx.commit();

    // Test: Add column within transaction, then commit
    ctx.begin();
    ctx.exec("ALTER TABLE AddCol ADD COLUMN new_col INTEGER DEFAULT 3");

    // Verify new column exists within transaction
    let results = ctx.query("SELECT * FROM AddCol");
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[0].get("new_col").unwrap(), &Value::I32(3));

    // Commit
    ctx.commit();

    // Verify column persisted after commit
    ctx.begin();
    let results = ctx.query("SELECT * FROM AddCol");
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[0].get("new_col").unwrap(), &Value::I32(3));
}

#[test]
fn test_alter_table_drop_column_with_rollback() {
    let mut ctx = TestContext::new();

    // Setup: Create table and insert data
    ctx.begin();
    ctx.exec("CREATE TABLE DropCol (id INTEGER, num INTEGER)");
    ctx.exec("INSERT INTO DropCol VALUES (1, 2)");
    ctx.commit();

    // Test: Drop column within transaction, then rollback
    ctx.begin();
    ctx.exec("ALTER TABLE DropCol DROP COLUMN num");

    // Verify column dropped within transaction
    let results = ctx.query("SELECT * FROM DropCol");
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[0].get("num"), None);

    // Rollback
    ctx.abort();

    // Verify column restored after rollback
    ctx.begin();
    let results = ctx.query("SELECT * FROM DropCol");
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[0].get("num").unwrap(), &Value::I32(2));
}

#[test]
fn test_alter_table_drop_column_with_commit() {
    let mut ctx = TestContext::new();

    // Setup: Create table and insert data
    ctx.begin();
    ctx.exec("CREATE TABLE DropCol (id INTEGER, num INTEGER)");
    ctx.exec("INSERT INTO DropCol VALUES (1, 2)");
    ctx.commit();

    // Test: Drop column within transaction, then commit
    ctx.begin();
    ctx.exec("ALTER TABLE DropCol DROP COLUMN num");

    // Verify column dropped within transaction
    let results = ctx.query("SELECT * FROM DropCol");
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[0].get("num"), None);

    // Commit
    ctx.commit();

    // Verify column permanently dropped after commit
    ctx.begin();
    let results = ctx.query("SELECT * FROM DropCol");
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[0].get("num"), None);
}
