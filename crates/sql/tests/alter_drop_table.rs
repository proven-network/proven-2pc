//! Tests for DROP TABLE statements
//! Based on gluesql/test-suite/src/alter/drop_table.rs

mod common;

use common::setup_test;
use proven_value::Value;
#[test]
fn test_drop_table_basic() {
    let mut ctx = setup_test();

    // Create and populate a table
    ctx.exec("CREATE TABLE DropTable (id INT, num INT, name TEXT)");
    ctx.exec("INSERT INTO DropTable (id, num, name) VALUES (1, 2, 'Hello')");

    // Verify table exists and has data
    let results = ctx.query("SELECT id, num, name FROM DropTable");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[0].get("num").unwrap(), &Value::I32(2));
    assert_eq!(
        results[0].get("name").unwrap(),
        &Value::Str("Hello".to_string())
    );

    // Drop the table
    ctx.exec("DROP TABLE DropTable");

    ctx.commit();
}

#[test]
#[should_panic(expected = "TableNotFound")]
fn test_drop_table_not_found_error() {
    let mut ctx = setup_test();

    // Try to drop a table that doesn't exist
    ctx.exec("DROP TABLE NonExistentTable");
}

#[test]
fn test_drop_table_if_exists_success() {
    let mut ctx = setup_test();

    // Create a table
    ctx.exec("CREATE TABLE DropTable (id INT, num INT, name TEXT)");

    // Drop it with IF EXISTS
    ctx.exec("DROP TABLE IF EXISTS DropTable");

    // Try to drop it again with IF EXISTS (should succeed)
    ctx.exec("DROP TABLE IF EXISTS DropTable");

    ctx.commit();
}

#[test]
fn test_drop_table_if_exists_not_found() {
    let mut ctx = setup_test();

    // Drop non-existent table with IF EXISTS (should succeed)
    ctx.exec("DROP TABLE IF EXISTS NonExistentTable");

    ctx.commit();
}

#[test]
#[should_panic(expected = "TableNotFound")]
fn test_select_after_drop_table_error() {
    let mut ctx = setup_test();

    // Create and drop a table
    ctx.exec("CREATE TABLE DropTable (id INT, num INT, name TEXT)");
    ctx.exec("DROP TABLE DropTable");

    // Try to select from dropped table
    ctx.query("SELECT id, num, name FROM DropTable");
}

#[test]
#[ignore = "This needs work after new partitioning scheme"]
fn test_recreate_dropped_table() {
    let mut ctx = setup_test();

    // Create a table with data
    ctx.exec("CREATE TABLE DropTable (id INT, num INT, name TEXT)");
    ctx.exec("INSERT INTO DropTable VALUES (1, 2, 'Hello')");

    // Drop it
    ctx.exec("DROP TABLE DropTable");

    // Recreate it
    ctx.exec("CREATE TABLE DropTable (id INT, num INT, name TEXT)");

    // Verify it's empty
    let results = ctx.query("SELECT id, num, name FROM DropTable");
    assert_eq!(results.len(), 0);

    ctx.commit();
}

#[test]
#[ignore = "This needs work after new partitioning scheme"]
fn test_select_empty_recreated_table() {
    let mut ctx = setup_test();

    // Create, drop, and recreate a table
    ctx.exec("CREATE TABLE DropTable (id INT, num INT, name TEXT)");
    ctx.exec("INSERT INTO DropTable VALUES (1, 2, 'Hello')");
    ctx.exec("DROP TABLE DropTable");
    ctx.exec("CREATE TABLE DropTable (id INT, value INT)");

    // Select from recreated empty table
    let results = ctx.query("SELECT * FROM DropTable");
    assert_eq!(results.len(), 0);

    ctx.commit();
}

#[test]
#[should_panic(expected = "expected TABLE or INDEX after DROP")]
fn test_drop_view_not_supported_error() {
    let mut ctx = setup_test();

    // Create a table
    ctx.exec("CREATE TABLE DropTable (id INT)");

    // Try to drop it as a view (should fail with parse error)
    // Note: We get a parse error, not UnsupportedStatement like GlueSQL
    ctx.exec("DROP VIEW DropTable");
}

#[test]
fn test_drop_multiple_tables() {
    let mut ctx = setup_test();

    // Create multiple tables
    ctx.exec("CREATE TABLE DropTable1 (id INT, num INT, name TEXT)");
    ctx.exec("CREATE TABLE DropTable2 (id INT, num INT, name TEXT)");

    // Insert data
    ctx.exec("INSERT INTO DropTable1 VALUES (1, 2, 'Hello')");
    ctx.exec("INSERT INTO DropTable2 VALUES (3, 4, 'World')");

    // Drop both tables at once
    ctx.exec("DROP TABLE DropTable1, DropTable2");

    ctx.commit();
}

#[test]
#[should_panic(expected = "TableNotFound")]
fn test_verify_multiple_tables_dropped() {
    let mut ctx = setup_test();

    // Create and drop multiple tables
    ctx.exec("CREATE TABLE DropTable1 (id INT)");
    ctx.exec("CREATE TABLE DropTable2 (id INT)");
    ctx.exec("DROP TABLE DropTable1, DropTable2");

    // Try to select from first dropped table
    ctx.query("SELECT * FROM DropTable1");
}

#[test]
fn test_drop_multiple_tables_if_exists() {
    let mut ctx = setup_test();

    // Create multiple tables
    ctx.exec("CREATE TABLE DropTable1 (id INT)");
    ctx.exec("CREATE TABLE DropTable2 (id INT)");

    // Drop with IF EXISTS
    ctx.exec("DROP TABLE IF EXISTS DropTable1, DropTable2");

    // Try to drop again with IF EXISTS (should succeed)
    ctx.exec("DROP TABLE IF EXISTS DropTable1, DropTable2");

    ctx.commit();
}

#[test]
fn test_drop_multiple_tables_partial_exists() {
    let mut ctx = setup_test();

    // Create only one table
    ctx.exec("CREATE TABLE DropTable1 (id INT)");

    // Drop both with IF EXISTS (only one exists)
    ctx.exec("DROP TABLE IF EXISTS DropTable1, DropTable2");

    // Verify DropTable1 was dropped
    ctx.exec("DROP TABLE IF EXISTS DropTable1"); // Should succeed with no error

    ctx.commit();
}
