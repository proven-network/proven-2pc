//! Tests for CREATE TABLE statements
//! Based on gluesql/test-suite/src/alter/create_table.rs

mod common;

use common::setup_test;

#[test]
fn test_create_table_basic() {
    let mut ctx = setup_test();

    // Test basic CREATE TABLE with various column types and constraints
    ctx.exec("CREATE TABLE CreateTable1 (id INTEGER NULL, num INTEGER, name TEXT)");

    // Verify table was created by inserting and selecting data
    ctx.exec("INSERT INTO CreateTable1 VALUES (NULL, 1, 'test')");
    ctx.exec("INSERT INTO CreateTable1 VALUES (2, 2, 'test2')");

    let results = ctx.query("SELECT id, num, name FROM CreateTable1 ORDER BY num");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap(), "Null");
    assert_eq!(results[0].get("num").unwrap(), "I32(1)");
    assert_eq!(results[0].get("name").unwrap(), "Str(test)");
    assert_eq!(results[1].get("id").unwrap(), "I32(2)");
    assert_eq!(results[1].get("num").unwrap(), "I32(2)");
    assert_eq!(results[1].get("name").unwrap(), "Str(test2)");

    ctx.commit();
}

#[test]
#[should_panic(expected = "DuplicateTable")]
fn test_create_table_already_exists_error() {
    let mut ctx = setup_test();

    // Create first table
    ctx.exec("CREATE TABLE CreateTable1 (id INTEGER)");

    // Test that creating table with same name throws error
    ctx.exec("CREATE TABLE CreateTable1 (id INTEGER NULL, num INTEGER)");
}

#[test]
fn test_create_table_if_not_exists() {
    let mut ctx = setup_test();

    // Test CREATE TABLE IF NOT EXISTS functionality
    ctx.exec("CREATE TABLE IF NOT EXISTS CreateTable2 (id INTEGER NULL, num INTEGER, name TEXT)");

    // Verify table was created
    ctx.exec("INSERT INTO CreateTable2 VALUES (NULL, 1, '1')");
    ctx.exec("INSERT INTO CreateTable2 VALUES (2, 2, '2')");

    let results = ctx.query("SELECT * FROM CreateTable2 ORDER BY num");
    assert_eq!(results.len(), 2);

    ctx.commit();
}

#[test]
fn test_create_table_if_not_exists_duplicate() {
    let mut ctx = setup_test();

    // Create first table
    ctx.exec("CREATE TABLE IF NOT EXISTS CreateTable2 (id INTEGER NULL, num INTEGER, name TEXT)");
    ctx.exec("INSERT INTO CreateTable2 VALUES (1, 1, 'test')");

    // Test CREATE TABLE IF NOT EXISTS when table already exists (should succeed silently)
    ctx.exec("CREATE TABLE IF NOT EXISTS CreateTable2 (id2 INTEGER NULL)");

    // Verify original table structure is preserved
    ctx.exec("INSERT INTO CreateTable2 VALUES (2, 2, 'test2')");
    let results = ctx.query("SELECT * FROM CreateTable2 ORDER BY num");
    assert_eq!(results.len(), 2);

    ctx.commit();
}

#[test]
fn test_insert_into_created_table() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE CreateTable2 (id INTEGER NULL, num INTEGER, name TEXT)");

    // Test INSERT operations on newly created table
    ctx.exec("INSERT INTO CreateTable2 VALUES (NULL, 1, '1')");
    ctx.exec("INSERT INTO CreateTable2 VALUES (2, 2, '2')");
    ctx.exec("INSERT INTO CreateTable2 VALUES (3, NULL, NULL)");

    let results = ctx.query("SELECT * FROM CreateTable2 ORDER BY num");
    assert_eq!(results.len(), 3);

    // Verify NULL handling - SQL standard: NULLs sort last in ASC order
    assert_eq!(results[0].get("num").unwrap(), "I32(1)");
    assert_eq!(results[1].get("num").unwrap(), "I32(2)");
    assert_eq!(results[2].get("num").unwrap(), "Null");

    ctx.commit();
}

#[test]
#[should_panic(expected = "Parse error")]
fn test_unsupported_data_type_error() {
    let mut ctx = setup_test();

    // Test that unsupported data types throw appropriate errors
    ctx.exec("CREATE TABLE Gluery (id SOMEWHAT)");
}

#[test]
#[should_panic(expected = "Parse error")]
fn test_unsupported_column_option_error() {
    let mut ctx = setup_test();

    // Test that unsupported column options throw appropriate errors
    ctx.exec("CREATE TABLE Gluery (id INTEGER CHECK (true))");
}

#[test]
#[ignore = "UNIQUE on FLOAT not yet validated"]
fn test_unique_constraint_on_unsupported_type_error() {
    let mut ctx = setup_test();

    // Test that UNIQUE constraint on unsupported data types fails
    ctx.exec("CREATE TABLE CreateTable3 (id INTEGER, ratio FLOAT UNIQUE)");
}

#[test]
#[should_panic(expected = "Expression type not supported in DEFAULT expressions")]
fn test_default_value_with_subquery_error() {
    let mut ctx = setup_test();

    // Test that DEFAULT values with subqueries are not supported
    ctx.exec("CREATE TABLE Gluery (id INTEGER DEFAULT (SELECT id FROM Wow))");
}

#[test]
#[ignore = "CREATE TABLE AS SELECT not yet implemented"]
fn test_create_table_as_select_schema_only() {
    let mut ctx = setup_test();

    // Setup source table
    ctx.exec("CREATE TABLE CreateTable2 (id INTEGER NULL, num INTEGER, name TEXT)");
    ctx.exec("INSERT INTO CreateTable2 VALUES (NULL, 1, '1')");
    ctx.exec("INSERT INTO CreateTable2 VALUES (2, 2, '2')");

    // Test CREATE TABLE AS SELECT with no data (schema only)
    ctx.exec("CREATE TABLE TargetTable AS SELECT * FROM CreateTable2 WHERE 1 = 0");

    // Verify empty table was created with correct schema
    let results = ctx.query("SELECT * FROM TargetTable");
    assert_eq!(results.len(), 0);

    // Verify we can insert into the new table
    ctx.exec("INSERT INTO TargetTable VALUES (3, 3, '3')");
    let results = ctx.query("SELECT * FROM TargetTable");
    assert_eq!(results.len(), 1);

    ctx.commit();
}

#[test]
#[ignore = "CREATE TABLE AS SELECT not yet implemented"]
fn test_create_table_as_select_with_data() {
    let mut ctx = setup_test();

    // Setup source table
    ctx.exec("CREATE TABLE CreateTable2 (id INTEGER NULL, num INTEGER, name TEXT)");
    ctx.exec("INSERT INTO CreateTable2 VALUES (NULL, 1, '1')");
    ctx.exec("INSERT INTO CreateTable2 VALUES (2, 2, '2')");

    // Test CREATE TABLE AS SELECT with data
    ctx.exec("CREATE TABLE TargetTableWithData AS SELECT * FROM CreateTable2");

    // Verify data was copied
    let results = ctx.query("SELECT * FROM TargetTableWithData ORDER BY num");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap(), "Null");
    assert_eq!(results[0].get("num").unwrap(), "I32(1)");
    assert_eq!(results[0].get("name").unwrap(), "Str(\"1\")");
    assert_eq!(results[1].get("id").unwrap(), "I32(2)");
    assert_eq!(results[1].get("num").unwrap(), "I32(2)");
    assert_eq!(results[1].get("name").unwrap(), "Str(\"2\")");

    ctx.commit();
}

#[test]
#[ignore = "CREATE TABLE AS SELECT not yet implemented"]
fn test_create_table_as_select_with_limit() {
    let mut ctx = setup_test();

    // Setup source table
    ctx.exec("CREATE TABLE CreateTable2 (id INTEGER NULL, num INTEGER, name TEXT)");
    ctx.exec("INSERT INTO CreateTable2 VALUES (NULL, 1, '1')");
    ctx.exec("INSERT INTO CreateTable2 VALUES (2, 2, '2')");

    // Test CREATE TABLE AS SELECT with LIMIT clause
    ctx.exec("CREATE TABLE TargetTableWithLimit AS SELECT * FROM CreateTable2 LIMIT 1");

    // Verify only one row was copied
    let results = ctx.query("SELECT * FROM TargetTableWithLimit");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), "Null");
    assert_eq!(results[0].get("num").unwrap(), "I32(1)");
    assert_eq!(results[0].get("name").unwrap(), "Str(\"1\")");

    ctx.commit();
}

#[test]
#[ignore = "CREATE TABLE AS SELECT not yet implemented"]
fn test_create_table_as_select_with_offset() {
    let mut ctx = setup_test();

    // Setup source table
    ctx.exec("CREATE TABLE CreateTable2 (id INTEGER NULL, num INTEGER, name TEXT)");
    ctx.exec("INSERT INTO CreateTable2 VALUES (NULL, 1, '1')");
    ctx.exec("INSERT INTO CreateTable2 VALUES (2, 2, '2')");

    // Test CREATE TABLE AS SELECT with OFFSET clause
    ctx.exec("CREATE TABLE TargetTableWithOffset AS SELECT * FROM CreateTable2 OFFSET 1");

    // Verify only second row was copied
    let results = ctx.query("SELECT * FROM TargetTableWithOffset");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), "I32(2)");
    assert_eq!(results[0].get("num").unwrap(), "I32(2)");
    assert_eq!(results[0].get("name").unwrap(), "Str(\"2\")");

    ctx.commit();
}

#[test]
#[ignore = "CREATE TABLE AS SELECT not yet implemented"]
fn test_create_table_as_select_target_exists_error() {
    let mut ctx = setup_test();

    // Setup source table
    ctx.exec("CREATE TABLE CreateTable2 (id INTEGER NULL, num INTEGER, name TEXT)");
    ctx.exec("INSERT INTO CreateTable2 VALUES (NULL, 1, '1')");

    // Create target table
    ctx.exec("CREATE TABLE TargetTableWithData AS SELECT * FROM CreateTable2");

    // Test that CREATE TABLE AS SELECT fails when target table exists
    ctx.exec("CREATE TABLE TargetTableWithData AS SELECT * FROM CreateTable2");
}

#[test]
#[ignore = "CREATE TABLE AS SELECT not yet implemented"]
fn test_create_table_as_select_source_not_found_error() {
    let mut ctx = setup_test();

    // Test that CREATE TABLE AS SELECT fails when source table doesn't exist
    ctx.exec("CREATE TABLE TargetTableWithData2 AS SELECT * FROM NonExistentTable");
}

#[test]
#[ignore = "Duplicate column validation not yet implemented"]
fn test_create_table_duplicate_column_error() {
    let mut ctx = setup_test();

    // Test that CREATE TABLE fails with duplicate column names
    ctx.exec("CREATE TABLE DuplicateColumns (id INT, id INT)");
}
