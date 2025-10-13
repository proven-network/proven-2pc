//! GENERATE_UUID function tests
//! Based on gluesql/test-suite/src/function/generate_uuid.rs

mod common;

use common::setup_test;

#[test]
fn test_generate_uuid_no_arguments() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT GENERATE_UUID() AS uuid");

    assert_eq!(results.len(), 1);
    let value = results[0].get("uuid").unwrap();

    // Should return a UUID value
    assert!(value.to_string().len() == 36); // UUID is 36 chars

    ctx.commit();
}

#[test]
fn test_generate_uuid_with_arguments_should_error() {
    let mut ctx = setup_test();

    let error = ctx.exec_error("SELECT GENERATE_UUID(0) AS uuid");

    // Should error - GENERATE_UUID takes no arguments
    assert!(
        error.contains("ExecutionError")
            || error.contains("no arguments")
            || error.contains("0")
            || error.contains("argument"),
        "Expected argument count error, got: {}",
        error
    );

    ctx.commit();
}

#[test]
fn test_generate_uuid_in_values() {
    let mut ctx = setup_test();

    let results = ctx.query("VALUES (GENERATE_UUID())");

    // Should return a single row
    assert_eq!(results.len(), 1);

    ctx.commit();
}

#[test]
fn test_generate_uuid_return_type() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT GENERATE_UUID() AS uuid");

    assert_eq!(results.len(), 1);
    let value = results[0].get("uuid").unwrap();

    // Should return Uuid type
    assert!(value.to_string().len() == 36); // UUID is 36 chars

    ctx.commit();
}

#[test]
fn test_generate_uuid_unique_within_transaction() {
    let mut ctx = setup_test();

    // Each call to GENERATE_UUID should generate a unique UUID
    let results1 = ctx.query("SELECT GENERATE_UUID() AS uuid");
    let uuid1 = results1[0].get("uuid").unwrap().clone();

    let results2 = ctx.query("SELECT GENERATE_UUID() AS uuid");
    let uuid2 = results2[0].get("uuid").unwrap().clone();

    // UUIDs should be different on each call
    assert_ne!(uuid1, uuid2);

    ctx.commit();
}

#[test]
fn test_generate_uuid_format_validity() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT GENERATE_UUID() AS uuid");

    assert_eq!(results.len(), 1);
    let value = results[0].get("uuid").unwrap();

    // Should contain UUID and have proper format
    assert!(value.to_string().len() == 36); // UUID is 36 chars
    // UUID format: 8-4-4-4-12 hex digits
    assert!(value.to_string().contains("-")); // UUIDs have hyphens

    ctx.commit();
}

#[test]
fn test_generate_uuid_in_insert() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Items (id UUID PRIMARY KEY, name TEXT)");
    ctx.exec("INSERT INTO Items VALUES (GENERATE_UUID(), 'test')");

    ctx.assert_row_count("SELECT * FROM Items", 1);

    let results = ctx.query("SELECT id FROM Items");
    let uuid_value = results[0].get("id").unwrap();
    assert!(uuid_value.to_string().len() == 36); // UUID is 36 chars

    ctx.commit();
}

#[test]
fn test_generate_uuid_multiple_inserts_unique_uuids() {
    let mut ctx = setup_test();

    // Each call to GENERATE_UUID() generates a unique UUID
    ctx.exec("CREATE TABLE Items (id UUID, name TEXT)");

    ctx.exec("INSERT INTO Items VALUES (GENERATE_UUID(), 'item1')");
    ctx.exec("INSERT INTO Items VALUES (GENERATE_UUID(), 'item2')");
    ctx.exec("INSERT INTO Items VALUES (GENERATE_UUID(), 'item3')");

    ctx.assert_row_count("SELECT * FROM Items", 3);

    // All UUIDs should be different
    let results = ctx.query("SELECT id FROM Items");
    assert_eq!(results.len(), 3);

    let uuid1 = &results[0].get("id").unwrap();
    let uuid2 = &results[1].get("id").unwrap();
    let uuid3 = &results[2].get("id").unwrap();

    assert_ne!(uuid1, uuid2, "UUIDs should be unique");
    assert_ne!(uuid2, uuid3, "UUIDs should be unique");
    assert_ne!(uuid1, uuid3, "UUIDs should be unique");

    ctx.commit();
}

#[test]
fn test_generate_uuid_with_multiple_arguments_should_error() {
    let mut ctx = setup_test();

    let error = ctx.exec_error("SELECT GENERATE_UUID(1, 2) AS uuid");

    // Should error - GENERATE_UUID takes no arguments
    assert!(
        error.contains("ExecutionError")
            || error.contains("no arguments")
            || error.contains("argument"),
        "Expected argument count error, got: {}",
        error
    );

    ctx.commit();
}

#[test]
fn test_generate_uuid_in_select_list() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Data (value INT)");
    ctx.exec("INSERT INTO Data VALUES (1), (2), (3)");

    let results = ctx.query("SELECT value, GENERATE_UUID() AS uuid FROM Data");

    // Should return 3 rows, each with a UUID
    assert_eq!(results.len(), 3);

    for result in results {
        assert!(result.get("uuid").unwrap().to_string().len() == 36); // UUID is 36 chars
    }

    ctx.commit();
}

#[test]
fn test_generate_uuid_multiple_calls_same_statement() {
    let mut ctx = setup_test();

    // Multiple calls to GENERATE_UUID in the same SELECT should produce different UUIDs
    let results = ctx.query("SELECT GENERATE_UUID() AS uuid1, GENERATE_UUID() AS uuid2");

    assert_eq!(results.len(), 1);
    let uuid1 = results[0].get("uuid1").unwrap();
    let uuid2 = results[0].get("uuid2").unwrap();

    // Even within the same statement, UUIDs should be unique
    assert_ne!(&uuid1, &uuid2, "UUIDs in same statement should be unique");

    ctx.commit();
}
