//! ENTRIES function tests
//! Based on gluesql/test-suite/src/function/entries.rs

mod common;

use common::setup_test;

#[test]
fn test_create_table_with_map_column() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Item (data MAP(VARCHAR, VARCHAR))");

    ctx.commit();
}

#[test]
fn test_insert_map_data() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Item (data MAP(VARCHAR, VARCHAR))");
    ctx.exec(r#"INSERT INTO Item VALUES ('{"name":"Proven"}')"#);

    ctx.assert_row_count("SELECT * FROM Item", 1);

    ctx.commit();
}

#[test]
fn test_entries_returns_key_value_pairs() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Item (data MAP(VARCHAR, VARCHAR))");
    ctx.exec(r#"INSERT INTO Item VALUES ('{"name":"Proven"}')"#);

    let results = ctx.query("SELECT ENTRIES(data) AS test FROM Item");

    assert_eq!(results.len(), 1);
    let value = results[0].get("test").unwrap();

    // Should return a list containing key-value pairs
    // Expected: [["name", "Proven"]]
    assert!(value.to_string().contains("List"));
    assert!(value.to_string().contains("name"));
    assert!(value.to_string().contains("Proven"));

    ctx.commit();
}

#[test]
fn test_entries_requires_map_value() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Item (data MAP(VARCHAR, VARCHAR))");
    ctx.exec(r#"INSERT INTO Item VALUES ('{"name":"Proven"}')"#);

    let error = ctx.exec_error("SELECT ENTRIES(1) FROM Item");

    // Should error - ENTRIES requires a map value
    assert!(
        error.contains("FunctionRequiresMapValue")
            || error.contains("map")
            || error.contains("Map")
            || error.contains("type"),
        "Expected map-related error, got: {}",
        error
    );

    ctx.commit();
}

#[test]
fn test_entries_with_multiple_keys() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Item (data MAP(VARCHAR, VARCHAR))");
    ctx.exec(r#"INSERT INTO Item VALUES ('{"name":"Proven","version":"1.0","active":"true"}')"#);

    let results = ctx.query("SELECT ENTRIES(data) AS test FROM Item");

    assert_eq!(results.len(), 1);
    let value = results[0].get("test").unwrap();

    // Should contain all key-value pairs
    assert!(value.to_string().contains("name"));
    assert!(value.to_string().contains("Proven"));
    assert!(value.to_string().contains("version"));
    assert!(value.to_string().contains("1.0"));
    assert!(value.to_string().contains("active"));

    ctx.commit();
}

#[test]
fn test_entries_with_empty_map() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Item (data MAP(VARCHAR, VARCHAR))");
    ctx.exec(r#"INSERT INTO Item VALUES ('{}')"#);

    let results = ctx.query("SELECT ENTRIES(data) AS test FROM Item");

    assert_eq!(results.len(), 1);
    let value = results[0].get("test").unwrap();

    // Should return empty list
    assert!(value.to_string().contains("List"));

    ctx.commit();
}

#[test]
fn test_entries_preserves_value_types() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Item (data MAP(VARCHAR, VARCHAR))");
    ctx.exec(r#"INSERT INTO Item VALUES ('{"count":"42","name":"test","enabled":"true"}')"#);

    let results = ctx.query("SELECT ENTRIES(data) AS test FROM Item");

    assert_eq!(results.len(), 1);
    let value = results[0].get("test").unwrap();

    // Should preserve values as strings in MAP(VARCHAR, VARCHAR)
    assert!(value.to_string().contains("42"));
    assert!(value.to_string().contains("test"));
    assert!(value.to_string().contains("true"));

    ctx.commit();
}

#[test]
fn test_entries_nested_values() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Item (data MAP(VARCHAR, VARCHAR))");
    ctx.exec(r#"INSERT INTO Item VALUES ('{"user":"nested","data":"value"}')"#);

    let results = ctx.query("SELECT ENTRIES(data) AS test FROM Item");

    assert_eq!(results.len(), 1);
    let value = results[0].get("test").unwrap();

    // Should contain the entries
    assert!(value.to_string().contains("user"));
    assert!(value.to_string().contains("nested"));

    ctx.commit();
}
