//! FROM_ENTRIES function tests

mod common;

use common::setup_test;

#[test]
fn test_from_entries_simple_pairs() {
    let mut ctx = setup_test();

    let results = ctx.query(r#"SELECT FROM_ENTRIES([['name', 'Alice'], ['age', '25']]) AS test"#);

    assert_eq!(results.len(), 1);
    let value = results[0].get("test").unwrap();

    // Should return a map with name and age keys
    assert!(value.contains("Map"));
    assert!(value.contains("name"));
    assert!(value.contains("Alice"));
    assert!(value.contains("age"));
    assert!(value.contains("25"));

    ctx.commit();
}

#[test]
fn test_from_entries_single_pair() {
    let mut ctx = setup_test();

    let results = ctx.query(r#"SELECT FROM_ENTRIES([['key', 'value']]) AS test"#);

    assert_eq!(results.len(), 1);
    let value = results[0].get("test").unwrap();

    // Should return a map with single entry
    assert!(value.contains("Map"));
    assert!(value.contains("key"));
    assert!(value.contains("value"));

    ctx.commit();
}

#[test]
fn test_from_entries_empty_list() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT FROM_ENTRIES([]) AS test");

    assert_eq!(results.len(), 1);
    let value = results[0].get("test").unwrap();

    // Should return empty map
    assert!(value.contains("Map"));

    ctx.commit();
}

#[test]
fn test_from_entries_with_null() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT FROM_ENTRIES(NULL) AS test");

    assert_eq!(results.len(), 1);
    let value = results[0].get("test").unwrap();

    // Should return NULL
    assert!(
        value.contains("Null") || value.contains("NULL"),
        "Expected NULL, got: {}",
        value
    );

    ctx.commit();
}

#[test]
fn test_from_entries_overwrites_duplicate_keys() {
    let mut ctx = setup_test();

    let results =
        ctx.query(r#"SELECT FROM_ENTRIES([['key', 'first'], ['key', 'second']]) AS test"#);

    assert_eq!(results.len(), 1);
    let value = results[0].get("test").unwrap();

    // Second value should overwrite the first
    assert!(value.contains("Map"));
    assert!(value.contains("key"));
    // Should contain 'second' (the last value for 'key')
    assert!(value.contains("second"));

    ctx.commit();
}

#[test]
fn test_from_entries_multiple_entries() {
    let mut ctx = setup_test();

    let results = ctx.query(
        r#"SELECT FROM_ENTRIES([
            ['name', 'Bob'],
            ['city', 'NYC'],
            ['country', 'USA']
        ]) AS test"#,
    );

    assert_eq!(results.len(), 1);
    let value = results[0].get("test").unwrap();

    // Should contain all keys and values
    assert!(value.contains("Map"));
    assert!(value.contains("name"));
    assert!(value.contains("Bob"));
    assert!(value.contains("city"));
    assert!(value.contains("NYC"));
    assert!(value.contains("country"));
    assert!(value.contains("USA"));

    ctx.commit();
}

#[test]
fn test_from_entries_non_list_should_error() {
    let mut ctx = setup_test();

    let error = ctx.exec_error("SELECT FROM_ENTRIES('invalid') AS test");

    // Should error - FROM_ENTRIES requires a list
    assert!(
        error.contains("TypeMismatch") || error.contains("list") || error.contains("array"),
        "Expected list/array-related error, got: {}",
        error
    );

    ctx.commit();
}

#[test]
fn test_from_entries_non_pair_should_error() {
    let mut ctx = setup_test();

    let error = ctx.exec_error("SELECT FROM_ENTRIES([['single']]) AS test");

    // Should error - each entry must be a [key, value] pair
    assert!(
        error.contains("ExecutionError")
            || error.contains("pair")
            || error.contains("[key, value]"),
        "Expected pair-related error, got: {}",
        error
    );

    ctx.commit();
}

#[test]
fn test_from_entries_non_string_key_should_error() {
    let mut ctx = setup_test();

    let error = ctx.exec_error("SELECT FROM_ENTRIES([[1, 'value']]) AS test");

    // Should error - keys must be strings
    assert!(
        error.contains("TypeMismatch") || error.contains("string") || error.contains("key"),
        "Expected string key error, got: {}",
        error
    );

    ctx.commit();
}

#[test]
fn test_from_entries_too_many_elements_should_error() {
    let mut ctx = setup_test();

    let error = ctx.exec_error("SELECT FROM_ENTRIES([['a', 'b', 'c']]) AS test");

    // Should error - each entry must have exactly 2 elements
    assert!(
        error.contains("ExecutionError")
            || error.contains("pair")
            || error.contains("[key, value]"),
        "Expected pair size error, got: {}",
        error
    );

    ctx.commit();
}

#[test]
fn test_from_entries_wrong_argument_count() {
    let mut ctx = setup_test();

    let error = ctx.exec_error(r#"SELECT FROM_ENTRIES([['a', 'b']], [['c', 'd']]) AS test"#);

    // Should error - FROM_ENTRIES takes exactly 1 argument
    assert!(
        error.contains("ExecutionError")
            || error.contains("1 argument")
            || error.contains("exactly"),
        "Expected argument count error, got: {}",
        error
    );

    ctx.commit();
}

#[test]
fn test_from_entries_inverse_of_entries() {
    let mut ctx = setup_test();

    // Create a map, convert to entries, then back to map
    ctx.exec("CREATE TABLE Item (data MAP(VARCHAR, VARCHAR))");
    ctx.exec(r#"INSERT INTO Item VALUES ('{"name":"Test","value":"123"}')"#);

    // First get the entries
    let entries_results = ctx.query("SELECT ENTRIES(data) AS entries FROM Item");
    assert_eq!(entries_results.len(), 1);

    // FROM_ENTRIES should be able to reconstruct a similar map
    // (though order may differ and it's hard to test exact equality)
    let results = ctx.query("SELECT FROM_ENTRIES(ENTRIES(data)) AS reconstructed FROM Item");
    assert_eq!(results.len(), 1);
    let value = results[0].get("reconstructed").unwrap();

    // Should be a map containing the original keys
    assert!(value.contains("Map"));
    assert!(value.contains("name"));
    assert!(value.contains("Test"));
    assert!(value.contains("value"));
    assert!(value.contains("123"));

    ctx.commit();
}

#[test]
fn test_from_entries_with_null_values() {
    let mut ctx = setup_test();

    let results = ctx.query(r#"SELECT FROM_ENTRIES([['key1', 'value1'], ['key2', NULL]]) AS test"#);

    assert_eq!(results.len(), 1);
    let value = results[0].get("test").unwrap();

    // Should accept NULL as a value
    assert!(value.contains("Map"));
    assert!(value.contains("key1"));
    assert!(value.contains("value1"));
    assert!(value.contains("key2"));

    ctx.commit();
}

#[test]
fn test_from_entries_with_integer_values() {
    let mut ctx = setup_test();

    // Maps should have homogeneous value types
    let results = ctx.query("SELECT FROM_ENTRIES([['count', 42], ['total', 100]]) AS test");

    assert_eq!(results.len(), 1);
    let value = results[0].get("test").unwrap();

    // Should create a map with integer values
    assert!(value.contains("Map"));
    assert!(value.contains("count"));
    assert!(value.contains("42"));
    assert!(value.contains("total"));
    assert!(value.contains("100"));

    ctx.commit();
}

#[test]
fn test_from_entries_with_boolean_values() {
    let mut ctx = setup_test();

    // Maps should have homogeneous value types
    let results = ctx.query("SELECT FROM_ENTRIES([['active', true], ['visible', false]]) AS test");

    assert_eq!(results.len(), 1);
    let value = results[0].get("test").unwrap();

    // Should create a map with boolean values
    assert!(value.contains("Map"));
    assert!(value.contains("active"));
    assert!(value.contains("visible"));

    ctx.commit();
}
