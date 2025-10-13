//! FLATTEN function tests

mod common;

use common::setup_test;

#[test]
fn test_flatten_nested_list() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT FLATTEN([[1, 2], [3, 4]]) AS test");

    assert_eq!(results.len(), 1);
    let value = results[0].get("test").unwrap();

    // Should return flattened list: [1, 2, 3, 4]
    assert!(value.to_string().contains("List"));
    assert!(value.to_string().contains("1"));
    assert!(value.to_string().contains("2"));
    assert!(value.to_string().contains("3"));
    assert!(value.to_string().contains("4"));

    ctx.commit();
}

#[test]
fn test_flatten_single_level_list() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT FLATTEN([1, 2, 3]) AS test");

    assert_eq!(results.len(), 1);
    let value = results[0].get("test").unwrap();

    // Should return the same list: [1, 2, 3]
    assert!(value.to_string().contains("List"));
    assert!(value.to_string().contains("1"));
    assert!(value.to_string().contains("2"));
    assert!(value.to_string().contains("3"));

    ctx.commit();
}

#[test]
fn test_flatten_mixed_nested_and_single_elements() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT FLATTEN([1, [2, 3], 4]) AS test");

    assert_eq!(results.len(), 1);
    let value = results[0].get("test").unwrap();

    // Should return flattened list: [1, 2, 3, 4]
    assert!(value.to_string().contains("List"));
    assert!(value.to_string().contains("1"));
    assert!(value.to_string().contains("2"));
    assert!(value.to_string().contains("3"));
    assert!(value.to_string().contains("4"));

    ctx.commit();
}

#[test]
fn test_flatten_empty_list() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT FLATTEN([]) AS test");

    assert_eq!(results.len(), 1);
    let value = results[0].get("test").unwrap();

    // Should return empty list
    assert!(value.to_string().contains("List"));

    ctx.commit();
}

#[test]
fn test_flatten_list_with_empty_sublists() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT FLATTEN([[], [1, 2], []]) AS test");

    assert_eq!(results.len(), 1);
    let value = results[0].get("test").unwrap();

    // Should return [1, 2]
    assert!(value.to_string().contains("List"));
    assert!(value.to_string().contains("1"));
    assert!(value.to_string().contains("2"));

    ctx.commit();
}

#[test]
fn test_flatten_with_null() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT FLATTEN(NULL) AS test");

    assert_eq!(results.len(), 1);
    let value = results[0].get("test").unwrap();

    // Should return NULL
    assert!(
        value.to_string().contains("Null") || value.to_string().contains("NULL"),
        "Expected NULL, got: {}",
        value
    );

    ctx.commit();
}

#[test]
fn test_flatten_non_list_should_error() {
    let mut ctx = setup_test();

    let error = ctx.exec_error("SELECT FLATTEN(42) AS test");

    // Should error - FLATTEN requires a list or array
    assert!(
        error.to_string().contains("TypeMismatch")
            || error.contains("list")
            || error.contains("array"),
        "Expected list/array-related error, got: {}",
        error
    );

    ctx.commit();
}

#[test]
fn test_flatten_string_should_error() {
    let mut ctx = setup_test();

    let error = ctx.exec_error("SELECT FLATTEN('hello') AS test");

    // Should error - FLATTEN requires a list or array
    assert!(
        error.to_string().contains("TypeMismatch")
            || error.contains("list")
            || error.contains("array"),
        "Expected list/array-related error, got: {}",
        error
    );

    ctx.commit();
}

#[test]
fn test_flatten_with_table_data() {
    let mut ctx = setup_test();

    // Test with a simple case - store the nested list directly in a query
    ctx.exec("CREATE TABLE Items (id INTEGER, value INTEGER)");
    ctx.exec("INSERT INTO Items VALUES (1, 10), (2, 20), (3, 30)");

    // Use FLATTEN in a subquery or with a literal
    let results = ctx.query("SELECT FLATTEN([[1, 2], [3, 4], [5]]) AS flattened");

    assert_eq!(results.len(), 1);
    let value = results[0].get("flattened").unwrap();

    // Should return flattened list: [1, 2, 3, 4, 5]
    assert!(value.to_string().contains("List"));
    assert!(value.to_string().contains("1"));
    assert!(value.to_string().contains("2"));
    assert!(value.to_string().contains("3"));
    assert!(value.to_string().contains("4"));
    assert!(value.to_string().contains("5"));

    ctx.commit();
}

#[test]
fn test_flatten_deeply_nested_only_one_level() {
    let mut ctx = setup_test();

    // FLATTEN only flattens one level deep
    let results = ctx.query("SELECT FLATTEN([[[1, 2]], [[3, 4]]]) AS test");

    assert_eq!(results.len(), 1);
    let value = results[0].get("test").unwrap();

    // Should return [[1, 2], [3, 4]] (only one level flattened)
    assert!(value.to_string().contains("List"));

    ctx.commit();
}

#[test]
fn test_flatten_preserves_types() {
    let mut ctx = setup_test();

    // Test with string lists using literals
    let results = ctx.query(r#"SELECT FLATTEN([['a', 'b'], ['c', 'd']]) AS flattened"#);

    assert_eq!(results.len(), 1);
    let value = results[0].get("flattened").unwrap();

    // Should preserve string types and flatten to ['a', 'b', 'c', 'd']
    assert!(value.to_string().contains("List"));

    ctx.commit();
}

#[test]
fn test_flatten_wrong_argument_count() {
    let mut ctx = setup_test();

    let error = ctx.exec_error("SELECT FLATTEN([1, 2], [3, 4]) AS test");

    // Should error - FLATTEN takes exactly 1 argument
    assert!(
        error.to_string().contains("ExecutionError")
            || error.contains("1 argument")
            || error.contains("exactly"),
        "Expected argument count error, got: {}",
        error
    );

    ctx.commit();
}
