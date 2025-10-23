//! PREPEND function tests
//! Based on gluesql/test-suite/src/function/prepend.rs

use crate::common::{TableBuilder, TestContext, setup_test};
use proven_value::Value;

/// Setup test table with list and element data
fn setup_prepend_table(ctx: &mut TestContext) {
    TableBuilder::new(ctx, "Prepend")
        .create_simple("id INTEGER, items INT[], element INTEGER, element2 TEXT");

    ctx.exec("INSERT INTO Prepend VALUES (1, '[1, 2, 3]', 0, 'Foo')");
}

#[test]
fn test_create_table_with_list_and_elements() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Prepend (id INTEGER, items INT[], element INTEGER, element2 TEXT)");

    ctx.commit();
}

#[test]
fn test_insert_list_data() {
    let mut ctx = setup_test();
    setup_prepend_table(&mut ctx);

    ctx.assert_row_count("SELECT * FROM Prepend", 1);

    ctx.commit();
}

#[test]
fn test_prepend_integer_to_list() {
    let mut ctx = setup_test();
    setup_prepend_table(&mut ctx);

    let results = ctx.query("SELECT PREPEND(element, items) as myprepend FROM Prepend");
    assert_eq!(results.len(), 1);

    let result_value = results[0].get("myprepend").unwrap();
    if let Value::List(list) = result_value {
        assert_eq!(list.len(), 4, "Should have 4 elements");
        // Check the prepended value and original values
        match &list[0] {
            Value::I32(v) => assert_eq!(*v, 0, "First element should be 0"),
            other => panic!("Expected integer, got: {:?}", other),
        }
        assert_eq!(list[1], Value::I32(1), "Second element should be 1");
        assert_eq!(list[2], Value::I32(2), "Third element should be 2");
        assert_eq!(list[3], Value::I32(3), "Fourth element should be 3");
    } else {
        panic!("Expected List value, got: {:?}", result_value);
    }

    ctx.commit();
}

#[test]
fn test_prepend_text_to_list() {
    let mut ctx = setup_test();
    setup_prepend_table(&mut ctx);

    // Prepending TEXT to INTEGER[] should error - lists can't have mixed types
    ctx.assert_error_contains(
        "SELECT PREPEND(element2, items) as myprepend FROM Prepend",
        "Cannot cast",
    );

    ctx.abort();
}

#[test]
fn test_prepend_non_list_should_error() {
    let mut ctx = setup_test();
    setup_prepend_table(&mut ctx);

    // The error message differs from GlueSQL
    ctx.assert_error_contains(
        "SELECT PREPEND(element, element2) as myprepend FROM Prepend",
        "expected: \"list\"",
    );

    ctx.commit();
}

#[test]
fn test_create_table_for_prepend_operation() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Foo (elements INT[])");

    ctx.commit();
}

#[test]
fn test_insert_with_prepend_function() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Foo (elements INT[])");
    ctx.exec("INSERT INTO Foo VALUES (PREPEND(0, CAST('[1, 2, 3]' AS INT[])))");

    ctx.assert_row_count("SELECT * FROM Foo", 1);

    ctx.commit();
}

#[test]
fn test_select_prepended_elements() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Foo (elements INT[])");
    ctx.exec("INSERT INTO Foo VALUES (PREPEND(0, CAST('[1, 2, 3]' AS INT[])))");

    let results = ctx.query("SELECT elements as myprepend FROM Foo");
    assert_eq!(results.len(), 1);

    let result_value = results[0].get("myprepend").unwrap();
    if let Value::List(list) = result_value {
        assert_eq!(list.len(), 4, "Should have 4 elements");
        assert_eq!(list[0], Value::I32(0), "First element should be 0");
        assert_eq!(list[1], Value::I32(1), "Second element should be 1");
        assert_eq!(list[2], Value::I32(2), "Third element should be 2");
        assert_eq!(list[3], Value::I32(3), "Fourth element should be 3");
    } else {
        panic!("Expected List value, got: {:?}", result_value);
    }

    ctx.commit();
}

#[test]
fn test_prepend_mixed_types() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE MixedTest (id INTEGER, items INT[], str_val TEXT)");
    ctx.exec("INSERT INTO MixedTest VALUES (1, '[1, 2, 3]', 'Hello')");

    // Prepending a string to an integer list should error - lists can't have mixed types
    ctx.assert_error_contains(
        "SELECT PREPEND(str_val, items) as result FROM MixedTest",
        "Cannot cast",
    );

    ctx.abort();
}

#[test]
fn test_prepend_with_cast() {
    let mut ctx = setup_test();

    // Test PREPEND with CAST directly in query
    let results = ctx.query("SELECT PREPEND(0, CAST('[1, 2, 3]' AS INT[])) as result");
    assert_eq!(results.len(), 1);

    let result_value = results[0].get("result").unwrap();
    if let Value::List(list) = result_value {
        assert_eq!(list.len(), 4, "Should have 4 elements");
        match &list[0] {
            Value::I32(v) => assert_eq!(*v, 0, "First element should be 0"),
            Value::I64(v) => assert_eq!(*v, 0, "First element should be 0"),
            other => panic!("Expected integer, got: {:?}", other),
        }
    } else {
        panic!("Expected List value, got: {:?}", result_value);
    }

    ctx.commit();
}
