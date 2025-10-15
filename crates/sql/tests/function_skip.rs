//! SKIP function tests
//! Based on gluesql/test-suite/src/function/skip.rs

mod common;

use common::{TableBuilder, setup_test};
use proven_value::Value;

/// Setup test table with list data
fn setup_skip_table(ctx: &mut common::TestContext) {
    TableBuilder::new(ctx, "Test").create_simple("id INTEGER, items LIST");

    ctx.exec("INSERT INTO Test (id, items) VALUES (1, '[1,2,3,4,5]')");
}

#[test]
fn test_create_table_with_list() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Test (id INTEGER, list LIST)");

    ctx.commit();
}

#[test]
fn test_insert_list_data() {
    let mut ctx = setup_test();
    setup_skip_table(&mut ctx);

    ctx.assert_row_count("SELECT * FROM Test", 1);

    ctx.commit();
}

#[test]
fn test_skip_elements_normal_usage() {
    let mut ctx = setup_test();
    setup_skip_table(&mut ctx);

    let results = ctx.query("SELECT SKIP(items, 2) as col1 FROM Test");
    assert_eq!(results.len(), 1);

    let result_value = results[0].get("col1").unwrap();
    if let Value::List(list) = result_value {
        assert_eq!(list.len(), 3, "Should have 3 elements after skipping 2");
        assert_eq!(list[0], Value::I64(3), "First element should be 3");
        assert_eq!(list[1], Value::I64(4), "Second element should be 4");
        assert_eq!(list[2], Value::I64(5), "Third element should be 5");
    } else {
        panic!("Expected List value, got: {:?}", result_value);
    }

    ctx.commit();
}

#[test]
fn test_skip_beyond_list_length() {
    let mut ctx = setup_test();
    setup_skip_table(&mut ctx);

    let results = ctx.query("SELECT SKIP(items, 6) as col1 FROM Test");
    assert_eq!(results.len(), 1);

    let result_value = results[0].get("col1").unwrap();
    if let Value::List(list) = result_value {
        assert_eq!(
            list.len(),
            0,
            "Should have 0 elements when skip count exceeds list length"
        );
    } else {
        panic!("Expected List value, got: {:?}", result_value);
    }

    ctx.commit();
}

#[test]
fn test_skip_with_null_list() {
    let mut ctx = setup_test();
    setup_skip_table(&mut ctx);

    let results = ctx.query("SELECT SKIP(NULL, 2) as col1 FROM Test");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("col1").unwrap(), &Value::Null);

    ctx.commit();
}

#[test]
fn test_skip_with_null_count_should_return_null() {
    let mut ctx = setup_test();
    setup_skip_table(&mut ctx);

    // GlueSQL behavior: SKIP with NULL count should return NULL
    let results = ctx.query("SELECT SKIP(items, NULL) as col1 FROM Test");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("col1").unwrap(), &Value::Null);

    ctx.commit();
}

#[test]
fn test_skip_non_integer_parameter_should_error() {
    let mut ctx = setup_test();
    setup_skip_table(&mut ctx);

    // The error message differs from GlueSQL
    ctx.assert_error_contains("SELECT SKIP(items, 'd') as col1 FROM Test", "integer");

    ctx.commit();
}

#[test]
fn test_skip_non_list_should_error() {
    let mut ctx = setup_test();
    setup_skip_table(&mut ctx);

    // The error message differs from GlueSQL
    ctx.assert_error_contains("SELECT SKIP(id, 2) as col1 FROM Test", "list or array");

    ctx.commit();
}

#[test]
fn test_skip_negative_size_should_error_gluesql() {
    let mut ctx = setup_test();
    setup_skip_table(&mut ctx);

    // SQL standard behavior: negative size should error
    ctx.assert_error_contains(
        "SELECT SKIP(items, -2) as col1 FROM Test",
        "SKIP count must be non-negative",
    );

    ctx.commit();
}

#[test]
fn test_skip_function_signature() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Test").create_simple("id INTEGER");

    // Test that SKIP requires exactly 2 arguments
    ctx.assert_error_contains("SELECT SKIP(id) FROM Test", "takes exactly 2 arguments");

    ctx.commit();
}

#[test]
fn test_skip_edge_cases() {
    let mut ctx = setup_test();
    setup_skip_table(&mut ctx);

    // Test SKIP with count=0 (should return original list)
    let results = ctx.query("SELECT SKIP(items, 0) as col1 FROM Test");
    assert_eq!(results.len(), 1);

    let result_value = results[0].get("col1").unwrap();
    if let Value::List(list) = result_value {
        assert_eq!(list.len(), 5, "Should have all 5 elements when skip=0");
        assert_eq!(list[0], Value::I64(1), "First element should be 1");
        assert_eq!(list[1], Value::I64(2), "Second element should be 2");
        assert_eq!(list[2], Value::I64(3), "Third element should be 3");
        assert_eq!(list[3], Value::I64(4), "Fourth element should be 4");
        assert_eq!(list[4], Value::I64(5), "Fifth element should be 5");
    } else {
        panic!("Expected List value, got: {:?}", result_value);
    }

    ctx.commit();
}
