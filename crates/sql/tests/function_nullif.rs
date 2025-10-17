//! NULLIF function tests
//! Based on gluesql/test-suite/src/function/nullif.rs

mod common;

use common::setup_test;
use proven_value::Value;

#[test]
fn test_nullif_equal_integers() {
    let mut ctx = setup_test();

    ctx.assert_query_value("SELECT NULLIF(0, 0) AS result", "result", Value::Null);

    ctx.commit();
}

#[test]
fn test_nullif_different_integers() {
    let mut ctx = setup_test();

    ctx.assert_query_value("SELECT NULLIF(1, 0) AS result", "result", Value::I32(1));

    ctx.commit();
}

#[test]
fn test_nullif_equal_strings() {
    let mut ctx = setup_test();

    ctx.assert_query_value(
        "SELECT NULLIF('hello', 'hello') AS result",
        "result",
        Value::Null,
    );

    ctx.commit();
}

#[test]
fn test_nullif_different_strings() {
    let mut ctx = setup_test();

    ctx.assert_query_value(
        "SELECT NULLIF('hello', 'helle') AS result",
        "result",
        Value::string("hello"),
    );

    ctx.commit();
}

#[test]
fn test_nullif_equal_dates() {
    let mut ctx = setup_test();

    ctx.assert_query_value(
        "SELECT NULLIF(TO_DATE('2025-01-01', '%Y-%m-%d'), TO_DATE('2025-01-01', '%Y-%m-%d')) AS result",
        "result",
        Value::Null,
    );

    ctx.commit();
}

#[test]
fn test_nullif_different_dates() {
    let mut ctx = setup_test();

    let results = ctx.query(
        "SELECT NULLIF(TO_DATE('2025-01-01', '%Y-%m-%d'), TO_DATE('2025-01-02', '%Y-%m-%d')) AS result"
    );

    assert_eq!(results.len(), 1);
    let result_value = results[0].get("result").unwrap();

    // Verify it's a date and contains 2025-01-01
    let date_str = result_value.to_string();
    assert!(
        date_str.contains("2025") && date_str.contains("01-01"),
        "Expected date 2025-01-01, got: {}",
        date_str
    );

    ctx.commit();
}

#[test]
fn test_nullif_no_arguments() {
    let mut ctx = setup_test();

    ctx.assert_error_contains("SELECT NULLIF() AS result", "2 arguments");

    ctx.commit();
}

#[test]
fn test_nullif_one_argument() {
    let mut ctx = setup_test();

    ctx.assert_error_contains("SELECT NULLIF(1) AS result", "2 arguments");

    ctx.commit();
}
