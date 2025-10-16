//! Bitwise AND operation tests
//! Based on gluesql/test-suite/src/bitwise_and.rs

mod common;

use common::setup_test;
use proven_value::Value;

#[test]
fn test_bitwise_and_literals() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT 29 & 15 AS and_result");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("and_result").unwrap(), &Value::I32(13));

    ctx.commit();
}

#[test]
fn test_bitwise_and_with_table() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Test (id INTEGER, lhs INTEGER, rhs INTEGER)");
    ctx.exec("INSERT INTO Test VALUES (1, 29, 15)");

    let results = ctx.query("SELECT lhs & rhs AS and_result FROM Test");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("and_result").unwrap(), &Value::I32(13));

    ctx.commit();
}

#[test]
fn test_bitwise_and_value_and_literal() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Test (id INTEGER, rhs INTEGER)");
    ctx.exec("INSERT INTO Test VALUES (1, 15)");

    let results = ctx.query("SELECT 29 & rhs AS and_result FROM Test");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("and_result").unwrap(), &Value::I32(13));

    ctx.commit();
}

#[test]
fn test_bitwise_and_multiple_values() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Test (id INTEGER, rhs INTEGER)");
    ctx.exec("INSERT INTO Test VALUES (1, 15)");

    let results = ctx.query("SELECT 29 & rhs & 3 AS and_result FROM Test");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("and_result").unwrap(), &Value::I32(1));

    ctx.commit();
}

#[test]
fn test_bitwise_and_with_null_left() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Test (id INTEGER, rhs INTEGER)");
    ctx.exec("INSERT INTO Test VALUES (1, 15)");

    let results = ctx.query("SELECT NULL & rhs AS and_result FROM Test");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("and_result").unwrap(), &Value::Null);

    ctx.commit();
}

#[test]
fn test_bitwise_and_with_null_right() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Test (id INTEGER, rhs INTEGER)");
    ctx.exec("INSERT INTO Test VALUES (1, 15)");

    let results = ctx.query("SELECT rhs & NULL AS and_result FROM Test");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("and_result").unwrap(), &Value::Null);

    ctx.commit();
}

#[test]
fn test_bitwise_and_null_and_literal() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT NULL & 12 AS and_result");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("and_result").unwrap(), &Value::Null);

    ctx.commit();
}

#[test]
fn test_bitwise_and_literal_and_null() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT 12 & NULL AS and_result");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("and_result").unwrap(), &Value::Null);

    ctx.commit();
}

#[test]
fn test_bitwise_and_with_float_should_error() {
    let mut ctx = setup_test();

    ctx.assert_error_contains("SELECT 1.1 & 12 AS and_result", "InvalidOperation");

    ctx.commit();
}

#[test]
fn test_bitwise_and_with_string_should_error() {
    let mut ctx = setup_test();

    ctx.assert_error_contains("SELECT 'ss' & 'sp' AS and_result", "InvalidOperation");

    ctx.commit();
}

#[test]
fn test_bitwise_and_different_integer_sizes() {
    let mut ctx = setup_test();

    // Test mixing different integer sizes - should promote to larger type
    let results = ctx.query("SELECT CAST(5 AS BIGINT) & CAST(3 AS INTEGER) AS result");
    assert_eq!(results.len(), 1);
    // Result should be I64(1) due to type promotion
    assert_eq!(results[0].get("result").unwrap(), &Value::I64(1));

    ctx.commit();
}
