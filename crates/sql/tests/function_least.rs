//! LEAST function tests
//! Companion to GREATEST - returns the minimum value

mod common;

use common::setup_test;

#[test]
fn test_least_integers() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT LEAST(1, 6, 9, 7, 0, 10) AS result");

    assert_eq!(results.len(), 1);
    let value = results[0].get("result").unwrap();
    assert!(value.contains("0"));

    ctx.commit();
}

#[test]
fn test_least_floats() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT LEAST(1.2, 6.8, 9.6, 7.4, 0.1, 10.5) AS result");

    assert_eq!(results.len(), 1);
    let value = results[0].get("result").unwrap();
    assert!(value.contains("0.1"));

    ctx.commit();
}

#[test]
fn test_least_strings() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT LEAST('bibibik', 'babamba', 'melona') AS result");

    assert_eq!(results.len(), 1);
    let value = results[0].get("result").unwrap();
    // 'babamba' is least lexicographically
    assert!(value.contains("babamba"));

    ctx.commit();
}

#[test]
fn test_least_dates() {
    let mut ctx = setup_test();

    let results = ctx.query(
        "SELECT LEAST(
            DATE '2023-07-17',
            DATE '2022-07-17',
            DATE '2023-06-17',
            DATE '2024-07-17',
            DATE '2024-07-18'
        ) AS result",
    );

    assert_eq!(results.len(), 1);
    let value = results[0].get("result").unwrap();
    // 2022-07-17 is the least date
    assert!(value.contains("2022-07-17"));

    ctx.commit();
}

#[test]
fn test_least_no_arguments_should_error() {
    let mut ctx = setup_test();

    let error = ctx.exec_error("SELECT LEAST() AS result");

    // Should error - LEAST requires at least 2 arguments
    assert!(
        error.contains("ExecutionError")
            || error.contains("at least 2")
            || error.contains("minimum"),
        "Expected argument count error, got: {}",
        error
    );

    ctx.commit();
}

#[test]
fn test_least_mixed_types_should_error() {
    let mut ctx = setup_test();

    let error = ctx.exec_error("SELECT LEAST(1, 2, 'bibibik') AS result");

    // Should error - arguments must be of same type
    assert!(
        error.contains("ExecutionError")
            || error.contains("same type")
            || error.contains("comparable")
            || error.contains("NonComparable"),
        "Expected type mismatch error, got: {}",
        error
    );

    ctx.commit();
}

#[test]
fn test_least_with_null_should_error() {
    let mut ctx = setup_test();

    let error = ctx.exec_error("SELECT LEAST(NULL, 'bibibik', 'babamba', 'melona') AS result");

    // Should error - NULL not accepted
    assert!(
        error.contains("ExecutionError")
            || error.contains("NULL")
            || error.contains("NonComparable"),
        "Expected NULL error, got: {}",
        error
    );

    ctx.commit();
}

#[test]
fn test_least_all_nulls_should_error() {
    let mut ctx = setup_test();

    let error = ctx.exec_error("SELECT LEAST(NULL, NULL, NULL) AS result");

    // Should error - NULL not accepted
    assert!(
        error.contains("ExecutionError")
            || error.contains("NULL")
            || error.contains("NonComparable"),
        "Expected NULL error, got: {}",
        error
    );

    ctx.commit();
}

#[test]
fn test_least_booleans() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT LEAST(true, false) AS result");

    assert_eq!(results.len(), 1);
    let value = results[0].get("result").unwrap();
    // false < true
    assert!(value.contains("false") || value.contains("Bool(false)"));

    ctx.commit();
}

#[test]
fn test_least_single_argument_should_error() {
    let mut ctx = setup_test();

    let error = ctx.exec_error("SELECT LEAST(42) AS result");

    // Should error - requires at least 2 arguments
    assert!(
        error.contains("ExecutionError")
            || error.contains("at least 2")
            || error.contains("minimum"),
        "Expected argument count error, got: {}",
        error
    );

    ctx.commit();
}

#[test]
fn test_least_two_arguments() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT LEAST(100, 50) AS result");

    assert_eq!(results.len(), 1);
    let value = results[0].get("result").unwrap();
    assert!(value.contains("50"));

    ctx.commit();
}

#[test]
fn test_least_with_table_data() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Numbers (a INT, b INT, c INT)");
    ctx.exec("INSERT INTO Numbers VALUES (1, 5, 3), (10, 2, 8), (4, 9, 6)");

    let results = ctx.query("SELECT LEAST(a, b, c) AS min_val FROM Numbers");

    assert_eq!(results.len(), 3);
    // Row 1: min(1, 5, 3) = 1
    assert!(results[0].get("min_val").unwrap().contains("1"));
    // Row 2: min(10, 2, 8) = 2
    assert!(results[1].get("min_val").unwrap().contains("2"));
    // Row 3: min(4, 9, 6) = 4
    assert!(results[2].get("min_val").unwrap().contains("4"));

    ctx.commit();
}

#[test]
fn test_least_and_greatest_together() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE TestValues (x INT, y INT, z INT)");
    ctx.exec("INSERT INTO TestValues VALUES (5, 10, 3)");

    let results =
        ctx.query("SELECT LEAST(x, y, z) AS min, GREATEST(x, y, z) AS max FROM TestValues");

    assert_eq!(results.len(), 1);
    assert!(results[0].get("min").unwrap().contains("3"));
    assert!(results[0].get("max").unwrap().contains("10"));

    ctx.commit();
}
