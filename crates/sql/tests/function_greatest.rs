//! GREATEST function tests
//! Based on gluesql/test-suite/src/function/greatest.rs

mod common;

use common::setup_test;

#[test]
fn test_greatest_integers() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT GREATEST(1, 6, 9, 7, 0, 10) AS goat");

    assert_eq!(results.len(), 1);
    let value = results[0].get("goat").unwrap();
    assert!(value.to_string().contains("10"));

    ctx.commit();
}

#[test]
fn test_greatest_floats() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT GREATEST(1.2, 6.8, 9.6, 7.4, 0.1, 10.5) AS goat");

    assert_eq!(results.len(), 1);
    let value = results[0].get("goat").unwrap();
    assert!(value.to_string().contains("10.5"));

    ctx.commit();
}

#[test]
fn test_greatest_strings() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT GREATEST('bibibik', 'babamba', 'melona') AS goat");

    assert_eq!(results.len(), 1);
    let value = results[0].get("goat").unwrap();
    // 'melona' is greatest lexicographically
    assert!(value.to_string().contains("melona"));

    ctx.commit();
}

#[test]
fn test_greatest_dates() {
    let mut ctx = setup_test();

    let results = ctx.query(
        "SELECT GREATEST(
            DATE '2023-07-17',
            DATE '2022-07-17',
            DATE '2023-06-17',
            DATE '2024-07-17',
            DATE '2024-07-18'
        ) AS goat",
    );

    assert_eq!(results.len(), 1);
    let value = results[0].get("goat").unwrap();
    // 2024-07-18 is the greatest date
    assert!(value.to_string().contains("2024-07-18"));

    ctx.commit();
}

#[test]
fn test_greatest_no_arguments_should_error() {
    let mut ctx = setup_test();

    let error = ctx.exec_error("SELECT GREATEST() AS goat");

    // Should error - GREATEST requires at least 2 arguments
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
fn test_greatest_mixed_types_should_error() {
    let mut ctx = setup_test();

    let error = ctx.exec_error("SELECT GREATEST(1, 2, 'bibibik') AS goat");

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
fn test_greatest_with_null_should_error() {
    let mut ctx = setup_test();

    let error = ctx.exec_error("SELECT GREATEST(NULL, 'bibibik', 'babamba', 'melona') AS goat");

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
fn test_greatest_all_nulls_should_error() {
    let mut ctx = setup_test();

    let error = ctx.exec_error("SELECT GREATEST(NULL, NULL, NULL) AS goat");

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
fn test_greatest_booleans() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT GREATEST(true, false) AS goat");

    assert_eq!(results.len(), 1);
    let value = results[0].get("goat").unwrap();
    // true > false
    assert!(value.to_string().contains("true") || value.to_string().contains("Bool(true)"));

    ctx.commit();
}

#[test]
fn test_greatest_single_argument_should_error() {
    let mut ctx = setup_test();

    let error = ctx.exec_error("SELECT GREATEST(42) AS goat");

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
fn test_greatest_two_arguments() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT GREATEST(100, 50) AS goat");

    assert_eq!(results.len(), 1);
    let value = results[0].get("goat").unwrap();
    assert!(value.to_string().contains("100"));

    ctx.commit();
}

#[test]
fn test_greatest_with_table_data() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Numbers (a INT, b INT, c INT)");
    ctx.exec("INSERT INTO Numbers VALUES (1, 5, 3), (10, 2, 8), (4, 9, 6)");

    let results = ctx.query("SELECT GREATEST(a, b, c) AS max_val FROM Numbers");

    assert_eq!(results.len(), 3);
    // Row 1: max(1, 5, 3) = 5
    assert!(results[0].get("max_val").unwrap().to_string().contains("5"));
    // Row 2: max(10, 2, 8) = 10
    assert!(
        results[1]
            .get("max_val")
            .unwrap()
            .to_string()
            .contains("10")
    );
    // Row 3: max(4, 9, 6) = 9
    assert!(results[2].get("max_val").unwrap().to_string().contains("9"));

    ctx.commit();
}
