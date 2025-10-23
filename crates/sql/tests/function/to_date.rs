//! TO_DATE, TO_TIME, and TO_TIMESTAMP function tests
//! Based on gluesql/test-suite/src/function/to_date.rs

use crate::common::setup_test;

#[test]
fn test_to_date_basic() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT TO_DATE('2017-06-15', '%Y-%m-%d') AS date");

    assert_eq!(results.len(), 1);
    let date_value = results[0].get("date").unwrap();
    let date_str = date_value.to_string();
    assert!(
        date_str.contains("2017") && date_str.contains("06") && date_str.contains("15"),
        "Expected date 2017-06-15, got: {}",
        date_str
    );

    ctx.commit();
}

#[test]
fn test_to_date_with_month_name() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT TO_DATE('2017-jun-15', '%Y-%b-%d') AS date");

    assert_eq!(results.len(), 1);
    let date_value = results[0].get("date").unwrap();
    let date_str = date_value.to_string();
    assert!(
        date_str.contains("2017") && date_str.contains("06") && date_str.contains("15"),
        "Expected date 2017-06-15, got: {}",
        date_str
    );

    ctx.commit();
}

#[test]
fn test_to_date_in_values() {
    let mut ctx = setup_test();

    let results = ctx.query("VALUES(TO_DATE('2017-06-15', '%Y-%m-%d'))");

    assert_eq!(results.len(), 1);
    // The column name will be "column1" in VALUES clause
    let date_value = results[0].values().next().unwrap();
    let date_str = date_value.to_string();
    assert!(
        date_str.contains("2017") && date_str.contains("06") && date_str.contains("15"),
        "Expected date 2017-06-15, got: {}",
        date_str
    );

    ctx.commit();
}

#[test]
fn test_to_date_requires_string() {
    let mut ctx = setup_test();

    ctx.assert_error_contains(
        "SELECT TO_DATE(DATE '2017-06-15', '%Y-%m-%d') AS date",
        "string",
    );

    ctx.commit();
}

#[test]
fn test_to_date_format_mismatch_too_long() {
    let mut ctx = setup_test();

    ctx.assert_error_contains("SELECT TO_DATE('2015-09-05', '%Y-%m') AS date", "parse");

    ctx.commit();
}

#[test]
fn test_to_date_invalid_date() {
    let mut ctx = setup_test();

    ctx.assert_error_contains("SELECT TO_DATE('2015-14-05', '%Y-%m-%d') AS date", "parse");

    ctx.commit();
}

#[test]
fn test_to_time_basic() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT TO_TIME('23:56:04', '%H:%M:%S') AS time");

    assert_eq!(results.len(), 1);
    let time_value = results[0].get("time").unwrap();
    let time_str = time_value.to_string();
    assert!(
        time_str.contains("23") && time_str.contains("56") && time_str.contains("04"),
        "Expected time 23:56:04, got: {}",
        time_str
    );

    ctx.commit();
}

#[test]
fn test_to_time_format_mismatch_too_short() {
    let mut ctx = setup_test();

    ctx.assert_error_contains("SELECT TO_TIME('23:56', '%H:%M:%S') AS time", "parse");

    ctx.commit();
}

#[test]
fn test_to_timestamp_basic() {
    let mut ctx = setup_test();

    let results =
        ctx.query("SELECT TO_TIMESTAMP('2015-09-05 23:56:04', '%Y-%m-%d %H:%M:%S') AS timestamp");

    assert_eq!(results.len(), 1);
    let ts_value = results[0].get("timestamp").unwrap();
    let ts_str = ts_value.to_string();
    assert!(
        ts_str.contains("2015") && ts_str.contains("09") && ts_str.contains("05"),
        "Expected timestamp with date 2015-09-05, got: {}",
        ts_str
    );

    ctx.commit();
}

#[test]
fn test_to_timestamp_invalid_format() {
    let mut ctx = setup_test();

    ctx.assert_error_contains(
        "SELECT TO_TIMESTAMP('2015-09-05 23:56:04', '%Y-%m-%d %H:%M:%') AS timestamp",
        "parse",
    );

    ctx.commit();
}
