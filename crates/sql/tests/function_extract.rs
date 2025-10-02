//! EXTRACT function tests
//! Based on gluesql/test-suite/src/function/extract.rs
//!
//! The EXTRACT function implements the SQL standard datetime field extraction:
//!
//! **Syntax**: `EXTRACT(field FROM source)`
//! - Example: `EXTRACT(YEAR FROM TIMESTAMP '2016-12-31 13:30:15')` returns `2016`
//! - Extracts date/time components (YEAR, MONTH, DAY, HOUR, MINUTE, SECOND)
//! - Works with TIMESTAMP, DATE, TIME, and INTERVAL types
//! - Returns I64 (integer) values per SQL standard
//!
//! ## Implementation Details
//!
//! - Parser support added in [expr_parser.rs](../../src/parsing/parser/expr_parser.rs)
//! - Function implementation in [extract.rs](../../src/functions/extract.rs)
//! - Keywords added to lexer: EXTRACT, YEAR, MONTH, DAY, HOUR, MINUTE, SECOND
//! - Uses `FROM` keyword (similar to CAST's `AS` keyword)

mod common;

use common::setup_test;

#[test]
fn test_extract_hour_from_timestamp() {
    let mut ctx = setup_test();

    let results =
        ctx.query(r#"SELECT EXTRACT(HOUR FROM TIMESTAMP '2016-12-31 13:30:15') as extract"#);

    assert_eq!(results.len(), 1);
    let value = results[0].get("extract").unwrap();
    assert!(value.contains("13"));

    ctx.commit();
}

#[test]
fn test_extract_year_from_timestamp() {
    let mut ctx = setup_test();

    let results =
        ctx.query(r#"SELECT EXTRACT(YEAR FROM TIMESTAMP '2016-12-31 13:30:15') as extract"#);

    assert_eq!(results.len(), 1);
    let value = results[0].get("extract").unwrap();
    assert!(value.contains("2016"));

    ctx.commit();
}

#[test]
fn test_extract_month_from_timestamp() {
    let mut ctx = setup_test();

    let results =
        ctx.query(r#"SELECT EXTRACT(MONTH FROM TIMESTAMP '2016-12-31 13:30:15') as extract"#);

    assert_eq!(results.len(), 1);
    let value = results[0].get("extract").unwrap();
    assert!(value.contains("12"));

    ctx.commit();
}

#[test]
fn test_extract_day_from_timestamp() {
    let mut ctx = setup_test();

    let results =
        ctx.query(r#"SELECT EXTRACT(DAY FROM TIMESTAMP '2016-12-31 13:30:15') as extract"#);

    assert_eq!(results.len(), 1);
    let value = results[0].get("extract").unwrap();
    assert!(value.contains("31"));

    ctx.commit();
}

#[test]
fn test_extract_minute_from_timestamp() {
    let mut ctx = setup_test();

    let results =
        ctx.query(r#"SELECT EXTRACT(MINUTE FROM TIMESTAMP '2016-12-31 13:30:15') as extract"#);

    assert_eq!(results.len(), 1);
    let value = results[0].get("extract").unwrap();
    assert!(value.contains("30"));

    ctx.commit();
}

#[test]
fn test_extract_second_from_timestamp() {
    let mut ctx = setup_test();

    let results =
        ctx.query(r#"SELECT EXTRACT(SECOND FROM TIMESTAMP '2016-12-31 13:30:15') as extract"#);

    assert_eq!(results.len(), 1);
    let value = results[0].get("extract").unwrap();
    assert!(value.contains("15"));

    ctx.commit();
}

#[test]
fn test_extract_second_from_time() {
    let mut ctx = setup_test();

    let results = ctx.query(r#"SELECT EXTRACT(SECOND FROM TIME '17:12:28') as extract"#);

    assert_eq!(results.len(), 1);
    let value = results[0].get("extract").unwrap();
    assert!(value.contains("28"));

    ctx.commit();
}

#[test]
fn test_extract_day_from_date() {
    let mut ctx = setup_test();

    let results = ctx.query(r#"SELECT EXTRACT(DAY FROM DATE '2021-10-06') as extract"#);

    assert_eq!(results.len(), 1);
    let value = results[0].get("extract").unwrap();
    assert!(value.contains("6"));

    ctx.commit();
}

#[test]
fn test_extract_year_from_interval() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT EXTRACT(YEAR FROM INTERVAL '3' YEAR) as extract");

    assert_eq!(results.len(), 1);
    let value = results[0].get("extract").unwrap();
    assert!(value.contains("3"));

    ctx.commit();
}

#[test]
fn test_extract_month_from_interval() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT EXTRACT(MONTH FROM INTERVAL '4' MONTH) as extract");

    assert_eq!(results.len(), 1);
    let value = results[0].get("extract").unwrap();
    assert!(value.contains("4"));

    ctx.commit();
}

#[test]
fn test_extract_day_from_interval() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT EXTRACT(DAY FROM INTERVAL '5' DAY) as extract");

    assert_eq!(results.len(), 1);
    let value = results[0].get("extract").unwrap();
    assert!(value.contains("5"));

    ctx.commit();
}

#[test]
fn test_extract_hour_from_interval() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT EXTRACT(HOUR FROM INTERVAL '6' HOUR) as extract");

    assert_eq!(results.len(), 1);
    let value = results[0].get("extract").unwrap();
    assert!(value.contains("6"));

    ctx.commit();
}

#[test]
fn test_extract_minute_from_interval() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT EXTRACT(MINUTE FROM INTERVAL '7' MINUTE) as extract");

    assert_eq!(results.len(), 1);
    let value = results[0].get("extract").unwrap();
    assert!(value.contains("7"));

    ctx.commit();
}

#[test]
fn test_extract_second_from_interval() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT EXTRACT(SECOND FROM INTERVAL '8' SECOND) as extract");

    assert_eq!(results.len(), 1);
    let value = results[0].get("extract").unwrap();
    assert!(value.contains("8"));

    ctx.commit();
}

#[test]
fn test_extract_format_not_matched_from_string() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Item (number TEXT)");
    ctx.exec("INSERT INTO Item VALUES ('1')");

    let error = ctx.exec_error(r#"SELECT EXTRACT(HOUR FROM number) as extract FROM Item"#);

    // Should error - cannot extract HOUR from a text value that isn't a timestamp
    assert!(
        error.contains("ExtractFormatNotMatched")
            || error.contains("format")
            || error.contains("HOUR"),
        "Expected ExtractFormatNotMatched error, got: {}",
        error
    );

    ctx.commit();
}

#[test]
fn test_extract_hour_from_year_interval_returns_zero() {
    let mut ctx = setup_test();

    // INTERVAL '7' YEAR has no HOUR component, so EXTRACT returns 0
    let results = ctx.query("SELECT EXTRACT(HOUR FROM INTERVAL '7' YEAR) as extract");

    assert_eq!(results.len(), 1);
    let value = results[0].get("extract").unwrap();
    // Should return 0 since YEAR intervals don't have hour components
    assert!(value.contains("0"));

    ctx.commit();
}

#[test]
fn test_extract_format_not_matched_from_integer() {
    let mut ctx = setup_test();

    let error = ctx.exec_error(r#"SELECT EXTRACT(HOUR FROM 100)"#);

    // Should error - cannot extract HOUR from an integer
    assert!(
        error.contains("Cannot extract")
            || error.contains("INT")
            || error.contains("expected TIMESTAMP"),
        "Expected type error for extracting from integer, got: {}",
        error
    );

    ctx.commit();
}

#[test]
fn test_extract_unsupported_datetime_field() {
    let mut ctx = setup_test();

    let error = ctx.exec_error("SELECT EXTRACT(microseconds FROM '2011-01-1')");

    // Should error - microseconds is not a supported datetime field
    assert!(
        error.contains("UnsupportedDateTimeField")
            || error.contains("microseconds")
            || error.contains("unsupported"),
        "Expected UnsupportedDateTimeField error, got: {}",
        error
    );

    ctx.commit();
}

#[test]
fn test_extract_with_plural_fields() {
    let mut ctx = setup_test();

    // Test that plural field names work the same as singular
    let r1 = ctx.query(r#"SELECT EXTRACT(YEARS FROM TIMESTAMP '2016-12-31 13:30:15') as y"#);
    let r2 = ctx.query(r#"SELECT EXTRACT(MONTHS FROM TIMESTAMP '2016-12-31 13:30:15') as m"#);
    let r3 = ctx.query(r#"SELECT EXTRACT(DAYS FROM TIMESTAMP '2016-12-31 13:30:15') as d"#);
    let r4 = ctx.query(r#"SELECT EXTRACT(HOURS FROM TIMESTAMP '2016-12-31 13:30:15') as h"#);
    let r5 = ctx.query(r#"SELECT EXTRACT(MINUTES FROM TIMESTAMP '2016-12-31 13:30:15') as min"#);
    let r6 = ctx.query(r#"SELECT EXTRACT(SECONDS FROM TIMESTAMP '2016-12-31 13:30:15') as s"#);

    assert!(r1[0].get("y").unwrap().contains("2016"));
    assert!(r2[0].get("m").unwrap().contains("12"));
    assert!(r3[0].get("d").unwrap().contains("31"));
    assert!(r4[0].get("h").unwrap().contains("13"));
    assert!(r5[0].get("min").unwrap().contains("30"));
    assert!(r6[0].get("s").unwrap().contains("15"));

    ctx.commit();
}
