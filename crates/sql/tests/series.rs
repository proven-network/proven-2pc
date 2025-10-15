//! SERIES table-valued function tests
//! Based on gluesql/test-suite/src/series.rs

mod common;

use common::setup_test;
use proven_value::Value;

#[test]
fn test_series_basic() {
    let mut ctx = setup_test();

    // SERIES(N) has internal column `n` (lowercase)
    assert_rows!(ctx, "SELECT * FROM SERIES(3)", 3);

    let results = ctx.query("SELECT * FROM SERIES(3)");
    assert_eq!(results[0].get("n"), Some(&Value::I64(1)));
    assert_eq!(results[1].get("n"), Some(&Value::I64(2)));
    assert_eq!(results[2].get("n"), Some(&Value::I64(3)));

    ctx.commit();
}

#[test]
fn test_series_case_insensitive() {
    let mut ctx = setup_test();

    // SERIES(N) with lowercase works
    assert_rows!(ctx, "SELECT * FROM sErIeS(3)", 3);

    let results = ctx.query("SELECT * FROM sErIeS(3)");
    assert_eq!(results[0].get("n"), Some(&Value::I64(1)));
    assert_eq!(results[1].get("n"), Some(&Value::I64(2)));
    assert_eq!(results[2].get("n"), Some(&Value::I64(3)));

    ctx.commit();
}

#[test]
fn test_series_with_table_alias() {
    let mut ctx = setup_test();

    // SERIES(N) with table alias
    assert_rows!(ctx, "SELECT S.* FROM SERIES(3) as S", 3);

    let results = ctx.query("SELECT S.* FROM SERIES(3) as S");
    assert_eq!(results[0].get("n"), Some(&Value::I64(1)));
    assert_eq!(results[1].get("n"), Some(&Value::I64(2)));
    assert_eq!(results[2].get("n"), Some(&Value::I64(3)));

    ctx.commit();
}

#[test]
fn test_series_with_unary_plus() {
    let mut ctx = setup_test();

    // SERIES with unary plus is allowed
    assert_rows!(ctx, "SELECT * FROM SERIES(+3)", 3);

    let results = ctx.query("SELECT * FROM SERIES(+3)");
    assert_eq!(results[0].get("n"), Some(&Value::I64(1)));
    assert_eq!(results[1].get("n"), Some(&Value::I64(2)));
    assert_eq!(results[2].get("n"), Some(&Value::I64(3)));

    ctx.commit();
}

#[test]
fn test_create_table_as_series() {
    let mut ctx = setup_test();

    // CTAS with SERIES(N)
    ctx.exec("CREATE TABLE SeriesTable AS SELECT * FROM SERIES(3)");

    // Verify table was created and data was inserted
    assert_rows!(ctx, "SELECT * FROM SeriesTable", 3);

    let results = ctx.query("SELECT * FROM SeriesTable");
    // Column should be named "n" from SERIES
    assert_eq!(results[0].get("n"), Some(&Value::I64(1)));
    assert_eq!(results[1].get("n"), Some(&Value::I64(2)));
    assert_eq!(results[2].get("n"), Some(&Value::I64(3)));

    ctx.commit();
}

#[test]
fn test_series_size_zero() {
    let mut ctx = setup_test();

    // SERIES with size 0 is allowed, returns empty result
    assert_rows!(ctx, "SELECT * FROM SERIES(0)", 0);

    ctx.commit();
}

#[test]
fn test_series_without_parentheses_should_error() {
    let mut ctx = setup_test();

    // SERIES without parentheses is a normal table name, should error
    // Note: table names are case-normalized to lowercase
    assert_error!(ctx, "SELECT * FROM SERIES", "series");

    ctx.abort();
}

#[test]
fn test_series_without_arguments_should_error() {
    let mut ctx = setup_test();

    // SERIES without size is not allowed
    assert_error!(ctx, "SELECT * FROM SERIES()");

    ctx.abort();
}

#[test]
fn test_series_negative_size_should_error() {
    let mut ctx = setup_test();

    // SERIES with unary minus is not allowed
    assert_error!(ctx, "SELECT * FROM SERIES(-1)");

    ctx.abort();
}

#[test]
fn test_select_without_table() {
    let mut ctx = setup_test();

    // SELECT without Table
    let results = ctx.query("SELECT 1, 'a', true, 1 + 2, 'a' || 'b'");
    assert_eq!(results.len(), 1);

    let row = &results[0];
    assert_eq!(row.get("1"), Some(&Value::I32(1)));
    assert_eq!(row.get("'a'"), Some(&Value::Str("a".to_string())));
    assert_eq!(row.get("true"), Some(&Value::Bool(true)));
    assert_eq!(row.get("1 + 2"), Some(&Value::I32(3)));
    assert_eq!(row.get("'a' || 'b'"), Some(&Value::Str("ab".to_string())));

    ctx.commit();
}

#[test]
fn test_select_without_table_scalar_subquery() {
    let mut ctx = setup_test();

    // SELECT without Table in Scalar subquery
    ctx.assert_query_value(
        "SELECT (SELECT 'Hello')",
        "(SELECT 'Hello')",
        Value::Str("Hello".to_string()),
    );

    ctx.commit();
}

#[test]
fn test_select_without_table_with_aliases() {
    let mut ctx = setup_test();

    // SELECT without Table with Column aliases
    let results = ctx.query("SELECT 1 AS id, (SELECT MAX(N) FROM SERIES(3)) AS max");
    assert_eq!(results.len(), 1);

    let row = &results[0];
    assert_eq!(row.get("id"), Some(&Value::I32(1)));
    assert_eq!(row.get("max"), Some(&Value::I64(3)));

    ctx.commit();
}

#[test]
fn test_select_without_table_in_derived() {
    let mut ctx = setup_test();

    // SELECT without Table in Derived
    let results = ctx.query("SELECT * FROM (SELECT 1) AS Derived");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("1"), Some(&Value::I32(1)));

    ctx.commit();
}

#[test]
fn test_select_star_without_table() {
    let mut ctx = setup_test();

    // `SELECT *` fetches column `n` (defaults to SERIES(1))
    let results = ctx.query("SELECT *");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("n"), Some(&Value::I64(1)));

    ctx.commit();
}

#[test]
fn test_create_table_as_select_without_table() {
    let mut ctx = setup_test();

    // CTAS without Table
    ctx.exec("CREATE TABLE TargetTable AS SELECT 1");

    // Verify table was created
    let results = ctx.query("SELECT * FROM TargetTable");
    assert_eq!(results.len(), 1);
    // Column name should be "1" since that's the expression
    assert_eq!(results[0].get("1"), Some(&Value::I32(1)));

    ctx.commit();
}
