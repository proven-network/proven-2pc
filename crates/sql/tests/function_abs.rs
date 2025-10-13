//! ABS function tests
//! Based on gluesql/test-suite/src/function/abs.rs

mod common;

use common::{TableBuilder, setup_test};
use proven_value::Value;

#[test]
fn test_abs_with_integers() {
    let mut ctx = setup_test();

    ctx.assert_query_value("SELECT ABS(1) AS abs1", "abs1", Value::I32(1));
    ctx.assert_query_value("SELECT ABS(-1) AS abs2", "abs2", Value::I32(1));
    ctx.assert_query_value("SELECT ABS(+1) AS abs3", "abs3", Value::I32(1));

    ctx.commit();
}

#[test]
fn test_abs_with_floats() {
    let mut ctx = setup_test();

    ctx.assert_query_value("SELECT ABS(1.5) AS abs1", "abs1", Value::F64(1.5));
    ctx.assert_query_value("SELECT ABS(-1.5) AS abs2", "abs2", Value::F64(1.5));
    ctx.assert_query_value("SELECT ABS(+1.5) AS abs3", "abs3", Value::F64(1.5));

    ctx.commit();
}

#[test]
fn test_abs_with_zeros() {
    let mut ctx = setup_test();

    ctx.assert_query_value("SELECT ABS(0) AS abs1", "abs1", Value::I32(0));
    ctx.assert_query_value("SELECT ABS(-0) AS abs2", "abs2", Value::I32(0));
    ctx.assert_query_value("SELECT ABS(+0) AS abs3", "abs3", Value::I32(0));

    ctx.commit();
}

#[test]
fn test_abs_with_table_columns() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "SingleItem")
        .create_simple("id INTEGER, int8 INTEGER, dec REAL")
        .insert_values("(0, -1, -2.0)");

    ctx.assert_query_value(
        "SELECT ABS(id) AS abs1 FROM SingleItem",
        "abs1",
        Value::I32(0),
    );
    ctx.assert_query_value(
        "SELECT ABS(int8) AS abs2 FROM SingleItem",
        "abs2",
        Value::I32(1),
    );
    ctx.assert_query_value(
        "SELECT ABS(dec) AS abs3 FROM SingleItem",
        "abs3",
        Value::F32(2.0),
    );

    ctx.commit();
}

#[test]
fn test_abs_with_string_input() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "SingleItem").create_simple("id INTEGER");

    ctx.assert_error_contains(
        "SELECT ABS('string') AS abs FROM SingleItem",
        "TypeMismatch",
    );

    ctx.abort();
}

#[test]
fn test_abs_with_null_input() {
    let mut ctx = setup_test();

    ctx.assert_query_value("SELECT ABS(NULL) AS abs", "abs", Value::Null);

    ctx.commit();
}

#[test]
fn test_abs_with_boolean_inputs() {
    let mut ctx = setup_test();

    ctx.assert_error_contains("SELECT ABS(TRUE) AS abs", "TypeMismatch");

    ctx.assert_error_contains("SELECT ABS(FALSE) AS abs", "TypeMismatch");

    ctx.abort();
}

#[test]
fn test_abs_with_wrong_number_of_arguments() {
    let mut ctx = setup_test();

    ctx.assert_error_contains(
        "SELECT ABS('string', 'string2') AS abs",
        "ABS takes exactly 1 argument",
    );

    ctx.abort();
}
