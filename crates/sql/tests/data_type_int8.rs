//! TINYINT data type tests (8-bit integer)
//! Based on gluesql/test-suite/src/data_type/int8.rs

mod common;

use common::{TableBuilder, setup_test};
use proven_value::Value;

fn setup_int8_table(ctx: &mut common::TestContext) {
    TableBuilder::new(ctx, "Item")
        .create_simple("field_one TINYINT, field_two TINYINT")
        .insert_values("(1, -1), (-2, 2), (3, 3), (-4, -4)");
}

#[test]
fn test_create_table_with_int8_columns() {
    let mut ctx = setup_test();
    ctx.exec("CREATE TABLE Item (field_one TINYINT, field_two TINYINT)");
    ctx.exec("INSERT INTO Item VALUES (1, -1), (-2, 2), (3, 3), (-4, -4)");
    assert_rows!(ctx, "SELECT * FROM Item", 4);
    ctx.commit();
}

#[test]
fn test_insert_int8_values() {
    let mut ctx = setup_test();
    setup_int8_table(&mut ctx);
    assert_rows!(ctx, "SELECT * FROM Item", 4);
    ctx.commit();
}

#[test]
fn test_insert_int8_overflow_positive_should_error() {
    let mut ctx = setup_test();
    ctx.exec("CREATE TABLE Item (field_one TINYINT, field_two TINYINT)");

    // 128 is beyond i8::MAX (127)
    ctx.exec_error("INSERT INTO Item VALUES (128, 128)");
    ctx.commit();
}

#[test]
fn test_insert_int8_overflow_negative_should_error() {
    let mut ctx = setup_test();
    ctx.exec("CREATE TABLE Item (field_one TINYINT, field_two TINYINT)");

    // -129 is beyond i8::MIN (-128)
    ctx.exec_error("INSERT INTO Item VALUES (-129, -129)");
    ctx.commit();
}

#[test]
fn test_select_int8_values() {
    let mut ctx = setup_test();
    setup_int8_table(&mut ctx);

    let results = ctx.query("SELECT field_one, field_two FROM Item ORDER BY field_one");
    assert_eq!(results.len(), 4);

    assert_eq!(results[0].get("field_one").unwrap(), &Value::I8(-4));
    assert_eq!(results[0].get("field_two").unwrap(), &Value::I8(-4));

    assert_eq!(results[1].get("field_one").unwrap(), &Value::I8(-2));
    assert_eq!(results[1].get("field_two").unwrap(), &Value::I8(2));

    assert_eq!(results[2].get("field_one").unwrap(), &Value::I8(1));
    assert_eq!(results[2].get("field_two").unwrap(), &Value::I8(-1));

    assert_eq!(results[3].get("field_one").unwrap(), &Value::I8(3));
    assert_eq!(results[3].get("field_two").unwrap(), &Value::I8(3));

    ctx.commit();
}

#[test]
fn test_int8_comparison_greater_than() {
    let mut ctx = setup_test();
    setup_int8_table(&mut ctx);

    assert_rows!(ctx, "SELECT field_one FROM Item WHERE field_one > 0", 2);

    let results = ctx.query("SELECT field_one FROM Item WHERE field_one > 0 ORDER BY field_one");
    assert_eq!(results[0].get("field_one").unwrap(), &Value::I8(1));
    assert_eq!(results[1].get("field_one").unwrap(), &Value::I8(3));

    ctx.commit();
}

#[test]
fn test_int8_comparison_greater_equal() {
    let mut ctx = setup_test();
    setup_int8_table(&mut ctx);

    assert_rows!(ctx, "SELECT field_one FROM Item WHERE field_one >= 0", 2);
    ctx.commit();
}

#[test]
fn test_int8_comparison_less_than() {
    let mut ctx = setup_test();
    setup_int8_table(&mut ctx);

    assert_rows!(ctx, "SELECT field_one FROM Item WHERE field_one < 0", 2);

    let results = ctx.query("SELECT field_one FROM Item WHERE field_one < 0 ORDER BY field_one");
    assert_eq!(results[0].get("field_one").unwrap(), &Value::I8(-4));
    assert_eq!(results[1].get("field_one").unwrap(), &Value::I8(-2));

    ctx.commit();
}

#[test]
fn test_int8_comparison_less_equal() {
    let mut ctx = setup_test();
    setup_int8_table(&mut ctx);

    assert_rows!(ctx, "SELECT field_one FROM Item WHERE field_one <= 0", 2);
    ctx.commit();
}

#[test]
fn test_int8_comparison_equality() {
    let mut ctx = setup_test();
    setup_int8_table(&mut ctx);

    assert_rows!(ctx, "SELECT field_one FROM Item WHERE field_one = 1", 1);
    ctx.assert_query_value(
        "SELECT field_one FROM Item WHERE field_one = 1",
        "field_one",
        Value::I8(1),
    );

    assert_rows!(ctx, "SELECT field_one FROM Item WHERE field_one = -2", 1);
    ctx.assert_query_value(
        "SELECT field_one FROM Item WHERE field_one = -2",
        "field_one",
        Value::I8(-2),
    );

    ctx.commit();
}

#[test]
fn test_int8_arithmetic_addition() {
    let mut ctx = setup_test();
    setup_int8_table(&mut ctx);

    let results = ctx.query("SELECT field_one + field_two AS plus FROM Item ORDER BY field_one");
    assert_eq!(results.len(), 4);

    assert_eq!(results[0].get("plus").unwrap(), &Value::I8(-8));
    assert_eq!(results[1].get("plus").unwrap(), &Value::I8(0));
    assert_eq!(results[2].get("plus").unwrap(), &Value::I8(0));
    assert_eq!(results[3].get("plus").unwrap(), &Value::I8(6));

    ctx.commit();
}

#[test]
fn test_int8_arithmetic_subtraction() {
    let mut ctx = setup_test();
    setup_int8_table(&mut ctx);

    let results = ctx.query("SELECT field_one - field_two AS sub FROM Item ORDER BY field_one");
    assert_eq!(results.len(), 4);

    assert_eq!(results[0].get("sub").unwrap(), &Value::I8(0));
    assert_eq!(results[1].get("sub").unwrap(), &Value::I8(-4));
    assert_eq!(results[2].get("sub").unwrap(), &Value::I8(2));
    assert_eq!(results[3].get("sub").unwrap(), &Value::I8(0));

    ctx.commit();
}

#[test]
fn test_int8_arithmetic_multiplication() {
    let mut ctx = setup_test();
    setup_int8_table(&mut ctx);

    let results = ctx.query("SELECT field_one * field_two AS mul FROM Item ORDER BY field_one");
    assert_eq!(results.len(), 4);

    assert_eq!(results[0].get("mul").unwrap(), &Value::I8(16));
    assert_eq!(results[1].get("mul").unwrap(), &Value::I8(-4));
    assert_eq!(results[2].get("mul").unwrap(), &Value::I8(-1));
    assert_eq!(results[3].get("mul").unwrap(), &Value::I8(9));

    ctx.commit();
}

#[test]
fn test_int8_arithmetic_division() {
    let mut ctx = setup_test();
    setup_int8_table(&mut ctx);

    let results = ctx.query("SELECT field_one / field_two AS div FROM Item ORDER BY field_one");
    assert_eq!(results.len(), 4);

    assert_eq!(results[0].get("div").unwrap(), &Value::I8(1));
    assert_eq!(results[1].get("div").unwrap(), &Value::I8(-1));
    assert_eq!(results[2].get("div").unwrap(), &Value::I8(-1));
    assert_eq!(results[3].get("div").unwrap(), &Value::I8(1));

    ctx.commit();
}

#[test]
fn test_int8_arithmetic_modulo() {
    let mut ctx = setup_test();
    setup_int8_table(&mut ctx);

    let results = ctx.query("SELECT field_one % field_two AS modulo FROM Item ORDER BY field_one");
    assert_eq!(results.len(), 4);

    assert_eq!(results[0].get("modulo").unwrap(), &Value::I8(0));
    assert_eq!(results[1].get("modulo").unwrap(), &Value::I8(0));
    assert_eq!(results[2].get("modulo").unwrap(), &Value::I8(0));
    assert_eq!(results[3].get("modulo").unwrap(), &Value::I8(0));

    ctx.commit();
}

#[test]
fn test_int8_boundary_values() {
    let mut ctx = setup_test();
    ctx.exec("CREATE TABLE Item (field_one TINYINT, field_two TINYINT)");

    let boundary_sql = format!("INSERT INTO Item VALUES ({}, {})", i8::MAX, i8::MIN);

    ctx.exec(&boundary_sql);
    assert_rows!(ctx, "SELECT * FROM Item", 1);

    let results = ctx.query("SELECT field_one, field_two FROM Item");
    assert_eq!(results[0].get("field_one").unwrap(), &Value::I8(i8::MAX));
    assert_eq!(results[0].get("field_two").unwrap(), &Value::I8(i8::MIN));

    ctx.commit();
}

#[test]
fn test_int8_cast_overflow_error() {
    let mut ctx = setup_test();
    setup_int8_table(&mut ctx);

    let cast_sql = format!("SELECT CAST({} AS TINYINT) FROM Item", i8::MAX as i16 + 1);

    ctx.exec_error(&cast_sql);
    ctx.commit();
}

#[test]
fn test_int8_cast_underflow_error() {
    let mut ctx = setup_test();
    setup_int8_table(&mut ctx);

    let cast_sql = format!("SELECT CAST({} AS TINYINT) FROM Item", i8::MIN as i16 - 1);

    ctx.exec_error(&cast_sql);
    ctx.commit();
}
