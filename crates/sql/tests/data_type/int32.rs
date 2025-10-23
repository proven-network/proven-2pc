//! Tests for INT data type functionality (32-bit integer)
//! Based on gluesql/test-suite/src/data_type/int32.rs

use crate::assert_rows;
use crate::common::{TableBuilder, TestContext, setup_test};
use proven_value::Value;

fn setup_int32_table(ctx: &mut TestContext) {
    TableBuilder::new(ctx, "Item")
        .create_simple("field_one INT, field_two INT")
        .insert_values("(1, -1), (-2, 2), (3, 3), (-4, -4)");
}

#[test]
fn test_int32_table_creation_and_insertion() {
    let mut ctx = setup_test();
    setup_int32_table(&mut ctx);

    assert_rows!(ctx, "SELECT * FROM Item", 4);
    ctx.commit();
}

#[test]
fn test_int32_overflow_insertion_error() {
    let mut ctx = setup_test();
    ctx.exec("CREATE TABLE Item (field_one INT, field_two INT)");

    let overflow_sql = format!(
        "INSERT INTO Item VALUES ({}, {})",
        i32::MAX as i64 + 1,
        i32::MIN as i64 - 1
    );

    ctx.exec_error(&overflow_sql);
    ctx.commit();
}

#[test]
fn test_int32_overflow_cast_error() {
    let mut ctx = setup_test();
    setup_int32_table(&mut ctx);

    let cast_sql = format!("SELECT CAST({} AS INT) FROM Item", i32::MAX as i64 + 1);

    ctx.exec_error(&cast_sql);
    ctx.commit();
}

#[test]
fn test_int32_underflow_cast_error() {
    let mut ctx = setup_test();
    setup_int32_table(&mut ctx);

    let cast_sql = format!("SELECT CAST({} AS INT) FROM Item", i32::MIN as i64 - 1);

    ctx.exec_error(&cast_sql);
    ctx.commit();
}

#[test]
fn test_int32_basic_select() {
    let mut ctx = setup_test();
    setup_int32_table(&mut ctx);

    let results = ctx.query("SELECT field_one, field_two FROM Item ORDER BY field_one");
    assert_eq!(results.len(), 4);

    assert_eq!(results[0].get("field_one").unwrap(), &Value::I32(-4));
    assert_eq!(results[0].get("field_two").unwrap(), &Value::I32(-4));

    assert_eq!(results[1].get("field_one").unwrap(), &Value::I32(-2));
    assert_eq!(results[1].get("field_two").unwrap(), &Value::I32(2));

    assert_eq!(results[2].get("field_one").unwrap(), &Value::I32(1));
    assert_eq!(results[2].get("field_two").unwrap(), &Value::I32(-1));

    assert_eq!(results[3].get("field_one").unwrap(), &Value::I32(3));
    assert_eq!(results[3].get("field_two").unwrap(), &Value::I32(3));

    ctx.commit();
}

#[test]
fn test_int32_where_equality() {
    let mut ctx = setup_test();
    setup_int32_table(&mut ctx);

    assert_rows!(ctx, "SELECT field_one FROM Item WHERE field_one = 1", 1);
    ctx.assert_query_value(
        "SELECT field_one FROM Item WHERE field_one = 1",
        "field_one",
        Value::I32(1),
    );

    ctx.commit();
}

#[test]
fn test_int32_where_greater_than() {
    let mut ctx = setup_test();
    setup_int32_table(&mut ctx);

    assert_rows!(ctx, "SELECT field_one FROM Item WHERE field_one > 0", 2);

    let results = ctx.query("SELECT field_one FROM Item WHERE field_one > 0 ORDER BY field_one");
    assert_eq!(results[0].get("field_one").unwrap(), &Value::I32(1));
    assert_eq!(results[1].get("field_one").unwrap(), &Value::I32(3));

    ctx.commit();
}

#[test]
fn test_int32_where_greater_equal() {
    let mut ctx = setup_test();
    setup_int32_table(&mut ctx);

    assert_rows!(ctx, "SELECT field_one FROM Item WHERE field_one >= 0", 2);

    ctx.commit();
}

#[test]
fn test_int32_where_negative_equality() {
    let mut ctx = setup_test();
    setup_int32_table(&mut ctx);

    assert_rows!(ctx, "SELECT field_one FROM Item WHERE field_one = -2", 1);
    ctx.assert_query_value(
        "SELECT field_one FROM Item WHERE field_one = -2",
        "field_one",
        Value::I32(-2),
    );

    ctx.commit();
}

#[test]
fn test_int32_where_less_than() {
    let mut ctx = setup_test();
    setup_int32_table(&mut ctx);

    assert_rows!(ctx, "SELECT field_one FROM Item WHERE field_one < 0", 2);

    let results = ctx.query("SELECT field_one FROM Item WHERE field_one < 0 ORDER BY field_one");
    assert_eq!(results[0].get("field_one").unwrap(), &Value::I32(-4));
    assert_eq!(results[1].get("field_one").unwrap(), &Value::I32(-2));

    ctx.commit();
}

#[test]
fn test_int32_where_less_equal() {
    let mut ctx = setup_test();
    setup_int32_table(&mut ctx);

    assert_rows!(ctx, "SELECT field_one FROM Item WHERE field_one <= 0", 2);

    ctx.commit();
}

#[test]
fn test_int32_arithmetic_addition() {
    let mut ctx = setup_test();
    setup_int32_table(&mut ctx);

    let results = ctx.query("SELECT field_one + field_two AS plus FROM Item ORDER BY field_one");
    assert_eq!(results.len(), 4);

    assert_eq!(results[0].get("plus").unwrap(), &Value::I32(-8));
    assert_eq!(results[1].get("plus").unwrap(), &Value::I32(0));
    assert_eq!(results[2].get("plus").unwrap(), &Value::I32(0));
    assert_eq!(results[3].get("plus").unwrap(), &Value::I32(6));

    ctx.commit();
}

#[test]
fn test_int32_arithmetic_subtraction() {
    let mut ctx = setup_test();
    setup_int32_table(&mut ctx);

    let results = ctx.query("SELECT field_one - field_two AS sub FROM Item ORDER BY field_one");
    assert_eq!(results.len(), 4);

    assert_eq!(results[0].get("sub").unwrap(), &Value::I32(0));
    assert_eq!(results[1].get("sub").unwrap(), &Value::I32(-4));
    assert_eq!(results[2].get("sub").unwrap(), &Value::I32(2));
    assert_eq!(results[3].get("sub").unwrap(), &Value::I32(0));

    ctx.commit();
}

#[test]
fn test_int32_arithmetic_multiplication() {
    let mut ctx = setup_test();
    setup_int32_table(&mut ctx);

    let results = ctx.query("SELECT field_one * field_two AS mul FROM Item ORDER BY field_one");
    assert_eq!(results.len(), 4);

    assert_eq!(results[0].get("mul").unwrap(), &Value::I32(16));
    assert_eq!(results[1].get("mul").unwrap(), &Value::I32(-4));
    assert_eq!(results[2].get("mul").unwrap(), &Value::I32(-1));
    assert_eq!(results[3].get("mul").unwrap(), &Value::I32(9));

    ctx.commit();
}

#[test]
fn test_int32_arithmetic_division() {
    let mut ctx = setup_test();
    setup_int32_table(&mut ctx);

    let results = ctx.query("SELECT field_one / field_two AS div FROM Item ORDER BY field_one");
    assert_eq!(results.len(), 4);

    assert_eq!(results[0].get("div").unwrap(), &Value::I32(1));
    assert_eq!(results[1].get("div").unwrap(), &Value::I32(-1));
    assert_eq!(results[2].get("div").unwrap(), &Value::I32(-1));
    assert_eq!(results[3].get("div").unwrap(), &Value::I32(1));

    ctx.commit();
}

#[test]
fn test_int32_arithmetic_modulo() {
    let mut ctx = setup_test();
    setup_int32_table(&mut ctx);

    let results = ctx.query("SELECT field_one % field_two AS modulo FROM Item ORDER BY field_one");
    assert_eq!(results.len(), 4);

    assert_eq!(results[0].get("modulo").unwrap(), &Value::I32(0));
    assert_eq!(results[1].get("modulo").unwrap(), &Value::I32(0));
    assert_eq!(results[2].get("modulo").unwrap(), &Value::I32(0));
    assert_eq!(results[3].get("modulo").unwrap(), &Value::I32(0));

    ctx.commit();
}

#[test]
fn test_int32_boundary_values() {
    let mut ctx = setup_test();
    ctx.exec("CREATE TABLE Item (field_one INT, field_two INT)");

    let boundary_sql = format!("INSERT INTO Item VALUES ({}, {})", i32::MAX, i32::MIN);

    ctx.exec(&boundary_sql);
    assert_rows!(ctx, "SELECT * FROM Item", 1);

    let results = ctx.query("SELECT field_one, field_two FROM Item");
    assert_eq!(results[0].get("field_one").unwrap(), &Value::I32(i32::MAX));
    assert_eq!(results[0].get("field_two").unwrap(), &Value::I32(i32::MIN));

    ctx.commit();
}
