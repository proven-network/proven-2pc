//! SMALLINT data type tests (16-bit integer)
//! Based on gluesql/test-suite/src/data_type/int16.rs

mod common;

use common::{TableBuilder, setup_test};

fn setup_int16_table(ctx: &mut common::TestContext) {
    TableBuilder::new(ctx, "Item")
        .create_simple("field_one SMALLINT, field_two SMALLINT")
        .insert_values("(100, -100), (-200, 200), (300, 300), (-400, -400)");
}

#[test]
fn test_int16_table_creation_and_insertion() {
    let mut ctx = setup_test();
    setup_int16_table(&mut ctx);
    assert_rows!(ctx, "SELECT * FROM Item", 4);
    ctx.commit();
}

#[test]
fn test_int16_overflow_insertion_error() {
    let mut ctx = setup_test();
    ctx.exec("CREATE TABLE Item (field_one SMALLINT, field_two SMALLINT)");

    let overflow_sql = format!(
        "INSERT INTO Item VALUES ({}, {})",
        i16::MAX as i32 + 1,
        i16::MIN as i32 - 1
    );

    ctx.exec_error(&overflow_sql);
    ctx.commit();
}

#[test]
fn test_int16_overflow_cast_error() {
    let mut ctx = setup_test();
    setup_int16_table(&mut ctx);

    let cast_sql = format!("SELECT CAST({} AS SMALLINT) FROM Item", i16::MAX as i32 + 1);

    ctx.exec_error(&cast_sql);
    ctx.commit();
}

#[test]
fn test_int16_underflow_cast_error() {
    let mut ctx = setup_test();
    setup_int16_table(&mut ctx);

    let cast_sql = format!("SELECT CAST({} AS SMALLINT) FROM Item", i16::MIN as i32 - 1);

    ctx.exec_error(&cast_sql);
    ctx.commit();
}

#[test]
fn test_int16_basic_select() {
    let mut ctx = setup_test();
    setup_int16_table(&mut ctx);

    let results = ctx.query("SELECT field_one, field_two FROM Item ORDER BY field_one");
    assert_eq!(results.len(), 4);

    assert_eq!(results[0].get("field_one").unwrap(), "I16(-400)");
    assert_eq!(results[0].get("field_two").unwrap(), "I16(-400)");

    assert_eq!(results[1].get("field_one").unwrap(), "I16(-200)");
    assert_eq!(results[1].get("field_two").unwrap(), "I16(200)");

    assert_eq!(results[2].get("field_one").unwrap(), "I16(100)");
    assert_eq!(results[2].get("field_two").unwrap(), "I16(-100)");

    assert_eq!(results[3].get("field_one").unwrap(), "I16(300)");
    assert_eq!(results[3].get("field_two").unwrap(), "I16(300)");

    ctx.commit();
}

#[test]
fn test_int16_where_equality() {
    let mut ctx = setup_test();
    setup_int16_table(&mut ctx);

    assert_rows!(ctx, "SELECT field_one FROM Item WHERE field_one = 100", 1);
    ctx.assert_query_contains(
        "SELECT field_one FROM Item WHERE field_one = 100",
        "field_one",
        "I16(100)",
    );

    ctx.commit();
}

#[test]
fn test_int16_where_greater_than() {
    let mut ctx = setup_test();
    setup_int16_table(&mut ctx);

    assert_rows!(ctx, "SELECT field_one FROM Item WHERE field_one > 0", 2);

    let results = ctx.query("SELECT field_one FROM Item WHERE field_one > 0 ORDER BY field_one");
    assert_eq!(results[0].get("field_one").unwrap(), "I16(100)");
    assert_eq!(results[1].get("field_one").unwrap(), "I16(300)");

    ctx.commit();
}

#[test]
fn test_int16_where_greater_equal() {
    let mut ctx = setup_test();
    setup_int16_table(&mut ctx);

    assert_rows!(ctx, "SELECT field_one FROM Item WHERE field_one >= 0", 2);
    ctx.commit();
}

#[test]
fn test_int16_where_negative_equality() {
    let mut ctx = setup_test();
    setup_int16_table(&mut ctx);

    assert_rows!(ctx, "SELECT field_one FROM Item WHERE field_one = -200", 1);
    ctx.assert_query_contains(
        "SELECT field_one FROM Item WHERE field_one = -200",
        "field_one",
        "I16(-200)",
    );

    ctx.commit();
}

#[test]
fn test_int16_where_less_than() {
    let mut ctx = setup_test();
    setup_int16_table(&mut ctx);

    assert_rows!(ctx, "SELECT field_one FROM Item WHERE field_one < 0", 2);

    let results = ctx.query("SELECT field_one FROM Item WHERE field_one < 0 ORDER BY field_one");
    assert_eq!(results[0].get("field_one").unwrap(), "I16(-400)");
    assert_eq!(results[1].get("field_one").unwrap(), "I16(-200)");

    ctx.commit();
}

#[test]
fn test_int16_where_less_equal() {
    let mut ctx = setup_test();
    setup_int16_table(&mut ctx);

    assert_rows!(ctx, "SELECT field_one FROM Item WHERE field_one <= 0", 2);
    ctx.commit();
}

#[test]
fn test_int16_arithmetic_addition() {
    let mut ctx = setup_test();
    setup_int16_table(&mut ctx);

    let results = ctx.query("SELECT field_one + field_two AS plus FROM Item ORDER BY field_one");
    assert_eq!(results.len(), 4);

    assert_eq!(results[0].get("plus").unwrap(), "I16(-800)");
    assert_eq!(results[1].get("plus").unwrap(), "I16(0)");
    assert_eq!(results[2].get("plus").unwrap(), "I16(0)");
    assert_eq!(results[3].get("plus").unwrap(), "I16(600)");

    ctx.commit();
}

#[test]
fn test_int16_arithmetic_subtraction() {
    let mut ctx = setup_test();
    setup_int16_table(&mut ctx);

    let results = ctx.query("SELECT field_one - field_two AS sub FROM Item ORDER BY field_one");
    assert_eq!(results.len(), 4);

    assert_eq!(results[0].get("sub").unwrap(), "I16(0)");
    assert_eq!(results[1].get("sub").unwrap(), "I16(-400)");
    assert_eq!(results[2].get("sub").unwrap(), "I16(200)");
    assert_eq!(results[3].get("sub").unwrap(), "I16(0)");

    ctx.commit();
}

#[test]
fn test_int16_arithmetic_multiplication() {
    let mut ctx = setup_test();
    ctx.exec("CREATE TABLE Item (field_one SMALLINT, field_two SMALLINT)");
    ctx.exec("INSERT INTO Item VALUES (10, -10), (-20, 20), (30, 30), (-40, -40)");

    let results = ctx.query("SELECT field_one * field_two AS mul FROM Item ORDER BY field_one");
    assert_eq!(results.len(), 4);

    assert_eq!(results[0].get("mul").unwrap(), "I16(1600)");
    assert_eq!(results[1].get("mul").unwrap(), "I16(-400)");
    assert_eq!(results[2].get("mul").unwrap(), "I16(-100)");
    assert_eq!(results[3].get("mul").unwrap(), "I16(900)");

    ctx.commit();
}

#[test]
fn test_int16_arithmetic_division() {
    let mut ctx = setup_test();
    setup_int16_table(&mut ctx);

    let results = ctx.query("SELECT field_one / field_two AS div FROM Item ORDER BY field_one");
    assert_eq!(results.len(), 4);

    assert_eq!(results[0].get("div").unwrap(), "I16(1)");
    assert_eq!(results[1].get("div").unwrap(), "I16(-1)");
    assert_eq!(results[2].get("div").unwrap(), "I16(-1)");
    assert_eq!(results[3].get("div").unwrap(), "I16(1)");

    ctx.commit();
}

#[test]
fn test_int16_arithmetic_modulo() {
    let mut ctx = setup_test();
    setup_int16_table(&mut ctx);

    let results = ctx.query("SELECT field_one % field_two AS modulo FROM Item ORDER BY field_one");
    assert_eq!(results.len(), 4);

    assert_eq!(results[0].get("modulo").unwrap(), "I16(0)");
    assert_eq!(results[1].get("modulo").unwrap(), "I16(0)");
    assert_eq!(results[2].get("modulo").unwrap(), "I16(0)");
    assert_eq!(results[3].get("modulo").unwrap(), "I16(0)");

    ctx.commit();
}

#[test]
fn test_int16_boundary_values() {
    let mut ctx = setup_test();
    ctx.exec("CREATE TABLE Item (field_one SMALLINT, field_two SMALLINT)");

    let boundary_sql = format!("INSERT INTO Item VALUES ({}, {})", i16::MAX, i16::MIN);

    ctx.exec(&boundary_sql);
    assert_rows!(ctx, "SELECT * FROM Item", 1);

    let results = ctx.query("SELECT field_one, field_two FROM Item");
    assert_eq!(
        results[0].get("field_one").unwrap(),
        &format!("I16({})", i16::MAX)
    );
    assert_eq!(
        results[0].get("field_two").unwrap(),
        &format!("I16({})", i16::MIN)
    );

    ctx.commit();
}
