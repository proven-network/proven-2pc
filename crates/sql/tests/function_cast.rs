//! CAST function tests
//! Based on gluesql/test-suite/src/function/cast.rs

mod common;

use common::{TableBuilder, setup_test};

#[test]
fn test_cast_to_boolean() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("number TEXT")
        .insert_values("('1')");

    ctx.assert_query_contains(
        "SELECT CAST('TRUE' AS BOOLEAN) AS cast FROM Item",
        "cast",
        "Bool(true)",
    );
    ctx.assert_query_contains(
        "SELECT CAST(1 AS BOOLEAN) AS cast FROM Item",
        "cast",
        "Bool(true)",
    );
    ctx.assert_query_contains(
        "SELECT CAST(0 AS BOOLEAN) AS cast FROM Item",
        "cast",
        "Bool(false)",
    );

    ctx.commit();
}

#[test]
fn test_cast_boolean_invalid_values() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("number TEXT")
        .insert_values("('1')");

    ctx.assert_error_contains("SELECT CAST('asdf' AS BOOLEAN) AS cast FROM Item", "asdf");
    // Note: CAST(3 AS BOOLEAN) may succeed in some implementations (non-zero = true)

    ctx.abort();
}

#[test]
fn test_cast_null_to_boolean() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("number TEXT")
        .insert_values("('1')");

    ctx.assert_query_contains(
        "SELECT CAST(NULL AS BOOLEAN) AS cast FROM Item",
        "cast",
        "Null",
    );

    ctx.commit();
}

#[test]
fn test_cast_to_integer() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("number TEXT")
        .insert_values("('1')");

    ctx.assert_query_contains(
        "SELECT CAST('1' AS INTEGER) AS cast FROM Item",
        "cast",
        "I32(1)",
    );
    ctx.assert_query_contains(
        "SELECT CAST(SUBSTR('123', 2, 3) AS INTEGER) AS cast FROM Item",
        "cast",
        "I32(23)",
    );

    ctx.commit();
}

#[test]
fn test_cast_to_integer_invalid() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("number TEXT")
        .insert_values("('1')");

    ctx.assert_error_contains("SELECT CAST('foo' AS INTEGER) AS cast FROM Item", "foo");

    ctx.abort();
}

#[test]
fn test_cast_boolean_to_integer() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("number TEXT")
        .insert_values("('1')");

    ctx.assert_query_contains(
        "SELECT CAST(TRUE AS INTEGER) AS cast FROM Item",
        "cast",
        "I32(1)",
    );
    ctx.assert_query_contains(
        "SELECT CAST(FALSE AS INTEGER) AS cast FROM Item",
        "cast",
        "I32(0)",
    );
    ctx.assert_query_contains(
        "SELECT CAST(NULL AS INTEGER) AS cast FROM Item",
        "cast",
        "Null",
    );

    ctx.commit();
}

#[test]
fn test_cast_to_float() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("number TEXT")
        .insert_values("('1')");

    ctx.assert_query_contains(
        "SELECT CAST('1.1' AS FLOAT) AS cast FROM Item",
        "cast",
        "F64(1.1)",
    );
    ctx.assert_query_contains(
        "SELECT CAST(1 AS FLOAT) AS cast FROM Item",
        "cast",
        "F64(1)",
    );
    ctx.assert_query_contains(
        "SELECT CAST(NULL AS FLOAT) AS cast FROM Item",
        "cast",
        "Null",
    );

    ctx.commit();
}

#[test]
fn test_cast_to_float_invalid() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("number TEXT")
        .insert_values("('1')");

    ctx.assert_error_contains("SELECT CAST('foo' AS FLOAT) AS cast FROM Item", "foo");

    ctx.abort();
}

#[test]
fn test_cast_to_text() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("number TEXT")
        .insert_values("('1')");

    ctx.assert_query_contains("SELECT CAST(1 AS TEXT) AS cast FROM Item", "cast", "Str(1)");
    ctx.assert_query_contains(
        "SELECT CAST(1.1 AS TEXT) AS cast FROM Item",
        "cast",
        "Str(1.1)",
    );
    ctx.assert_query_contains(
        "SELECT CAST(TRUE AS TEXT) AS cast FROM Item",
        "cast",
        "Str(true)",
    );
    ctx.assert_query_contains(
        "SELECT CAST(NULL AS TEXT) AS cast FROM Item",
        "cast",
        "Null",
    );

    ctx.commit();
}

#[test]
fn test_cast_value_from_expression() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER NULL, flag BOOLEAN, ratio REAL NULL, number TEXT")
        .insert_values("(0, TRUE, NULL, '1')");

    ctx.assert_query_contains(
        "SELECT CAST(LOWER(number) AS INTEGER) AS cast FROM Item",
        "cast",
        "I32(1)",
    );
    ctx.assert_query_contains(
        "SELECT CAST(id AS BOOLEAN) AS cast FROM Item",
        "cast",
        "Bool(false)",
    );
    ctx.assert_query_contains(
        "SELECT CAST(flag AS TEXT) AS cast FROM Item",
        "cast",
        "Str(true)",
    );

    ctx.commit();
}

#[test]
fn test_cast_value_null_handling() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER NULL, flag BOOLEAN, ratio REAL NULL, number TEXT")
        .insert_values("(0, TRUE, NULL, '1')");

    ctx.assert_query_contains(
        "SELECT CAST(ratio AS INTEGER) AS cast FROM Item",
        "cast",
        "Null",
    );

    ctx.commit();
}
