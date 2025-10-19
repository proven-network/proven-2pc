//! Tests for SUM aggregate function
//! Based on gluesql/test-suite/src/aggregate/sum.rs

mod common;

use common::{TableBuilder, setup_test};
use proven_value::Value;

#[test]
fn test_sum_nullable_column() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, quantity INTEGER, age INTEGER, total INTEGER")
        .insert_values(
            "(1, 10, 11, 1), \
             (2, 0, 90, 2), \
             (3, 9, NULL, 3), \
             (4, 3, 3, 1), \
             (5, 25, NULL, 1)",
        );

    // SUM of column with NULLs should return sum of non-NULL values
    ctx.assert_query_value("SELECT SUM(age) FROM Item", "SUM(age)", Value::I32(104));

    ctx.commit();
}

#[test]
fn test_sum_multiple_columns() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, quantity INTEGER, age INTEGER, total INTEGER")
        .insert_values(
            "(1, 10, 11, 1), \
             (2, 0, 90, 2), \
             (3, 9, NULL, 3), \
             (4, 3, 3, 1), \
             (5, 25, NULL, 1)",
        );

    let results = ctx.query("SELECT SUM(id), SUM(quantity) FROM Item");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("SUM(id)").unwrap(), &Value::I32(15));
    assert_eq!(results[0].get("SUM(quantity)").unwrap(), &Value::I32(47));

    ctx.commit();
}

#[test]
fn test_sum_with_coalesce_function() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, quantity INTEGER, age INTEGER, total INTEGER")
        .insert_values(
            "(1, 10, 11, 1), \
             (2, 0, 90, 2), \
             (3, 9, NULL, 3), \
             (4, 3, 3, 1), \
             (5, 25, NULL, 1)",
        );

    // With COALESCE, NULL values become 0
    ctx.assert_query_value(
        "SELECT SUM(COALESCE(age, 0)) FROM Item",
        "SUM(COALESCE(age, 0))",
        Value::I32(104),
    );

    ctx.commit();
}

#[test]
fn test_sum_with_literal_expression() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, quantity INTEGER")
        .insert_values(
            "(1, 10), \
             (2, 0), \
             (3, 9), \
             (4, 3), \
             (5, 25)",
        );

    // SUM(1 + 2) = 3 * 5 rows = 15
    ctx.assert_query_value("SELECT SUM(1 + 2) FROM Item", "SUM(1 + 2)", Value::I32(15));

    ctx.commit();
}

#[test]
fn test_sum_with_column_expression() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, quantity INTEGER")
        .insert_values(
            "(1, 10), \
             (2, 0), \
             (3, 9), \
             (4, 3), \
             (5, 25)",
        );

    // SUM(id + 1) = (1+1) + (2+1) + (3+1) + (4+1) + (5+1) = 20
    ctx.assert_query_value(
        "SELECT SUM(id + 1) FROM Item",
        "SUM(id + 1)",
        Value::I32(20),
    );

    ctx.commit();
}

#[test]
fn test_sum_with_multiplication() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, quantity INTEGER")
        .insert_values(
            "(1, 10), \
             (2, 0), \
             (3, 9), \
             (4, 3), \
             (5, 25)",
        );

    // SUM(id * quantity) = 1*10 + 2*0 + 3*9 + 4*3 + 5*25 = 174
    ctx.assert_query_value(
        "SELECT SUM(id * quantity) FROM Item",
        "SUM(id * quantity)",
        Value::I32(174),
    );

    ctx.commit();
}

#[test]
fn test_sum_with_case_expression() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, quantity INTEGER")
        .insert_values(
            "(1, 10), \
             (2, 0), \
             (3, 9), \
             (4, 3), \
             (5, 25)",
        );

    // SUM only quantities where id > 3: 3 + 25 = 28
    ctx.assert_query_value(
        "SELECT SUM(CASE WHEN id > 3 THEN quantity ELSE 0 END) FROM Item",
        "SUM(CASE WHEN id > 3 THEN quantity ELSE 0 END)",
        Value::I32(28),
    );

    ctx.commit();
}

#[test]
fn test_sum_distinct_values() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, value INTEGER")
        .insert_values(
            "(1, 10), \
             (2, 20), \
             (3, 10), \
             (4, 30), \
             (5, 20), \
             (6, 10)",
        );

    // SUM(DISTINCT value) = 10 + 20 + 30 = 60
    ctx.assert_query_value(
        "SELECT SUM(DISTINCT value) FROM Item",
        "SUM(DISTINCT value)",
        Value::I32(60),
    );

    ctx.commit();
}

#[test]
fn test_sum_distinct_nullable_column() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, age INTEGER")
        .insert_values(
            "(1, 11), \
             (2, 90), \
             (3, NULL), \
             (4, 3), \
             (5, NULL), \
             (6, 11), \
             (7, 90)",
        );

    // SUM(DISTINCT age) = 11 + 90 + 3 = 104 (duplicates removed, NULLs ignored)
    ctx.assert_query_value(
        "SELECT SUM(DISTINCT age) FROM Item",
        "SUM(DISTINCT age)",
        Value::I32(104),
    );

    ctx.commit();
}

#[test]
fn test_sum_handles_null_propagation() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, value INTEGER")
        .insert_values(
            "(1, NULL), \
             (2, NULL), \
             (3, NULL)",
        );

    // SUM of all NULLs should return NULL
    ctx.assert_query_value("SELECT SUM(value) FROM Item", "SUM(value)", Value::Null);

    ctx.commit();
}
