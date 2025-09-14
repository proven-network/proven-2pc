//! Tests for COUNT aggregate function
//! Based on gluesql/test-suite/src/aggregate/count.rs

mod common;

use common::{TableBuilder, setup_test};

#[test]
fn test_count_all_rows() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, quantity INTEGER, age INTEGER, total INTEGER")
        .insert_values(
            "(1, NULL, 11, 1), \
             (2, 0, 90, 2), \
             (3, 9, NULL, 3), \
             (4, 3, 3, 1), \
             (5, 25, NULL, 1), \
             (6, 15, 11, 2), \
             (7, 20, 90, 1), \
             (1, NULL, 11, 1)",
        );

    ctx.assert_query_contains("SELECT COUNT(*) FROM Item", "COUNT(*)", "I64(8)");

    ctx.commit();
}

#[test]
fn test_count_specific_columns() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, quantity INTEGER, age INTEGER, total INTEGER")
        .insert_values(
            "(1, NULL, 11, 1), \
             (2, 0, 90, 2), \
             (3, 9, NULL, 3), \
             (4, 3, 3, 1), \
             (5, 25, NULL, 1), \
             (6, 15, 11, 2), \
             (7, 20, 90, 1), \
             (1, NULL, 11, 1)",
        );

    let results = ctx.query("SELECT COUNT(age), COUNT(quantity) FROM Item");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("COUNT(age)").unwrap(), "I64(6)");
    assert_eq!(results[0].get("COUNT(quantity)").unwrap(), "I64(6)");

    ctx.commit();
}

#[test]
fn test_count_null_literal() {
    let mut ctx = setup_test();

    ctx.assert_query_contains("SELECT COUNT(NULL)", "COUNT(NULL)", "I64(0)");

    ctx.commit();
}

#[test]
fn test_count_distinct_values() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, quantity INTEGER, age INTEGER, total INTEGER")
        .insert_values(
            "(1, NULL, 11, 1), \
             (2, 0, 90, 2), \
             (3, 9, NULL, 3), \
             (4, 3, 3, 1), \
             (5, 25, NULL, 1), \
             (6, 15, 11, 2), \
             (7, 20, 90, 1), \
             (1, NULL, 11, 1)",
        );

    ctx.assert_query_contains(
        "SELECT COUNT(DISTINCT id) FROM Item",
        "COUNT(DISTINCT id)",
        "I64(7)",
    );

    ctx.commit();
}

#[test]
fn test_count_distinct_nullable_column() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, quantity INTEGER, age INTEGER, total INTEGER")
        .insert_values(
            "(1, NULL, 11, 1), \
             (2, 0, 90, 2), \
             (3, 9, NULL, 3), \
             (4, 3, 3, 1), \
             (5, 25, NULL, 1), \
             (6, 15, 11, 2), \
             (7, 20, 90, 1), \
             (1, NULL, 11, 1)",
        );

    ctx.assert_query_contains(
        "SELECT COUNT(DISTINCT age) FROM Item",
        "COUNT(DISTINCT age)",
        "I64(3)",
    );

    ctx.commit();
}

#[test]
fn test_count_vs_count_distinct() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, quantity INTEGER, age INTEGER, total INTEGER")
        .insert_values(
            "(1, NULL, 11, 1), \
             (2, 0, 90, 2), \
             (3, 9, NULL, 3), \
             (4, 3, 3, 1), \
             (5, 25, NULL, 1), \
             (6, 15, 11, 2), \
             (7, 20, 90, 1), \
             (1, NULL, 11, 1)",
        );

    let results = ctx.query("SELECT COUNT(age), COUNT(DISTINCT age) FROM Item");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("COUNT(age)").unwrap(), "I64(6)");
    assert_eq!(results[0].get("COUNT(DISTINCT age)").unwrap(), "I64(3)");

    ctx.commit();
}

#[test]
fn test_count_distinct_all_columns() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, quantity INTEGER, age INTEGER, total INTEGER")
        .insert_values(
            "(1, NULL, 11, 1), \
             (2, 0, 90, 2), \
             (3, 9, NULL, 3), \
             (4, 3, 3, 1), \
             (5, 25, NULL, 1), \
             (6, 15, 11, 2), \
             (7, 20, 90, 1), \
             (1, NULL, 11, 1)",
        );

    ctx.assert_query_contains(
        "SELECT COUNT(DISTINCT *) FROM Item",
        "COUNT(DISTINCT *)",
        "I64(7)",
    );

    ctx.commit();
}

#[test]
fn test_count_with_null_values() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, value INTEGER")
        .insert_values(
            "(1, 10), \
             (2, NULL), \
             (3, 20), \
             (4, NULL), \
             (5, 30)",
        );

    ctx.assert_query_contains("SELECT COUNT(*) FROM Item", "COUNT(*)", "I64(5)");

    ctx.assert_query_contains("SELECT COUNT(value) FROM Item", "COUNT(value)", "I64(3)");

    ctx.commit();
}
