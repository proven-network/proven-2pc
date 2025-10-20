//! Tests for MIN aggregate function
//! Based on gluesql/test-suite/src/aggregate/min.rs

mod common;

use common::{TableBuilder, setup_test};
use proven_value::Value;

#[test]
fn test_min_nullable_column() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, age INTEGER")
        .insert_values(
            "(1, 11), \
             (2, 90), \
             (3, NULL), \
             (4, 3), \
             (5, NULL)",
        );

    // MIN ignores NULL values
    ctx.assert_query_value("SELECT MIN(age) FROM Item", "MIN(age)", Value::I32(3));

    ctx.commit();
}

#[test]
fn test_min_multiple_columns() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, quantity INTEGER, age INTEGER")
        .insert_values(
            "(1, 10, 11), \
             (2, 0, 90), \
             (3, 9, 5), \
             (4, 3, 3), \
             (5, 25, 15)",
        );

    let results = ctx.query("SELECT MIN(id), MIN(quantity) FROM Item");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("MIN(id)").unwrap(), &Value::I32(1));
    assert_eq!(results[0].get("MIN(quantity)").unwrap(), &Value::I32(0));

    ctx.commit();
}

#[test]
fn test_min_with_expression() {
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

    // MIN(id + quantity) = MIN(11, 2, 12, 7, 30) = 2
    ctx.assert_query_value(
        "SELECT MIN(id + quantity) FROM Item",
        "MIN(id + quantity)",
        Value::I32(2),
    );

    ctx.commit();
}

#[test]
fn test_min_in_complex_expression() {
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

    // SUM(quantity) = 47, MIN(quantity) = 0
    // SUM(quantity) * 2 + MIN(quantity) - 3 / 1 = 47*2 + 0 - 3 = 91
    let results = ctx.query("SELECT SUM(quantity) * 2 + MIN(quantity) - 3 / 1 FROM Item");
    assert_eq!(results.len(), 1);
    // Check the complex expression result
    for (key, value) in &results[0] {
        println!("Column: {}, Value: {}", key, value);
        // Should be 91 as I32
        assert!(value.to_string().contains("91"));
    }

    ctx.commit();
}

#[test]
fn test_min_with_case_expression() {
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

    // MIN of ids where quantity > 5: MIN(1, 3, 5) = 1
    ctx.assert_query_value(
        "SELECT MIN(CASE WHEN quantity > 5 THEN id END) FROM Item",
        "MIN(CASE WHEN quantity > 5 THEN id END)",
        Value::I32(1),
    );

    ctx.commit();
}

#[test]
fn test_min_distinct_values() {
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

    // MIN(DISTINCT value) should be same as MIN(value) = 10
    ctx.assert_query_value(
        "SELECT MIN(DISTINCT value) FROM Item",
        "MIN(DISTINCT value)",
        Value::I32(10),
    );

    ctx.commit();
}

#[test]
fn test_min_distinct_nullable_column() {
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

    // MIN(DISTINCT age) = MIN(11, 90, 3) = 3
    ctx.assert_query_value(
        "SELECT MIN(DISTINCT age) FROM Item",
        "MIN(DISTINCT age)",
        Value::I32(3),
    );

    ctx.commit();
}

#[test]
fn test_min_handles_nulls() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, value INTEGER")
        .insert_values(
            "(1, NULL), \
             (2, NULL), \
             (3, NULL)",
        );

    // MIN of all NULLs should return NULL
    ctx.assert_query_value("SELECT MIN(value) FROM Item", "MIN(value)", Value::Null);

    ctx.commit();
}

#[test]
fn test_min_strings() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, name TEXT")
        .insert_values(
            "(1, 'zebra'), \
             (2, 'apple'), \
             (3, 'banana'), \
             (4, NULL), \
             (5, 'cherry')",
        );

    // MIN on strings returns lexicographically smallest
    ctx.assert_query_value(
        "SELECT MIN(name) FROM Item",
        "MIN(name)",
        Value::Str("apple".to_string()),
    );

    ctx.commit();
}

#[test]
fn test_min_dates() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Events")
        .create_simple("id INTEGER, event_date DATE")
        .insert_values(
            "(1, '2024-03-15'), \
             (2, '2024-01-10'), \
             (3, '2024-12-25'), \
             (4, NULL), \
             (5, '2024-06-30')",
        );

    // MIN on dates returns earliest date (2024-01-10)
    ctx.assert_query_value(
        "SELECT MIN(event_date) FROM Events",
        "MIN(event_date)",
        Value::Date(chrono::NaiveDate::from_ymd_opt(2024, 1, 10).unwrap()),
    );

    ctx.commit();
}
