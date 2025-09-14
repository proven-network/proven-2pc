//! Tests for MAX aggregate function
//! Based on gluesql/test-suite/src/aggregate/max.rs

mod common;

use common::{setup_test, TableBuilder};

#[test]
fn test_max_nullable_column() {
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

    // MAX ignores NULL values
    ctx.assert_query_contains("SELECT MAX(age) FROM Item", "MAX(age)", "I32(90)");

    ctx.commit();
}

#[test]
fn test_max_multiple_columns() {
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

    let results = ctx.query("SELECT MAX(id), MAX(quantity) FROM Item");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("MAX(id)").unwrap(), "I32(5)");
    assert_eq!(results[0].get("MAX(quantity)").unwrap(), "I32(25)");

    ctx.commit();
}

#[test]
fn test_max_with_expression() {
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

    // MAX(id - quantity) = MAX(-9, 2, -6, 1, -20) = 2
    ctx.assert_query_contains("SELECT MAX(id - quantity) FROM Item", "MAX(id - quantity)", "I32(2)");

    ctx.commit();
}

#[test]
#[ignore = "Multiple aggregates in expression not yet supported"]
fn test_max_in_complex_expression() {
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

    // SUM(quantity) = 47, MAX(quantity) = 25
    // SUM(quantity) * 2 + MAX(quantity) - 3 / 1 = 47*2 + 25 - 3 = 116
    let results = ctx.query("SELECT SUM(quantity) * 2 + MAX(quantity) - 3 / 1 FROM Item");
    assert_eq!(results.len(), 1);
    // Check the complex expression result
    for (key, value) in &results[0] {
        println!("Column: {}, Value: {}", key, value);
        // Should be 116 as I32
        assert!(value.contains("116"));
    }

    ctx.commit();
}

#[test]
fn test_max_distinct_values() {
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

    // MAX(DISTINCT value) should be same as MAX(value) = 30
    ctx.assert_query_contains("SELECT MAX(DISTINCT value) FROM Item", "MAX(DISTINCT value)", "I32(30)");

    ctx.commit();
}

#[test]
fn test_max_distinct_nullable_column() {
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

    // MAX(DISTINCT age) = MAX(11, 90, 3) = 90
    ctx.assert_query_contains("SELECT MAX(DISTINCT age) FROM Item", "MAX(DISTINCT age)", "I32(90)");

    ctx.commit();
}

#[test]
fn test_max_handles_nulls() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, value INTEGER")
        .insert_values(
            "(1, NULL), \
             (2, NULL), \
             (3, NULL)",
        );

    // MAX of all NULLs should return NULL
    ctx.assert_query_contains("SELECT MAX(value) FROM Item", "MAX(value)", "Null");

    ctx.commit();
}

#[test]
fn test_max_strings() {
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

    // MAX on strings returns lexicographically largest
    ctx.assert_query_contains("SELECT MAX(name) FROM Item", "MAX(name)", "Str(\"zebra\")");

    ctx.commit();
}

#[test]
#[ignore = "Date literal parsing not yet implemented"]
fn test_max_dates() {
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

    // MAX on dates returns latest date
    ctx.assert_query_contains("SELECT MAX(event_date) FROM Events", "MAX(event_date)", "Date(2024-12-25)");

    ctx.commit();
}