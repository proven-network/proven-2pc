//! AVG aggregate function tests
//! Based on gluesql/test-suite/src/aggregate/avg.rs

mod common;

use common::{TableBuilder, setup_test};

#[test]
fn test_avg_basic() {
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

    // AVG(id) = (1+2+3+4+5)/5 = 3
    ctx.assert_query_contains("SELECT AVG(id) FROM Item", "AVG(id)", "Decimal(3)");

    // AVG(quantity) = (10+0+9+3+25)/5 = 9.4
    ctx.assert_query_contains(
        "SELECT AVG(quantity) FROM Item",
        "AVG(quantity)",
        "Decimal(9.40)",
    );

    ctx.commit();
}

#[test]
fn test_avg_with_null_values() {
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

    // AVG(age) = (11+90+3)/3 = 34.666...
    ctx.assert_query_contains(
        "SELECT AVG(age) FROM Item",
        "AVG(age)",
        "Decimal(34.666666666666666666666666667)",
    );

    ctx.commit();
}

#[test]
fn test_avg_multiple_columns() {
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

    let results = ctx.query("SELECT AVG(id), AVG(quantity) FROM Item");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("AVG(id)").unwrap(), "Decimal(3)");
    assert_eq!(results[0].get("AVG(quantity)").unwrap(), "Decimal(9.40)");

    ctx.commit();
}

#[test]
fn test_avg_distinct() {
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

    // AVG(DISTINCT value) = (10+20+30)/3 = 20
    ctx.assert_query_contains(
        "SELECT AVG(DISTINCT value) FROM Item",
        "AVG(DISTINCT value)",
        "Decimal(20)",
    );

    ctx.commit();
}

#[test]
fn test_avg_distinct_with_nulls() {
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

    // AVG(DISTINCT age) = (11+90+3)/3 = 34.666...
    ctx.assert_query_contains(
        "SELECT AVG(DISTINCT age) FROM Item",
        "AVG(DISTINCT age)",
        "Decimal(34.666666666666666666666666667)",
    );

    ctx.commit();
}

#[test]
fn test_avg_all_nulls() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, value INTEGER")
        .insert_values(
            "(1, NULL), \
             (2, NULL), \
             (3, NULL)",
        );

    // AVG of all NULLs should return NULL
    ctx.assert_query_contains("SELECT AVG(value) FROM Item", "AVG(value)", "Null");

    ctx.commit();
}

#[test]
fn test_avg_with_expression() {
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

    // AVG(id + 1) = (2+3+4+5+6)/5 = 4
    ctx.assert_query_contains("SELECT AVG(id + 1) FROM Item", "AVG(id + 1)", "Decimal(4)");

    // AVG(quantity * 2) = (20+0+18+6+50)/5 = 18.8
    ctx.assert_query_contains(
        "SELECT AVG(quantity * 2) FROM Item",
        "AVG(quantity * 2)",
        "Decimal(18.80)",
    );

    ctx.commit();
}
