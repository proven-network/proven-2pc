//! AVG aggregate function tests
//! Based on gluesql/test-suite/src/aggregate/avg.rs

mod common;

use common::{TableBuilder, setup_test};
use proven_value::Value;

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
    ctx.assert_query_value(
        "SELECT AVG(id) FROM Item",
        "AVG(id)",
        Value::Decimal(rust_decimal::Decimal::from(3)),
    );

    // AVG(quantity) = (10+0+9+3+25)/5 = 9.4
    ctx.assert_query_value(
        "SELECT AVG(quantity) FROM Item",
        "AVG(quantity)",
        Value::Decimal(std::str::FromStr::from_str("9.40").unwrap()),
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
    ctx.assert_query_value(
        "SELECT AVG(age) FROM Item",
        "AVG(age)",
        Value::Decimal(std::str::FromStr::from_str("34.666666666666666666666666667").unwrap()),
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
    assert_eq!(
        results[0].get("AVG(id)").unwrap(),
        &Value::Decimal(std::str::FromStr::from_str("3").unwrap())
    );
    assert_eq!(
        results[0].get("AVG(quantity)").unwrap(),
        &Value::Decimal(std::str::FromStr::from_str("9.40").unwrap())
    );

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
    ctx.assert_query_value(
        "SELECT AVG(DISTINCT value) FROM Item",
        "AVG(DISTINCT value)",
        Value::Decimal(std::str::FromStr::from_str("20").unwrap()),
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
    ctx.assert_query_value(
        "SELECT AVG(DISTINCT age) FROM Item",
        "AVG(DISTINCT age)",
        Value::Decimal(std::str::FromStr::from_str("34.666666666666666666666666667").unwrap()),
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
    ctx.assert_query_value("SELECT AVG(value) FROM Item", "AVG(value)", Value::Null);

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
    ctx.assert_query_value(
        "SELECT AVG(id + 1) FROM Item",
        "AVG(id + 1)",
        Value::Decimal(rust_decimal::Decimal::from(4)),
    );

    // AVG(quantity * 2) = (20+0+18+6+50)/5 = 18.8
    ctx.assert_query_value(
        "SELECT AVG(quantity * 2) FROM Item",
        "AVG(quantity * 2)",
        Value::Decimal(std::str::FromStr::from_str("18.80").unwrap()),
    );

    ctx.commit();
}
