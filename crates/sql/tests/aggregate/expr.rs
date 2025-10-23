//! Aggregate expressions tests
//! Based on gluesql/test-suite/src/aggregate/expr.rs

use crate::common::{TableBuilder, setup_test};
use proven_value::Value;

#[test]
fn test_between_with_aggregates() {
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

    // SUM(quantity) = 47, MIN(quantity) = 0, MAX(quantity) = 25
    // 47 is NOT between 0 and 25, so result is false
    ctx.assert_query_value(
        "SELECT SUM(quantity) BETWEEN MIN(quantity) AND MAX(quantity) AS test FROM Item",
        "test",
        Value::Bool(false),
    );

    ctx.commit();
}

#[test]
fn test_case_comparing_aggregates() {
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

    // SUM(quantity) = 47, MIN(quantity) = 0
    // Since SUM != MIN, return COUNT(id) which is 5
    ctx.assert_query_value(
        "SELECT CASE SUM(quantity) WHEN MIN(quantity) THEN MAX(id) ELSE COUNT(id) END AS test FROM Item",
        "test",
        Value::I64(5),
    );

    ctx.commit();
}

#[test]
fn test_case_when_with_aggregate_condition() {
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

    // SUM(quantity) = 47, which is > 30
    // So return MAX(id) which is 5
    ctx.assert_query_value(
        "SELECT CASE WHEN SUM(quantity) > 30 THEN MAX(id) ELSE MIN(id) END AS test FROM Item",
        "test",
        Value::I32(5),
    );

    ctx.commit();
}
