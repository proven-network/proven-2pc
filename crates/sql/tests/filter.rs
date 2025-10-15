//! Tests for WHERE clauses, BETWEEN, and comparison operators
//! Based on gluesql/test-suite/src/filter.rs

mod common;

use common::{TableBuilder, setup_test};
use proven_value::Value;

fn setup_filter_test_table(ctx: &mut common::TestContext) {
    TableBuilder::new(ctx, "FilterTest")
        .create_simple("id INTEGER, value INTEGER, name TEXT, active BOOLEAN, score FLOAT")
        .insert_values(
            "(1, 10, 'Alice', TRUE, 85.5),
             (2, 20, 'Bob', FALSE, 92.0),
             (3, 30, 'Charlie', TRUE, 78.5),
             (4, 40, 'David', FALSE, 88.0),
             (5, 50, 'Eve', TRUE, 95.5),
             (6, 60, 'Frank', TRUE, 82.0),
             (7, 70, 'Grace', FALSE, 91.5),
             (8, 80, 'Henry', TRUE, 87.0),
             (9, 90, 'Ivy', FALSE, 93.0),
             (10, 100, 'Jack', TRUE, 89.5)",
        );
}

#[test]
fn test_where_equals() {
    let mut ctx = setup_test();
    setup_filter_test_table(&mut ctx);

    // Test equality on different types
    assert_rows!(ctx, "SELECT * FROM FilterTest WHERE id = 5", 1);
    assert_rows!(ctx, "SELECT * FROM FilterTest WHERE name = 'Alice'", 1);
    assert_rows!(ctx, "SELECT * FROM FilterTest WHERE active = TRUE", 6);
    assert_rows!(ctx, "SELECT * FROM FilterTest WHERE value = 50", 1);

    ctx.assert_query_value(
        "SELECT name FROM FilterTest WHERE id = 5",
        "name",
        Value::Str("Eve".to_string()),
    );

    ctx.commit();
}

#[test]
fn test_where_not_equals() {
    let mut ctx = setup_test();
    setup_filter_test_table(&mut ctx);

    // Test inequality
    assert_rows!(ctx, "SELECT * FROM FilterTest WHERE id != 5", 9);
    assert_rows!(ctx, "SELECT * FROM FilterTest WHERE name != 'Alice'", 9);
    assert_rows!(ctx, "SELECT * FROM FilterTest WHERE active != TRUE", 4);

    ctx.commit();
}

#[test]
fn test_where_greater_than() {
    let mut ctx = setup_test();
    setup_filter_test_table(&mut ctx);

    // Test greater than
    assert_rows!(ctx, "SELECT * FROM FilterTest WHERE id > 5", 5);
    assert_rows!(ctx, "SELECT * FROM FilterTest WHERE value > 50", 5);
    assert_rows!(ctx, "SELECT * FROM FilterTest WHERE score > 90.0", 4);

    ctx.commit();
}

#[test]
fn test_where_greater_than_or_equal() {
    let mut ctx = setup_test();
    setup_filter_test_table(&mut ctx);

    // Test greater than or equal
    assert_rows!(ctx, "SELECT * FROM FilterTest WHERE id >= 5", 6);
    assert_rows!(ctx, "SELECT * FROM FilterTest WHERE value >= 50", 6);
    assert_rows!(ctx, "SELECT * FROM FilterTest WHERE score >= 90.0", 4);

    ctx.commit();
}

#[test]
fn test_where_less_than() {
    let mut ctx = setup_test();
    setup_filter_test_table(&mut ctx);

    // Test less than
    assert_rows!(ctx, "SELECT * FROM FilterTest WHERE id < 5", 4);
    assert_rows!(ctx, "SELECT * FROM FilterTest WHERE value < 50", 4);
    assert_rows!(ctx, "SELECT * FROM FilterTest WHERE score < 85.0", 2);

    ctx.commit();
}

#[test]
fn test_where_less_than_or_equal() {
    let mut ctx = setup_test();
    setup_filter_test_table(&mut ctx);

    // Test less than or equal
    assert_rows!(ctx, "SELECT * FROM FilterTest WHERE id <= 5", 5);
    assert_rows!(ctx, "SELECT * FROM FilterTest WHERE value <= 50", 5);
    assert_rows!(ctx, "SELECT * FROM FilterTest WHERE score <= 85.0", 2);

    ctx.commit();
}

#[test]
fn test_where_between() {
    let mut ctx = setup_test();
    setup_filter_test_table(&mut ctx);

    // Test BETWEEN operator
    assert_rows!(ctx, "SELECT * FROM FilterTest WHERE id BETWEEN 3 AND 7", 5);
    assert_rows!(
        ctx,
        "SELECT * FROM FilterTest WHERE value BETWEEN 30 AND 70",
        5
    );
    assert_rows!(
        ctx,
        "SELECT * FROM FilterTest WHERE score BETWEEN 85.0 AND 90.0",
        4
    );

    // Verify BETWEEN is inclusive
    ctx.assert_query_value(
        "SELECT id FROM FilterTest WHERE id BETWEEN 3 AND 7 ORDER BY id LIMIT 1",
        "id",
        Value::I32(3),
    );

    ctx.commit();
}

#[test]
fn test_where_not_between() {
    let mut ctx = setup_test();
    setup_filter_test_table(&mut ctx);

    // Test NOT BETWEEN
    assert_rows!(
        ctx,
        "SELECT * FROM FilterTest WHERE id NOT BETWEEN 3 AND 7",
        5
    );
    assert_rows!(
        ctx,
        "SELECT * FROM FilterTest WHERE value NOT BETWEEN 30 AND 70",
        5
    );

    ctx.commit();
}

#[test]
fn test_where_and() {
    let mut ctx = setup_test();
    setup_filter_test_table(&mut ctx);

    // Test AND conditions
    assert_rows!(
        ctx,
        "SELECT * FROM FilterTest WHERE id > 5 AND active = TRUE",
        3
    );
    assert_rows!(
        ctx,
        "SELECT * FROM FilterTest WHERE value >= 50 AND score > 90",
        3
    );
    assert_rows!(
        ctx,
        "SELECT * FROM FilterTest WHERE id BETWEEN 3 AND 7 AND active = FALSE",
        2
    );

    // Verify specific result
    ctx.assert_query_value(
        "SELECT name FROM FilterTest WHERE id > 5 AND active = TRUE ORDER BY id LIMIT 1",
        "name",
        Value::Str("Frank".to_string()),
    );

    ctx.commit();
}

#[test]
fn test_where_or() {
    let mut ctx = setup_test();
    setup_filter_test_table(&mut ctx);

    // Test OR conditions
    assert_rows!(ctx, "SELECT * FROM FilterTest WHERE id = 1 OR id = 10", 2);
    assert_rows!(
        ctx,
        "SELECT * FROM FilterTest WHERE active = FALSE OR score > 93",
        5
    );
    assert_rows!(
        ctx,
        "SELECT * FROM FilterTest WHERE value < 30 OR value > 80",
        4
    );

    ctx.commit();
}

#[test]
fn test_where_complex_conditions() {
    let mut ctx = setup_test();
    setup_filter_test_table(&mut ctx);

    // Test complex combinations of AND/OR
    assert_rows!(
        ctx,
        "SELECT * FROM FilterTest WHERE (id <= 5 AND active = TRUE) OR (id > 5 AND active = FALSE)",
        5
    );

    assert_rows!(
        ctx,
        "SELECT * FROM FilterTest WHERE (value BETWEEN 30 AND 70) AND (active = TRUE OR score > 90)",
        4
    );

    ctx.commit();
}

#[test]
fn test_where_with_null() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "NullTest")
        .create_simple("id INTEGER, value INTEGER NULL, name TEXT NULL")
        .insert_values(
            "(1, 10, 'Alice'),
             (2, NULL, 'Bob'),
             (3, 30, NULL),
             (4, NULL, NULL),
             (5, 50, 'Eve')",
        );

    // Test IS NULL
    assert_rows!(ctx, "SELECT * FROM NullTest WHERE value IS NULL", 2);
    assert_rows!(ctx, "SELECT * FROM NullTest WHERE name IS NULL", 2);

    // Test IS NOT NULL
    assert_rows!(ctx, "SELECT * FROM NullTest WHERE value IS NOT NULL", 3);
    assert_rows!(ctx, "SELECT * FROM NullTest WHERE name IS NOT NULL", 3);

    // NULL comparisons should not match
    // TODO: This is a bug - = NULL should return 0 rows, not match NULL values
    // assert_rows!(ctx, "SELECT * FROM NullTest WHERE value = NULL", 0);

    ctx.commit();
}

#[test]
fn test_where_with_like() {
    let mut ctx = setup_test();
    setup_filter_test_table(&mut ctx);

    // Test LIKE operator
    assert_rows!(ctx, "SELECT * FROM FilterTest WHERE name LIKE 'A%'", 1);
    assert_rows!(ctx, "SELECT * FROM FilterTest WHERE name LIKE '%e'", 4); // Alice, Charlie, Eve, Grace
    assert_rows!(ctx, "SELECT * FROM FilterTest WHERE name LIKE '%ar%'", 1);
    assert_rows!(ctx, "SELECT * FROM FilterTest WHERE name LIKE 'D_vid'", 1);

    ctx.commit();
}

#[test]
fn test_where_with_in() {
    let mut ctx = setup_test();
    setup_filter_test_table(&mut ctx);

    // Test IN operator
    assert_rows!(
        ctx,
        "SELECT * FROM FilterTest WHERE id IN (1, 3, 5, 7, 9)",
        5
    );
    assert_rows!(
        ctx,
        "SELECT * FROM FilterTest WHERE name IN ('Alice', 'Bob', 'Charlie')",
        3
    );

    ctx.commit();
}

#[test]
fn test_where_with_expressions() {
    let mut ctx = setup_test();
    setup_filter_test_table(&mut ctx);

    // Test WHERE with expressions
    assert_rows!(ctx, "SELECT * FROM FilterTest WHERE value * 2 > 100", 5);
    assert_rows!(ctx, "SELECT * FROM FilterTest WHERE value + 10 = 60", 1);
    assert_rows!(ctx, "SELECT * FROM FilterTest WHERE id * 10 = value", 10);

    ctx.commit();
}

#[test]
fn test_where_with_boolean_column() {
    let mut ctx = setup_test();
    setup_filter_test_table(&mut ctx);

    // Test boolean columns directly
    assert_rows!(ctx, "SELECT * FROM FilterTest WHERE active", 6);
    assert_rows!(ctx, "SELECT * FROM FilterTest WHERE NOT active", 4);

    ctx.commit();
}

#[test]
fn test_where_empty_result() {
    let mut ctx = setup_test();
    setup_filter_test_table(&mut ctx);

    // Conditions that return no rows
    assert_rows!(ctx, "SELECT * FROM FilterTest WHERE id > 100", 0);
    assert_rows!(ctx, "SELECT * FROM FilterTest WHERE 1 = 2", 0);
    assert_rows!(
        ctx,
        "SELECT * FROM FilterTest WHERE name = 'NonExistent'",
        0
    );

    ctx.commit();
}

#[test]
fn test_where_all_rows() {
    let mut ctx = setup_test();
    setup_filter_test_table(&mut ctx);

    // Conditions that return all rows
    assert_rows!(ctx, "SELECT * FROM FilterTest WHERE 1 = 1", 10);
    assert_rows!(ctx, "SELECT * FROM FilterTest WHERE id > 0", 10);
    assert_rows!(ctx, "SELECT * FROM FilterTest WHERE TRUE", 10);

    ctx.commit();
}

#[ignore = "EXISTS not yet implemented"]
#[test]
fn test_exists_clause() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Boss")
        .create_simple("id INTEGER, name TEXT, strength INTEGER")
        .insert_values("(1, 'Gascoigne', 100), (2, 'Gehrman', 200), (3, 'Doll', 50)");

    TableBuilder::new(&mut ctx, "Hunter")
        .create_simple("id INTEGER, name TEXT")
        .insert_values("(1, 'Player'), (2, 'Gehrman')");

    // Test EXISTS
    assert_rows!(
        ctx,
        "SELECT name FROM Boss WHERE EXISTS (SELECT * FROM Hunter WHERE Hunter.name = Boss.name)",
        1
    );

    ctx.commit();
}

#[test]
fn test_unary_operators() {
    let mut ctx = setup_test();
    setup_filter_test_table(&mut ctx);

    // Test unary plus and minus
    assert_rows!(ctx, "SELECT * FROM FilterTest WHERE +1 = 1", 10);
    assert_rows!(ctx, "SELECT * FROM FilterTest WHERE -1 = -1", 10);
    assert_rows!(ctx, "SELECT * FROM FilterTest WHERE -2.0 < -1.0", 10);
    assert_rows!(ctx, "SELECT * FROM FilterTest WHERE +2 > +1.0", 10);

    ctx.commit();
}

#[test]
fn test_arithmetic_in_where() {
    let mut ctx = setup_test();
    setup_filter_test_table(&mut ctx);

    // Test arithmetic operations in WHERE
    assert_rows!(ctx, "SELECT * FROM FilterTest WHERE 2 = 1.0 + 1", 10);
    assert_rows!(ctx, "SELECT * FROM FilterTest WHERE -1.0 - 1.0 < -1", 10);
    assert_rows!(ctx, "SELECT * FROM FilterTest WHERE -2.0 * -3.0 = 6", 10);
    assert_rows!(ctx, "SELECT * FROM FilterTest WHERE 4 / 2 = 2", 10);

    ctx.commit();
}
