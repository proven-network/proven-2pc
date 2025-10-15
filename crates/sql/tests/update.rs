//! Tests for UPDATE statements functionality
//! Based on gluesql/test-suite/src/update.rs

mod common;

use common::{TableBuilder, TestContext, setup_test, setup_with_tables};
use proven_value::Value;

fn setup_test_tables(ctx: &mut TestContext) {
    // Use TableBuilder for cleaner setup
    TableBuilder::new(ctx, "TableA")
        .create_simple("id INTEGER, num INTEGER, num2 INTEGER, name TEXT")
        .insert_values(
            "(1, 2, 4, 'Hello'), (1, 9, 5, 'World'), (3, 4, 7, 'Great'), (4, 7, 10, 'Job')",
        );

    TableBuilder::new(ctx, "TableB")
        .create_simple("id INTEGER, num INTEGER, rank INTEGER")
        .insert_values("(1, 2, 1), (1, 9, 2), (3, 4, 3), (4, 7, 4)");
}

#[test]
fn test_update_all_rows() {
    let mut ctx = setup_test();
    setup_test_tables(&mut ctx);

    // Update all rows without WHERE clause
    ctx.exec("UPDATE TableA SET id = 2");

    // Verify all ids are now 2
    assert_rows!(ctx, "SELECT * FROM TableA", 4);

    // Check each row has id=2
    for i in 1..=4 {
        ctx.assert_query_value(
            &format!("SELECT id FROM TableA LIMIT 1 OFFSET {}", i - 1),
            "id",
            Value::I32(2),
        );
    }

    ctx.commit();
}

#[test]
fn test_update_with_where_clause() {
    let mut ctx = setup_test();
    setup_test_tables(&mut ctx);

    // Update specific rows with WHERE clause (use unique value to avoid conflicts)
    ctx.exec("UPDATE TableA SET id = 99 WHERE num = 9");

    // Verify only the matching row was updated
    assert_rows!(ctx, "SELECT * FROM TableA", 4);
    ctx.assert_query_value("SELECT id FROM TableA WHERE num = 9", "id", Value::I32(99));

    // Verify only one row has the new id
    assert_rows!(ctx, "SELECT * FROM TableA WHERE id = 99", 1);

    ctx.commit();
}

#[test]
fn test_update_with_function_in_set() {
    let mut ctx = setup_test();
    setup_test_tables(&mut ctx);

    // Update using SUBSTR function
    ctx.exec("UPDATE TableA SET name = SUBSTR('John', 1) WHERE num = 9");

    // Verify the name was updated
    ctx.assert_query_value(
        "SELECT name FROM TableA WHERE num = 9",
        "name",
        Value::Str("John".to_string()),
    );

    ctx.commit();
}

#[test]
fn test_update_with_subquery_in_set() {
    let mut ctx = setup_test();
    setup_test_tables(&mut ctx);

    // Update using a subquery in SET clause
    ctx.exec(
        "UPDATE TableA SET num2 = (SELECT num FROM TableA WHERE num = 9 LIMIT 1) WHERE num = 9",
    );

    // Verify num2 was updated to 9 for the row with num=9
    ctx.assert_query_value(
        "SELECT num2 FROM TableA WHERE num = 9",
        "num2",
        Value::I32(9),
    );

    ctx.commit();
}

#[test]
fn test_update_with_correlated_subquery() {
    let mut ctx = setup_test();
    setup_test_tables(&mut ctx);

    // Update using a correlated subquery
    ctx.exec(
        "UPDATE TableA SET num2 = (SELECT rank FROM TableB WHERE num = TableA.num) WHERE num = 7",
    );

    // Verify num2 was updated to 4 (rank from TableB where num=7)
    ctx.assert_query_value(
        "SELECT num2 FROM TableA WHERE num = 7",
        "num2",
        Value::I32(4),
    );

    ctx.commit();
}

#[test]
fn test_update_with_complex_where_subquery() {
    let mut ctx = setup_test();
    setup_test_tables(&mut ctx);

    // Update with subqueries in both SET and WHERE clauses
    ctx.exec("UPDATE TableA SET num2 = (SELECT rank FROM TableB WHERE num = TableA.num) WHERE num = (SELECT MIN(num) FROM TableA)");

    // Verify num2 was updated for the row with minimum num value (2)
    ctx.assert_query_value(
        "SELECT num2 FROM TableA WHERE num = 2",
        "num2",
        Value::I32(1),
    );

    ctx.commit();
}

#[test]
fn test_update_join_not_supported_error() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "ErrTestTable")
        .create_simple("id INTEGER")
        .insert_values("(1), (9)");

    // UPDATE with JOIN should fail
    assert_error!(
        ctx,
        "UPDATE TableA INNER JOIN ErrTestTable ON 1 = 1 SET id = 1"
    );

    ctx.abort();
}

#[test]
fn test_update_subquery_table_factor_error() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "ErrTestTable").create_simple("id INTEGER");

    // UPDATE with subquery as table should fail
    assert_error!(ctx, "UPDATE (SELECT * FROM ErrTestTable) SET id = 1");

    ctx.abort();
}

#[test]
fn test_update_compound_identifier_error() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "ErrTestTable")
        .create_simple("id INTEGER")
        .insert_values("(1)");

    // UPDATE with compound identifier should fail
    assert_error!(
        ctx,
        "UPDATE ErrTestTable SET ErrTestTable.id = 1 WHERE id = 1"
    );

    ctx.abort();
}

#[test]
fn test_update_table_not_found_error() {
    let mut ctx = setup_test();

    // UPDATE on non-existent table should fail
    assert_error!(ctx, "UPDATE Nothing SET a = 1");

    ctx.abort();
}

#[test]
fn test_update_column_not_found_error() {
    let mut ctx = setup_test();
    setup_test_tables(&mut ctx);

    // UPDATE with non-existent column should fail
    assert_error!(ctx, "UPDATE TableA SET Foo = 1");

    ctx.abort();
}

#[test]
fn test_update_multiple_columns() {
    let mut ctx = setup_test();
    setup_test_tables(&mut ctx);

    // Update multiple columns at once
    ctx.exec("UPDATE TableA SET id = 10, name = 'Updated' WHERE num = 2");

    // Verify both columns were updated
    ctx.assert_query_value("SELECT id FROM TableA WHERE num = 2", "id", Value::I32(10));
    ctx.assert_query_value(
        "SELECT name FROM TableA WHERE num = 2",
        "name",
        Value::Str("Updated".to_string()),
    );

    ctx.commit();
}

#[test]
fn test_update_with_arithmetic_expression() {
    let mut ctx = setup_test();
    setup_test_tables(&mut ctx);

    // Update using arithmetic expression
    ctx.exec("UPDATE TableA SET num2 = num * 2 WHERE id = 1");

    // Verify num2 was calculated correctly
    assert_rows!(ctx, "SELECT * FROM TableA WHERE id = 1", 2);

    // Check the calculations
    ctx.assert_query_value(
        "SELECT num2 FROM TableA WHERE id = 1 AND num = 2",
        "num2",
        Value::I32(4),
    );
    ctx.assert_query_value(
        "SELECT num2 FROM TableA WHERE id = 1 AND num = 9",
        "num2",
        Value::I32(18),
    );

    ctx.commit();
}

#[test]
fn test_update_with_predefined_tables() {
    // Use the helper that sets up common test tables
    let mut ctx = setup_with_tables();

    // Update the predefined users table
    ctx.exec("UPDATE users SET age = age + 1 WHERE name = 'Alice'");

    // Verify the update
    ctx.assert_query_value(
        "SELECT age FROM users WHERE name = 'Alice'",
        "age",
        Value::I32(26),
    );

    // Other users should be unchanged
    ctx.assert_query_value(
        "SELECT age FROM users WHERE name = 'Bob'",
        "age",
        Value::I32(30),
    );

    ctx.commit();
}

#[test]
fn test_update_using_standard_table() {
    let mut ctx = setup_test();

    // Use the standard table schema
    TableBuilder::new(&mut ctx, "StandardTest")
        .create_standard() // id INTEGER PRIMARY KEY, name TEXT, value INTEGER
        .insert_rows(5); // Insert 5 rows with generated data

    // Update using the standard schema
    ctx.exec("UPDATE StandardTest SET value = value * 2 WHERE id <= 3");

    // Verify updates
    ctx.assert_query_value(
        "SELECT value FROM StandardTest WHERE id = 1",
        "value",
        Value::I32(20),
    );
    ctx.assert_query_value(
        "SELECT value FROM StandardTest WHERE id = 2",
        "value",
        Value::I32(40),
    );
    ctx.assert_query_value(
        "SELECT value FROM StandardTest WHERE id = 3",
        "value",
        Value::I32(60),
    );

    // Verify non-updated rows
    ctx.assert_query_value(
        "SELECT value FROM StandardTest WHERE id = 4",
        "value",
        Value::I32(40),
    );
    ctx.assert_query_value(
        "SELECT value FROM StandardTest WHERE id = 5",
        "value",
        Value::I32(50),
    );

    ctx.commit();
}
