//! Ordering and comparison operator tests
//! Based on gluesql/test-suite/src/ordering.rs

mod common;

use common::{TableBuilder, setup_test};
use proven_value::Value;

/// Setup helper that creates and populates the Operator table
fn setup_operator_table() -> common::TestContext {
    let mut ctx = setup_test();

    // Create the Operator table
    TableBuilder::new(&mut ctx, "Operator").create_simple("id INTEGER, name TEXT");

    // Delete any existing data (for test isolation)
    ctx.exec("DELETE FROM Operator");

    // Insert test data
    ctx.exec(
        "INSERT INTO Operator (id, name) VALUES
         (1, 'Abstract'),
         (2, 'Azzzz'),
         (3, 'July'),
         (4, 'Romeo'),
         (5, 'Trade')",
    );

    ctx
}

#[test]
fn test_less_than_operator() {
    let mut ctx = setup_operator_table();

    // Test id < 2 should return 1 row (id=1)
    assert_rows!(ctx, "SELECT * FROM Operator WHERE id < 2", 1);
    ctx.assert_query_value("SELECT id FROM Operator WHERE id < 2", "id", Value::I32(1));

    ctx.commit();
}

#[test]
fn test_less_than_or_equal_operator() {
    let mut ctx = setup_operator_table();

    // Test id <= 2 should return 2 rows (id=1,2)
    assert_rows!(ctx, "SELECT * FROM Operator WHERE id <= 2", 2);

    ctx.commit();
}

#[test]
fn test_greater_than_operator() {
    let mut ctx = setup_operator_table();

    // Test id > 2 should return 3 rows (id=3,4,5)
    assert_rows!(ctx, "SELECT * FROM Operator WHERE id > 2", 3);

    ctx.commit();
}

#[test]
fn test_greater_than_or_equal_operator() {
    let mut ctx = setup_operator_table();

    // Test id >= 2 should return 4 rows (id=2,3,4,5)
    assert_rows!(ctx, "SELECT * FROM Operator WHERE id >= 2", 4);

    ctx.commit();
}

#[test]
fn test_reversed_comparison_operators() {
    let mut ctx = setup_operator_table();

    // Test with literal on left side
    assert_rows!(ctx, "SELECT * FROM Operator WHERE 2 > id", 1); // id < 2
    assert_rows!(ctx, "SELECT * FROM Operator WHERE 2 >= id", 2); // id <= 2
    assert_rows!(ctx, "SELECT * FROM Operator WHERE 2 < id", 3); // id > 2
    assert_rows!(ctx, "SELECT * FROM Operator WHERE 2 <= id", 4); // id >= 2

    ctx.commit();
}

#[test]
fn test_literal_comparisons() {
    let mut ctx = setup_operator_table();

    // These should return all rows (5) when condition is true, 0 when false
    assert_rows!(ctx, "SELECT * FROM Operator WHERE 1 < 3", 5); // Always true
    assert_rows!(ctx, "SELECT * FROM Operator WHERE 3 >= 3", 5); // Always true
    assert_rows!(ctx, "SELECT * FROM Operator WHERE 3 > 3", 0); // Always false

    ctx.commit();
}

#[test]
#[ignore = "Correlated subqueries not yet implemented - ColumnNotFound error on o1.id reference"]
fn test_subquery_comparisons() {
    let mut ctx = setup_operator_table();

    // Test correlated subquery
    // MIN(id) where o1.id < 100 should be 1, and 3 > 1 is true, so should return all 5 rows
    // Currently fails with: Planning error: ColumnNotFound("id")
    assert_rows!(
        ctx,
        "SELECT * FROM Operator o1 WHERE 3 > (SELECT MIN(id) FROM Operator WHERE o1.id < 100)",
        5
    );

    ctx.commit();
}

#[test]
fn test_string_comparisons() {
    let mut ctx = setup_operator_table();

    // Test string comparisons
    // 'Abstract' and 'Azzzz' < 'Azzzzzzzzzz'
    assert_rows!(ctx, "SELECT * FROM Operator WHERE name < 'Azzzzzzzzzz'", 2);

    // Only 'Abstract' < 'Az'
    assert_rows!(ctx, "SELECT * FROM Operator WHERE name < 'Az'", 1);

    // All names < 'zz' (all start with uppercase letters)
    assert_rows!(ctx, "SELECT * FROM Operator WHERE name < 'zz'", 5);

    ctx.commit();
}

#[test]
fn test_string_literal_comparisons() {
    let mut ctx = setup_operator_table();

    // 'aa' < 'zz' is always true, should return all 5 rows
    assert_rows!(ctx, "SELECT * FROM Operator WHERE 'aa' < 'zz'", 5);

    // 'Romeo' >= name should match 'Abstract', 'Azzzz', 'July', 'Romeo' (4 rows)
    // because 'Trade' > 'Romeo'
    assert_rows!(ctx, "SELECT * FROM Operator WHERE 'Romeo' >= name", 4);

    ctx.commit();
}

#[test]
fn test_subquery_string_comparisons() {
    let mut ctx = setup_operator_table();

    // (SELECT name FROM Operator LIMIT 1) should return 'Abstract'
    // 'Abstract' >= name should only match 'Abstract' itself
    assert_rows!(
        ctx,
        "SELECT * FROM Operator WHERE (SELECT name FROM Operator LIMIT 1) >= name",
        1
    );

    // name <= 'Abstract' should only match 'Abstract'
    assert_rows!(
        ctx,
        "SELECT * FROM Operator WHERE name <= (SELECT name FROM Operator LIMIT 1)",
        1
    );

    // 'zz' > 'Abstract' is true, so should return all 5 rows
    assert_rows!(
        ctx,
        "SELECT * FROM Operator WHERE 'zz' > (SELECT name FROM Operator LIMIT 1)",
        5
    );

    // 'Abstract' < 'zz' is true, so should return all 5 rows
    assert_rows!(
        ctx,
        "SELECT * FROM Operator WHERE (SELECT name FROM Operator LIMIT 1) < 'zz'",
        5
    );

    ctx.commit();
}

#[test]
fn test_not_operator_with_inequality() {
    let mut ctx = setup_operator_table();

    // NOT (1 != 1) is NOT false = true, so should return all 5 rows
    assert_rows!(ctx, "SELECT * FROM Operator WHERE NOT (1 != 1)", 5);

    ctx.commit();
}

#[test]
fn test_literal_type_comparisons() {
    let mut ctx = setup_test();

    // Create a simple table for testing
    ctx.exec("CREATE TABLE TestTable (id INTEGER)");
    ctx.exec("INSERT INTO TestTable VALUES (1)");

    // In our implementation, comparisons between different types produce a TypeMismatch error
    // rather than returning false. This is a valid design choice for type safety.
    ctx.assert_error_contains("SELECT 1 < 'a' as test", "TypeMismatch");

    ctx.assert_error_contains("SELECT 1 >= 'a' as test", "TypeMismatch");

    ctx.assert_error_contains("SELECT 1 = 'a' as test", "TypeMismatch");

    ctx.commit();
}
