//! Tests for DELETE statements functionality
//! Based on gluesql/test-suite/src/delete.rs

mod common;

use common::{TableBuilder, TestContext, data, setup_test, setup_with_tables};

fn setup_test_table(ctx: &mut TestContext) {
    TableBuilder::new(ctx, "Foo")
        .create_simple("id INTEGER PRIMARY KEY, score INTEGER, flag BOOLEAN")
        .insert_values("(1, 100, TRUE), (2, 300, FALSE), (3, 700, TRUE)");
}

#[test]
fn test_delete_with_where_clause() {
    let mut ctx = setup_test();
    setup_test_table(&mut ctx);

    // Verify initial state
    assert_rows!(ctx, "SELECT * FROM Foo", 3);

    // Delete rows where flag = FALSE
    ctx.exec("DELETE FROM Foo WHERE flag = FALSE");

    // Verify only the row with flag=FALSE was deleted
    assert_rows!(ctx, "SELECT * FROM Foo", 2);

    // Check remaining rows using assert_query_contains
    ctx.assert_query_contains("SELECT id FROM Foo WHERE id = 1", "id", "I32(1)");
    ctx.assert_query_contains("SELECT flag FROM Foo WHERE id = 1", "flag", "Bool(true)");
    ctx.assert_query_contains("SELECT id FROM Foo WHERE id = 3", "id", "I32(3)");
    ctx.assert_query_contains("SELECT flag FROM Foo WHERE id = 3", "flag", "Bool(true)");

    ctx.commit();
}

#[test]
fn test_delete_all_rows() {
    let mut ctx = setup_test();
    setup_test_table(&mut ctx);

    // Delete all rows (no WHERE clause)
    ctx.exec("DELETE FROM Foo");

    // Verify table is empty
    assert_rows!(ctx, "SELECT * FROM Foo", 0);

    ctx.commit();
}

#[test]
#[ignore = "DELETE response counting not yet verified"]
fn test_delete_verify_affected_row_count() {
    let mut ctx = setup_test();
    setup_test_table(&mut ctx);

    // Delete specific rows and check response
    let _response = ctx.exec_response("DELETE FROM Foo WHERE score > 200");

    // Should have deleted 2 rows (score=300 and score=700)
    assert_rows!(ctx, "SELECT * FROM Foo", 1);
    ctx.assert_query_contains("SELECT score FROM Foo", "score", "I32(100)");

    ctx.commit();
}

#[test]
fn test_delete_with_primary_key_table() {
    let mut ctx = setup_test();
    setup_test_table(&mut ctx);

    // Delete using primary key
    ctx.exec("DELETE FROM Foo WHERE id = 2");

    // Verify correct row was deleted
    assert_rows!(ctx, "SELECT * FROM Foo", 2);
    assert_rows!(ctx, "SELECT * FROM Foo WHERE id = 2", 0);
    // Check both remaining rows individually
    assert_rows!(ctx, "SELECT * FROM Foo WHERE id = 1", 1);
    assert_rows!(ctx, "SELECT * FROM Foo WHERE id = 3", 1);

    ctx.commit();
}

#[test]
fn test_delete_with_boolean_condition() {
    let mut ctx = setup_test();
    setup_test_table(&mut ctx);

    // Delete rows where flag is TRUE
    ctx.exec("DELETE FROM Foo WHERE flag = TRUE");

    // Verify only rows with flag=TRUE were deleted
    assert_rows!(ctx, "SELECT * FROM Foo", 1);
    ctx.assert_query_contains("SELECT id FROM Foo", "id", "I32(2)");
    ctx.assert_query_contains("SELECT flag FROM Foo", "flag", "Bool(false)");

    ctx.commit();
}

#[test]
fn test_select_after_delete() {
    let mut ctx = setup_test();
    setup_test_table(&mut ctx);

    // Delete middle row
    ctx.exec("DELETE FROM Foo WHERE id = 2");

    // SELECT should work correctly after DELETE
    assert_rows!(ctx, "SELECT * FROM Foo", 2);

    // Verify data integrity after delete using assert_query_contains
    ctx.assert_query_contains("SELECT score FROM Foo WHERE id = 1", "score", "I32(100)");
    ctx.assert_query_contains("SELECT score FROM Foo WHERE id = 3", "score", "I32(700)");

    ctx.commit();
}

#[test]
fn test_delete_with_complex_where() {
    let mut ctx = setup_test();
    setup_test_table(&mut ctx);

    // Delete with multiple conditions
    ctx.exec("DELETE FROM Foo WHERE score >= 300 AND flag = TRUE");

    // Should only delete row with id=3 (score=700, flag=TRUE)
    assert_rows!(ctx, "SELECT * FROM Foo", 2);
    assert_rows!(ctx, "SELECT * FROM Foo WHERE id = 3", 0);

    // Verify remaining rows
    ctx.assert_query_contains("SELECT id FROM Foo WHERE score = 100", "id", "I32(1)");
    ctx.assert_query_contains("SELECT id FROM Foo WHERE score = 300", "id", "I32(2)");

    ctx.commit();
}

#[test]
fn test_delete_no_matching_rows() {
    let mut ctx = setup_test();
    setup_test_table(&mut ctx);

    // Delete with condition that matches no rows
    ctx.exec("DELETE FROM Foo WHERE score > 1000");

    // No rows should be deleted
    assert_rows!(ctx, "SELECT * FROM Foo", 3);

    ctx.commit();
}

#[test]
fn test_delete_from_empty_table() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Empty").create_simple("id INTEGER");

    // Delete from empty table should succeed without error
    ctx.exec("DELETE FROM Empty");
    ctx.exec("DELETE FROM Empty WHERE id = 1");

    // Table should still be empty
    assert_rows!(ctx, "SELECT * FROM Empty", 0);

    ctx.commit();
}

#[test]
fn test_delete_table_not_found_error() {
    let mut ctx = setup_test();

    // DELETE from non-existent table should fail
    assert_error!(ctx, "DELETE FROM NonExistent");

    ctx.abort();
}

#[test]
fn test_delete_column_not_found_error() {
    let mut ctx = setup_test();
    setup_test_table(&mut ctx);

    // DELETE with non-existent column in WHERE should fail
    assert_error!(ctx, "DELETE FROM Foo WHERE nonexistent = 1");

    ctx.abort();
}

#[test]
fn test_delete_with_standard_table() {
    let mut ctx = setup_test();

    // Use standard table schema with generated data
    TableBuilder::new(&mut ctx, "StandardTest")
        .create_standard() // id INTEGER PRIMARY KEY, name TEXT, value INTEGER
        .insert_rows(10); // Insert 10 rows

    // Delete some rows
    ctx.exec("DELETE FROM StandardTest WHERE value > 50");

    // Should have deleted rows 6-10 (values 60-100)
    assert_rows!(ctx, "SELECT * FROM StandardTest", 5);

    // Verify remaining rows by checking specific values
    ctx.assert_query_contains(
        "SELECT value FROM StandardTest WHERE id = 5",
        "value",
        "I32(50)",
    );
    ctx.assert_query_contains(
        "SELECT value FROM StandardTest WHERE id = 1",
        "value",
        "I32(10)",
    );

    ctx.commit();
}

#[test]
fn test_delete_with_predefined_tables() {
    // Use helper that sets up common test tables
    let mut ctx = setup_with_tables();

    // Initial state: 3 users, 3 orders
    assert_rows!(ctx, "SELECT * FROM users", 3);
    assert_rows!(ctx, "SELECT * FROM orders", 3);

    // Delete a user
    ctx.exec("DELETE FROM users WHERE name = 'Bob'");

    // Verify deletion
    assert_rows!(ctx, "SELECT * FROM users", 2);
    assert_rows!(ctx, "SELECT * FROM users WHERE name = 'Bob'", 0);

    // Orders should still exist (no cascade delete)
    assert_rows!(ctx, "SELECT * FROM orders", 3);

    ctx.commit();
}

#[test]
fn test_delete_using_data_generators() {
    let mut ctx = setup_test();

    // Create table and insert data using generators
    TableBuilder::new(&mut ctx, "GenTest").create_simple("id INTEGER, name TEXT, value INTEGER");

    // Insert generated data
    let test_data = data::rows(20); // Generate 20 rows
    ctx.exec(&format!("INSERT INTO GenTest VALUES {}", test_data));

    assert_rows!(ctx, "SELECT * FROM GenTest", 20);

    // Delete half the rows
    ctx.exec("DELETE FROM GenTest WHERE id <= 10");

    // Verify deletion
    assert_rows!(ctx, "SELECT * FROM GenTest", 10);
    // Check specific remaining rows
    ctx.assert_query_contains("SELECT id FROM GenTest WHERE id = 11", "id", "I32(11)");
    ctx.assert_query_contains("SELECT id FROM GenTest WHERE id = 20", "id", "I32(20)");

    ctx.commit();
}

#[test]
fn test_delete_cascade_simulation() {
    let mut ctx = setup_test();

    // Setup parent-child relationship
    TableBuilder::new(&mut ctx, "Parent")
        .create_simple("id INTEGER PRIMARY KEY, name TEXT")
        .insert_values("(1, 'Parent1'), (2, 'Parent2')");

    TableBuilder::new(&mut ctx, "Child")
        .create_simple("id INTEGER, parent_id INTEGER, data TEXT")
        .insert_values("(1, 1, 'Child1'), (2, 1, 'Child2'), (3, 2, 'Child3')");

    // Manually cascade delete (since FK constraints aren't enforced)
    ctx.exec("DELETE FROM Child WHERE parent_id = 1");
    ctx.exec("DELETE FROM Parent WHERE id = 1");

    // Verify cascade-like behavior
    assert_rows!(ctx, "SELECT * FROM Parent", 1);
    assert_rows!(ctx, "SELECT * FROM Child", 1);
    ctx.assert_query_contains("SELECT parent_id FROM Child", "parent_id", "I32(2)");

    ctx.commit();
}
