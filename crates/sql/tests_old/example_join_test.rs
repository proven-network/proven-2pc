//! Example of using the common test utilities
//! This shows how the join tests would look with the new helpers

mod common;

use common::{setup_test, TableBuilder, TestContext};

#[test]
fn test_inner_join_with_helpers() {
    let mut ctx = setup_test();

    // Create and populate tables using builders
    TableBuilder::new(&mut ctx, "users")
        .create_simple("id INT PRIMARY KEY, name TEXT, age INT")
        .insert_values("(1, 'Alice', 25), (2, 'Bob', 30), (3, 'Charlie', 35)");

    TableBuilder::new(&mut ctx, "orders")
        .create_simple("id INT PRIMARY KEY, user_id INT, amount INT, status TEXT")
        .insert_values("(1, 1, 100, 'pending'), (2, 1, 200, 'shipped'), (3, 2, 150, 'delivered')");

    // Test INNER JOIN
    ctx.assert_row_count(
        "SELECT u.name, o.amount FROM users u INNER JOIN orders o ON u.id = o.user_id",
        3
    );

    // Check specific values
    ctx.assert_query_contains(
        "SELECT u.name FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.amount = 100",
        "name",
        "String(\"Alice\")"
    );

    ctx.commit();
}

#[test]
fn test_left_join_with_helpers() {
    let mut ctx = setup_test();

    // Setup tables
    TableBuilder::new(&mut ctx, "users")
        .create_simple("id INT PRIMARY KEY, name TEXT")
        .insert_values("(1, 'Alice'), (2, 'Bob'), (3, 'Charlie')");

    TableBuilder::new(&mut ctx, "orders")
        .create_simple("id INT PRIMARY KEY, user_id INT, amount INT")
        .insert_values("(1, 1, 100), (2, 2, 200)");

    // Test LEFT JOIN - should include Charlie with NULL order
    ctx.assert_row_count(
        "SELECT u.name, o.amount FROM users u LEFT JOIN orders o ON u.id = o.user_id",
        3
    );

    // Verify Charlie has NULL amount
    let results = ctx.query(
        "SELECT u.name, o.amount FROM users u LEFT JOIN orders o ON u.id = o.user_id WHERE u.name = 'Charlie'"
    );
    assert_eq!(results[0].get("amount").unwrap(), "Null");

    ctx.commit();
}

#[test]
fn test_join_errors() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "users")
        .create_standard();

    // Test join on non-existent table
    ctx.assert_error_contains(
        "SELECT * FROM users u JOIN nonexistent n ON u.id = n.id",
        "not found"
    );

    // Test ambiguous column reference
    TableBuilder::new(&mut ctx, "products")
        .create_standard();

    ctx.assert_error_contains(
        "SELECT id FROM users JOIN products ON users.id = products.id",
        "ambiguous"
    );

    ctx.abort();
}

#[test]
fn test_multiple_joins() {
    let mut ctx = TestContext::new();
    ctx.begin();

    // Create related tables
    ctx.exec("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)");
    ctx.exec("CREATE TABLE posts (id INT PRIMARY KEY, user_id INT, title TEXT)");
    ctx.exec("CREATE TABLE comments (id INT PRIMARY KEY, post_id INT, text TEXT)");

    // Insert test data
    ctx.exec("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')");
    ctx.exec("INSERT INTO posts VALUES (1, 1, 'First Post'), (2, 1, 'Second Post')");
    ctx.exec("INSERT INTO comments VALUES (1, 1, 'Nice!'), (2, 1, 'Great!')");

    // Test multiple joins
    let count = ctx.query_count(
        "SELECT u.name, p.title, c.text
         FROM users u
         JOIN posts p ON u.id = p.user_id
         JOIN comments c ON p.id = c.post_id"
    );

    assert_eq!(count, 2, "Expected 2 rows from triple join");

    ctx.commit();
}

// Using the macros
#[test]
fn test_with_macros() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "test")
        .create_standard()
        .insert_rows(5);

    assert_rows!(ctx, "SELECT * FROM test", 5);
    assert_rows!(ctx, "SELECT * FROM test WHERE id > 3", 2);

    assert_error!(ctx, "SELECT * FROM nonexistent");
    assert_error!(ctx, "SELECT * FROM nonexistent", "not found");

    ctx.commit();
}
