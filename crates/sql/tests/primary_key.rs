//! Tests for PRIMARY KEY constraint functionality
//! Based on gluesql/test-suite/src/primary_key.rs

mod common;

use common::setup_test;

#[test]
fn test_primary_key_basic_operations() {
    let mut ctx = setup_test();

    // Create table with PRIMARY KEY
    ctx.exec("CREATE TABLE Allegro (id INTEGER PRIMARY KEY, name TEXT)");

    // Insert test data
    ctx.exec("INSERT INTO Allegro VALUES (1, 'hello'), (3, 'world')");

    // Verify data was inserted correctly
    assert_rows!(ctx, "SELECT * FROM Allegro", 2);

    // Check specific rows
    let results = ctx.query("SELECT id, name FROM Allegro ORDER BY id");
    assert_eq!(results.len(), 2);
    assert!(results[0].get("id").unwrap().contains("1"));
    assert!(results[0].get("name").unwrap().contains("hello"));
    assert!(results[1].get("id").unwrap().contains("3"));
    assert!(results[1].get("name").unwrap().contains("world"));

    ctx.commit();
}

#[test]
fn test_primary_key_where_equality() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Allegro (id INTEGER PRIMARY KEY, name TEXT)");
    ctx.exec("INSERT INTO Allegro VALUES (1, 'hello'), (3, 'world')");

    // Test WHERE clause with equality on PRIMARY KEY
    let results = ctx.query("SELECT id, name FROM Allegro WHERE id = 1");
    assert_eq!(results.len(), 1);
    assert!(results[0].get("id").unwrap().contains("1"));
    assert!(results[0].get("name").unwrap().contains("hello"));

    ctx.commit();
}

#[test]
fn test_primary_key_where_comparison() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Allegro (id INTEGER PRIMARY KEY, name TEXT)");
    ctx.exec("INSERT INTO Allegro VALUES (1, 'hello'), (3, 'world')");

    // Test WHERE clause with comparison operator on PRIMARY KEY
    let results = ctx.query("SELECT id, name FROM Allegro WHERE id < 2");
    assert_eq!(results.len(), 1);
    assert!(results[0].get("id").unwrap().contains("1"));
    assert!(results[0].get("name").unwrap().contains("hello"));

    ctx.commit();
}

#[test]
fn test_primary_key_self_join() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Allegro (id INTEGER PRIMARY KEY, name TEXT)");
    ctx.exec("INSERT INTO Allegro VALUES (1, 'hello'), (3, 'world')");

    // Test self-join on PRIMARY KEY
    let results = ctx.query("SELECT a.id FROM Allegro a JOIN Allegro a2 WHERE a.id = a2.id");
    assert_eq!(results.len(), 2);

    // Check that we get both ids
    let ids: Vec<String> = results
        .iter()
        .map(|row| row.get("id").unwrap().clone())
        .collect();
    assert!(ids.iter().any(|id| id.contains("1")));
    assert!(ids.iter().any(|id| id.contains("3")));

    ctx.commit();
}

#[test]
fn test_primary_key_in_subquery() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Allegro (id INTEGER PRIMARY KEY, name TEXT)");
    ctx.exec("INSERT INTO Allegro VALUES (1, 'hello'), (3, 'world')");

    // Test IN subquery with PRIMARY KEY
    let results =
        ctx.query("SELECT id FROM Allegro WHERE id IN (SELECT id FROM Allegro WHERE id = id)");
    assert_eq!(results.len(), 2);

    ctx.commit();
}

#[test]
fn test_primary_key_ordering() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Allegro (id INTEGER PRIMARY KEY, name TEXT)");

    // Insert values in random order
    ctx.exec("INSERT INTO Allegro VALUES (5, 'neon'), (2, 'foo'), (4, 'bar')");

    // Insert more values
    ctx.exec("INSERT INTO Allegro VALUES (1, 'hello'), (3, 'world')");

    // Verify PRIMARY KEY maintains order
    let results = ctx.query("SELECT id, name FROM Allegro ORDER BY id");
    assert_eq!(results.len(), 5);

    // Check order
    assert!(results[0].get("id").unwrap().contains("1"));
    assert!(results[0].get("name").unwrap().contains("hello"));
    assert!(results[1].get("id").unwrap().contains("2"));
    assert!(results[1].get("name").unwrap().contains("foo"));
    assert!(results[2].get("id").unwrap().contains("3"));
    assert!(results[2].get("name").unwrap().contains("world"));
    assert!(results[3].get("id").unwrap().contains("4"));
    assert!(results[3].get("name").unwrap().contains("bar"));
    assert!(results[4].get("id").unwrap().contains("5"));
    assert!(results[4].get("name").unwrap().contains("neon"));

    ctx.commit();
}

#[test]
fn test_primary_key_modulo_operator() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Allegro (id INTEGER PRIMARY KEY, name TEXT)");
    ctx.exec("INSERT INTO Allegro VALUES (1, 'hello'), (2, 'foo'), (3, 'world'), (4, 'bar')");

    // Test modulo operator on PRIMARY KEY
    let results = ctx.query("SELECT id, name FROM Allegro WHERE id % 2 = 0");
    assert_eq!(results.len(), 2);

    // Check that we only get even ids
    assert!(results[0].get("id").unwrap().contains("2"));
    assert!(results[0].get("name").unwrap().contains("foo"));
    assert!(results[1].get("id").unwrap().contains("4"));
    assert!(results[1].get("name").unwrap().contains("bar"));

    ctx.commit();
}

#[test]
fn test_primary_key_delete_operations() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Allegro (id INTEGER PRIMARY KEY, name TEXT)");
    ctx.exec("INSERT INTO Allegro VALUES (1, 'hello'), (2, 'foo'), (3, 'world'), (4, 'bar'), (5, 'neon')");

    // Delete rows where id > 3
    ctx.exec("DELETE FROM Allegro WHERE id > 3");

    // Verify only rows with id <= 3 remain
    let results = ctx.query("SELECT id, name FROM Allegro ORDER BY id");
    assert_eq!(results.len(), 3);

    assert!(results[0].get("id").unwrap().contains("1"));
    assert!(results[0].get("name").unwrap().contains("hello"));
    assert!(results[1].get("id").unwrap().contains("2"));
    assert!(results[1].get("name").unwrap().contains("foo"));
    assert!(results[2].get("id").unwrap().contains("3"));
    assert!(results[2].get("name").unwrap().contains("world"));

    ctx.commit();
}

#[test]
fn test_primary_key_with_function_result() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Strslice (name TEXT PRIMARY KEY)");

    // Test inserting value derived from function
    ctx.exec("INSERT INTO Strslice VALUES (SUBSTR(SUBSTR('foo', 1), 1))");

    // Verify the result
    let results = ctx.query("SELECT name FROM Strslice");
    assert_eq!(results.len(), 1);
    assert!(results[0].get("name").unwrap().contains("foo"));

    ctx.commit();
}

#[test]
fn test_primary_key_unique_constraint() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Allegro (id INTEGER PRIMARY KEY, name TEXT)");
    ctx.exec("INSERT INTO Allegro VALUES (1, 'hello'), (3, 'world')");

    // Try to insert duplicate PRIMARY KEY - should fail
    assert_error!(
        ctx,
        "INSERT INTO Allegro VALUES (1, 'another hello')",
        "UniqueConstraintViolation"
    );

    // Verify original data is unchanged
    assert_rows!(ctx, "SELECT * FROM Allegro", 2);

    ctx.commit();
}

#[test]
fn test_primary_key_not_null_constraint() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Allegro (id INTEGER PRIMARY KEY, name TEXT)");

    // Try to insert NULL PRIMARY KEY - should fail
    assert_error!(ctx, "INSERT INTO Allegro VALUES (NULL, 'hello')", "NULL");

    // Verify no data was inserted
    assert_rows!(ctx, "SELECT * FROM Allegro", 0);

    ctx.commit();
}

#[test]
fn test_primary_key_update_valid() {
    // Test that PRIMARY KEY can be updated to a unique, non-NULL value (SQL standard behavior)
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
    ctx.exec("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')");

    // Update PRIMARY KEY to a new unique value - should succeed
    ctx.exec("UPDATE users SET id = 100 WHERE id = 1");

    // Verify the update worked
    let results = ctx.query("SELECT id, name FROM users ORDER BY id");
    assert_eq!(results.len(), 2);
    assert!(results[0].get("id").unwrap().contains("2"));
    assert!(results[0].get("name").unwrap().contains("Bob"));
    assert!(results[1].get("id").unwrap().contains("100"));
    assert!(results[1].get("name").unwrap().contains("Alice"));

    // Verify old id no longer exists
    assert_rows!(ctx, "SELECT * FROM users WHERE id = 1", 0);

    ctx.commit();
}

#[test]
fn test_primary_key_update_duplicate_fails() {
    // Test that updating PRIMARY KEY to duplicate value fails (violates UNIQUE constraint)
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
    ctx.exec("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')");

    // Try to update PRIMARY KEY to existing value - should fail
    assert_error!(
        ctx,
        "UPDATE users SET id = 2 WHERE id = 1",
        "UniqueConstraintViolation"
    );

    // Verify data is unchanged
    assert_rows!(ctx, "SELECT * FROM users WHERE id = 1", 1);
    assert_rows!(ctx, "SELECT * FROM users WHERE id = 2", 1);

    ctx.commit();
}

#[test]
fn test_primary_key_index_update_integrity() {
    // Test that indexes are properly maintained when PRIMARY KEY is updated
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE products (sku INTEGER PRIMARY KEY, name TEXT, price INTEGER)");
    ctx.exec(
        "INSERT INTO products VALUES (100, 'Widget', 10), (200, 'Gadget', 20), (300, 'Doodad', 30)",
    );

    // Update PRIMARY KEY value
    ctx.exec("UPDATE products SET sku = 150 WHERE sku = 100");

    // Verify we can find the row with new PRIMARY KEY
    let results = ctx.query("SELECT * FROM products WHERE sku = 150");
    assert_eq!(results.len(), 1);
    assert!(results[0].get("name").unwrap().contains("Widget"));
    assert!(results[0].get("price").unwrap().contains("10"));

    // Verify we cannot find it with old PRIMARY KEY
    assert_rows!(ctx, "SELECT * FROM products WHERE sku = 100", 0);

    // Verify index still enforces uniqueness with the new value
    assert_error!(
        ctx,
        "INSERT INTO products VALUES (150, 'Duplicate', 50)",
        "UniqueConstraintViolation"
    );

    // Update multiple PRIMARY KEYs in sequence
    ctx.exec("UPDATE products SET sku = 250 WHERE sku = 200");
    ctx.exec("UPDATE products SET sku = 350 WHERE sku = 300");

    // Verify all rows are findable with their new keys
    assert_rows!(ctx, "SELECT * FROM products WHERE sku = 150", 1);
    assert_rows!(ctx, "SELECT * FROM products WHERE sku = 250", 1);
    assert_rows!(ctx, "SELECT * FROM products WHERE sku = 350", 1);

    // Verify old keys don't exist
    assert_rows!(
        ctx,
        "SELECT * FROM products WHERE sku IN (100, 200, 300)",
        0
    );

    // Verify total row count is unchanged
    assert_rows!(ctx, "SELECT * FROM products", 3);

    ctx.commit();
}
