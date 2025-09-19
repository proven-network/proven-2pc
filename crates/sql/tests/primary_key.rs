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
#[ignore = "Self-join not yet fully implemented"]
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
#[ignore = "IN subquery not yet fully implemented"]
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
#[ignore = "Modulo operator not yet implemented"]
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
#[ignore = "SUBSTR function not yet implemented"]
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
        "Unique constraint violation"
    );

    // Verify original data is unchanged
    assert_rows!(ctx, "SELECT * FROM Allegro", 2);

    ctx.commit();
}

#[test]
#[ignore = "NOT NULL constraint on PRIMARY KEY not yet enforced"]
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
#[ignore = "UPDATE restriction on PRIMARY KEY not yet implemented"]
fn test_primary_key_update_not_allowed() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Allegro (id INTEGER PRIMARY KEY, name TEXT)");
    ctx.exec("INSERT INTO Allegro VALUES (1, 'hello'), (3, 'world')");

    // Try to UPDATE PRIMARY KEY column - should fail
    assert_error!(
        ctx,
        "UPDATE Allegro SET id = 100 WHERE id = 1",
        "PRIMARY KEY"
    );

    // Verify data is unchanged
    let results = ctx.query("SELECT id FROM Allegro WHERE id = 1");
    assert_eq!(results.len(), 1);
    assert!(results[0].get("id").unwrap().contains("1"));

    ctx.commit();
}
