//! Basic SQL operations tests
//! Based on gluesql/test-suite/src/basic.rs

mod common;

use common::{TableBuilder, setup_test};
use proven_value::Value;

#[test]
fn test_create_table_with_columns() {
    let mut ctx = setup_test();

    // Create table with columns
    TableBuilder::new(&mut ctx, "Test")
        .create_simple("id INTEGER, num INTEGER, name TEXT")
        .insert_values("(1, 2, 'Hello')");

    // Verify data was inserted
    assert_rows!(ctx, "SELECT * FROM Test", 1);

    ctx.commit();
}

#[test]
fn test_create_multiple_tables() {
    let mut ctx = setup_test();

    // Create and populate multiple tables
    TableBuilder::new(&mut ctx, "Test")
        .create_simple("id INTEGER, num INTEGER, name TEXT")
        .insert_values("(1, 2, 'Hello')");

    TableBuilder::new(&mut ctx, "TestA")
        .create_simple("id INTEGER, num INTEGER, name TEXT")
        .insert_values("(3, 4, 'World')");

    // Verify both tables
    assert_rows!(ctx, "SELECT * FROM Test", 1);
    assert_rows!(ctx, "SELECT * FROM TestA", 1);

    ctx.commit();
}

#[test]
fn test_create_table_without_columns() {
    let mut ctx = setup_test();

    // Create table without columns
    ctx.exec("CREATE TABLE EmptyTest");

    // Should be able to select from it (returns empty)
    assert_rows!(ctx, "SELECT * FROM EmptyTest", 0);

    ctx.commit();
}

#[test]
fn test_insert_single_row() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Test")
        .create_simple("id INTEGER, num INTEGER, name TEXT")
        .insert_values("(1, 2, 'Hello')");

    // Verify insertion
    assert_rows!(ctx, "SELECT * FROM Test", 1);
    ctx.assert_query_value(
        "SELECT name FROM Test WHERE id = 1",
        "name",
        Value::Str("Hello".to_string()),
    );

    ctx.commit();
}

#[test]
fn test_insert_multiple_rows_separately() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Test").create_simple("id INTEGER, num INTEGER, name TEXT");

    // Insert rows one by one
    ctx.exec("INSERT INTO Test (id, num, name) VALUES (1, 2, 'Hello')");
    ctx.exec("INSERT INTO Test (id, num, name) VALUES (1, 9, 'World')");

    assert_rows!(ctx, "SELECT * FROM Test", 2);

    ctx.commit();
}

#[test]
fn test_insert_multiple_rows_in_one_statement() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Test")
        .create_simple("id INTEGER, num INTEGER, name TEXT")
        .insert_values("(3, 4, 'Great'), (4, 7, 'Job')");

    assert_rows!(ctx, "SELECT * FROM Test", 2);

    // Verify specific values
    ctx.assert_query_value(
        "SELECT name FROM Test WHERE id = 3",
        "name",
        Value::Str("Great".to_string()),
    );
    ctx.assert_query_value(
        "SELECT name FROM Test WHERE id = 4",
        "name",
        Value::Str("Job".to_string()),
    );

    ctx.commit();
}

#[test]
fn test_insert_select() {
    let mut ctx = setup_test();

    // Create and populate source table
    TableBuilder::new(&mut ctx, "Test")
        .create_simple("id INTEGER, num INTEGER, name TEXT")
        .insert_values("(1, 2, 'Hello'), (1, 9, 'World'), (3, 4, 'Great'), (4, 7, 'Job')");

    // Create target table
    TableBuilder::new(&mut ctx, "TestA").create_simple("id INTEGER, num INTEGER, name TEXT");

    // Insert from select
    ctx.exec("INSERT INTO TestA (id, num, name) SELECT id, num, name FROM Test");

    // Both tables should have same data
    assert_rows!(ctx, "SELECT * FROM Test", 4);
    assert_rows!(ctx, "SELECT * FROM TestA", 4);

    ctx.commit();
}

#[test]
fn test_insert_partial_columns() {
    let mut ctx = setup_test();

    // Create tables
    TableBuilder::new(&mut ctx, "Test")
        .create_simple("id INTEGER, num INTEGER, name TEXT")
        .insert_values("(1, 2, 'Hello'), (1, 9, 'World'), (3, 4, 'Great'), (4, 7, 'Job')");

    TableBuilder::new(&mut ctx, "TestB").create_simple("id INTEGER");

    // Insert only id column
    ctx.exec("INSERT INTO TestB (id) SELECT id FROM Test");

    assert_rows!(ctx, "SELECT * FROM TestB", 4);

    // Verify only id column exists
    let results = ctx.query("SELECT * FROM TestB");
    assert_eq!(results[0].len(), 1); // Only one column
    assert!(results[0].contains_key("id"));

    ctx.commit();
}

#[test]
fn test_select_all_from_table() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "TestB")
        .create_simple("id INTEGER")
        .insert_values("(1), (1), (3), (4)");

    // Select all and verify
    assert_rows!(ctx, "SELECT * FROM TestB", 4);
    ctx.assert_query_value("SELECT * FROM TestB LIMIT 1", "id", Value::I32(1));

    ctx.commit();
}

#[test]
fn test_select_specific_columns() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "TestA")
        .create_simple("id INTEGER, num INTEGER, name TEXT")
        .insert_values("(1, 2, 'Hello'), (1, 9, 'World'), (3, 4, 'Great'), (4, 7, 'Job')");

    assert_rows!(ctx, "SELECT id, num, name FROM TestA", 4);
    ctx.assert_query_value(
        "SELECT name FROM TestA WHERE id = 1 LIMIT 1",
        "name",
        Value::Str("Hello".to_string()),
    );

    ctx.commit();
}

#[test]
fn test_select_from_empty_table() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE EmptyTest");
    assert_rows!(ctx, "SELECT * FROM EmptyTest", 0);

    ctx.commit();
}

#[test]
#[ignore = "Subqueries in FROM clause not yet supported"]
fn test_select_from_subquery_empty() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE EmptyTest");
    assert_rows!(ctx, "SELECT * FROM (SELECT * FROM EmptyTest) AS Empty", 0);

    ctx.commit();
}

#[test]
fn test_count_rows() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Test")
        .create_simple("id INTEGER, num INTEGER, name TEXT")
        .insert_values("(1, 2, 'Hello'), (1, 9, 'World'), (3, 4, 'Great'), (4, 7, 'Job')");

    assert_rows!(ctx, "SELECT * FROM Test", 4);

    ctx.commit();
}

#[test]
fn test_update_all_rows() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Test")
        .create_simple("id INTEGER, num INTEGER, name TEXT")
        .insert_values("(1, 2, 'Hello'), (1, 9, 'World'), (3, 4, 'Great'), (4, 7, 'Job')");

    // Update all rows
    ctx.exec("UPDATE Test SET id = 2");

    // Verify all ids are now 2
    let results = ctx.query("SELECT id FROM Test");
    for row in results {
        assert_eq!(row.get("id").unwrap(), &Value::I32(2));
    }

    ctx.commit();
}

#[test]
fn test_select_after_update() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Test")
        .create_simple("id INTEGER, num INTEGER, name TEXT")
        .insert_values("(1, 2, 'Hello'), (1, 9, 'World'), (3, 4, 'Great'), (4, 7, 'Job')");

    ctx.exec("UPDATE Test SET id = 2");

    // Select after update
    assert_rows!(ctx, "SELECT id FROM Test", 4);

    // All should have id = 2 - verify by checking all rows
    let results = ctx.query("SELECT id FROM Test");
    assert_eq!(results.len(), 4);
    for row in &results {
        assert_eq!(row.get("id").unwrap(), &Value::I32(2));
    }

    ctx.commit();
}

#[test]
fn test_select_multiple_columns_after_update() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Test")
        .create_simple("id INTEGER, num INTEGER, name TEXT")
        .insert_values("(1, 2, 'Hello'), (1, 9, 'World'), (3, 4, 'Great'), (4, 7, 'Job')");

    ctx.exec("UPDATE Test SET id = 2");

    // Select multiple columns
    let results = ctx.query("SELECT id, num FROM Test ORDER BY num");
    assert_eq!(results.len(), 4);

    // Verify id is updated but num is unchanged
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(2));
    assert_eq!(results[0].get("num").unwrap(), &Value::I32(2));
    assert_eq!(results[3].get("id").unwrap(), &Value::I32(2));
    assert_eq!(results[3].get("num").unwrap(), &Value::I32(9));

    ctx.commit();
}

#[test]
fn test_compound_object_not_supported() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Test").create_simple("id INTEGER");

    // Try to use compound object (schema.table notation)
    assert_error!(ctx, "SELECT id FROM FOO.Test", "CompoundObjectNotSupported");

    ctx.abort();
}
