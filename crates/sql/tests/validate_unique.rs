//! Unique constraint validation tests
//! Based on gluesql/test-suite/src/validate/unique.rs

mod common;

use common::setup_test;

/// Setup tables for unique constraint tests
fn setup_unique_tables(ctx: &mut common::TestContext) {
    // Table A: Single unique constraint on id
    ctx.exec(
        r#"
        CREATE TABLE TestA (
            id INTEGER UNIQUE,
            num INT
        )
        "#,
    );

    // Table B: Multiple unique constraints
    ctx.exec(
        r#"
        CREATE TABLE TestB (
            id INTEGER UNIQUE,
            num INT UNIQUE
        )
        "#,
    );

    // Table C: Nullable unique constraint
    ctx.exec(
        r#"
        CREATE TABLE TestC (
            id INTEGER NULL UNIQUE,
            num INT
        )
        "#,
    );
}

#[test]
fn test_create_table_with_unique_constraint() {
    let mut ctx = setup_test();

    ctx.exec(
        r#"
        CREATE TABLE TestA (
            id INTEGER UNIQUE,
            num INT
        )
        "#,
    );

    // Verify table was created
    ctx.exec("INSERT INTO TestA VALUES (1, 1)");
    assert_rows!(ctx, "SELECT * FROM TestA", 1);

    ctx.commit();
}

#[test]
fn test_create_table_with_multiple_unique_constraints() {
    let mut ctx = setup_test();

    ctx.exec(
        r#"
        CREATE TABLE TestB (
            id INTEGER UNIQUE,
            num INT UNIQUE
        )
        "#,
    );

    // Verify table was created
    ctx.exec("INSERT INTO TestB VALUES (1, 1)");
    assert_rows!(ctx, "SELECT * FROM TestB", 1);

    ctx.commit();
}

#[test]
fn test_create_table_with_nullable_unique_constraint() {
    let mut ctx = setup_test();

    ctx.exec(
        r#"
        CREATE TABLE TestC (
            id INTEGER NULL UNIQUE,
            num INT
        )
        "#,
    );

    // Verify table was created and NULL can be inserted
    ctx.exec("INSERT INTO TestC VALUES (NULL, 1)");
    assert_rows!(ctx, "SELECT * FROM TestC", 1);

    ctx.commit();
}

#[test]
fn test_insert_valid_unique_values() {
    let mut ctx = setup_test();
    setup_unique_tables(&mut ctx);

    // Insert different ids, same num is ok
    ctx.exec("INSERT INTO TestA VALUES (1, 1)");
    ctx.exec("INSERT INTO TestA VALUES (2, 1), (3, 1)");

    assert_rows!(ctx, "SELECT * FROM TestA", 3);

    ctx.commit();
}

#[test]
fn test_insert_multiple_nulls_in_unique_column() {
    let mut ctx = setup_test();
    setup_unique_tables(&mut ctx);

    // Multiple NULLs are allowed in a unique column
    ctx.exec("INSERT INTO TestC VALUES (NULL, 1)");
    ctx.exec("INSERT INTO TestC VALUES (2, 2), (NULL, 3)");

    assert_rows!(ctx, "SELECT * FROM TestC", 3);

    ctx.commit();
}

#[test]
fn test_update_to_null_in_unique_column() {
    let mut ctx = setup_test();
    setup_unique_tables(&mut ctx);

    // Insert initial data
    ctx.exec("INSERT INTO TestC VALUES (1, 1)");
    ctx.exec("INSERT INTO TestC VALUES (2, 2), (NULL, 3)");

    // Update to NULL should work
    ctx.exec("UPDATE TestC SET id = NULL WHERE num = 1");

    // Verify the update
    let results = ctx.query("SELECT id FROM TestC WHERE num = 1");
    assert_eq!(results.len(), 1);
    assert!(results[0].values().next().unwrap().contains("Null"));

    ctx.commit();
}

#[test]
fn test_insert_duplicate_unique_value_error() {
    let mut ctx = setup_test();
    setup_unique_tables(&mut ctx);

    // Insert initial data
    ctx.exec("INSERT INTO TestA VALUES (1, 1)");
    ctx.exec("INSERT INTO TestA VALUES (2, 1), (3, 1)");

    // Try to insert duplicate id
    ctx.assert_error_contains(
        "INSERT INTO TestA VALUES (2, 2)",
        "Unique constraint violation on index 'id'",
    );

    ctx.commit();
}

#[test]
fn test_insert_duplicate_in_same_statement() {
    let mut ctx = setup_test();
    setup_unique_tables(&mut ctx);

    // Try to insert duplicate id in the same statement
    ctx.assert_error_contains(
        "INSERT INTO TestA VALUES (4, 4), (4, 5)",
        "Unique constraint violation on index 'id'",
    );

    ctx.commit();
}

#[test]
fn test_update_to_duplicate_unique_value() {
    let mut ctx = setup_test();
    setup_unique_tables(&mut ctx);

    // Insert initial data
    ctx.exec("INSERT INTO TestA VALUES (1, 1)");
    ctx.exec("INSERT INTO TestA VALUES (2, 1), (3, 1)");

    // Try to update to duplicate id
    ctx.assert_error_contains(
        "UPDATE TestA SET id = 2 WHERE id = 1",
        "Unique constraint violation on index 'id'",
    );

    ctx.commit();
}

#[test]
fn test_insert_duplicate_unique_id_in_multiple_unique_table() {
    let mut ctx = setup_test();
    setup_unique_tables(&mut ctx);

    // Insert initial data
    ctx.exec("INSERT INTO TestB VALUES (1, 1)");
    ctx.exec("INSERT INTO TestB VALUES (2, 2), (3, 3)");

    // Try to insert duplicate id (1 is duplicate, 4 is new)
    ctx.assert_error_contains(
        "INSERT INTO TestB VALUES (1, 4)",
        "Unique constraint violation on index",
    );

    ctx.commit();
}

#[test]
fn test_insert_duplicate_unique_num_in_multiple_unique_table() {
    let mut ctx = setup_test();
    setup_unique_tables(&mut ctx);

    // Insert initial data
    ctx.exec("INSERT INTO TestB VALUES (1, 1)");
    ctx.exec("INSERT INTO TestB VALUES (2, 2), (3, 3)");

    // Try to insert duplicate num
    ctx.assert_error_contains(
        "INSERT INTO TestB VALUES (4, 2)",
        "Unique constraint violation on index 'num'",
    );

    ctx.commit();
}

#[test]
fn test_insert_multiple_values_with_duplicate_unique() {
    let mut ctx = setup_test();
    setup_unique_tables(&mut ctx);

    // Try to insert duplicate num in the same statement
    ctx.assert_error_contains(
        "INSERT INTO TestB VALUES (5, 5), (6, 5)",
        "Unique constraint violation on index 'num'",
    );

    ctx.commit();
}

#[test]
fn test_update_to_duplicate_num_in_multiple_unique_table() {
    let mut ctx = setup_test();
    setup_unique_tables(&mut ctx);

    // Insert initial data
    ctx.exec("INSERT INTO TestB VALUES (1, 1)");
    ctx.exec("INSERT INTO TestB VALUES (2, 2), (3, 3)");

    // Try to update to duplicate num
    ctx.assert_error_contains(
        "UPDATE TestB SET num = 2 WHERE id = 1",
        "Unique constraint violation on index 'num'",
    );

    ctx.commit();
}

#[test]
fn test_insert_duplicate_non_null_unique_value() {
    let mut ctx = setup_test();
    setup_unique_tables(&mut ctx);

    // Insert initial data with NULL and non-NULL values
    ctx.exec("INSERT INTO TestC VALUES (NULL, 1)");
    ctx.exec("INSERT INTO TestC VALUES (2, 2), (NULL, 3)");

    // Try to insert duplicate non-null id
    ctx.assert_error_contains(
        "INSERT INTO TestC VALUES (2, 4)",
        "Unique constraint violation on index 'id'",
    );

    ctx.commit();
}

#[test]
fn test_insert_duplicate_with_null_and_non_null() {
    let mut ctx = setup_test();
    setup_unique_tables(&mut ctx);

    // Try to insert with duplicate non-null value in the same statement
    ctx.assert_error_contains(
        "INSERT INTO TestC VALUES (NULL, 5), (3, 5), (3, 6)",
        "Unique constraint violation on index 'id'",
    );

    ctx.commit();
}

#[test]
fn test_update_all_to_same_unique_value() {
    let mut ctx = setup_test();
    setup_unique_tables(&mut ctx);

    // Insert initial data
    ctx.exec("INSERT INTO TestC VALUES (NULL, 1)");
    ctx.exec("INSERT INTO TestC VALUES (2, 2), (NULL, 3)");
    ctx.exec("UPDATE TestC SET id = 1 WHERE num = 1");

    // Try to update all rows to the same value (would create duplicates)
    ctx.assert_error_contains(
        "UPDATE TestC SET id = 1",
        "Unique constraint violation on index 'id'",
    );

    ctx.commit();
}
