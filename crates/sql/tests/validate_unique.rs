//! Unique constraint validation tests
//! Based on gluesql/test-suite/src/validate/unique.rs

mod common;

use common::setup_test;
use proven_sql::SqlResponse;

use crate::common::TestContext;

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

#[test]
fn test_unique_constraint_cleaned_on_abort() {
    let mut ctx = TestContext::new();

    // Create a table with a unique constraint
    ctx.begin();
    ctx.exec(
        "CREATE TABLE test_unique (
        id VARCHAR PRIMARY KEY,
        unique_field VARCHAR UNIQUE,
        data VARCHAR
    )",
    );
    ctx.commit();

    // First transaction: Insert a record with unique value
    ctx.begin();
    ctx.exec("INSERT INTO test_unique (id, unique_field, data) VALUES ('record_001', 'unique_value_1', 'First attempt')");

    // Abort the transaction
    ctx.abort();

    // Second transaction: Try to insert the same unique value (should succeed after abort)
    ctx.begin();
    let response = ctx.exec_response("INSERT INTO test_unique (id, unique_field, data) VALUES ('record_001', 'unique_value_1', 'Second attempt')");

    match response {
        SqlResponse::Error(e) => {
            panic!(
                "Insert after abort failed: {}. This indicates the unique index wasn't properly cleaned up!",
                e
            );
        }
        _ => {
            // Success - the insert worked after abort
        }
    }
    ctx.commit();

    // Third transaction: Try to insert duplicate (should fail due to unique constraint)
    ctx.begin();
    let error = ctx.exec_error("INSERT INTO test_unique (id, unique_field, data) VALUES ('record_002', 'unique_value_1', 'Should fail')");
    assert!(
        error.contains("Unique constraint violation"),
        "Expected unique constraint violation, got: {}",
        error
    );
    ctx.abort();
}

#[test]
fn test_multiple_abort_retry_unique_constraint() {
    let mut ctx = TestContext::new();

    // Create a table with a unique constraint
    ctx.begin();
    ctx.exec(
        "CREATE TABLE test_retry (
        id VARCHAR PRIMARY KEY,
        unique_field VARCHAR UNIQUE
    )",
    );
    ctx.commit();

    // Simulate multiple abort/retry cycles with the same unique value
    for attempt in 1..=5 {
        ctx.begin();

        let sql = format!(
            "INSERT INTO test_retry (id, unique_field) VALUES ('record_{:03}', 'same_unique_value')",
            attempt
        );

        if attempt < 5 {
            // Execute and abort the first 4 attempts
            ctx.exec(&sql);
            ctx.abort();
            println!("Attempt {}: Aborted transaction", attempt);
        } else {
            // Commit the last attempt
            let response = ctx.exec_response(&sql);
            match response {
                SqlResponse::Error(e) => {
                    panic!(
                        "Final attempt failed: {}. This suggests index cleanup issues after multiple aborts!",
                        e
                    );
                }
                _ => {
                    ctx.commit();
                    println!("Attempt {}: Successfully committed", attempt);
                }
            }
        }
    }

    // Verify the data was inserted correctly
    ctx.begin();
    ctx.assert_row_count(
        "SELECT * FROM test_retry WHERE unique_field = 'same_unique_value'",
        1,
    );
    ctx.commit();
}

#[test]
fn test_concurrent_unique_constraint_violations() {
    let mut ctx = TestContext::new();

    // Create a table with unique constraint
    ctx.begin();
    ctx.exec(
        "CREATE TABLE test_concurrent (
        id VARCHAR PRIMARY KEY,
        unique_field VARCHAR UNIQUE
    )",
    );
    ctx.commit();

    // First transaction: Insert and commit
    ctx.begin();
    ctx.exec("INSERT INTO test_concurrent (id, unique_field) VALUES ('record_001', 'unique_1')");
    ctx.commit();

    // Second transaction: Try to insert duplicate, then abort
    ctx.begin();
    ctx.assert_error_contains(
        "INSERT INTO test_concurrent (id, unique_field) VALUES ('record_002', 'unique_1')",
        "Unique constraint violation",
    );
    ctx.abort();

    // Third transaction: Should still not be able to insert duplicate
    ctx.begin();
    ctx.assert_error_contains(
        "INSERT INTO test_concurrent (id, unique_field) VALUES ('record_003', 'unique_1')",
        "Unique constraint violation",
    );
    ctx.abort();

    // Verify only one row exists
    ctx.begin();
    ctx.assert_row_count("SELECT * FROM test_concurrent", 1);
    ctx.commit();
}
