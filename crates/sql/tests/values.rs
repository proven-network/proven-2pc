//! VALUES clause tests
//! Based on gluesql/test-suite/src/values.rs

mod common;

use common::setup_test;
use proven_value::Value;

#[test]
fn test_create_table_for_values() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Items (id INTEGER, name TEXT, status TEXT)");

    // Verify table was created (by trying to insert a valid row)
    ctx.exec("INSERT INTO Items (id, name) VALUES (1, 'test')");
    assert_rows!(ctx, "SELECT * FROM Items", 1);

    ctx.commit();
}

#[test]
fn test_values_single_column() {
    let mut ctx = setup_test();

    // Test VALUES (1), (2), (3) - returns 3 rows with column1
    let results = ctx.query("VALUES (1), (2), (3)");
    assert_eq!(results.len(), 3);

    // Check values
    ctx.assert_query_value("VALUES (1), (2), (3) LIMIT 1", "column1", Value::I32(1));

    ctx.commit();
}

#[test]
fn test_values_multiple_columns() {
    let mut ctx = setup_test();

    // Test VALUES (1, 'a'), (2, 'b') - returns 2 rows with column1, column2
    let results = ctx.query("VALUES (1, 'a'), (2, 'b')");
    assert_eq!(results.len(), 2);

    ctx.commit();
}

#[test]
fn test_values_with_order_by() {
    let mut ctx = setup_test();

    // Test VALUES (1, 'a'), (2, 'b') ORDER BY column1 DESC - returns rows in descending order
    let results = ctx.query("VALUES (1, 'a'), (2, 'b') ORDER BY column1 DESC");
    assert_eq!(results.len(), 2);
    // First row should have column1 = 2

    ctx.commit();
}

#[test]
fn test_values_with_limit() {
    let mut ctx = setup_test();

    // Test VALUES (1), (2) LIMIT 1 - returns only first row
    let results = ctx.query("VALUES (1), (2) LIMIT 1");
    assert_eq!(results.len(), 1);

    ctx.commit();
}

#[test]
fn test_values_with_offset() {
    let mut ctx = setup_test();

    // Test VALUES (1), (2) OFFSET 1 - returns only second row
    let results = ctx.query("VALUES (1), (2) OFFSET 1");
    assert_eq!(results.len(), 1);

    ctx.commit();
}

#[test]
fn test_values_with_null() {
    let mut ctx = setup_test();

    // Test VALUES (1, NULL), (2, NULL) - returns rows with NULL values
    let results = ctx.query("VALUES (1, NULL), (2, NULL)");
    assert_eq!(results.len(), 2);

    ctx.commit();
}

#[test]
fn test_values_different_number_of_values_error() {
    let mut ctx = setup_test();

    // Test VALUES (1), (2, 'b') - should error with different column counts
    assert_error!(ctx, "VALUES (1), (2, 'b')", "2 values but 1 columns");

    ctx.abort();
}

#[test]
fn test_values_different_number_of_values_reverse_error() {
    let mut ctx = setup_test();

    // Test VALUES (1, 'a'), (2) - should error with different column counts
    assert_error!(ctx, "VALUES (1, 'a'), (2)", "1 values but 2 columns");

    ctx.abort();
}

#[test]
fn test_values_incompatible_data_types_text_number() {
    let mut ctx = setup_test();

    // Test VALUES (1, 'a'), (2, 3) - should error with incompatible types (string vs number)
    assert_error!(
        ctx,
        "VALUES (1, 'a'), (2, 3)",
        "Incompatible types in VALUES"
    );

    ctx.abort();
}

#[test]
fn test_values_incompatible_data_types_int_text() {
    let mut ctx = setup_test();

    // Test VALUES (1, 'a'), ('b', 'c') - should error with incompatible types (int vs string)
    assert_error!(
        ctx,
        "VALUES (1, 'a'), ('b', 'c')",
        "Incompatible types in VALUES"
    );

    ctx.abort();
}

#[test]
fn test_values_incompatible_data_types_with_null() {
    let mut ctx = setup_test();

    // Test VALUES (1, NULL), (2, 'a'), (3, 4) - should error with incompatible types (string vs int after NULL)
    assert_error!(
        ctx,
        "VALUES (1, NULL), (2, 'a'), (3, 4)",
        "Incompatible types in VALUES"
    );

    ctx.abort();
}

#[test]
fn test_values_type_promotion_int_to_bigint() {
    let mut ctx = setup_test();

    // Test that I32 and I64 can be mixed (promotes to I64)
    let results = ctx.query("VALUES (1), (9223372036854775807)"); // 1 is I32, large number is I64
    assert_eq!(results.len(), 2);

    ctx.commit();
}

#[test]
fn test_values_null_with_consistent_types() {
    let mut ctx = setup_test();

    // Test that NULL works with consistent non-NULL values
    let results = ctx.query("VALUES (1, NULL), (2, NULL), (3, NULL)");
    assert_eq!(results.len(), 3);

    ctx.commit();
}

#[test]
fn test_create_table_as_values() {
    let mut ctx = setup_test();

    // Test CREATE TABLE TableFromValues AS VALUES (1, 'a', True, Null, Null), (2, 'b', False, 3, Null)
    ctx.exec("CREATE TABLE TableFromValues AS VALUES (1, 'a', True, Null, Null), (2, 'b', False, 3, Null)");

    assert_rows!(ctx, "SELECT * FROM TableFromValues", 2);

    ctx.commit();
}

#[test]
fn test_select_from_values_table() {
    let mut ctx = setup_test();

    // First create the table
    ctx.exec("CREATE TABLE TableFromValues AS VALUES (1, 'a', True, Null, Null), (2, 'b', False, 3, Null)");

    // Test SELECT * FROM TableFromValues - verify data types and values
    assert_rows!(ctx, "SELECT * FROM TableFromValues", 2);

    // Verify specific values
    ctx.assert_query_value(
        "SELECT column1 FROM TableFromValues WHERE column1 = 1",
        "column1",
        Value::I32(1),
    );
    ctx.assert_query_value(
        "SELECT column2 FROM TableFromValues WHERE column1 = 1",
        "column2",
        Value::Str("a".to_string()),
    );
    ctx.assert_query_value(
        "SELECT column3 FROM TableFromValues WHERE column1 = 1",
        "column3",
        Value::Bool(true),
    );

    ctx.commit();
}

#[test]
#[ignore = "SHOW COLUMNS not yet implemented"]
fn test_show_columns_from_values_table() {
    let mut ctx = setup_test();

    // First create the table
    ctx.exec("CREATE TABLE TableFromValues AS VALUES (1, 'a', True, Null, Null), (2, 'b', False, 3, Null)");

    // Test SHOW COLUMNS FROM TableFromValues - verify column types (Int, Text, Boolean, Int, Text)
    let results = ctx.query("SHOW COLUMNS FROM TableFromValues");
    assert_eq!(results.len(), 5);

    ctx.commit();
}

#[test]
fn test_values_as_subquery() {
    let mut ctx = setup_test();

    // Test SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS Derived
    let results = ctx.query("SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS Derived");
    assert_eq!(results.len(), 2);

    ctx.commit();
}

#[test]
fn test_values_subquery_with_column_aliases() {
    let mut ctx = setup_test();

    // Test SELECT column1 AS id, column2 AS name FROM (VALUES (1, 'a'), (2, 'b')) AS Derived
    let results = ctx
        .query("SELECT column1 AS id, column2 AS name FROM (VALUES (1, 'a'), (2, 'b')) AS Derived");
    assert_eq!(results.len(), 2);

    // Verify column aliases
    assert!(results[0].contains_key("id"));
    assert!(results[0].contains_key("name"));

    ctx.commit();
}

#[test]
fn test_insert_with_values() {
    let mut ctx = setup_test();

    // Create table first
    ctx.exec("CREATE TABLE Items (id INTEGER, name TEXT, status TEXT)");

    // Test INSERT INTO Items (id) VALUES (1)
    ctx.exec("INSERT INTO Items (id) VALUES (1)");

    assert_rows!(ctx, "SELECT * FROM Items", 1);
    ctx.assert_query_value("SELECT id FROM Items", "id", Value::I32(1));
    // status should have default value 'ACTIVE'

    ctx.commit();
}

#[test]
fn test_insert_wrong_column_name_error() {
    let mut ctx = setup_test();

    // Create table first
    ctx.exec("CREATE TABLE Items (id INTEGER, name TEXT, status TEXT)");

    // Test INSERT INTO Items (id2) VALUES (1) - should error with column not found
    assert_error!(ctx, "INSERT INTO Items (id2) VALUES (1)", "ColumnNotFound");

    ctx.abort();
}

#[test]
fn test_insert_missing_required_column_error() {
    let mut ctx = setup_test();

    // Create table with NOT NULL constraint
    ctx.exec("CREATE TABLE Items (id INTEGER NOT NULL, name TEXT, status TEXT)");

    // Test INSERT INTO Items (name) VALUES ('glue') - should error when id is not provided
    assert_error!(
        ctx,
        "INSERT INTO Items (name) VALUES ('glue')",
        "NullConstraintViolation"
    );

    ctx.abort();
}

#[test]
fn test_insert_column_values_mismatch_error() {
    let mut ctx = setup_test();

    // Create table first
    ctx.exec("CREATE TABLE Items (id INTEGER, name TEXT, status TEXT)");

    // Test INSERT INTO Items (id) VALUES (3, 'sql') - should error with column/value mismatch
    assert_error!(
        ctx,
        "INSERT INTO Items (id) VALUES (3, 'sql')",
        "2 values but 1 columns"
    );

    ctx.abort();
}

#[test]
fn test_insert_too_many_values_error() {
    let mut ctx = setup_test();

    // Create table first
    ctx.exec("CREATE TABLE Items (id INTEGER, name TEXT, status TEXT)");

    // Test INSERT INTO Items VALUES (100, 'a', 'b', 1) - should error with too many values
    assert_error!(
        ctx,
        "INSERT INTO Items VALUES (100, 'a', 'b', 1)",
        "4 values but 3 columns"
    );

    ctx.abort();
}

#[test]
fn test_insert_table_not_found_error() {
    let mut ctx = setup_test();

    // Test INSERT INTO Nothing VALUES (1) - should error with TableNotFound
    assert_error!(ctx, "INSERT INTO Nothing VALUES (1)", "TableNotFound");

    ctx.abort();
}
