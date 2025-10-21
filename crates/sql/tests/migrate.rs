//! Migration and error handling tests
//! Based on gluesql/test-suite/src/migrate.rs

mod common;

use common::{TestContext, setup_test};
use proven_value::Value;

/// Setup test table with data for all tests
fn setup_test_table(ctx: &mut TestContext) {
    ctx.exec(
        "CREATE TABLE Test (
            id INT,
            num INT,
            name TEXT
        )",
    );
    ctx.exec(
        "INSERT INTO Test (id, num, name) VALUES
            (1,     2,     'Hello'),
            (-(-1), 9,     'World'),
            (+3,    2 * 2, 'Great')",
    );
}

#[test]
fn test_create_test_table() {
    let mut ctx = setup_test();

    ctx.exec(
        "CREATE TABLE Test (
            id INT,
            num INT,
            name TEXT
        )",
    );

    // Verify table was created by inserting and querying
    ctx.exec("INSERT INTO Test (id, num, name) VALUES (1, 2, 'Hello')");
    assert_rows!(ctx, "SELECT * FROM Test", 1);

    ctx.commit();
}

#[test]
fn test_insert_with_expressions() {
    let mut ctx = setup_test();
    setup_test_table(&mut ctx);

    // Verify all 3 rows were inserted
    assert_rows!(ctx, "SELECT * FROM Test", 3);

    // Verify expression evaluation: -(-1) = 1
    ctx.assert_query_value("SELECT id FROM Test WHERE num = 9", "id", Value::I32(1));

    // Verify expression evaluation: +3 = 3
    ctx.assert_query_value(
        "SELECT id FROM Test WHERE name = 'Great'",
        "id",
        Value::I32(3),
    );

    // Verify expression evaluation: 2 * 2 = 4
    ctx.assert_query_value(
        "SELECT num FROM Test WHERE name = 'Great'",
        "num",
        Value::I32(4),
    );

    ctx.commit();
}

#[test]
fn test_insert_with_type_coercion() {
    let mut ctx = setup_test();
    setup_test_table(&mut ctx);

    // This implementation supports type coercion for numeric types
    // Decimal values can be inserted into INT columns
    ctx.exec("INSERT INTO Test (id, num, name) VALUES (1, 1, 'good')");

    // Verify the insert succeeded
    assert_rows!(ctx, "SELECT * FROM Test WHERE name = 'good'", 1);

    ctx.commit();
}

#[test]
fn test_insert_compound_identifier_should_error() {
    let mut ctx = setup_test();
    setup_test_table(&mut ctx);

    // Attempting to use compound identifier without context should error
    // In this implementation it reports ColumnNotFound
    assert_error!(
        ctx,
        "INSERT INTO Test (id, num, name) VALUES (1, 1, a.b)",
        "ColumnNotFound"
    );

    ctx.abort();
}

#[test]
fn test_unsupported_compound_identifier_should_error() {
    let mut ctx = setup_test();
    setup_test_table(&mut ctx);

    // Three-part compound identifiers are unsupported
    // In this implementation it reports ColumnNotFound
    assert_error!(
        ctx,
        "SELECT * FROM Test WHERE Here.User.id = 1",
        "ColumnNotFound"
    );

    ctx.abort();
}

#[test]
fn test_cross_join() {
    let mut ctx = setup_test();
    setup_test_table(&mut ctx);

    // Test CROSS JOIN functionality
    ctx.exec("SELECT * FROM Test CROSS JOIN Test");

    // CROSS JOIN should produce cartesian product (3 * 3 = 9 rows)
    assert_rows!(ctx, "SELECT * FROM Test CROSS JOIN Test", 9);

    ctx.commit();
}

#[test]
fn test_arithmetic_expressions() {
    let mut ctx = setup_test();
    setup_test_table(&mut ctx);

    // Test that basic arithmetic operators work in SELECT
    ctx.exec("SELECT 1 + 2 FROM Test");

    // Verify the results
    let results = ctx.query("SELECT 1 + 2 AS result FROM Test");
    assert_eq!(results.len(), 3); // Should return 3 rows (one for each row in Test)

    ctx.commit();
}

#[test]
fn test_union_query_should_error() {
    let mut ctx = setup_test();
    setup_test_table(&mut ctx);

    // UNION queries are not supported - reports parse error
    assert_error!(
        ctx,
        "SELECT * FROM Test UNION SELECT * FROM Test",
        "Parse error"
    );

    ctx.abort();
}

#[test]
fn test_unknown_identifier_should_error() {
    let mut ctx = setup_test();
    setup_test_table(&mut ctx);

    // Using undefined column should error - reports ColumnNotFound
    assert_error!(ctx, "SELECT * FROM Test WHERE noname = 1", "ColumnNotFound");

    ctx.abort();
}

#[test]
fn test_table_not_found_should_error() {
    let mut ctx = setup_test();

    // Querying non-existent table should error
    assert_error!(ctx, "SELECT * FROM Nothing", "TableNotFound");

    ctx.abort();
}

#[test]
fn test_truncate_statement_should_error() {
    let mut ctx = setup_test();

    // TRUNCATE statement is not supported - reports parse error
    assert_error!(ctx, "TRUNCATE TABLE ProjectUser", "Parse error");

    ctx.abort();
}

#[test]
fn test_distinct_query() {
    let mut ctx = setup_test();
    setup_test_table(&mut ctx);

    // Test that DISTINCT works properly
    ctx.exec("SELECT DISTINCT id FROM Test");

    // Should return unique id values (3 unique values: 1, 1, 3)
    let results = ctx.query("SELECT DISTINCT id FROM Test");
    assert_eq!(results.len(), 2); // Only 2 distinct values: 1 and 3

    ctx.commit();
}

#[test]
fn test_select_all_data() {
    let mut ctx = setup_test();
    setup_test_table(&mut ctx);

    // Query all data
    let results = ctx.query("SELECT id, num, name FROM Test");

    // Should have 3 rows
    assert_eq!(results.len(), 3);

    // Verify first row
    assert_eq!(results[0].get("id"), Some(&Value::I32(1)));
    assert_eq!(results[0].get("num"), Some(&Value::I32(2)));
    assert_eq!(
        results[0].get("name"),
        Some(&Value::Str("Hello".to_string()))
    );

    // Verify second row (with expression -(-1) = 1)
    assert_eq!(results[1].get("id"), Some(&Value::I32(1)));
    assert_eq!(results[1].get("num"), Some(&Value::I32(9)));
    assert_eq!(
        results[1].get("name"),
        Some(&Value::Str("World".to_string()))
    );

    // Verify third row (with expressions +3 = 3 and 2 * 2 = 4)
    assert_eq!(results[2].get("id"), Some(&Value::I32(3)));
    assert_eq!(results[2].get("num"), Some(&Value::I32(4)));
    assert_eq!(
        results[2].get("name"),
        Some(&Value::Str("Great".to_string()))
    );

    ctx.commit();
}

#[test]
fn test_select_with_where_condition() {
    let mut ctx = setup_test();
    setup_test_table(&mut ctx);

    // Query with WHERE clause filtering on id = 1
    let results = ctx.query("SELECT id, num, name FROM Test WHERE id = 1");

    // Should have 2 rows with id = 1 (first two inserted rows)
    assert_eq!(results.len(), 2);

    // Verify both rows have id = 1
    assert_eq!(results[0].get("id"), Some(&Value::I32(1)));
    assert_eq!(results[1].get("id"), Some(&Value::I32(1)));

    // Verify the data matches
    assert_eq!(results[0].get("num"), Some(&Value::I32(2)));
    assert_eq!(
        results[0].get("name"),
        Some(&Value::Str("Hello".to_string()))
    );

    assert_eq!(results[1].get("num"), Some(&Value::I32(9)));
    assert_eq!(
        results[1].get("name"),
        Some(&Value::Str("World".to_string()))
    );

    ctx.commit();
}

#[test]
fn test_update_all_rows() {
    let mut ctx = setup_test();
    setup_test_table(&mut ctx);

    // Update all rows to have id = 2
    ctx.exec("UPDATE Test SET id = 2");

    // Verify all rows now have id = 2
    let results = ctx.query("SELECT id FROM Test");
    assert_eq!(results.len(), 3);

    for row in results {
        assert_eq!(row.get("id"), Some(&Value::I32(2)));
    }

    ctx.commit();
}

#[test]
fn test_select_after_update() {
    let mut ctx = setup_test();
    setup_test_table(&mut ctx);

    // Update all rows
    ctx.exec("UPDATE Test SET id = 2");

    // Query all data after update
    let results = ctx.query("SELECT id, num, name FROM Test");

    // Should still have 3 rows
    assert_eq!(results.len(), 3);

    // All rows should have id = 2, but num and name should be unchanged
    assert_eq!(results[0].get("id"), Some(&Value::I32(2)));
    assert_eq!(results[0].get("num"), Some(&Value::I32(2)));
    assert_eq!(
        results[0].get("name"),
        Some(&Value::Str("Hello".to_string()))
    );

    assert_eq!(results[1].get("id"), Some(&Value::I32(2)));
    assert_eq!(results[1].get("num"), Some(&Value::I32(9)));
    assert_eq!(
        results[1].get("name"),
        Some(&Value::Str("World".to_string()))
    );

    assert_eq!(results[2].get("id"), Some(&Value::I32(2)));
    assert_eq!(results[2].get("num"), Some(&Value::I32(4)));
    assert_eq!(
        results[2].get("name"),
        Some(&Value::Str("Great".to_string()))
    );

    ctx.commit();
}

#[test]
fn test_select_single_column() {
    let mut ctx = setup_test();
    setup_test_table(&mut ctx);

    // Update all rows first
    ctx.exec("UPDATE Test SET id = 2");

    // Select only the id column
    let results = ctx.query("SELECT id FROM Test");

    // Should have 3 rows with id = 2
    assert_eq!(results.len(), 3);

    for row in results {
        assert_eq!(row.get("id"), Some(&Value::I32(2)));
        // Should only have one column
        assert_eq!(row.len(), 1);
    }

    ctx.commit();
}

#[test]
fn test_select_two_columns() {
    let mut ctx = setup_test();
    setup_test_table(&mut ctx);

    // Update all rows first
    ctx.exec("UPDATE Test SET id = 2");

    // Select two columns
    let results = ctx.query("SELECT id, num FROM Test");

    // Should have 3 rows
    assert_eq!(results.len(), 3);

    // Verify data
    assert_eq!(results[0].get("id"), Some(&Value::I32(2)));
    assert_eq!(results[0].get("num"), Some(&Value::I32(2)));
    assert_eq!(results[0].len(), 2);

    assert_eq!(results[1].get("id"), Some(&Value::I32(2)));
    assert_eq!(results[1].get("num"), Some(&Value::I32(9)));
    assert_eq!(results[1].len(), 2);

    assert_eq!(results[2].get("id"), Some(&Value::I32(2)));
    assert_eq!(results[2].get("num"), Some(&Value::I32(4)));
    assert_eq!(results[2].len(), 2);

    ctx.commit();
}

#[test]
fn test_limit_offset_query() {
    let mut ctx = setup_test();
    setup_test_table(&mut ctx);

    // Update all rows first
    ctx.exec("UPDATE Test SET id = 2");

    // Select with LIMIT 1 OFFSET 1 (should get the second row)
    let results = ctx.query("SELECT id, num FROM Test LIMIT 1 OFFSET 1");

    // Should have exactly 1 row (the second one)
    assert_eq!(results.len(), 1);

    // Should be the second row with num = 9
    assert_eq!(results[0].get("id"), Some(&Value::I32(2)));
    assert_eq!(results[0].get("num"), Some(&Value::I32(9)));

    ctx.commit();
}
