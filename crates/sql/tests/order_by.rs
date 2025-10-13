//! Tests for ORDER BY clause functionality
//! Based on gluesql/test-suite/src/ordering.rs

mod common;

use common::{TableBuilder, setup_test};
use proven_value::Value;
#[test]
fn test_order_by_single_column_asc() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "OrderTest")
        .create_simple("id INTEGER, value INTEGER, name TEXT")
        .insert_values(
            "(3, 30, 'Charlie'),
             (1, 10, 'Alice'),
             (2, 20, 'Bob'),
             (5, 50, 'Eve'),
             (4, 40, 'David')",
        );

    // Test ORDER BY ascending (explicit)
    let results = ctx.query("SELECT id FROM OrderTest ORDER BY id ASC");
    assert_eq!(results.len(), 5);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(2));
    assert_eq!(results[2].get("id").unwrap(), &Value::I32(3));
    assert_eq!(results[3].get("id").unwrap(), &Value::I32(4));
    assert_eq!(results[4].get("id").unwrap(), &Value::I32(5));

    ctx.commit();
}

#[test]
fn test_order_by_single_column_desc() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "OrderTest")
        .create_simple("id INTEGER, value INTEGER, name TEXT")
        .insert_values(
            "(3, 30, 'Charlie'),
             (1, 10, 'Alice'),
             (2, 20, 'Bob'),
             (5, 50, 'Eve'),
             (4, 40, 'David')",
        );

    // Test ORDER BY descending
    let results = ctx.query("SELECT id FROM OrderTest ORDER BY id DESC");
    assert_eq!(results.len(), 5);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(5));
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(4));
    assert_eq!(results[2].get("id").unwrap(), &Value::I32(3));
    assert_eq!(results[3].get("id").unwrap(), &Value::I32(2));
    assert_eq!(results[4].get("id").unwrap(), &Value::I32(1));

    ctx.commit();
}

#[test]
fn test_order_by_default_ascending() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "OrderTest")
        .create_simple("id INTEGER, value INTEGER")
        .insert_values("(3, 30), (1, 10), (2, 20)");

    // Test ORDER BY without ASC/DESC (should default to ASC)
    let results = ctx.query("SELECT id FROM OrderTest ORDER BY id");
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(2));
    assert_eq!(results[2].get("id").unwrap(), &Value::I32(3));

    ctx.commit();
}

#[test]
fn test_order_by_text_column() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "OrderTest")
        .create_simple("id INTEGER, name TEXT")
        .insert_values(
            "(1, 'Zebra'),
             (2, 'Apple'),
             (3, 'Mango'),
             (4, 'Banana'),
             (5, 'Cherry')",
        );

    // Test ORDER BY text column
    let results = ctx.query("SELECT name FROM OrderTest ORDER BY name ASC");
    assert_eq!(results.len(), 5);
    assert_eq!(
        results[0].get("name").unwrap(),
        &Value::Str("Apple".to_string())
    );
    assert_eq!(
        results[1].get("name").unwrap(),
        &Value::Str("Banana".to_string())
    );
    assert_eq!(
        results[2].get("name").unwrap(),
        &Value::Str("Cherry".to_string())
    );
    assert_eq!(
        results[3].get("name").unwrap(),
        &Value::Str("Mango".to_string())
    );
    assert_eq!(
        results[4].get("name").unwrap(),
        &Value::Str("Zebra".to_string())
    );

    ctx.commit();
}

#[test]
fn test_order_by_multiple_columns() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "OrderTest")
        .create_simple("category TEXT, value INTEGER, name TEXT")
        .insert_values(
            "('B', 20, 'Item2'),
             ('A', 30, 'Item3'),
             ('B', 10, 'Item1'),
             ('A', 10, 'Item4'),
             ('A', 20, 'Item5')",
        );

    // Test ORDER BY multiple columns
    let results =
        ctx.query("SELECT category, value, name FROM OrderTest ORDER BY category ASC, value DESC");
    assert_eq!(results.len(), 5);

    // Category A sorted by value DESC
    assert_eq!(
        results[0].get("category").unwrap(),
        &Value::Str("A".to_string())
    );
    assert_eq!(results[0].get("value").unwrap(), &Value::I32(30));
    assert_eq!(
        results[1].get("category").unwrap(),
        &Value::Str("A".to_string())
    );
    assert_eq!(results[1].get("value").unwrap(), &Value::I32(20));
    assert_eq!(
        results[2].get("category").unwrap(),
        &Value::Str("A".to_string())
    );
    assert_eq!(results[2].get("value").unwrap(), &Value::I32(10));

    // Category B sorted by value DESC
    assert_eq!(
        results[3].get("category").unwrap(),
        &Value::Str("B".to_string())
    );
    assert_eq!(results[3].get("value").unwrap(), &Value::I32(20));
    assert_eq!(
        results[4].get("category").unwrap(),
        &Value::Str("B".to_string())
    );
    assert_eq!(results[4].get("value").unwrap(), &Value::I32(10));

    ctx.commit();
}

#[test]
fn test_order_by_with_where_clause() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "OrderTest")
        .create_simple("id INTEGER, value INTEGER, active BOOLEAN")
        .insert_values(
            "(1, 50, TRUE),
             (2, 30, FALSE),
             (3, 40, TRUE),
             (4, 20, TRUE),
             (5, 60, FALSE),
             (6, 10, TRUE)",
        );

    // Test ORDER BY with WHERE clause
    let results =
        ctx.query("SELECT id, value FROM OrderTest WHERE active = TRUE ORDER BY value DESC");
    assert_eq!(results.len(), 4);
    assert_eq!(results[0].get("value").unwrap(), &Value::I32(50));
    assert_eq!(results[1].get("value").unwrap(), &Value::I32(40));
    assert_eq!(results[2].get("value").unwrap(), &Value::I32(20));
    assert_eq!(results[3].get("value").unwrap(), &Value::I32(10));

    ctx.commit();
}

#[test]
fn test_order_by_with_null_values() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "NullTest")
        .create_simple("id INTEGER, value INTEGER NULL")
        .insert_values(
            "(1, 30),
             (2, NULL),
             (3, 10),
             (4, NULL),
             (5, 20)",
        );

    // Test ORDER BY with NULL values - SQL standard: NULLs come last in ASC
    let results = ctx.query("SELECT id, value FROM NullTest ORDER BY value ASC");
    assert_eq!(results.len(), 5);
    assert_eq!(results[0].get("value").unwrap(), &Value::I32(10));
    assert_eq!(results[1].get("value").unwrap(), &Value::I32(20));
    assert_eq!(results[2].get("value").unwrap(), &Value::I32(30));
    assert_eq!(results[3].get("value").unwrap(), &Value::Null);
    assert_eq!(results[4].get("value").unwrap(), &Value::Null);

    // Test ORDER BY with NULL values DESC - SQL standard: NULLs come first
    let results = ctx.query("SELECT id, value FROM NullTest ORDER BY value DESC");
    assert_eq!(results.len(), 5);
    assert_eq!(results[0].get("value").unwrap(), &Value::Null);
    assert_eq!(results[1].get("value").unwrap(), &Value::Null);
    assert_eq!(results[2].get("value").unwrap(), &Value::I32(30));
    assert_eq!(results[3].get("value").unwrap(), &Value::I32(20));
    assert_eq!(results[4].get("value").unwrap(), &Value::I32(10));

    ctx.commit();
}

#[test]
fn test_order_by_with_expressions() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "OrderTest")
        .create_simple("id INTEGER, a INTEGER, b INTEGER")
        .insert_values(
            "(1, 10, 5),
             (2, 20, 8),
             (3, 15, 10),
             (4, 5, 15),
             (5, 30, 2)",
        );

    // Test ORDER BY with expression
    let results = ctx.query("SELECT id, a, b, a + b AS sum FROM OrderTest ORDER BY a + b DESC");
    assert_eq!(results.len(), 5);
    assert_eq!(results[0].get("sum").unwrap(), &Value::I32(32)); // 30 + 2
    assert_eq!(results[1].get("sum").unwrap(), &Value::I32(28)); // 20 + 8
    assert_eq!(results[2].get("sum").unwrap(), &Value::I32(25)); // 15 + 10
    assert_eq!(results[3].get("sum").unwrap(), &Value::I32(20)); // 5 + 15
    assert_eq!(results[4].get("sum").unwrap(), &Value::I32(15)); // 10 + 5

    ctx.commit();
}

#[test]
fn test_order_by_boolean_column() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "OrderTest")
        .create_simple("id INTEGER, active BOOLEAN")
        .insert_values(
            "(1, TRUE),
             (2, FALSE),
             (3, TRUE),
             (4, FALSE),
             (5, TRUE)",
        );

    // Test ORDER BY boolean (FALSE < TRUE)
    let results = ctx.query("SELECT id, active FROM OrderTest ORDER BY active ASC");
    assert_eq!(results.len(), 5);
    assert_eq!(results[0].get("active").unwrap(), &Value::Bool(false));
    assert_eq!(results[1].get("active").unwrap(), &Value::Bool(false));
    assert_eq!(results[2].get("active").unwrap(), &Value::Bool(true));
    assert_eq!(results[3].get("active").unwrap(), &Value::Bool(true));
    assert_eq!(results[4].get("active").unwrap(), &Value::Bool(true));

    ctx.commit();
}

#[test]
fn test_order_by_float_column() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "OrderTest")
        .create_simple("id INTEGER, score FLOAT")
        .insert_values(
            "(1, 95.5),
             (2, 87.3),
             (3, 92.8),
             (4, 88.1),
             (5, 90.0)",
        );

    // Test ORDER BY float values - just check the order is correct
    let results = ctx.query("SELECT id FROM OrderTest ORDER BY score DESC");
    assert_eq!(results.len(), 5);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1)); // 95.5
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(3)); // 92.8
    assert_eq!(results[2].get("id").unwrap(), &Value::I32(5)); // 90.0
    assert_eq!(results[3].get("id").unwrap(), &Value::I32(4)); // 88.1
    assert_eq!(results[4].get("id").unwrap(), &Value::I32(2)); // 87.3

    ctx.commit();
}

#[test]
fn test_order_by_with_limit() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "OrderTest")
        .create_simple("id INTEGER, value INTEGER")
        .insert_values(
            "(1, 50),
             (2, 30),
             (3, 70),
             (4, 20),
             (5, 60),
             (6, 40),
             (7, 10)",
        );

    // Test ORDER BY with LIMIT
    let results = ctx.query("SELECT id, value FROM OrderTest ORDER BY value DESC LIMIT 3");
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].get("value").unwrap(), &Value::I32(70));
    assert_eq!(results[1].get("value").unwrap(), &Value::I32(60));
    assert_eq!(results[2].get("value").unwrap(), &Value::I32(50));

    ctx.commit();
}

#[test]
fn test_order_by_with_duplicate_values() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "OrderTest")
        .create_simple("id INTEGER, category TEXT, value INTEGER")
        .insert_values(
            "(1, 'A', 100),
             (2, 'B', 100),
             (3, 'C', 100),
             (4, 'A', 200),
             (5, 'B', 200)",
        );

    // When ordering by value alone, rows with same value maintain stable order
    let results = ctx.query("SELECT id, value FROM OrderTest ORDER BY value ASC");
    assert_eq!(results.len(), 5);

    // All value=100 rows should come first
    assert_eq!(results[0].get("value").unwrap(), &Value::I32(100));
    assert_eq!(results[1].get("value").unwrap(), &Value::I32(100));
    assert_eq!(results[2].get("value").unwrap(), &Value::I32(100));

    // Then value=200 rows
    assert_eq!(results[3].get("value").unwrap(), &Value::I32(200));
    assert_eq!(results[4].get("value").unwrap(), &Value::I32(200));

    ctx.commit();
}

#[test]
#[ignore = "ORDER BY column position not yet implemented"]
fn test_order_by_column_position() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "OrderTest")
        .create_simple("id INTEGER, name TEXT, value INTEGER")
        .insert_values(
            "(1, 'Alice', 30),
             (2, 'Bob', 20),
             (3, 'Charlie', 10)",
        );

    // Test ORDER BY column position (ORDER BY 2 means second column)
    let results = ctx.query("SELECT id, name, value FROM OrderTest ORDER BY 3 DESC");
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].get("value").unwrap(), &Value::I32(30));
    assert_eq!(results[1].get("value").unwrap(), &Value::I32(20));
    assert_eq!(results[2].get("value").unwrap(), &Value::I32(10));

    ctx.commit();
}

#[test]
#[ignore = "ORDER BY with CASE expression not yet implemented"]
fn test_order_by_case_expression() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "OrderTest")
        .create_simple("id INTEGER, status TEXT")
        .insert_values(
            "(1, 'pending'),
             (2, 'completed'),
             (3, 'failed'),
             (4, 'pending'),
             (5, 'completed')",
        );

    // Test ORDER BY with CASE expression
    let results = ctx.query(
        "SELECT id, status FROM OrderTest
         ORDER BY CASE
           WHEN status = 'failed' THEN 1
           WHEN status = 'pending' THEN 2
           WHEN status = 'completed' THEN 3
         END ASC",
    );

    assert_eq!(results.len(), 5);
    assert_eq!(
        results[0].get("status").unwrap(),
        &Value::Str("\"failed\"".to_string())
    );
    assert_eq!(
        results[1].get("status").unwrap(),
        &Value::Str("\"pending\"".to_string())
    );
    assert_eq!(
        results[2].get("status").unwrap(),
        &Value::Str("\"pending\"".to_string())
    );

    ctx.commit();
}
