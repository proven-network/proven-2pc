//! Tests for DISTINCT clause functionality
//! Based on gluesql/test-suite/src/distinct.rs

mod common;

use common::{TableBuilder, setup_test};
use proven_value::Value;
#[test]
#[ignore = "DISTINCT not yet implemented"]
fn test_distinct_single_column() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "DistinctTest")
        .create_simple("id INTEGER, category TEXT, value INTEGER")
        .insert_values(
            "(1, 'A', 100),
             (2, 'B', 200),
             (3, 'A', 150),
             (4, 'C', 300),
             (5, 'B', 250),
             (6, 'A', 100)",
        );

    // Test DISTINCT on single column
    let results = ctx.query("SELECT DISTINCT category FROM DistinctTest ORDER BY category");
    assert_eq!(results.len(), 3);
    assert_eq!(
        results[0].get("category").unwrap(),
        &Value::Str("\"A\"".to_string())
    );
    assert_eq!(
        results[1].get("category").unwrap(),
        &Value::Str("\"B\"".to_string())
    );
    assert_eq!(
        results[2].get("category").unwrap(),
        &Value::Str("\"C\"".to_string())
    );

    ctx.commit();
}

#[test]
#[ignore = "DISTINCT not yet implemented"]
fn test_distinct_multiple_columns() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "DistinctTest")
        .create_simple("category TEXT, subcategory TEXT, value INTEGER")
        .insert_values(
            "('A', 'X', 100),
             ('A', 'Y', 200),
             ('A', 'X', 150),
             ('B', 'X', 300),
             ('B', 'Y', 250),
             ('A', 'X', 100)",
        );

    // Test DISTINCT on multiple columns
    let results = ctx.query(
        "SELECT DISTINCT category, subcategory FROM DistinctTest ORDER BY category, subcategory",
    );
    assert_eq!(results.len(), 4);
    // Should have: (A,X), (A,Y), (B,X), (B,Y)

    ctx.commit();
}

#[test]
#[ignore = "DISTINCT not yet implemented"]
fn test_distinct_with_null_values() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "NullTest")
        .create_simple("id INTEGER, value INTEGER NULL")
        .insert_values(
            "(1, 10),
             (2, NULL),
             (3, 20),
             (4, 10),
             (5, NULL),
             (6, 30)",
        );

    // Test DISTINCT with NULL values
    let results = ctx.query("SELECT DISTINCT value FROM NullTest ORDER BY value");
    assert_eq!(results.len(), 4); // NULL, 10, 20, 30
    assert_eq!(results[0].get("value").unwrap(), &Value::Null);
    assert_eq!(results[1].get("value").unwrap(), &Value::I64(10));
    assert_eq!(results[2].get("value").unwrap(), &Value::I64(20));
    assert_eq!(results[3].get("value").unwrap(), &Value::I64(30));

    ctx.commit();
}

#[test]
#[ignore = "DISTINCT not yet implemented"]
fn test_distinct_all_columns() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "DistinctTest")
        .create_simple("id INTEGER, name TEXT, value INTEGER")
        .insert_values(
            "(1, 'Alice', 100),
             (2, 'Bob', 200),
             (1, 'Alice', 100),
             (3, 'Charlie', 300),
             (2, 'Bob', 200)",
        );

    // Test DISTINCT * (all columns)
    let results = ctx.query("SELECT DISTINCT * FROM DistinctTest ORDER BY id");
    assert_eq!(results.len(), 3);

    ctx.commit();
}

#[test]
#[ignore = "DISTINCT not yet implemented"]
fn test_distinct_with_where_clause() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "DistinctTest")
        .create_simple("id INTEGER, category TEXT, value INTEGER")
        .insert_values(
            "(1, 'A', 100),
             (2, 'B', 200),
             (3, 'A', 150),
             (4, 'C', 300),
             (5, 'B', 250),
             (6, 'A', 100),
             (7, 'C', 350)",
        );

    // Test DISTINCT with WHERE clause
    let results = ctx.query("SELECT DISTINCT category FROM DistinctTest WHERE value > 150");
    assert_eq!(results.len(), 2); // B and C

    ctx.commit();
}

#[test]
#[ignore = "DISTINCT not yet implemented"]
fn test_distinct_with_order_by() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "DistinctTest")
        .create_simple("id INTEGER, name TEXT")
        .insert_values(
            "(1, 'Zebra'),
             (2, 'Apple'),
             (3, 'Zebra'),
             (4, 'Banana'),
             (5, 'Apple'),
             (6, 'Cherry')",
        );

    // Test DISTINCT with ORDER BY
    let results = ctx.query("SELECT DISTINCT name FROM DistinctTest ORDER BY name ASC");
    assert_eq!(results.len(), 4);
    assert_eq!(
        results[0].get("name").unwrap(),
        &Value::Str("\"Apple\"".to_string())
    );
    assert_eq!(
        results[1].get("name").unwrap(),
        &Value::Str("\"Banana\"".to_string())
    );
    assert_eq!(
        results[2].get("name").unwrap(),
        &Value::Str("\"Cherry\"".to_string())
    );
    assert_eq!(
        results[3].get("name").unwrap(),
        &Value::Str("\"Zebra\"".to_string())
    );

    ctx.commit();
}

#[test]
#[ignore = "DISTINCT not yet implemented"]
fn test_distinct_with_limit() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "DistinctTest")
        .create_simple("category TEXT")
        .insert_values("('A'), ('B'), ('A'), ('C'), ('B'), ('D'), ('E'), ('A')");

    // Test DISTINCT with LIMIT
    let results = ctx.query("SELECT DISTINCT category FROM DistinctTest ORDER BY category LIMIT 3");
    assert_eq!(results.len(), 3);
    assert_eq!(
        results[0].get("category").unwrap(),
        &Value::Str("\"A\"".to_string())
    );
    assert_eq!(
        results[1].get("category").unwrap(),
        &Value::Str("\"B\"".to_string())
    );
    assert_eq!(
        results[2].get("category").unwrap(),
        &Value::Str("\"C\"".to_string())
    );

    ctx.commit();
}

#[test]
#[ignore = "DISTINCT not yet implemented"]
fn test_distinct_with_expressions() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "DistinctTest")
        .create_simple("a INTEGER, b INTEGER")
        .insert_values(
            "(1, 9),
             (2, 8),
             (3, 7),
             (1, 9),
             (4, 6),
             (2, 8)",
        );

    // Test DISTINCT with expressions
    let results = ctx.query("SELECT DISTINCT a + b AS sum FROM DistinctTest ORDER BY sum");
    assert_eq!(results.len(), 4); // 10, 10, 10, 10 becomes just 10
    assert_eq!(results[0].get("sum").unwrap(), &Value::I64(10));

    ctx.commit();
}

#[test]
#[ignore = "DISTINCT not yet implemented"]
fn test_distinct_on_boolean() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "BoolTest")
        .create_simple("id INTEGER, active BOOLEAN")
        .insert_values(
            "(1, TRUE),
             (2, FALSE),
             (3, TRUE),
             (4, FALSE),
             (5, TRUE),
             (6, NULL)",
        );

    // Test DISTINCT on boolean column
    let results = ctx.query("SELECT DISTINCT active FROM BoolTest ORDER BY active");
    assert_eq!(results.len(), 3); // NULL, FALSE, TRUE
    assert_eq!(results[0].get("active").unwrap(), &Value::Null);
    assert_eq!(results[1].get("active").unwrap(), &Value::Bool(false));
    assert_eq!(results[2].get("active").unwrap(), &Value::Bool(true));

    ctx.commit();
}

#[test]
#[ignore = "DISTINCT not yet implemented"]
fn test_distinct_count() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "DistinctTest")
        .create_simple("category TEXT")
        .insert_values("('A'), ('B'), ('A'), ('C'), ('B'), ('A')");

    // Test COUNT(DISTINCT)
    let results = ctx.query("SELECT COUNT(DISTINCT category) AS unique_count FROM DistinctTest");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("unique_count").unwrap(), &Value::I64(3));

    ctx.commit();
}

#[test]
#[ignore = "DISTINCT not yet implemented"]
fn test_distinct_with_group_by() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Sales")
        .create_simple("region TEXT, product TEXT, amount INTEGER")
        .insert_values(
            "('North', 'A', 100),
             ('North', 'B', 200),
             ('North', 'A', 100),
             ('South', 'A', 150),
             ('South', 'B', 250),
             ('South', 'A', 150)",
        );

    // Test DISTINCT with GROUP BY
    let results = ctx.query(
        "SELECT region, COUNT(DISTINCT product) as product_count
         FROM Sales
         GROUP BY region
         ORDER BY region",
    );
    assert_eq!(results.len(), 2);
    assert_eq!(
        results[0].get("region").unwrap(),
        &Value::Str("\"North\"".to_string())
    );
    assert_eq!(results[0].get("product_count").unwrap(), &Value::I64(2));
    assert_eq!(
        results[1].get("region").unwrap(),
        &Value::Str("\"South\"".to_string())
    );
    assert_eq!(results[1].get("product_count").unwrap(), &Value::I64(2));

    ctx.commit();
}

#[test]
#[ignore = "DISTINCT not yet implemented"]
fn test_distinct_empty_table() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "EmptyTest").create_simple("id INTEGER, value TEXT");

    // Test DISTINCT on empty table
    let results = ctx.query("SELECT DISTINCT value FROM EmptyTest");
    assert_eq!(results.len(), 0);

    ctx.commit();
}

#[test]
#[ignore = "DISTINCT not yet implemented"]
fn test_distinct_single_row() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "SingleRow")
        .create_simple("id INTEGER, value TEXT")
        .insert_values("(1, 'Only')");

    // Test DISTINCT with single row
    let results = ctx.query("SELECT DISTINCT value FROM SingleRow");
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].get("value").unwrap(),
        &Value::Str("\"Only\"".to_string())
    );

    ctx.commit();
}

#[test]
#[ignore = "DISTINCT ON not yet implemented"]
fn test_distinct_on() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "DistinctOnTest")
        .create_simple("category TEXT, item TEXT, price INTEGER")
        .insert_values(
            "('A', 'Item1', 100),
             ('A', 'Item2', 150),
             ('B', 'Item3', 200),
             ('B', 'Item4', 180),
             ('C', 'Item5', 300)",
        );

    // Test DISTINCT ON (PostgreSQL extension)
    let results = ctx.query(
        "SELECT DISTINCT ON (category) category, item, price
         FROM DistinctOnTest
         ORDER BY category, price DESC",
    );
    assert_eq!(results.len(), 3);
    // Should get the highest priced item from each category

    ctx.commit();
}
