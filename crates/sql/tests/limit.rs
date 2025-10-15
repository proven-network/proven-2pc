//! Tests for LIMIT and OFFSET clause functionality
//! Based on gluesql/test-suite/src/limit.rs

mod common;

use common::{TableBuilder, setup_test};
use proven_value::Value;
fn setup_limit_test_table(ctx: &mut common::TestContext) {
    TableBuilder::new(ctx, "LimitTest")
        .create_simple("id INTEGER, name TEXT, value INTEGER")
        .insert_values(
            "(1, 'Item1', 10),
             (2, 'Item2', 20),
             (3, 'Item3', 30),
             (4, 'Item4', 40),
             (5, 'Item5', 50),
             (6, 'Item6', 60),
             (7, 'Item7', 70),
             (8, 'Item8', 80),
             (9, 'Item9', 90),
             (10, 'Item10', 100)",
        );
}

#[test]
fn test_limit_basic() {
    let mut ctx = setup_test();
    setup_limit_test_table(&mut ctx);

    // Test basic LIMIT
    let results = ctx.query("SELECT * FROM LimitTest LIMIT 3");
    assert_eq!(results.len(), 3);

    ctx.commit();
}

#[test]
fn test_limit_with_order_by() {
    let mut ctx = setup_test();
    setup_limit_test_table(&mut ctx);

    // Test LIMIT with ORDER BY
    let results = ctx.query("SELECT id, value FROM LimitTest ORDER BY value DESC LIMIT 3");
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].get("value").unwrap(), &Value::I32(100));
    assert_eq!(results[1].get("value").unwrap(), &Value::I32(90));
    assert_eq!(results[2].get("value").unwrap(), &Value::I32(80));

    ctx.commit();
}

#[test]
fn test_limit_zero() {
    let mut ctx = setup_test();
    setup_limit_test_table(&mut ctx);

    // Test LIMIT 0 (should return no rows)
    let results = ctx.query("SELECT * FROM LimitTest LIMIT 0");
    assert_eq!(results.len(), 0);

    ctx.commit();
}

#[test]
fn test_limit_larger_than_table() {
    let mut ctx = setup_test();
    setup_limit_test_table(&mut ctx);

    // Test LIMIT larger than table size
    let results = ctx.query("SELECT * FROM LimitTest LIMIT 20");
    assert_eq!(results.len(), 10); // Table only has 10 rows

    ctx.commit();
}

#[test]
fn test_offset_basic() {
    let mut ctx = setup_test();
    setup_limit_test_table(&mut ctx);

    // Test basic OFFSET
    let results = ctx.query("SELECT id FROM LimitTest ORDER BY id ASC OFFSET 5");
    assert_eq!(results.len(), 5);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(6));
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(7));
    assert_eq!(results[2].get("id").unwrap(), &Value::I32(8));
    assert_eq!(results[3].get("id").unwrap(), &Value::I32(9));
    assert_eq!(results[4].get("id").unwrap(), &Value::I32(10));

    ctx.commit();
}

#[test]
fn test_limit_with_offset() {
    let mut ctx = setup_test();
    setup_limit_test_table(&mut ctx);

    // Test LIMIT with OFFSET (pagination)
    let results = ctx.query("SELECT id, name FROM LimitTest ORDER BY id ASC LIMIT 3 OFFSET 3");
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(4));
    assert_eq!(
        results[0].get("name").unwrap(),
        &Value::Str("Item4".to_string())
    );
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(5));
    assert_eq!(
        results[1].get("name").unwrap(),
        &Value::Str("Item5".to_string())
    );
    assert_eq!(results[2].get("id").unwrap(), &Value::I32(6));
    assert_eq!(
        results[2].get("name").unwrap(),
        &Value::Str("Item6".to_string())
    );

    ctx.commit();
}

#[test]
fn test_offset_zero() {
    let mut ctx = setup_test();
    setup_limit_test_table(&mut ctx);

    // Test OFFSET 0 (should return all rows)
    let results = ctx.query("SELECT * FROM LimitTest ORDER BY id ASC OFFSET 0");
    assert_eq!(results.len(), 10);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));

    ctx.commit();
}

#[test]
fn test_offset_larger_than_table() {
    let mut ctx = setup_test();
    setup_limit_test_table(&mut ctx);

    // Test OFFSET larger than table size
    let results = ctx.query("SELECT * FROM LimitTest OFFSET 15");
    assert_eq!(results.len(), 0); // No rows left after offset

    ctx.commit();
}

#[test]
fn test_pagination_scenario() {
    let mut ctx = setup_test();
    setup_limit_test_table(&mut ctx);

    // Test pagination - Page 1
    let page1 = ctx.query("SELECT id, name FROM LimitTest ORDER BY id ASC LIMIT 3 OFFSET 0");
    assert_eq!(page1.len(), 3);
    assert_eq!(page1[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(page1[2].get("id").unwrap(), &Value::I32(3));

    // Page 2
    let page2 = ctx.query("SELECT id, name FROM LimitTest ORDER BY id ASC LIMIT 3 OFFSET 3");
    assert_eq!(page2.len(), 3);
    assert_eq!(page2[0].get("id").unwrap(), &Value::I32(4));
    assert_eq!(page2[2].get("id").unwrap(), &Value::I32(6));

    // Page 3
    let page3 = ctx.query("SELECT id, name FROM LimitTest ORDER BY id ASC LIMIT 3 OFFSET 6");
    assert_eq!(page3.len(), 3);
    assert_eq!(page3[0].get("id").unwrap(), &Value::I32(7));
    assert_eq!(page3[2].get("id").unwrap(), &Value::I32(9));

    // Page 4 (partial page)
    let page4 = ctx.query("SELECT id, name FROM LimitTest ORDER BY id ASC LIMIT 3 OFFSET 9");
    assert_eq!(page4.len(), 1); // Only 1 row left
    assert_eq!(page4[0].get("id").unwrap(), &Value::I32(10));

    ctx.commit();
}

#[test]
fn test_limit_offset_with_where() {
    let mut ctx = setup_test();
    setup_limit_test_table(&mut ctx);

    // Test LIMIT/OFFSET with WHERE clause
    let results = ctx.query(
        "SELECT id, value FROM LimitTest WHERE value >= 50 ORDER BY value ASC LIMIT 2 OFFSET 1",
    );
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("value").unwrap(), &Value::I32(60));
    assert_eq!(results[1].get("value").unwrap(), &Value::I32(70));

    ctx.commit();
}

#[test]
fn test_limit_offset_with_complex_query() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "ComplexTest")
        .create_simple("category TEXT, item TEXT, price INTEGER")
        .insert_values(
            "('A', 'Item1', 100),
             ('B', 'Item2', 200),
             ('A', 'Item3', 150),
             ('B', 'Item4', 250),
             ('A', 'Item5', 120),
             ('C', 'Item6', 300),
             ('A', 'Item7', 180),
             ('B', 'Item8', 220)",
        );

    // Complex query with WHERE, ORDER BY, LIMIT, OFFSET
    let results = ctx.query(
        "SELECT item, price FROM ComplexTest
         WHERE category = 'A'
         ORDER BY price DESC
         LIMIT 2 OFFSET 1",
    );

    assert_eq!(results.len(), 2);
    assert_eq!(
        results[0].get("item").unwrap(),
        &Value::Str("Item3".to_string())
    );
    assert_eq!(results[0].get("price").unwrap(), &Value::I32(150));
    assert_eq!(
        results[1].get("item").unwrap(),
        &Value::Str("Item5".to_string())
    );
    assert_eq!(results[1].get("price").unwrap(), &Value::I32(120));

    ctx.commit();
}

#[test]
fn test_limit_one() {
    let mut ctx = setup_test();
    setup_limit_test_table(&mut ctx);

    // Test LIMIT 1 (common use case)
    let results = ctx.query("SELECT * FROM LimitTest ORDER BY value DESC LIMIT 1");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(10));
    assert_eq!(results[0].get("value").unwrap(), &Value::I32(100));

    ctx.commit();
}

#[test]
fn test_top_n_queries() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Scores")
        .create_simple("player TEXT, score INTEGER")
        .insert_values(
            "('Alice', 95),
             ('Bob', 87),
             ('Charlie', 92),
             ('David', 88),
             ('Eve', 96),
             ('Frank', 90)",
        );

    // Top 3 scores
    let results = ctx.query("SELECT player, score FROM Scores ORDER BY score DESC LIMIT 3");
    assert_eq!(results.len(), 3);
    assert_eq!(
        results[0].get("player").unwrap(),
        &Value::Str("Eve".to_string())
    );
    assert_eq!(results[0].get("score").unwrap(), &Value::I32(96));
    assert_eq!(
        results[1].get("player").unwrap(),
        &Value::Str("Alice".to_string())
    );
    assert_eq!(results[1].get("score").unwrap(), &Value::I32(95));
    assert_eq!(
        results[2].get("player").unwrap(),
        &Value::Str("Charlie".to_string())
    );
    assert_eq!(results[2].get("score").unwrap(), &Value::I32(92));

    ctx.commit();
}

#[test]
fn test_limit_with_null_values() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "NullTest")
        .create_simple("id INTEGER, value INTEGER NULL")
        .insert_values(
            "(1, 10),
             (2, NULL),
             (3, 30),
             (4, NULL),
             (5, 50)",
        );

    // LIMIT with NULL values in ORDER BY
    let results = ctx.query("SELECT id, value FROM NullTest ORDER BY value ASC LIMIT 3");
    assert_eq!(results.len(), 3);
    // SQL standard: NULLs come last in ASC order
    assert_eq!(results[0].get("value").unwrap(), &Value::I32(10));
    assert_eq!(results[1].get("value").unwrap(), &Value::I32(30));
    assert_eq!(results[2].get("value").unwrap(), &Value::I32(50));

    ctx.commit();
}

#[test]
fn test_offset_with_limit_all() {
    let mut ctx = setup_test();
    setup_limit_test_table(&mut ctx);

    // Test OFFSET with very large LIMIT
    let results = ctx.query("SELECT id FROM LimitTest ORDER BY id ASC LIMIT 1000 OFFSET 7");
    assert_eq!(results.len(), 3); // Only 3 rows left after offset
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(8));
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(9));
    assert_eq!(results[2].get("id").unwrap(), &Value::I32(10));

    ctx.commit();
}

#[test]
fn test_limit_with_expressions() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "ExprTest")
        .create_simple("a INTEGER, b INTEGER")
        .insert_values(
            "(1, 9),
             (2, 8),
             (3, 7),
             (4, 6),
             (5, 5)",
        );

    // Test LIMIT with expressions in SELECT
    let results = ctx.query("SELECT a, b, a + b AS sum FROM ExprTest ORDER BY a + b DESC LIMIT 2");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("sum").unwrap(), &Value::I32(10)); // 1 + 9
    assert_eq!(results[1].get("sum").unwrap(), &Value::I32(10)); // 2 + 8 or 3 + 7

    ctx.commit();
}

#[test]
fn test_limit_in_subquery() {
    let mut ctx = setup_test();
    setup_limit_test_table(&mut ctx);

    // Test LIMIT in subquery
    let results = ctx.query(
        "SELECT * FROM LimitTest
         WHERE id IN (SELECT id FROM LimitTest ORDER BY value DESC LIMIT 3)",
    );
    assert_eq!(results.len(), 3);

    ctx.commit();
}

#[test]
#[ignore = "FETCH FIRST syntax not yet implemented"]
fn test_fetch_first_syntax() {
    let mut ctx = setup_test();
    setup_limit_test_table(&mut ctx);

    // Test SQL standard FETCH FIRST syntax
    let results = ctx.query("SELECT * FROM LimitTest FETCH FIRST 3 ROWS ONLY");
    assert_eq!(results.len(), 3);

    ctx.commit();
}
