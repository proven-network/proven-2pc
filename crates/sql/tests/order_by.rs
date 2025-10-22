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
        &Value::Str("failed".to_string())
    );
    assert_eq!(
        results[1].get("status").unwrap(),
        &Value::Str("pending".to_string())
    );
    assert_eq!(
        results[2].get("status").unwrap(),
        &Value::Str("pending".to_string())
    );

    ctx.commit();
}

#[test]
fn test_order_by_aggregate_alias() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Sales")
        .create_simple("product TEXT, quantity INTEGER, price FLOAT")
        .insert_values(
            "('Apple', 10, 1.5),
             ('Banana', 20, 0.8),
             ('Apple', 15, 1.5),
             ('Cherry', 5, 3.0),
             ('Banana', 10, 0.8)",
        );

    // Test ORDER BY aggregate alias - should be able to use alias in ORDER BY
    // This is standard SQL behavior: aliases in SELECT can be referenced in ORDER BY
    let results = ctx.query(
        "SELECT product, SUM(quantity) as total_qty
         FROM Sales
         GROUP BY product
         ORDER BY total_qty DESC",
    );

    assert_eq!(results.len(), 3);

    // Banana: 20 + 10 = 30
    assert_eq!(
        results[0].get("product").unwrap(),
        &Value::Str("Banana".to_string())
    );
    assert_eq!(results[0].get("total_qty").unwrap(), &Value::I32(30));

    // Apple: 10 + 15 = 25
    assert_eq!(
        results[1].get("product").unwrap(),
        &Value::Str("Apple".to_string())
    );
    assert_eq!(results[1].get("total_qty").unwrap(), &Value::I32(25));

    // Cherry: 5
    assert_eq!(
        results[2].get("product").unwrap(),
        &Value::Str("Cherry".to_string())
    );
    assert_eq!(results[2].get("total_qty").unwrap(), &Value::I32(5));

    ctx.commit();
}

#[test]
fn test_order_by_multiple_aggregate_aliases() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Orders")
        .create_simple("customer TEXT, product TEXT, quantity INTEGER, price FLOAT")
        .insert_values(
            "('Alice', 'Apple', 5, 1.5),
             ('Bob', 'Banana', 10, 0.8),
             ('Alice', 'Cherry', 3, 3.0),
             ('Bob', 'Apple', 7, 1.5),
             ('Alice', 'Apple', 2, 1.5)",
        );

    // Test ORDER BY with multiple aggregate aliases
    let results = ctx.query(
        "SELECT customer, COUNT(*) as order_count, SUM(quantity) as total_items
         FROM Orders
         GROUP BY customer
         ORDER BY total_items DESC, order_count ASC",
    );

    assert_eq!(results.len(), 2);

    // Bob: 2 orders, 17 items total (10 + 7)
    assert_eq!(
        results[0].get("customer").unwrap(),
        &Value::Str("Bob".to_string())
    );
    assert_eq!(results[0].get("order_count").unwrap(), &Value::I64(2));
    assert_eq!(results[0].get("total_items").unwrap(), &Value::I32(17));

    // Alice: 3 orders, 10 items total (5 + 3 + 2)
    assert_eq!(
        results[1].get("customer").unwrap(),
        &Value::Str("Alice".to_string())
    );
    assert_eq!(results[1].get("order_count").unwrap(), &Value::I64(3));
    assert_eq!(results[1].get("total_items").unwrap(), &Value::I32(10));

    ctx.commit();
}

#[test]
fn test_order_by_aggregate_expression_alias() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Products")
        .create_simple("category TEXT, quantity INTEGER, price FLOAT")
        .insert_values(
            "('Electronics', 10, 100.0),
             ('Books', 50, 15.0),
             ('Electronics', 5, 200.0),
             ('Books', 30, 25.0),
             ('Clothing', 20, 30.0)",
        );

    // Test ORDER BY with alias for complex aggregate expression
    let results = ctx.query(
        "SELECT category, SUM(quantity * price) as revenue
         FROM Products
         GROUP BY category
         ORDER BY revenue DESC",
    );

    assert_eq!(results.len(), 3);

    // Electronics: (10 * 100) + (5 * 200) = 2000
    assert_eq!(
        results[0].get("category").unwrap(),
        &Value::Str("Electronics".to_string())
    );
    assert_eq!(results[0].get("revenue").unwrap(), &Value::F64(2000.0));

    // Books: (50 * 15) + (30 * 25) = 1500
    assert_eq!(
        results[1].get("category").unwrap(),
        &Value::Str("Books".to_string())
    );
    assert_eq!(results[1].get("revenue").unwrap(), &Value::F64(1500.0));

    // Clothing: 20 * 30 = 600
    assert_eq!(
        results[2].get("category").unwrap(),
        &Value::Str("Clothing".to_string())
    );
    assert_eq!(results[2].get("revenue").unwrap(), &Value::F64(600.0));

    ctx.commit();
}

#[test]
fn test_order_by_count_alias() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Events")
        .create_simple("event_type TEXT, user_id INTEGER")
        .insert_values(
            "('login', 1),
             ('click', 2),
             ('login', 1),
             ('click', 1),
             ('login', 3),
             ('click', 2),
             ('login', 1)",
        );

    // Test ORDER BY with COUNT alias - common use case
    let results = ctx.query(
        "SELECT event_type, COUNT(*) as cnt
         FROM Events
         GROUP BY event_type
         ORDER BY cnt DESC",
    );

    assert_eq!(results.len(), 2);

    // login: 4 occurrences
    assert_eq!(
        results[0].get("event_type").unwrap(),
        &Value::Str("login".to_string())
    );
    assert_eq!(results[0].get("cnt").unwrap(), &Value::I64(4));

    // click: 3 occurrences
    assert_eq!(
        results[1].get("event_type").unwrap(),
        &Value::Str("click".to_string())
    );
    assert_eq!(results[1].get("cnt").unwrap(), &Value::I64(3));

    ctx.commit();
}

#[test]
fn test_order_by_with_distinct() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Products")
        .create_simple("category TEXT, subcategory TEXT, price FLOAT")
        .insert_values(
            "('Electronics', 'Phones', 299.99),
             ('Electronics', 'Laptops', 999.99),
             ('Electronics', 'Phones', 399.99),
             ('Books', 'Fiction', 12.99),
             ('Books', 'Fiction', 15.99)",
        );

    // Test DISTINCT with ORDER BY
    let results = ctx.query("SELECT DISTINCT category FROM Products ORDER BY category DESC");
    assert_eq!(results.len(), 2);
    assert_eq!(
        results[0].get("category").unwrap(),
        &Value::Str("Electronics".to_string())
    );
    assert_eq!(
        results[1].get("category").unwrap(),
        &Value::Str("Books".to_string())
    );

    ctx.commit();
}

#[test]
fn test_order_by_non_aggregate_alias() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Numbers")
        .create_simple("value INTEGER")
        .insert_values("(5), (2), (8), (1), (9)");

    // Test ORDER BY with computed column alias
    let results =
        ctx.query("SELECT value, value * 2 as doubled FROM Numbers ORDER BY doubled DESC");
    assert_eq!(results.len(), 5);
    assert_eq!(results[0].get("doubled").unwrap(), &Value::I32(18)); // 9 * 2
    assert_eq!(results[1].get("doubled").unwrap(), &Value::I32(16)); // 8 * 2
    assert_eq!(results[2].get("doubled").unwrap(), &Value::I32(10)); // 5 * 2
    assert_eq!(results[3].get("doubled").unwrap(), &Value::I32(4)); // 2 * 2
    assert_eq!(results[4].get("doubled").unwrap(), &Value::I32(2)); // 1 * 2

    ctx.commit();
}

#[test]
fn test_order_by_group_by_column() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Sales")
        .create_simple("region TEXT, amount INTEGER")
        .insert_values(
            "('West', 100),
             ('East', 200),
             ('West', 150),
             ('North', 75),
             ('East', 300)",
        );

    // Test ORDER BY on GROUP BY column
    let results = ctx.query(
        "SELECT region, SUM(amount) as total
         FROM Sales
         GROUP BY region
         ORDER BY region ASC",
    );

    assert_eq!(results.len(), 3);
    assert_eq!(
        results[0].get("region").unwrap(),
        &Value::Str("East".to_string())
    );
    assert_eq!(
        results[1].get("region").unwrap(),
        &Value::Str("North".to_string())
    );
    assert_eq!(
        results[2].get("region").unwrap(),
        &Value::Str("West".to_string())
    );

    ctx.commit();
}

#[test]
fn test_order_by_with_offset_and_limit() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Items")
        .create_simple("id INTEGER, value INTEGER")
        .insert_values(
            "(1, 10),
             (2, 20),
             (3, 30),
             (4, 40),
             (5, 50),
             (6, 60),
             (7, 70)",
        );

    // Test ORDER BY with both LIMIT and OFFSET (pagination)
    let results = ctx.query("SELECT id, value FROM Items ORDER BY value DESC LIMIT 3 OFFSET 2");
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].get("value").unwrap(), &Value::I32(50)); // 3rd from top
    assert_eq!(results[1].get("value").unwrap(), &Value::I32(40)); // 4th
    assert_eq!(results[2].get("value").unwrap(), &Value::I32(30)); // 5th

    ctx.commit();
}

#[test]
#[should_panic(expected = "must appear in GROUP BY")]
fn test_order_by_invalid_column_in_group_by() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Orders")
        .create_simple("customer TEXT, product TEXT, quantity INTEGER")
        .insert_values(
            "('Alice', 'Widget', 5),
             ('Bob', 'Gadget', 3),
             ('Alice', 'Gadget', 2)",
        );

    // This should fail: ordering by 'product' which is not in GROUP BY
    ctx.query(
        "SELECT customer, SUM(quantity) as total
         FROM Orders
         GROUP BY customer
         ORDER BY product",
    );
}

#[test]
fn test_order_by_mixing_alias_and_column() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Data")
        .create_simple("category TEXT, value INTEGER, status TEXT")
        .insert_values(
            "('A', 100, 'active'),
             ('B', 200, 'inactive'),
             ('A', 150, 'active'),
             ('C', 50, 'active'),
             ('B', 175, 'active')",
        );

    // Test ORDER BY mixing alias and direct column reference
    let results = ctx.query(
        "SELECT category, value, value * 2 as doubled, status
         FROM Data
         ORDER BY category ASC, doubled DESC",
    );

    assert_eq!(results.len(), 5);
    // Category A, sorted by doubled DESC
    assert_eq!(
        results[0].get("category").unwrap(),
        &Value::Str("A".to_string())
    );
    assert_eq!(results[0].get("doubled").unwrap(), &Value::I32(300)); // 150 * 2
    assert_eq!(
        results[1].get("category").unwrap(),
        &Value::Str("A".to_string())
    );
    assert_eq!(results[1].get("doubled").unwrap(), &Value::I32(200)); // 100 * 2

    ctx.commit();
}

#[test]
fn test_order_by_with_empty_result() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Empty")
        .create_simple("id INTEGER, value TEXT")
        .insert_values("(1, 'test')");

    // Query that returns no results
    let results = ctx.query("SELECT id, value FROM Empty WHERE id > 100 ORDER BY value");
    assert_eq!(results.len(), 0);

    ctx.commit();
}

// ============================================================================
// Error Cases - Testing validation
// ============================================================================

#[test]
#[should_panic(expected = "Column index")]
fn test_order_by_column_position_out_of_bounds() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Test")
        .create_simple("id INTEGER, name TEXT")
        .insert_values("(1, 'test')");

    // ORDER BY position 99 when only 2 columns exist
    ctx.query("SELECT id, name FROM Test ORDER BY 99");
}

#[test]
#[should_panic]
fn test_order_by_nonexistent_column() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Test")
        .create_simple("id INTEGER, name TEXT")
        .insert_values("(1, 'test')");

    // ORDER BY column that doesn't exist
    ctx.query("SELECT id FROM Test ORDER BY nonexistent_column");
}

#[test]
#[should_panic(expected = "must appear in GROUP BY")]
fn test_order_by_aggregate_without_group_by() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Test")
        .create_simple("id INTEGER, value INTEGER")
        .insert_values("(1, 100), (2, 200)");

    // ORDER BY aggregate function when SELECT has non-aggregate columns
    // This should fail because we're mixing aggregates with non-aggregates
    ctx.query("SELECT id FROM Test ORDER BY COUNT(*)");
}

#[test]
#[should_panic(expected = "must appear in GROUP BY")]
fn test_order_by_aggregate_in_non_aggregate_query() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Test")
        .create_simple("id INTEGER, value INTEGER")
        .insert_values("(1, 100), (2, 200)");

    // Trying to use aggregate in ORDER BY without any aggregation context
    ctx.query("SELECT id, value FROM Test ORDER BY SUM(value)");
}

// ============================================================================
// Complex Scenarios
// ============================================================================

#[test]
fn test_order_by_with_offset_no_limit() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Numbers")
        .create_simple("value INTEGER")
        .insert_values("(10), (20), (30), (40), (50)");

    // Test OFFSET without LIMIT
    let results = ctx.query("SELECT value FROM Numbers ORDER BY value ASC OFFSET 2");
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].get("value").unwrap(), &Value::I32(30));
    assert_eq!(results[1].get("value").unwrap(), &Value::I32(40));
    assert_eq!(results[2].get("value").unwrap(), &Value::I32(50));

    ctx.commit();
}

#[test]
fn test_order_by_group_by_with_aggregate_and_non_aggregate() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Sales")
        .create_simple("region TEXT, product TEXT, amount INTEGER")
        .insert_values(
            "('West', 'Widget', 100),
             ('East', 'Gadget', 200),
             ('West', 'Widget', 150),
             ('East', 'Widget', 300)",
        );

    // ORDER BY mixing GROUP BY column and aggregate alias
    let results = ctx.query(
        "SELECT region, SUM(amount) as total
         FROM Sales
         GROUP BY region
         ORDER BY region ASC, total DESC",
    );

    assert_eq!(results.len(), 2);
    assert_eq!(
        results[0].get("region").unwrap(),
        &Value::Str("East".to_string())
    );
    assert_eq!(results[0].get("total").unwrap(), &Value::I32(500));
    assert_eq!(
        results[1].get("region").unwrap(),
        &Value::Str("West".to_string())
    );
    assert_eq!(results[1].get("total").unwrap(), &Value::I32(250));

    ctx.commit();
}

// ============================================================================
// GROUP BY Interaction Tests
// ============================================================================

#[test]
fn test_order_by_expression_with_group_by_column() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Data")
        .create_simple("category TEXT, value INTEGER")
        .insert_values(
            "('A', 10),
             ('B', 20),
             ('A', 15),
             ('C', 5)",
        );

    // ORDER BY expression involving GROUP BY column
    let results = ctx.query(
        "SELECT category, SUM(value) as total
         FROM Data
         GROUP BY category
         ORDER BY LENGTH(category) DESC, category ASC",
    );

    assert_eq!(results.len(), 3);
    // All have same length (1), so alphabetical order
    assert_eq!(
        results[0].get("category").unwrap(),
        &Value::Str("A".to_string())
    );

    ctx.commit();
}

// ============================================================================
// Special Cases
// ============================================================================

#[test]
fn test_order_by_with_single_row() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "SingleRow")
        .create_simple("id INTEGER, value TEXT")
        .insert_values("(1, 'only')");

    // ORDER BY with single row
    let results = ctx.query("SELECT id, value FROM SingleRow ORDER BY id DESC");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(
        results[0].get("value").unwrap(),
        &Value::Str("only".to_string())
    );

    ctx.commit();
}

#[test]
fn test_order_by_all_same_values() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Uniform")
        .create_simple("id INTEGER, category TEXT, value INTEGER")
        .insert_values(
            "(1, 'same', 100),
             (2, 'same', 100),
             (3, 'same', 100),
             (4, 'same', 100)",
        );

    // ORDER BY column where all values are identical
    let results = ctx.query("SELECT id FROM Uniform ORDER BY category, value");
    assert_eq!(results.len(), 4);
    // Order should be stable (based on insertion order or id)
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(2));

    ctx.commit();
}

#[test]
fn test_order_by_qualified_column_name() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Products")
        .create_simple("id INTEGER, name TEXT, price FLOAT")
        .insert_values(
            "(1, 'Widget', 9.99),
             (2, 'Gadget', 19.99),
             (3, 'Tool', 14.99)",
        );

    // ORDER BY with table-qualified column name
    let results = ctx.query("SELECT id, name, price FROM Products ORDER BY Products.price ASC");
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1)); // 9.99
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(3)); // 14.99
    assert_eq!(results[2].get("id").unwrap(), &Value::I32(2)); // 19.99

    ctx.commit();
}

#[test]
fn test_order_by_with_table_alias() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Items")
        .create_simple("id INTEGER, value INTEGER")
        .insert_values("(1, 30), (2, 10), (3, 20)");

    // ORDER BY with aliased table reference
    let results = ctx.query("SELECT t.id, t.value FROM Items t ORDER BY t.value ASC");
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(2)); // value 10
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(3)); // value 20
    assert_eq!(results[2].get("id").unwrap(), &Value::I32(1)); // value 30

    ctx.commit();
}

// ============================================================================
// Additional Edge Cases
// ============================================================================

#[test]
fn test_order_by_with_negative_numbers() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Numbers")
        .create_simple("value INTEGER")
        .insert_values("(5), (-3), (0), (-10), (2)");

    let results = ctx.query("SELECT value FROM Numbers ORDER BY value ASC");
    assert_eq!(results.len(), 5);
    assert_eq!(results[0].get("value").unwrap(), &Value::I32(-10));
    assert_eq!(results[1].get("value").unwrap(), &Value::I32(-3));
    assert_eq!(results[2].get("value").unwrap(), &Value::I32(0));
    assert_eq!(results[3].get("value").unwrap(), &Value::I32(2));
    assert_eq!(results[4].get("value").unwrap(), &Value::I32(5));

    ctx.commit();
}

#[test]
fn test_order_by_mixed_null_and_values() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "MixedData")
        .create_simple("id INTEGER, category TEXT NULL, value INTEGER")
        .insert_values(
            "(1, 'A', 100),
             (2, NULL, 200),
             (3, 'B', 150),
             (4, NULL, 50)",
        );

    // ORDER BY with NULLs and non-NULLs, then by value
    let results = ctx.query("SELECT id FROM MixedData ORDER BY category ASC, value DESC");
    assert_eq!(results.len(), 4);
    // Non-NULL categories first (A, B), then NULLs
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1)); // A, 100
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(3)); // B, 150

    ctx.commit();
}

#[test]
fn test_order_by_case_insensitive_text() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Words")
        .create_simple("word TEXT")
        .insert_values(
            "('apple'),
             ('BANANA'),
             ('Cherry'),
             ('date')",
        );

    // Text ordering (case-sensitive in most SQL databases)
    let results = ctx.query("SELECT word FROM Words ORDER BY word ASC");
    assert_eq!(results.len(), 4);
    // Uppercase typically comes before lowercase in ASCII ordering

    ctx.commit();
}

#[test]
fn test_order_by_very_long_text() {
    let mut ctx = setup_test();

    let long_text = "a".repeat(1000);
    let longer_text = "b".repeat(1000);

    TableBuilder::new(&mut ctx, "LongText").create_simple("id INTEGER, content TEXT");

    ctx.exec_with_params(
        "INSERT INTO LongText VALUES (?, ?)",
        vec![Value::I32(1), Value::Str(longer_text.clone())],
    );
    ctx.exec_with_params(
        "INSERT INTO LongText VALUES (?, ?)",
        vec![Value::I32(2), Value::Str(long_text.clone())],
    );

    let results = ctx.query("SELECT id FROM LongText ORDER BY content ASC");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(2)); // "aaa..." comes before "bbb..."
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(1));

    ctx.commit();
}

#[test]
fn test_order_by_multiple_aggregates() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Metrics")
        .create_simple("category TEXT, value INTEGER")
        .insert_values(
            "('A', 10),
             ('A', 20),
             ('B', 30),
             ('B', 40),
             ('C', 5)",
        );

    // ORDER BY multiple aggregate aliases
    let results = ctx.query(
        "SELECT category, COUNT(*) as cnt, SUM(value) as total, AVG(value) as avg
         FROM Metrics
         GROUP BY category
         ORDER BY cnt DESC, total ASC",
    );

    assert_eq!(results.len(), 3);
    // A and B have count=2, C has count=1
    // Among A and B, A has lower total (30 vs 70)
    assert_eq!(
        results[0].get("category").unwrap(),
        &Value::Str("A".to_string())
    );
    assert_eq!(results[0].get("cnt").unwrap(), &Value::I64(2));

    ctx.commit();
}
