//! EXPLAIN statement tests

mod common;

use common::setup_test;

#[test]
fn test_explain_simple_select() {
    let mut ctx = setup_test();

    // Create a simple table
    ctx.exec("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)");

    // Test EXPLAIN on a simple SELECT
    let plan = ctx.exec_response("EXPLAIN SELECT * FROM users");

    // Verify we get an EXPLAIN plan back (not a query result)
    assert!(plan.is_explain_plan(), "Expected EXPLAIN plan response");

    let plan_text = plan.as_explain_plan().expect("Expected plan text");
    println!("EXPLAIN SELECT * FROM users:\n{}", plan_text);

    // Check that plan contains expected keywords
    assert!(plan_text.contains("Query"), "Plan should contain 'Query'");
    assert!(plan_text.contains("Scan"), "Plan should contain 'Scan'");
    assert!(
        plan_text.contains("users"),
        "Plan should mention table name"
    );

    ctx.commit();
}

#[test]
fn test_explain_select_with_filter() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE products (id INTEGER, name TEXT, price INTEGER)");

    let plan = ctx.exec_response("EXPLAIN SELECT * FROM products WHERE price > 100");

    let plan_text = plan.as_explain_plan().expect("Expected plan text");
    println!("EXPLAIN SELECT with WHERE:\n{}", plan_text);

    assert!(plan_text.contains("Filter"), "Plan should contain 'Filter'");
    assert!(plan_text.contains("Scan"), "Plan should contain 'Scan'");

    ctx.commit();
}

#[test]
fn test_explain_select_with_order_by() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE items (id INTEGER, name TEXT, score INTEGER)");

    let plan = ctx.exec_response("EXPLAIN SELECT * FROM items ORDER BY score DESC");

    let plan_text = plan.as_explain_plan().expect("Expected plan text");
    println!("EXPLAIN SELECT with ORDER BY:\n{}", plan_text);

    assert!(plan_text.contains("Order"), "Plan should contain 'Order'");

    ctx.commit();
}

#[test]
fn test_explain_select_with_limit() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE data (id INTEGER, value INTEGER)");

    let plan = ctx.exec_response("EXPLAIN SELECT * FROM data LIMIT 10");

    let plan_text = plan.as_explain_plan().expect("Expected plan text");
    println!("EXPLAIN SELECT with LIMIT:\n{}", plan_text);

    assert!(plan_text.contains("Limit"), "Plan should contain 'Limit'");

    ctx.commit();
}

#[test]
fn test_explain_insert() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE test (id INTEGER, name TEXT)");

    let plan = ctx.exec_response("EXPLAIN INSERT INTO test VALUES (1, 'hello')");

    let plan_text = plan.as_explain_plan().expect("Expected plan text");
    println!("EXPLAIN INSERT:\n{}", plan_text);

    assert!(plan_text.contains("Insert"), "Plan should contain 'Insert'");
    assert!(plan_text.contains("test"), "Plan should mention table name");

    ctx.commit();
}

#[test]
fn test_explain_join() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE users (id INTEGER, name TEXT)");
    ctx.exec("CREATE TABLE orders (id INTEGER, user_id INTEGER, total INTEGER)");

    let plan =
        ctx.exec_response("EXPLAIN SELECT * FROM users JOIN orders ON users.id = orders.user_id");

    let plan_text = plan.as_explain_plan().expect("Expected plan text");
    println!("EXPLAIN JOIN:\n{}", plan_text);

    assert!(plan_text.contains("join"), "Plan should contain 'join'");

    ctx.commit();
}
