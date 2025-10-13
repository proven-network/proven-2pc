//! Tests for SELECT projection functionality
//! Based on gluesql/test-suite/src/project.rs

mod common;

use common::{TableBuilder, setup_test};
use proven_value::Value;
fn setup_project_test_tables(ctx: &mut common::TestContext) {
    TableBuilder::new(ctx, "ProjectUser")
        .create_simple("id INTEGER, name TEXT, age INTEGER")
        .insert_values(
            "(1, 'Alice', 25),
             (2, 'Bob', 30),
             (3, 'Charlie', 35)",
        );

    TableBuilder::new(ctx, "ProjectItem")
        .create_simple("id INTEGER, player_id INTEGER, item_name TEXT, quantity INTEGER")
        .insert_values(
            "(1, 1, 'Sword', 1),
             (2, 1, 'Shield', 1),
             (3, 2, 'Bow', 1),
             (4, 2, 'Arrow', 50),
             (5, 3, 'Staff', 1)",
        );
}

#[test]
fn test_select_all_columns() {
    let mut ctx = setup_test();
    setup_project_test_tables(&mut ctx);

    // Test SELECT * (all columns)
    let results = ctx.query("SELECT * FROM ProjectUser");
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].len(), 3); // 3 columns
    assert!(results[0].contains_key("id"));
    assert!(results[0].contains_key("name"));
    assert!(results[0].contains_key("age"));

    ctx.commit();
}

#[test]
fn test_select_specific_columns() {
    let mut ctx = setup_test();
    setup_project_test_tables(&mut ctx);

    // Test selecting specific columns
    let results = ctx.query("SELECT id, name FROM ProjectUser ORDER BY id");
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].len(), 2); // Only 2 columns
    assert!(results[0].contains_key("id"));
    assert!(results[0].contains_key("name"));
    assert!(!results[0].contains_key("age")); // age not selected

    // Verify values
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(
        results[0].get("name").unwrap(),
        &Value::Str("Alice".to_string())
    );

    ctx.commit();
}

#[test]
fn test_select_single_column() {
    let mut ctx = setup_test();
    setup_project_test_tables(&mut ctx);

    // Test selecting single column
    let results = ctx.query("SELECT name FROM ProjectUser ORDER BY id");
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].len(), 1); // Only 1 column
    assert!(results[0].contains_key("name"));

    assert_eq!(
        results[0].get("name").unwrap(),
        &Value::Str("Alice".to_string())
    );
    assert_eq!(
        results[1].get("name").unwrap(),
        &Value::Str("Bob".to_string())
    );
    assert_eq!(
        results[2].get("name").unwrap(),
        &Value::Str("Charlie".to_string())
    );

    ctx.commit();
}

#[test]
fn test_select_column_ordering() {
    let mut ctx = setup_test();
    setup_project_test_tables(&mut ctx);

    // Test that columns appear in the order specified
    let results = ctx.query("SELECT age, name, id FROM ProjectUser WHERE id = 1");
    assert_eq!(results.len(), 1);

    // Check the values are correct regardless of order
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(
        results[0].get("name").unwrap(),
        &Value::Str("Alice".to_string())
    );
    assert_eq!(results[0].get("age").unwrap(), &Value::I32(25));

    ctx.commit();
}

#[test]
fn test_select_with_expressions() {
    let mut ctx = setup_test();
    setup_project_test_tables(&mut ctx);

    // Test SELECT with expressions
    let results = ctx.query("SELECT id, age * 2 AS double_age FROM ProjectUser ORDER BY id");
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[0].get("double_age").unwrap(), &Value::I32(50));
    assert_eq!(results[1].get("double_age").unwrap(), &Value::I32(60));
    assert_eq!(results[2].get("double_age").unwrap(), &Value::I32(70));

    ctx.commit();
}

#[test]
fn test_select_with_alias() {
    let mut ctx = setup_test();
    setup_project_test_tables(&mut ctx);

    // Test column aliases
    let results =
        ctx.query("SELECT id AS user_id, name AS user_name FROM ProjectUser WHERE id = 2");
    assert_eq!(results.len(), 1);
    assert!(results[0].contains_key("user_id"));
    assert!(results[0].contains_key("user_name"));
    assert_eq!(results[0].get("user_id").unwrap(), &Value::I32(2));
    assert_eq!(
        results[0].get("user_name").unwrap(),
        &Value::Str("Bob".to_string())
    );

    ctx.commit();
}

#[test]
fn test_select_literal_values() {
    let mut ctx = setup_test();
    setup_project_test_tables(&mut ctx);

    // Test SELECT with literal values
    let results =
        ctx.query("SELECT 1 AS one, 'hello' AS greeting, TRUE AS flag FROM ProjectUser LIMIT 1");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("one").unwrap(), &Value::I32(1));
    assert_eq!(
        results[0].get("greeting").unwrap(),
        &Value::Str("hello".to_string())
    );
    assert_eq!(results[0].get("flag").unwrap(), &Value::Bool(true));

    ctx.commit();
}

#[test]
fn test_select_mixed_columns_and_literals() {
    let mut ctx = setup_test();
    setup_project_test_tables(&mut ctx);

    // Test mixing columns with literals
    let results = ctx.query("SELECT id, name, 'USER' AS type FROM ProjectUser WHERE id = 1");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(
        results[0].get("name").unwrap(),
        &Value::Str("Alice".to_string())
    );
    assert_eq!(
        results[0].get("type").unwrap(),
        &Value::Str("USER".to_string())
    );

    ctx.commit();
}

#[test]
fn test_select_arithmetic_expressions() {
    let mut ctx = setup_test();
    setup_project_test_tables(&mut ctx);

    // Test arithmetic in projections
    let results = ctx.query(
        "SELECT id + 10 AS id_plus_10, age - 5 AS age_minus_5 FROM ProjectUser WHERE id = 2",
    );
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id_plus_10").unwrap(), &Value::I32(12));
    assert_eq!(results[0].get("age_minus_5").unwrap(), &Value::I32(25));

    ctx.commit();
}

#[test]
fn test_select_with_where_not_in_projection() {
    let mut ctx = setup_test();
    setup_project_test_tables(&mut ctx);

    // Test that WHERE clause columns don't need to be in projection
    let results = ctx.query("SELECT name FROM ProjectUser WHERE age > 25 ORDER BY id");
    assert_eq!(results.len(), 2);
    assert_eq!(
        results[0].get("name").unwrap(),
        &Value::Str("Bob".to_string())
    );
    assert_eq!(
        results[1].get("name").unwrap(),
        &Value::Str("Charlie".to_string())
    );

    ctx.commit();
}

#[test]
fn test_select_duplicate_column() {
    let mut ctx = setup_test();
    setup_project_test_tables(&mut ctx);

    // Test selecting same column multiple times
    let results = ctx.query("SELECT id, id, name FROM ProjectUser WHERE id = 1");
    assert_eq!(results.len(), 1);
    // Both id references should return same value
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));

    ctx.commit();
}

#[test]
fn test_select_with_complex_expressions() {
    let mut ctx = setup_test();
    setup_project_test_tables(&mut ctx);

    // Test complex expressions in projection
    let results = ctx.query(
        "SELECT (age * 2) + 10 AS calc, id * 100 AS id_scaled FROM ProjectUser WHERE id = 1",
    );
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("calc").unwrap(), &Value::I32(60)); // (25 * 2) + 10
    assert_eq!(results[0].get("id_scaled").unwrap(), &Value::I32(100));

    ctx.commit();
}

#[test]
fn test_select_from_empty_table() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "EmptyTable").create_simple("id INTEGER, value TEXT");

    // Test projection from empty table
    let results = ctx.query("SELECT id, value FROM EmptyTable");
    assert_eq!(results.len(), 0);

    ctx.commit();
}

#[test]
fn test_select_with_null_values() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "NullTable")
        .create_simple("id INTEGER, value INTEGER NULL, name TEXT NULL")
        .insert_values(
            "(1, 10, 'Alice'),
             (2, NULL, 'Bob'),
             (3, 30, NULL)",
        );

    // Test projection with NULL values
    let results = ctx.query("SELECT id, value, name FROM NullTable ORDER BY id");
    assert_eq!(results.len(), 3);
    assert_eq!(results[1].get("value").unwrap(), &Value::Null);
    assert_eq!(results[2].get("name").unwrap(), &Value::Null);

    ctx.commit();
}

#[test]
fn test_select_boolean_expressions() {
    let mut ctx = setup_test();
    setup_project_test_tables(&mut ctx);

    // Test boolean expressions in projection
    let results = ctx.query("SELECT id, age > 25 AS is_older FROM ProjectUser ORDER BY id");
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].get("is_older").unwrap(), &Value::Bool(false)); // Alice is 25
    assert_eq!(results[1].get("is_older").unwrap(), &Value::Bool(true)); // Bob is 30
    assert_eq!(results[2].get("is_older").unwrap(), &Value::Bool(true)); // Charlie is 35

    ctx.commit();
}

#[test]
#[ignore = "CASE expressions not yet implemented"]
fn test_select_case_expression() {
    let mut ctx = setup_test();
    setup_project_test_tables(&mut ctx);

    // Test CASE expression in projection
    let results = ctx.query(
        "SELECT name,
         CASE
           WHEN age < 30 THEN 'Young'
           WHEN age < 35 THEN 'Middle'
           ELSE 'Senior'
         END AS age_group
         FROM ProjectUser ORDER BY id",
    );
    assert_eq!(results.len(), 3);
    assert_eq!(
        results[0].get("age_group").unwrap(),
        &Value::Str("Young".to_string())
    );
    assert_eq!(
        results[1].get("age_group").unwrap(),
        &Value::Str("Middle".to_string())
    );
    assert_eq!(
        results[2].get("age_group").unwrap(),
        &Value::Str("Senior".to_string())
    );

    ctx.commit();
}

#[test]
#[ignore = "Subqueries in SELECT not yet implemented"]
fn test_select_with_subquery() {
    let mut ctx = setup_test();
    setup_project_test_tables(&mut ctx);

    // Test subquery in projection
    let results = ctx.query(
        "SELECT name,
         (SELECT COUNT(*) FROM ProjectItem WHERE player_id = ProjectUser.id) AS item_count
         FROM ProjectUser ORDER BY id",
    );
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].get("item_count").unwrap(), &Value::I32(2)); // Alice has 2 items
    assert_eq!(results[1].get("item_count").unwrap(), &Value::I32(2)); // Bob has 2 items
    assert_eq!(results[2].get("item_count").unwrap(), &Value::I32(1)); // Charlie has 1 item

    ctx.commit();
}

#[test]
#[ignore = "String concatenation not yet implemented"]
fn test_select_string_concatenation() {
    let mut ctx = setup_test();
    setup_project_test_tables(&mut ctx);

    // Test string concatenation in projection
    let results = ctx.query("SELECT name || ' (User)' AS full_name FROM ProjectUser WHERE id = 1");
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].get("full_name").unwrap(),
        &Value::Str("Alice (User)".to_string())
    );

    ctx.commit();
}
