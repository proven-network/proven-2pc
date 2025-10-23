//! IN list expression tests
//! Based on gluesql/test-suite/src/expr/in_list.rs

use crate::common::setup_test;
use proven_value::Value;

#[test]
fn test_in_with_integer_list() {
    let mut ctx = setup_test();

    // Test IN with integers
    let results = ctx.query("SELECT (2 IN (1, 2, 3)) as res");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("res").unwrap(), &Value::Bool(true));

    let results = ctx.query("SELECT (5 IN (1, 2, 3)) as res");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("res").unwrap(), &Value::Bool(false));

    let results = ctx.query("SELECT (1 IN (1)) as res");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("res").unwrap(), &Value::Bool(true));

    ctx.commit();
}

#[test]
fn test_in_with_string_list() {
    let mut ctx = setup_test();

    // Test IN with strings
    let results = ctx.query("SELECT ('banana' IN ('apple', 'banana', 'cherry')) as res");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("res").unwrap(), &Value::Bool(true));

    let results = ctx.query("SELECT ('grape' IN ('apple', 'banana', 'cherry')) as res");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("res").unwrap(), &Value::Bool(false));

    ctx.commit();
}

#[test]
fn test_not_in_with_list() {
    let mut ctx = setup_test();

    // Test NOT IN
    let results = ctx.query("SELECT (5 NOT IN (1, 2, 3)) as res");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("res").unwrap(), &Value::Bool(true));

    let results = ctx.query("SELECT (2 NOT IN (1, 2, 3)) as res");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("res").unwrap(), &Value::Bool(false));

    ctx.commit();
}

#[test]
fn test_in_with_null_target() {
    let mut ctx = setup_test();

    // NULL IN (...) should return NULL
    let results = ctx.query("SELECT (NULL IN (1, 2, 3)) as res");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("res").unwrap(), &Value::Null);

    // NULL IN (...) with NULL in list should still return NULL
    let results = ctx.query("SELECT (NULL IN (1, 2, 3, NULL)) as res");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("res").unwrap(), &Value::Null);

    ctx.commit();
}

#[test]
fn test_in_with_null_in_list() {
    let mut ctx = setup_test();

    // Value IN (..., NULL, ...) returns:
    // - true if value matches a non-NULL item
    // - NULL if value doesn't match any non-NULL item (due to NULL comparison)
    let results = ctx.query("SELECT (2 IN (1, 2, NULL)) as res");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("res").unwrap(), &Value::Bool(true));

    let results = ctx.query("SELECT (5 IN (1, 2, NULL)) as res");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("res").unwrap(), &Value::Null);

    ctx.commit();
}

#[test]
fn test_in_with_single_value() {
    let mut ctx = setup_test();

    // Test IN with single value list
    let results = ctx.query("SELECT (42 IN (42)) as res");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("res").unwrap(), &Value::Bool(true));

    let results = ctx.query("SELECT (42 IN (99)) as res");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("res").unwrap(), &Value::Bool(false));

    ctx.commit();
}

#[test]
fn test_in_with_duplicate_values() {
    let mut ctx = setup_test();

    // Test IN with duplicate values in list (should work the same)
    let results = ctx.query("SELECT (2 IN (1, 2, 2, 3, 2)) as res");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("res").unwrap(), &Value::Bool(true));

    let results = ctx.query("SELECT (5 IN (1, 2, 2, 3, 2)) as res");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("res").unwrap(), &Value::Bool(false));

    ctx.commit();
}

#[test]
fn test_in_with_expressions() {
    let mut ctx = setup_test();

    // Test IN with expressions
    let results = ctx.query("SELECT (10 IN (5 + 5, 3 * 4, 2 + 2)) as res");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("res").unwrap(), &Value::Bool(true));

    let results = ctx.query("SELECT ((2 + 3) IN (1, 5, 10)) as res");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("res").unwrap(), &Value::Bool(true));

    ctx.commit();
}

#[test]
fn test_in_with_columns() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE InTest (id INTEGER, value INTEGER)");
    ctx.exec("INSERT INTO InTest VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)");

    // Test IN with column reference in WHERE clause
    let results = ctx.query("SELECT id FROM InTest WHERE value IN (20, 30, 60) ORDER BY id");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(2));
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(3));

    ctx.commit();
}

#[test]
fn test_in_with_boolean_values() {
    let mut ctx = setup_test();

    // Test IN with boolean values
    let results = ctx.query("SELECT (TRUE IN (TRUE, FALSE)) as res");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("res").unwrap(), &Value::Bool(true));

    let results = ctx.query("SELECT (FALSE IN (TRUE)) as res");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("res").unwrap(), &Value::Bool(false));

    ctx.commit();
}

#[test]
fn test_in_with_large_list() {
    let mut ctx = setup_test();

    // Test IN with large list
    let large_list = (1..=100)
        .map(|n| n.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let results = ctx.query(&format!("SELECT (50 IN ({})) as res", large_list));
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("res").unwrap(), &Value::Bool(true));

    let results = ctx.query(&format!("SELECT (150 IN ({})) as res", large_list));
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("res").unwrap(), &Value::Bool(false));

    ctx.commit();
}

#[test]
fn test_not_in_with_null_in_list() {
    let mut ctx = setup_test();

    // NOT IN with NULL in list:
    // - false if value matches a non-NULL item
    // - NULL if value doesn't match any non-NULL item (due to NULL comparison)
    let results = ctx.query("SELECT (2 NOT IN (1, 2, NULL)) as res");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("res").unwrap(), &Value::Bool(false));

    let results = ctx.query("SELECT (5 NOT IN (1, 2, NULL)) as res");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("res").unwrap(), &Value::Null);

    ctx.commit();
}

#[test]
fn test_in_with_negative_numbers() {
    let mut ctx = setup_test();

    // Test IN with negative numbers
    let results = ctx.query("SELECT (-5 IN (-10, -5, 0, 5, 10)) as res");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("res").unwrap(), &Value::Bool(true));

    let results = ctx.query("SELECT (-3 IN (-10, -5, 0, 5, 10)) as res");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("res").unwrap(), &Value::Bool(false));

    ctx.commit();
}

#[test]
fn test_in_with_subquery() {
    let mut ctx = setup_test();

    // Create tables for subquery tests
    ctx.exec("CREATE TABLE Products (id INTEGER, name TEXT, category_id INTEGER)");
    ctx.exec("CREATE TABLE Categories (id INTEGER, name TEXT)");

    ctx.exec("INSERT INTO Categories VALUES (1, 'Electronics'), (2, 'Books'), (3, 'Clothing')");
    ctx.exec("INSERT INTO Products VALUES (1, 'Laptop', 1), (2, 'Phone', 1), (3, 'Novel', 2), (4, 'Shirt', 3), (5, 'Tablet', 1)");

    // Test IN with subquery - find products in Electronics category
    let results = ctx.query(
        "SELECT name FROM Products
         WHERE category_id IN (SELECT id FROM Categories WHERE name = 'Electronics')
         ORDER BY name",
    );
    assert_eq!(results.len(), 3);
    assert_eq!(
        results[0].get("name").unwrap(),
        &Value::Str("Laptop".to_string())
    );
    assert_eq!(
        results[1].get("name").unwrap(),
        &Value::Str("Phone".to_string())
    );
    assert_eq!(
        results[2].get("name").unwrap(),
        &Value::Str("Tablet".to_string())
    );

    // Test NOT IN with subquery
    let results = ctx.query(
        "SELECT name FROM Products
         WHERE category_id NOT IN (SELECT id FROM Categories WHERE name = 'Electronics')
         ORDER BY name",
    );
    assert_eq!(results.len(), 2);
    assert_eq!(
        results[0].get("name").unwrap(),
        &Value::Str("Novel".to_string())
    );
    assert_eq!(
        results[1].get("name").unwrap(),
        &Value::Str("Shirt".to_string())
    );

    // Test IN with subquery returning empty set
    let results = ctx.query(
        "SELECT name FROM Products
         WHERE category_id IN (SELECT id FROM Categories WHERE name = 'NonExistent')
         ORDER BY name",
    );
    assert_eq!(results.len(), 0);

    // Test IN with subquery returning multiple values
    let results = ctx.query(
        "SELECT name FROM Products
         WHERE category_id IN (SELECT id FROM Categories WHERE name IN ('Electronics', 'Books'))
         ORDER BY name",
    );
    assert_eq!(results.len(), 4);

    ctx.commit();
}
