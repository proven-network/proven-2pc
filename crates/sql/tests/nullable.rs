//! Tests for nullable column functionality and NULL handling
//! Based on gluesql/test-suite/src/nullable.rs

mod common;
use common::setup_test;
use proven_value::Value;
#[test]
fn test_nullable_column_basic_operations() {
    let mut ctx = setup_test();

    // Create table with NULL and NOT NULL columns
    ctx.exec(
        "CREATE TABLE Test (
            id INTEGER NULL,
            num INTEGER NOT NULL,
            name TEXT
        )",
    );

    // Insert data with NULL values
    ctx.exec("INSERT INTO Test (id, num, name) VALUES (NULL, 2, 'Hello')");
    ctx.exec("INSERT INTO Test (id, num, name) VALUES (1, 9, 'World')");
    ctx.exec("INSERT INTO Test (id, num, name) VALUES (3, 4, 'Great')");

    // Select all data
    let results = ctx.query("SELECT id, num, name FROM Test ORDER BY num");
    assert_eq!(results.len(), 3);

    assert_eq!(results[0].get("id").unwrap(), &Value::Null);
    assert_eq!(results[0].get("num").unwrap(), &Value::I32(2));
    assert_eq!(
        results[0].get("name").unwrap(),
        &Value::Str("Hello".to_string())
    );

    assert_eq!(results[1].get("id").unwrap(), &Value::I32(3));
    assert_eq!(results[1].get("num").unwrap(), &Value::I32(4));
    assert_eq!(
        results[1].get("name").unwrap(),
        &Value::Str("Great".to_string())
    );

    assert_eq!(results[2].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[2].get("num").unwrap(), &Value::I32(9));
    assert_eq!(
        results[2].get("name").unwrap(),
        &Value::Str("World".to_string())
    );

    ctx.commit();
}

#[test]
fn test_is_null_condition() {
    let mut ctx = setup_test();

    ctx.exec(
        "CREATE TABLE Test (
            id INTEGER NULL,
            num INTEGER NOT NULL,
            name TEXT
        )",
    );

    ctx.exec("INSERT INTO Test VALUES (NULL, 2, 'Hello')");
    ctx.exec("INSERT INTO Test VALUES (1, 9, 'World')");
    ctx.exec("INSERT INTO Test VALUES (3, 4, 'Great')");

    // Test IS NULL condition
    let results = ctx.query("SELECT id, num FROM Test WHERE id IS NULL");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), &Value::Null);
    assert_eq!(results[0].get("num").unwrap(), &Value::I32(2));

    ctx.commit();
}

#[test]
fn test_is_null_with_additional_condition() {
    let mut ctx = setup_test();

    ctx.exec(
        "CREATE TABLE Test (
            id INTEGER NULL,
            num INTEGER NOT NULL,
            name TEXT
        )",
    );

    ctx.exec("INSERT INTO Test VALUES (NULL, 2, 'Hello')");
    ctx.exec("INSERT INTO Test VALUES (NULL, 5, 'Bye')");
    ctx.exec("INSERT INTO Test VALUES (1, 9, 'World')");

    // Test IS NULL combined with other conditions
    let results = ctx.query("SELECT id, num FROM Test WHERE id IS NULL AND name = 'Hello'");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), &Value::Null);
    assert_eq!(results[0].get("num").unwrap(), &Value::I32(2));

    ctx.commit();
}

#[test]
fn test_function_result_is_null() {
    let mut ctx = setup_test();

    ctx.exec(
        "CREATE TABLE Test (
            id INTEGER NULL,
            num INTEGER NOT NULL,
            name TEXT
        )",
    );

    ctx.exec("INSERT INTO Test VALUES (1, 2, 'Hello')");
    ctx.exec("INSERT INTO Test VALUES (2, 3, NULL)");

    // Test IS NULL on function results
    let results = ctx.query("SELECT name FROM Test WHERE SUBSTR(name, 1) IS NULL");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("name").unwrap(), &Value::Null);

    ctx.commit();
}

#[test]
fn test_is_not_null_condition() {
    let mut ctx = setup_test();

    ctx.exec(
        "CREATE TABLE Test (
            id INTEGER NULL,
            num INTEGER NOT NULL,
            name TEXT
        )",
    );

    ctx.exec("INSERT INTO Test VALUES (NULL, 2, 'Hello')");
    ctx.exec("INSERT INTO Test VALUES (1, 9, 'World')");
    ctx.exec("INSERT INTO Test VALUES (3, 4, 'Great')");

    // Test IS NOT NULL condition
    let results = ctx.query("SELECT id, num FROM Test WHERE id IS NOT NULL ORDER BY id");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[0].get("num").unwrap(), &Value::I32(9));
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(3));
    assert_eq!(results[1].get("num").unwrap(), &Value::I32(4));

    ctx.commit();
}

#[test]
fn test_arithmetic_expression_is_null() {
    let mut ctx = setup_test();

    ctx.exec(
        "CREATE TABLE Test (
            id INTEGER NULL,
            num INTEGER NOT NULL,
            name TEXT
        )",
    );

    ctx.exec("INSERT INTO Test VALUES (NULL, 2, 'Hello')");
    ctx.exec("INSERT INTO Test VALUES (1, 9, 'World')");
    ctx.exec("INSERT INTO Test VALUES (3, 4, 'Great')");

    // Test IS NULL on arithmetic expression
    let results = ctx.query("SELECT id, num FROM Test WHERE id + 1 IS NULL");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), &Value::Null);
    assert_eq!(results[0].get("num").unwrap(), &Value::I32(2));

    ctx.commit();
}

#[test]
fn test_arithmetic_expression_is_not_null() {
    let mut ctx = setup_test();

    ctx.exec(
        "CREATE TABLE Test (
            id INTEGER NULL,
            num INTEGER NOT NULL,
            name TEXT
        )",
    );

    ctx.exec("INSERT INTO Test VALUES (NULL, 2, 'Hello')");
    ctx.exec("INSERT INTO Test VALUES (1, 9, 'World')");
    ctx.exec("INSERT INTO Test VALUES (3, 4, 'Great')");

    // Test IS NOT NULL on arithmetic expression
    let results = ctx.query("SELECT id, num FROM Test WHERE id + 1 IS NOT NULL ORDER BY id");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[0].get("num").unwrap(), &Value::I32(9));
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(3));
    assert_eq!(results[1].get("num").unwrap(), &Value::I32(4));

    ctx.commit();
}

#[test]
fn test_literal_is_null_false() {
    let mut ctx = setup_test();

    ctx.exec(
        "CREATE TABLE Test (
            id INTEGER NULL,
            num INTEGER NOT NULL,
            name TEXT
        )",
    );

    ctx.exec("INSERT INTO Test VALUES (NULL, 2, 'Hello')");
    ctx.exec("INSERT INTO Test VALUES (1, 9, 'World')");

    // Test IS NULL on numeric literal (should return no rows)
    let results = ctx.query("SELECT id, num FROM Test WHERE 100 IS NULL");
    assert_eq!(results.len(), 0);

    ctx.commit();
}

#[test]
fn test_literal_is_not_null_true() {
    let mut ctx = setup_test();

    ctx.exec(
        "CREATE TABLE Test (
            id INTEGER NULL,
            num INTEGER NOT NULL,
            name TEXT
        )",
    );

    ctx.exec("INSERT INTO Test VALUES (NULL, 2, 'Hello')");
    ctx.exec("INSERT INTO Test VALUES (1, 9, 'World')");
    ctx.exec("INSERT INTO Test VALUES (3, 4, 'Great')");

    // Test IS NOT NULL on numeric literal (should return all rows)
    let results = ctx.query("SELECT id, num FROM Test WHERE 100 IS NOT NULL ORDER BY num");
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].get("num").unwrap(), &Value::I32(2));
    assert_eq!(results[1].get("num").unwrap(), &Value::I32(4));
    assert_eq!(results[2].get("num").unwrap(), &Value::I32(9));

    ctx.commit();
}

#[test]
fn test_literal_null_is_null() {
    let mut ctx = setup_test();

    ctx.exec(
        "CREATE TABLE Test (
            id INTEGER NULL,
            num INTEGER NOT NULL,
            name TEXT
        )",
    );

    ctx.exec("INSERT INTO Test VALUES (NULL, 2, 'Hello')");
    ctx.exec("INSERT INTO Test VALUES (1, 9, 'World')");
    ctx.exec("INSERT INTO Test VALUES (3, 4, 'Great')");

    // Test NULL IS NULL (should return all rows)
    let results = ctx.query("SELECT id, num FROM Test WHERE NULL IS NULL ORDER BY num");
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].get("num").unwrap(), &Value::I32(2));
    assert_eq!(results[1].get("num").unwrap(), &Value::I32(4));
    assert_eq!(results[2].get("num").unwrap(), &Value::I32(9));

    ctx.commit();
}

#[test]
fn test_literal_null_is_not_null() {
    let mut ctx = setup_test();

    ctx.exec(
        "CREATE TABLE Test (
            id INTEGER NULL,
            num INTEGER NOT NULL,
            name TEXT
        )",
    );

    ctx.exec("INSERT INTO Test VALUES (NULL, 2, 'Hello')");
    ctx.exec("INSERT INTO Test VALUES (1, 9, 'World')");

    // Test NULL IS NOT NULL (should return no rows)
    let results = ctx.query("SELECT id, num FROM Test WHERE NULL IS NOT NULL");
    assert_eq!(results.len(), 0);

    ctx.commit();
}

#[test]
fn test_null_arithmetic_propagation() {
    let mut ctx = setup_test();

    ctx.exec(
        "CREATE TABLE Test (
            id INTEGER NULL,
            num INTEGER NOT NULL,
            name TEXT
        )",
    );

    ctx.exec("INSERT INTO Test VALUES (NULL, 2, 'Hello')");
    ctx.exec("INSERT INTO Test VALUES (1, 9, 'World')");
    ctx.exec("INSERT INTO Test VALUES (3, 4, 'Great')");

    // Test NULL + value results in NULL (should return all rows)
    let results = ctx.query("SELECT id, num FROM Test WHERE (NULL + id) IS NULL ORDER BY num");
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].get("num").unwrap(), &Value::I32(2));
    assert_eq!(results[1].get("num").unwrap(), &Value::I32(4));
    assert_eq!(results[2].get("num").unwrap(), &Value::I32(9));

    ctx.commit();
}

#[test]
fn test_null_plus_null() {
    let mut ctx = setup_test();

    ctx.exec(
        "CREATE TABLE Test (
            id INTEGER NULL,
            num INTEGER NOT NULL,
            name TEXT
        )",
    );

    ctx.exec("INSERT INTO Test VALUES (NULL, 2, 'Hello')");
    ctx.exec("INSERT INTO Test VALUES (1, 9, 'World')");
    ctx.exec("INSERT INTO Test VALUES (3, 4, 'Great')");

    // Test NULL + NULL results in NULL (should return all rows)
    let results = ctx.query("SELECT id, num FROM Test WHERE (NULL + NULL) IS NULL ORDER BY num");
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].get("num").unwrap(), &Value::I32(2));
    assert_eq!(results[1].get("num").unwrap(), &Value::I32(4));
    assert_eq!(results[2].get("num").unwrap(), &Value::I32(9));

    ctx.commit();
}

#[test]
fn test_string_literal_null_is_not_null() {
    let mut ctx = setup_test();

    ctx.exec(
        "CREATE TABLE Test (
            id INTEGER NULL,
            num INTEGER NOT NULL,
            name TEXT
        )",
    );

    ctx.exec("INSERT INTO Test VALUES (NULL, 2, 'Hello')");
    ctx.exec("INSERT INTO Test VALUES (1, 9, 'World')");
    ctx.exec("INSERT INTO Test VALUES (3, 4, 'Great')");

    // Test string 'NULL' IS NULL (should return no rows)
    let results = ctx.query("SELECT id, num FROM Test WHERE 'NULL' IS NULL");
    assert_eq!(results.len(), 0);

    // Test string 'NULL' IS NOT NULL (should return all rows)
    let results = ctx.query("SELECT id, num FROM Test WHERE 'NULL' IS NOT NULL ORDER BY num");
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].get("num").unwrap(), &Value::I32(2));
    assert_eq!(results[1].get("num").unwrap(), &Value::I32(4));
    assert_eq!(results[2].get("num").unwrap(), &Value::I32(9));

    ctx.commit();
}

#[test]
fn test_null_arithmetic_operations() {
    let mut ctx = setup_test();

    ctx.exec(
        "CREATE TABLE Test (
            id INTEGER NULL,
            num INTEGER NOT NULL,
            name TEXT
        )",
    );

    ctx.exec("INSERT INTO Test VALUES (NULL, 2, 'Hello')");
    ctx.exec("INSERT INTO Test VALUES (1, 9, 'World')");
    ctx.exec("INSERT INTO Test VALUES (3, 4, 'Great')");

    // Test various arithmetic operations with NULL - addition
    let results = ctx.query("SELECT id, num FROM Test WHERE id + 1 IS NULL");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), &Value::Null);

    let results = ctx.query("SELECT id, num FROM Test WHERE 1 + id IS NULL");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), &Value::Null);

    // Subtraction
    let results = ctx.query("SELECT id, num FROM Test WHERE id - 1 IS NULL");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), &Value::Null);

    let results = ctx.query("SELECT id, num FROM Test WHERE 1 - id IS NULL");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), &Value::Null);

    // Multiplication
    let results = ctx.query("SELECT id, num FROM Test WHERE id * 1 IS NULL");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), &Value::Null);

    let results = ctx.query("SELECT id, num FROM Test WHERE 1 * id IS NULL");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), &Value::Null);

    // Division
    let results = ctx.query("SELECT id, num FROM Test WHERE id / 1 IS NULL");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), &Value::Null);

    let results = ctx.query("SELECT id, num FROM Test WHERE 1 / id IS NULL");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), &Value::Null);

    ctx.commit();
}

#[test]
fn test_select_null_arithmetic_results() {
    let mut ctx = setup_test();

    ctx.exec(
        "CREATE TABLE Test (
            id INTEGER NULL,
            num INTEGER NOT NULL,
            name TEXT
        )",
    );

    ctx.exec("INSERT INTO Test VALUES (NULL, 2, 'Hello')");

    // Test SELECT of arithmetic expressions involving NULL
    let results = ctx.query(
        "SELECT id + 1 AS add1, 1 + id AS add2, id - 1 AS sub1, 1 - id AS sub2,
                id * 1 AS mul1, 1 * id AS mul2, id / 1 AS div1, 1 / id AS div2
         FROM Test WHERE id IS NULL",
    );
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("add1").unwrap(), &Value::Null);
    assert_eq!(results[0].get("add2").unwrap(), &Value::Null);
    assert_eq!(results[0].get("sub1").unwrap(), &Value::Null);
    assert_eq!(results[0].get("sub2").unwrap(), &Value::Null);
    assert_eq!(results[0].get("mul1").unwrap(), &Value::Null);
    assert_eq!(results[0].get("mul2").unwrap(), &Value::Null);
    assert_eq!(results[0].get("div1").unwrap(), &Value::Null);
    assert_eq!(results[0].get("div2").unwrap(), &Value::Null);

    ctx.commit();
}

#[test]
fn test_update_null_values() {
    let mut ctx = setup_test();

    ctx.exec(
        "CREATE TABLE Test (
            id INTEGER NULL,
            num INTEGER NOT NULL,
            name TEXT
        )",
    );

    ctx.exec("INSERT INTO Test VALUES (NULL, 2, 'Hello')");
    ctx.exec("INSERT INTO Test VALUES (1, 9, 'World')");
    ctx.exec("INSERT INTO Test VALUES (3, 4, 'Great')");

    // Update all id values to 2
    ctx.exec("UPDATE Test SET id = 2");

    // Verify all id values are now 2
    let results = ctx.query("SELECT id FROM Test ORDER BY num");
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(2));
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(2));
    assert_eq!(results[2].get("id").unwrap(), &Value::I32(2));

    // Verify full data
    let results = ctx.query("SELECT id, num FROM Test ORDER BY num");
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(2));
    assert_eq!(results[0].get("num").unwrap(), &Value::I32(2));
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(2));
    assert_eq!(results[1].get("num").unwrap(), &Value::I32(4));
    assert_eq!(results[2].get("id").unwrap(), &Value::I32(2));
    assert_eq!(results[2].get("num").unwrap(), &Value::I32(9));

    ctx.commit();
}

#[test]
fn test_not_null_constraint_violation() {
    let mut ctx = setup_test();

    ctx.exec(
        "CREATE TABLE Test (
            id INTEGER NULL,
            num INTEGER NOT NULL,
            name TEXT
        )",
    );

    // Try to insert NULL into NOT NULL column
    let error = ctx.exec_error("INSERT INTO Test VALUES (1, NULL, 'ok')");
    assert!(
        error.contains("non-nullable column"),
        "Expected NullConstraintViolation error, got: {}",
        error
    );

    ctx.commit();
}

#[test]
fn test_nullable_text_column() {
    let mut ctx = setup_test();

    // Create table with explicitly nullable TEXT column
    ctx.exec(
        "CREATE TABLE Foo (
            id INTEGER,
            name TEXT NULL
        )",
    );

    // Insert with NULL value
    ctx.exec("INSERT INTO Foo (id, name) VALUES (1, 'Hello')");
    ctx.exec("INSERT INTO Foo (id, name) VALUES (2, NULL)");

    // Verify data
    let results = ctx.query("SELECT id, name FROM Foo ORDER BY id");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(
        results[0].get("name").unwrap(),
        &Value::Str("Hello".to_string())
    );
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(2));
    assert_eq!(results[1].get("name").unwrap(), &Value::Null);

    ctx.commit();
}

#[test]
fn test_implicit_null_insert() {
    let mut ctx = setup_test();

    // Create table with nullable TEXT column
    ctx.exec(
        "CREATE TABLE Foo (
            id INTEGER,
            name TEXT NULL
        )",
    );

    // Insert without specifying nullable column (should default to NULL)
    ctx.exec("INSERT INTO Foo (id) VALUES (1)");

    // Verify implicit NULL was inserted
    let results = ctx.query("SELECT id, name FROM Foo");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[0].get("name").unwrap(), &Value::Null);

    ctx.commit();
}

#[test]
fn test_expression_comparison_with_null() {
    let mut ctx = setup_test();

    ctx.exec(
        "CREATE TABLE Test (
            id INTEGER NULL,
            num INTEGER NOT NULL
        )",
    );

    ctx.exec("INSERT INTO Test VALUES (NULL, 2)");
    ctx.exec("INSERT INTO Test VALUES (1, 9)");
    ctx.exec("INSERT INTO Test VALUES (3, 4)");

    // Test 8 + 3 IS NULL (should return no rows)
    let results = ctx.query("SELECT id, num FROM Test WHERE 8 + 3 IS NULL");
    assert_eq!(results.len(), 0);

    // Test 8 + 3 IS NOT NULL (should return all rows)
    let results = ctx.query("SELECT id, num FROM Test WHERE 8 + 3 IS NOT NULL ORDER BY num");
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].get("num").unwrap(), &Value::I32(2));
    assert_eq!(results[1].get("num").unwrap(), &Value::I32(4));
    assert_eq!(results[2].get("num").unwrap(), &Value::I32(9));

    ctx.commit();
}
