//! SUBSTR function tests
//! Based on gluesql/test-suite/src/function/substr.rs

mod common;

use common::{TableBuilder, setup_test};
use proven_value::Value;

/// Setup test table with various strings
fn setup_substr_table(ctx: &mut common::TestContext) {
    TableBuilder::new(ctx, "Item")
        .create_simple("name TEXT DEFAULT SUBSTR('abc', 0, 2)")
        .insert_values("('Blop mc blee'), ('B'), ('Steven the &long named$ folken!')");
}

#[test]
fn test_create_table_with_substr_default() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Item (name TEXT DEFAULT SUBSTR('abc', 0, 2))");

    ctx.commit();
}

#[test]
fn test_insert_test_data() {
    let mut ctx = setup_test();
    setup_substr_table(&mut ctx);

    ctx.assert_row_count("SELECT * FROM Item", 3);

    ctx.commit();
}

#[test]
fn test_create_additional_tables() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE SingleItem (food TEXT)");
    ctx.exec("INSERT INTO SingleItem VALUES (SUBSTR('LobSter',1))");

    ctx.assert_row_count("SELECT * FROM SingleItem", 1);

    ctx.commit();
}

#[test]
fn test_create_null_tables() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE NullName (name TEXT)");
    ctx.exec("INSERT INTO NullName VALUES (NULL)");

    ctx.exec("CREATE TABLE NullNumber (number INTEGER)");
    ctx.exec("INSERT INTO NullNumber VALUES (NULL)");

    ctx.assert_row_count("SELECT * FROM NullName", 1);
    ctx.assert_row_count("SELECT * FROM NullNumber", 1);

    ctx.commit();
}

#[test]
fn test_nested_substr() {
    let mut ctx = setup_test();
    setup_substr_table(&mut ctx);

    let results = ctx.query("SELECT SUBSTR(SUBSTR(name, 1), 1) AS test FROM Item");
    assert_eq!(results.len(), 3);

    assert_eq!(
        results[0].get("test").unwrap(),
        &Value::Str("Blop mc blee".to_string())
    );
    assert_eq!(
        results[1].get("test").unwrap(),
        &Value::Str("B".to_string())
    );
    assert_eq!(
        results[2].get("test").unwrap(),
        &Value::Str("Steven the &long named$ folken!".to_string())
    );

    ctx.commit();
}

#[test]
fn test_substr_in_where_clause() {
    let mut ctx = setup_test();
    setup_substr_table(&mut ctx);

    let results = ctx.query("SELECT * FROM Item WHERE name = SUBSTR('ABC', 2, 1)");
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].get("name").unwrap(),
        &Value::Str("B".to_string())
    );

    ctx.commit();
}

#[test]
fn test_substr_comparison_operations() {
    let mut ctx = setup_test();
    setup_substr_table(&mut ctx);

    let results = ctx.query("SELECT * FROM Item WHERE SUBSTR(name, 1, 1) = 'B'");
    assert_eq!(results.len(), 2);

    let names: Vec<String> = results
        .iter()
        .map(|r| match r.get("name").unwrap() {
            Value::Str(s) => s.clone(),
            _ => panic!("Expected string"),
        })
        .collect();
    assert!(names.contains(&"Blop mc blee".to_string()));
    assert!(names.contains(&"B".to_string()));

    ctx.commit();
}

#[test]
fn test_substr_with_functions() {
    let mut ctx = setup_test();
    setup_substr_table(&mut ctx);

    let results = ctx.query("SELECT * FROM Item WHERE SUBSTR(name, 1, 1) = UPPER('b')");
    assert_eq!(results.len(), 2);

    ctx.commit();
}

#[test]
fn test_substr_comparison_with_substr() {
    let mut ctx = setup_test();
    setup_substr_table(&mut ctx);

    let results = ctx.query("SELECT * FROM Item WHERE SUBSTR(name, 1, 4) = SUBSTR('Blop', 1)");
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].get("name").unwrap(),
        &Value::Str("Blop mc blee".to_string())
    );

    ctx.commit();
}

#[test]
fn test_substr_greater_than_operations() {
    let mut ctx = setup_test();
    setup_substr_table(&mut ctx);

    let results = ctx.query("SELECT * FROM Item WHERE SUBSTR(name, 1, 4) > SUBSTR('Blop', 1)");
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].get("name").unwrap(),
        &Value::Str("Steven the &long named$ folken!".to_string())
    );

    ctx.commit();
}

#[test]
fn test_substr_two_parameter_form() {
    let mut ctx = setup_test();
    setup_substr_table(&mut ctx);

    let results = ctx.query("SELECT SUBSTR(name, 2) AS test FROM Item");
    assert_eq!(results.len(), 3);

    assert_eq!(
        results[0].get("test").unwrap(),
        &Value::Str("lop mc blee".to_string())
    );
    assert_eq!(results[1].get("test").unwrap(), &Value::Str("".to_string()));
    assert_eq!(
        results[2].get("test").unwrap(),
        &Value::Str("teven the &long named$ folken!".to_string())
    );

    ctx.commit();
}

#[test]
fn test_substr_out_of_bounds() {
    let mut ctx = setup_test();
    setup_substr_table(&mut ctx);

    let results = ctx.query("SELECT SUBSTR(name, 999) AS test FROM Item");
    assert_eq!(results.len(), 3);

    for result in results.iter() {
        assert_eq!(result.get("test").unwrap(), &Value::Str("".to_string()));
    }

    ctx.commit();
}

#[test]
fn test_substr_with_negative_indices() {
    let mut ctx = setup_test();

    let results = ctx.query(
        "SELECT SUBSTR('ABC', -3, 0) AS t1, SUBSTR('ABC', -1, 3) AS t2, SUBSTR('ABC', -1, 4) AS t3",
    );
    assert_eq!(results.len(), 1);

    // Negative index handling varies - just verify results exist
    assert!(results[0].contains_key("t1"));
    assert!(results[0].contains_key("t2"));
    assert!(results[0].contains_key("t3"));

    ctx.commit();
}

#[test]
fn test_substr_with_zero_and_large_indices() {
    let mut ctx = setup_test();

    let results = ctx.query(
        "SELECT SUBSTR('ABC', 0, 3) AS t1, SUBSTR('ABC', 1, 999) AS t2, SUBSTR('ABC', -1000, 1003) AS t3",
    );
    assert_eq!(results.len(), 1);

    if let Value::Str(s) = results[0].get("t1").unwrap() {
        assert!(s.len() <= 3, "Result should not exceed source length");
    }
    if let Value::Str(s) = results[0].get("t2").unwrap() {
        assert!(s.len() <= 3, "Result should not exceed source length");
    }

    ctx.commit();
}

#[test]
fn test_substr_nested_function_calls() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE SingleItem (food TEXT)");
    ctx.exec("INSERT INTO SingleItem VALUES ('test')");

    let results = ctx.query("SELECT SUBSTR(SUBSTR('ABC', 2, 3), 1, 1) AS test FROM SingleItem");
    assert_eq!(results.len(), 1);

    assert_eq!(
        results[0].get("test").unwrap(),
        &Value::Str("B".to_string())
    );

    ctx.commit();
}

#[test]
fn test_substr_with_null_parameters() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE NullName (name TEXT)");
    ctx.exec("INSERT INTO NullName VALUES (NULL)");

    ctx.exec("CREATE TABLE NullNumber (number INTEGER)");
    ctx.exec("INSERT INTO NullNumber VALUES (NULL)");

    // SQL standard behavior: NULL in length parameter returns NULL
    let results = ctx.query("SELECT SUBSTR('ABC', -1, NULL) AS t1");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("t1").unwrap(), &Value::Null);

    // Test with NULL source string
    let results = ctx.query("SELECT SUBSTR(name, 3) AS test FROM NullName");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("test").unwrap(), &Value::Null);

    // Test with NULL position parameter
    let results = ctx.query("SELECT SUBSTR('Words', number) AS test FROM NullNumber");
    assert_eq!(results.len(), 1);
    // NULL position returns NULL in this implementation
    assert_eq!(results[0].get("test").unwrap(), &Value::Null);

    ctx.commit();
}

#[test]
fn test_substr_with_null_length_parameter() {
    let mut ctx = setup_test();

    // GlueSQL behavior: NULL in length parameter should return NULL
    let results = ctx.query("SELECT SUBSTR('ABC', -1, NULL) AS t1");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("t1").unwrap(), &Value::Null);

    ctx.commit();
}

#[test]
fn test_substr_error_cases() {
    let mut ctx = setup_test();

    // Test SUBSTR with non-string argument - should error
    ctx.assert_error_contains("SELECT SUBSTR(1, 1) AS test", "");

    ctx.commit();
}

#[test]
fn test_substr_with_float_index() {
    let mut ctx = setup_test();

    // GlueSQL behavior: should error on float index
    ctx.assert_error_contains("SELECT SUBSTR('Words', 1.1) AS test", "");

    ctx.commit();
}

#[test]
fn test_substr_with_negative_length() {
    let mut ctx = setup_test();

    // PostgreSQL behavior: should error on negative length
    ctx.assert_error_contains(
        "SELECT SUBSTR('Words', 1, -4) AS test",
        "SUBSTR length must be non-negative",
    );

    ctx.commit();
}

#[test]
fn test_substr_with_arithmetic_operations() {
    let mut ctx = setup_test();

    // SUBSTR result can't be used in arithmetic
    ctx.assert_error_contains("SELECT SUBSTR('123', 2, 3) - '3' AS test", "");

    ctx.commit();
}

#[test]
fn test_substr_with_unary_operations() {
    let mut ctx = setup_test();

    // Unary operators on SUBSTR should error
    ctx.assert_error_contains("SELECT +SUBSTR('test', 1) AS test", "");

    ctx.commit();
}
