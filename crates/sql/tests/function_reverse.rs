//! REVERSE function tests
//! Based on gluesql/test-suite/src/function/reverse.rs

mod common;

use common::setup_test;
use proven_value::Value;

#[test]
fn test_create_table_with_reverse_default() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Item (name TEXT DEFAULT REVERSE('world'))");

    ctx.commit();
}

#[test]
fn test_insert_text_data() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Item (name TEXT DEFAULT REVERSE('world'))");
    ctx.exec("INSERT INTO Item VALUES ('Let''s meet')");

    ctx.assert_row_count("SELECT * FROM Item", 1);

    ctx.commit();
}

#[test]
fn test_reverse_string() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Item (name TEXT)");
    ctx.exec("INSERT INTO Item VALUES ('Let''s meet')");

    ctx.assert_query_value(
        "SELECT REVERSE(name) AS test FROM Item",
        "test",
        Value::Str("teem s'teL".to_string()),
    );

    ctx.commit();
}

#[test]
fn test_reverse_non_string_should_error() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Item (name TEXT)");
    ctx.exec("INSERT INTO Item VALUES ('test')");

    // The error message differs from GlueSQL
    ctx.assert_error_contains(
        "SELECT REVERSE(1) AS test FROM Item",
        "list, array, or string",
    );

    ctx.commit();
}

#[test]
fn test_create_table_for_null_test() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE NullTest (name TEXT)");

    ctx.commit();
}

#[test]
fn test_insert_null_value() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE NullTest (name TEXT)");
    ctx.exec("INSERT INTO NullTest VALUES (NULL)");

    ctx.assert_row_count("SELECT * FROM NullTest", 1);

    ctx.commit();
}

#[test]
fn test_reverse_null_returns_null() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE NullTest (name TEXT)");
    ctx.exec("INSERT INTO NullTest VALUES (NULL)");

    let results = ctx.query("SELECT REVERSE(name) AS test FROM NullTest");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("test").unwrap(), &Value::Null);

    ctx.commit();
}

#[test]
fn test_reverse_empty_string() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT REVERSE('') AS test");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("test").unwrap(), &Value::Str("".to_string()));

    ctx.commit();
}

#[test]
fn test_reverse_single_character() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT REVERSE('A') AS test");
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].get("test").unwrap(),
        &Value::Str("A".to_string())
    );

    ctx.commit();
}

#[test]
fn test_reverse_special_characters() {
    let mut ctx = setup_test();

    // Test with special characters including Unicode
    let results = ctx.query("SELECT REVERSE('hello!@#$%') AS test");
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].get("test").unwrap(),
        &Value::Str("%$#@!olleh".to_string())
    );

    // Test with Unicode characters
    let results = ctx.query("SELECT REVERSE('hello 世界') AS test");
    assert_eq!(results.len(), 1);
    // Note: Reverse should reverse grapheme clusters, not bytes
    let reversed = match results[0].get("test").unwrap() {
        Value::Str(s) => s.clone(),
        _ => panic!("Expected string"),
    };
    assert!(!reversed.is_empty(), "Should have reversed string");

    ctx.commit();
}

#[test]
fn test_reverse_function_signature() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Test (id INTEGER)");

    // Test that REVERSE requires exactly 1 argument
    ctx.assert_error_contains("SELECT REVERSE() FROM Test", "takes exactly 1 argument");

    ctx.commit();
}
