//! FIND_IDX function tests
//! Based on gluesql/test-suite/src/function/find_idx.rs

mod common;

use common::setup_test;
use proven_sql::Value;

#[test]
fn test_create_table_for_text_search() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Meal (menu TEXT NULL)");

    ctx.commit();
}

#[test]
fn test_insert_text_data() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Meal (menu TEXT NULL)");
    ctx.exec("INSERT INTO Meal VALUES ('pork')");
    ctx.exec("INSERT INTO Meal VALUES ('burger')");

    ctx.assert_row_count("SELECT * FROM Meal", 2);

    ctx.commit();
}

#[test]
fn test_find_idx_substring_search() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Meal (menu TEXT NULL)");
    ctx.exec("INSERT INTO Meal VALUES ('pork')");
    ctx.exec("INSERT INTO Meal VALUES ('burger')");

    let results = ctx.query("SELECT FIND_IDX(menu, 'rg') AS test FROM Meal");

    assert_eq!(results.len(), 2);
    // 'pork' doesn't contain 'rg', should return 0
    assert!(results[0].get("test").unwrap().to_string().contains("0"));
    // 'burger' contains 'rg' at index 3
    assert!(results[1].get("test").unwrap().to_string().contains("3"));

    ctx.commit();
}

#[test]
fn test_find_idx_with_start_offset() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Meal (menu TEXT NULL)");
    ctx.exec("INSERT INTO Meal VALUES ('pork')");
    ctx.exec("INSERT INTO Meal VALUES ('burger')");

    let results = ctx.query("SELECT FIND_IDX(menu, 'r', 4) AS test FROM Meal");

    assert_eq!(results.len(), 2);
    // 'pork' doesn't contain 'r' starting from index 4, should return 0
    assert!(results[0].get("test").unwrap().to_string().contains("0"));
    // 'burger' contains 'r' at index 6 when searching from index 4
    assert!(results[1].get("test").unwrap().to_string().contains("6"));

    ctx.commit();
}

#[test]
fn test_find_idx_empty_substring() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT FIND_IDX('cheese', '') AS test");

    assert_eq!(results.len(), 1);
    // Empty string found at beginning (index 0)
    assert!(results[0].get("test").unwrap().to_string().contains("0"));

    ctx.commit();
}

#[test]
fn test_find_idx_single_character() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT FIND_IDX('cheese', 's') AS test");

    assert_eq!(results.len(), 1);
    // 's' is at index 5 in 'cheese'
    assert!(results[0].get("test").unwrap().to_string().contains("5"));

    ctx.commit();
}

#[test]
fn test_find_idx_with_offset() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT FIND_IDX('cheese burger', 'e', 5) AS test");

    assert_eq!(results.len(), 1);
    // 'e' is at index 6 when searching from index 5
    assert!(results[0].get("test").unwrap().to_string().contains("6"));

    ctx.commit();
}

#[test]
fn test_find_idx_with_null_substring() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT FIND_IDX('cheese', NULL) AS test");

    assert_eq!(results.len(), 1);
    let value = results[0].get("test").unwrap();
    // Should return NULL
    assert!(
        value == &Value::Null || value.to_string().contains("NULL"),
        "Expected NULL, got: {}",
        value
    );

    ctx.commit();
}

#[test]
fn test_find_idx_non_string_substring_should_error() {
    let mut ctx = setup_test();

    let error = ctx.exec_error("SELECT FIND_IDX('cheese', 1) AS test");

    // Should error - FIND_IDX requires string value for second argument
    assert!(
        error.contains("FunctionRequiresStringValue")
            || error.contains("string")
            || error.contains("String")
            || error.contains("type"),
        "Expected string-related error, got: {}",
        error
    );

    ctx.commit();
}

#[test]
fn test_find_idx_non_integer_offset_should_error() {
    let mut ctx = setup_test();

    let error = ctx.exec_error("SELECT FIND_IDX('cheese', 's', '5') AS test");

    // Should error - FIND_IDX requires integer value for third argument
    assert!(
        error.contains("FunctionRequiresIntegerValue")
            || error.contains("integer")
            || error.contains("Integer")
            || error.contains("type"),
        "Expected integer-related error, got: {}",
        error
    );

    ctx.commit();
}

#[test]
fn test_find_idx_negative_offset_should_error() {
    let mut ctx = setup_test();

    let error = ctx.exec_error("SELECT FIND_IDX('cheese', 's', -1) AS test");

    // Should error - FIND_IDX requires non-negative offset
    assert!(
        error.contains("NonPositiveIntegerOffsetInFindIdx")
            || error.contains("negative")
            || error.contains("positive")
            || error.contains("offset"),
        "Expected offset-related error, got: {}",
        error
    );

    ctx.commit();
}

#[test]
fn test_find_idx_function_signatures() {
    let mut ctx = setup_test();

    // Test 2-argument form
    // 'hello world': positions are 1-indexed, 'world' starts at position 7
    let results = ctx.query("SELECT FIND_IDX('hello world', 'world') AS test");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("test").unwrap(), &Value::I64(7));

    // Test 3-argument form
    // 'hello world': h=1, e=2, l=3, l=4, o=5, space=6, w=7, o=8
    // Starting from position 5, looking for 'o', finds 'o' at position 5
    let results = ctx.query("SELECT FIND_IDX('hello world', 'o', 5) AS test");
    assert_eq!(results.len(), 1);
    assert!(results[0].get("test").unwrap().to_string().contains("5"));

    ctx.commit();
}
