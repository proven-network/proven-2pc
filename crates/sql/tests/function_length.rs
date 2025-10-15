//! LENGTH function tests
//! Based on gluesql/test-suite/src/function/length.rs

mod common;

use common::setup_test;
use proven_value::Value;

#[test]
fn test_length_with_string() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT LENGTH('Hello.') as len");
    assert_eq!(results.len(), 1);

    match results[0].get("len").unwrap() {
        Value::U64(len) => assert_eq!(*len, 6),
        Value::I64(len) => assert_eq!(*len, 6),
        Value::I32(len) => assert_eq!(*len, 6),
        other => panic!("Expected numeric value, got: {:?}", other),
    }

    ctx.commit();
}

#[test]
fn test_length_with_list() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT LENGTH(CAST('[1, 2, 3]' AS LIST)) as len");
    assert_eq!(results.len(), 1);

    match results[0].get("len").unwrap() {
        Value::U64(len) => assert_eq!(*len, 3),
        Value::I64(len) => assert_eq!(*len, 3),
        Value::I32(len) => assert_eq!(*len, 3),
        other => panic!("Expected numeric value, got: {:?}", other),
    }

    ctx.commit();
}

#[test]
fn test_length_with_map() {
    let mut ctx = setup_test();

    // Create a table with a MAP column and test LENGTH on it
    ctx.exec("CREATE TABLE MapTest (id INTEGER, data MAP(VARCHAR, VARCHAR))");
    ctx.exec(r#"INSERT INTO MapTest VALUES (1, '{"a": "1", "b": "5", "c": "9", "d": "10"}')"#);

    let results = ctx.query("SELECT LENGTH(data) as len FROM MapTest WHERE id=1");
    assert_eq!(results.len(), 1);

    match results[0].get("len").unwrap() {
        Value::U64(len) => assert_eq!(*len, 4),
        Value::I64(len) => assert_eq!(*len, 4),
        Value::I32(len) => assert_eq!(*len, 4),
        other => panic!("Expected numeric value, got: {:?}", other),
    }

    ctx.commit();
}

#[test]
fn test_length_with_korean_characters() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT LENGTH('한글') as len");
    assert_eq!(results.len(), 1);

    match results[0].get("len").unwrap() {
        Value::U64(len) => assert_eq!(*len, 2),
        Value::I64(len) => assert_eq!(*len, 2),
        Value::I32(len) => assert_eq!(*len, 2),
        other => panic!("Expected numeric value, got: {:?}", other),
    }

    ctx.commit();
}

#[test]
fn test_length_with_mixed_korean_ascii() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT LENGTH('한글 abc') as len");
    assert_eq!(results.len(), 1);

    match results[0].get("len").unwrap() {
        Value::U64(len) => assert_eq!(*len, 6),
        Value::I64(len) => assert_eq!(*len, 6),
        Value::I32(len) => assert_eq!(*len, 6),
        other => panic!("Expected numeric value, got: {:?}", other),
    }

    ctx.commit();
}

#[test]
fn test_length_with_accented_characters() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT LENGTH('é') as len");
    assert_eq!(results.len(), 1);

    match results[0].get("len").unwrap() {
        Value::U64(len) => assert_eq!(*len, 1),
        Value::I64(len) => assert_eq!(*len, 1),
        Value::I32(len) => assert_eq!(*len, 1),
        other => panic!("Expected numeric value, got: {:?}", other),
    }

    ctx.commit();
}

#[test]
fn test_length_with_emoji_simple() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT LENGTH('🧑') as len");
    assert_eq!(results.len(), 1);

    match results[0].get("len").unwrap() {
        Value::U64(len) => assert_eq!(*len, 1),
        Value::I64(len) => assert_eq!(*len, 1),
        Value::I32(len) => assert_eq!(*len, 1),
        other => panic!("Expected numeric value, got: {:?}", other),
    }

    ctx.commit();
}

#[test]
fn test_length_with_emoji_with_modifier() {
    let mut ctx = setup_test();

    // Heart emoji with variation selector counts as 2 characters
    let results = ctx.query("SELECT LENGTH('❤️') as len");
    assert_eq!(results.len(), 1);

    match results[0].get("len").unwrap() {
        Value::U64(len) => assert_eq!(*len, 2),
        Value::I64(len) => assert_eq!(*len, 2),
        Value::I32(len) => assert_eq!(*len, 2),
        other => panic!("Expected numeric value, got: {:?}", other),
    }

    ctx.commit();
}

#[test]
fn test_length_with_emoji_compound() {
    let mut ctx = setup_test();

    // Woman scientist emoji with zero-width joiner counts as 3 characters
    let results = ctx.query("SELECT LENGTH('👩‍🔬') as len");
    assert_eq!(results.len(), 1);

    match results[0].get("len").unwrap() {
        Value::U64(len) => assert_eq!(*len, 3),
        Value::I64(len) => assert_eq!(*len, 3),
        Value::I32(len) => assert_eq!(*len, 3),
        other => panic!("Expected numeric value, got: {:?}", other),
    }

    ctx.commit();
}
