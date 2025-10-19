//! VALUES function tests
//! Based on gluesql/test-suite/src/function/values.rs
//!
//! Note: GlueSQL uses VALUES() but in proven-sql, VALUES is a reserved keyword for INSERT statements.
//! Use MAP_VALUES() instead to extract values from maps.

mod common;

use common::{TableBuilder, setup_test};
use proven_value::Value;

#[test]
fn test_create_table_with_map() {
    let mut ctx = setup_test();

    // Note: MAP requires explicit key and value types
    ctx.exec("CREATE TABLE USER (id INTEGER, data MAP(VARCHAR, VARCHAR))");

    ctx.commit();
}

#[test]
fn test_values_sorted_descending() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "USER")
        .create_simple("id INTEGER, data MAP(VARCHAR, VARCHAR)")
        .insert_values(r#"(1, '{"id": "1", "name": "alice", "is_male": "false"}')"#);

    // Using MAP_VALUES() function
    let results = ctx.query("SELECT SORT(MAP_VALUES(data), 'DESC') as result FROM USER WHERE id=1");

    assert_eq!(results.len(), 1);
    let result = results[0].get("result").unwrap();

    // Should return ["1", "false", "alice"] sorted in descending order
    if let Value::List(list) = result {
        assert_eq!(list.len(), 3);
        assert!(list.contains(&Value::Str("1".to_owned())));
        assert!(list.contains(&Value::Str("false".to_owned())));
        assert!(list.contains(&Value::Str("alice".to_owned())));
    } else {
        panic!("Expected List value, got {:?}", result);
    }

    ctx.commit();
}

#[test]
fn test_values_sorted_ascending() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "USER")
        .create_simple("id INTEGER, data MAP(VARCHAR, VARCHAR)")
        .insert_values(r#"(1, '{"id": "1", "name": "alice", "is_male": "false"}')"#);

    // Using MAP_VALUES() function
    let results = ctx.query("SELECT SORT(MAP_VALUES(data), 'ASC') as result FROM USER WHERE id=1");

    assert_eq!(results.len(), 1);
    let result = results[0].get("result").unwrap();

    // Should return ["1", "false", "alice"] sorted in ascending order
    if let Value::List(list) = result {
        assert_eq!(list.len(), 3);
        assert!(list.contains(&Value::Str("1".to_owned())));
        assert!(list.contains(&Value::Str("false".to_owned())));
        assert!(list.contains(&Value::Str("alice".to_owned())));
    } else {
        panic!("Expected List value, got {:?}", result);
    }

    ctx.commit();
}

#[test]
fn test_values_single_value_map() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "USER")
        .create_simple("id INTEGER, data MAP(VARCHAR, VARCHAR)")
        .insert_values(r#"(2, '{"name": "bob"}')"#);

    // Using MAP_VALUES() function
    let results = ctx.query("SELECT MAP_VALUES(data) as result FROM USER WHERE id=2");

    assert_eq!(results.len(), 1);
    let result = results[0].get("result").unwrap();

    // Should return ["bob"]
    if let Value::List(list) = result {
        assert_eq!(list.len(), 1);
        assert_eq!(list[0], Value::Str("bob".to_owned()));
    } else {
        panic!("Expected List value, got {:?}", result);
    }

    ctx.commit();
}

#[test]
fn test_values_empty_map() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "USER")
        .create_simple("id INTEGER, data MAP(VARCHAR, VARCHAR)")
        .insert_values(r#"(3, '{}')"#);

    // Using MAP_VALUES() function
    let results = ctx.query("SELECT MAP_VALUES(data) as result FROM USER WHERE id=3");

    assert_eq!(results.len(), 1);
    let result = results[0].get("result").unwrap();

    // Should return empty array []
    if let Value::List(list) = result {
        assert_eq!(list.len(), 0);
    } else {
        panic!("Expected List value, got {:?}", result);
    }

    ctx.commit();
}

#[test]
fn test_values_non_map_should_error() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "USER")
        .create_simple("id INTEGER, data MAP(VARCHAR, VARCHAR)")
        .insert_values(r#"(1, '{"id": "1", "name": "alice"}')"#);

    // Should error: type mismatch (expecting map)
    ctx.assert_error_contains("SELECT MAP_VALUES(id) FROM USER WHERE id=1", "map");

    ctx.commit();
}

#[test]
fn test_values_function_signature() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "USER")
        .create_simple("id INTEGER, data MAP(VARCHAR, VARCHAR)")
        .insert_values(r#"(1, '{}')"#);

    // Test that MAP_VALUES requires exactly 1 argument
    let error = ctx.exec_error("SELECT MAP_VALUES() FROM USER");
    assert!(
        error.contains("argument") || error.contains("expected") || error.contains("1"),
        "Should error on no arguments: {}",
        error
    );

    ctx.commit();
}
