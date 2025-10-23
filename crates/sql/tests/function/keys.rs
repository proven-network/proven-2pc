//! KEYS function tests
//! Based on gluesql/test-suite/src/function/keys.rs

use crate::common::{TableBuilder, TestContext, setup_test};
use proven_value::Value;

/// Setup test table with map data
fn setup_keys_table(ctx: &mut TestContext) {
    TableBuilder::new(ctx, "USER").create_simple("id INTEGER, data MAP(VARCHAR, VARCHAR)");

    ctx.exec(r#"INSERT INTO USER VALUES (1, '{"id": "1", "name": "alice", "is_male": "false"}')"#);
    ctx.exec(r#"INSERT INTO USER VALUES (2, '{"name": "bob"}')"#);
    ctx.exec(r#"INSERT INTO USER VALUES (3, '{}')"#);
}

#[test]
fn test_create_table_with_map() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE USER (id INTEGER, data MAP(VARCHAR, VARCHAR))");

    ctx.commit();
}

#[test]
fn test_insert_various_maps() {
    let mut ctx = setup_test();
    setup_keys_table(&mut ctx);

    ctx.assert_row_count("SELECT * FROM USER", 3);

    ctx.commit();
}

#[test]
fn test_keys_returns_all_keys_sorted() {
    let mut ctx = setup_test();
    setup_keys_table(&mut ctx);

    let results = ctx.query("SELECT SORT(KEYS(data), 'ASC') as result FROM USER WHERE id=1");

    assert_eq!(results.len(), 1, "Should return 1 row");

    // Check the keys are returned as a list
    let result_value = results[0].get("result").unwrap();
    if let Value::List(keys) = result_value {
        // Extract string values from the list
        let key_strings: Vec<String> = keys
            .iter()
            .map(|v| match v {
                Value::Str(s) => s.clone(),
                _ => panic!("Expected string in keys list"),
            })
            .collect();

        // Should be sorted alphabetically
        assert_eq!(key_strings.len(), 3, "Should have 3 keys");
        assert_eq!(key_strings[0], "id");
        assert_eq!(key_strings[1], "is_male");
        assert_eq!(key_strings[2], "name");
    } else {
        panic!("Expected List value, got: {:?}", result_value);
    }

    ctx.commit();
}

#[test]
fn test_keys_single_key_map() {
    let mut ctx = setup_test();
    setup_keys_table(&mut ctx);

    let results = ctx.query("SELECT KEYS(data) as result FROM USER WHERE id=2");

    assert_eq!(results.len(), 1, "Should return 1 row");

    let result_value = results[0].get("result").unwrap();
    if let Value::List(keys) = result_value {
        assert_eq!(keys.len(), 1, "Should have 1 key");
        assert_eq!(keys[0], Value::Str("name".to_string()));
    } else {
        panic!("Expected List value, got: {:?}", result_value);
    }

    ctx.commit();
}

#[test]
fn test_keys_empty_map() {
    let mut ctx = setup_test();
    setup_keys_table(&mut ctx);

    let results = ctx.query("SELECT KEYS(data) as result FROM USER WHERE id=3");

    assert_eq!(results.len(), 1, "Should return 1 row");

    let result_value = results[0].get("result").unwrap();
    if let Value::List(keys) = result_value {
        assert_eq!(keys.len(), 0, "Should have 0 keys for empty map");
    } else {
        panic!("Expected List value, got: {:?}", result_value);
    }

    ctx.commit();
}

#[test]
fn test_keys_non_map_should_error() {
    let mut ctx = setup_test();
    setup_keys_table(&mut ctx);

    // The error message differs from GlueSQL
    ctx.assert_error_contains("SELECT KEYS(id) FROM USER WHERE id=1", "expected: \"map\"");

    ctx.commit();
}

#[test]
fn test_keys_function_signature() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Test").create_simple("id INTEGER");

    // Test that KEYS requires exactly 1 argument
    ctx.assert_error_contains("SELECT KEYS() FROM Test", "takes exactly 1 argument");

    ctx.commit();
}

#[test]
fn test_keys_with_null_map() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "NullTest")
        .create_simple("id INTEGER, data MAP(VARCHAR, VARCHAR)")
        .insert_values("(1, NULL)");

    // KEYS should handle NULL values (likely returns NULL or empty list)
    let results = ctx.query("SELECT KEYS(data) as result FROM NullTest WHERE id=1");

    assert_eq!(results.len(), 1, "Should return 1 row");

    ctx.commit();
}
