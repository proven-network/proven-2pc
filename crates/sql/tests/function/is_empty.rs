//! IS_EMPTY function tests
//! Based on gluesql/test-suite/src/function/is_empty.rs

use crate::common::{TableBuilder, TestContext, setup_test};
use proven_value::Value;

/// Setup test table with list and map data
fn setup_is_empty_table(ctx: &mut TestContext) {
    TableBuilder::new(ctx, "IsEmpty")
        .create_simple("id INTEGER, list_items INT[], map_items MAP(VARCHAR, VARCHAR)");

    // Insert rows with empty and non-empty lists/maps
    ctx.exec("INSERT INTO IsEmpty VALUES (1, '[]', '{\"a\": \"1\", \"b\": \"20\"}')");
    ctx.exec("INSERT INTO IsEmpty VALUES (2, '[1, 2, 3]', '{\"a\": \"2\", \"b\": \"30\", \"c\": \"true\"}')");
    ctx.exec("INSERT INTO IsEmpty VALUES (3, '[]', '{}')");
    ctx.exec("INSERT INTO IsEmpty VALUES (4, '[10]', '{}')");
}

#[test]
fn test_create_table_with_list_and_map() {
    let mut ctx = setup_test();

    ctx.exec(
        "CREATE TABLE IsEmpty (id INTEGER, list_items INT[], map_items MAP(VARCHAR, VARCHAR))",
    );

    ctx.commit();
}

#[test]
fn test_insert_various_empty_and_non_empty_collections() {
    let mut ctx = setup_test();
    setup_is_empty_table(&mut ctx);

    ctx.assert_row_count("SELECT * FROM IsEmpty", 4);

    ctx.commit();
}

#[test]
fn test_is_empty_list_returns_true() {
    let mut ctx = setup_test();
    setup_is_empty_table(&mut ctx);

    let results = ctx.query("SELECT id FROM IsEmpty WHERE IS_EMPTY(list_items)");

    assert_eq!(results.len(), 2, "Should return 2 rows with empty lists");

    // Check that ids 1 and 3 are returned
    let ids: Vec<i32> = results
        .iter()
        .map(|row| match row.get("id").unwrap() {
            Value::I32(id) => *id,
            Value::I64(id) => *id as i32,
            other => panic!("Expected integer for id, got: {:?}", other),
        })
        .collect();

    assert!(ids.contains(&1), "Should contain id 1");
    assert!(ids.contains(&3), "Should contain id 3");

    ctx.commit();
}

#[test]
fn test_is_empty_list_returns_false() {
    let mut ctx = setup_test();
    setup_is_empty_table(&mut ctx);

    ctx.assert_query_value(
        "SELECT IS_EMPTY(list_items) as result FROM IsEmpty WHERE id=2",
        "result",
        Value::Bool(false),
    );

    ctx.commit();
}

#[test]
fn test_is_empty_map_returns_true() {
    let mut ctx = setup_test();
    setup_is_empty_table(&mut ctx);

    let results = ctx.query("SELECT id FROM IsEmpty WHERE IS_EMPTY(map_items)");

    assert_eq!(results.len(), 2, "Should return 2 rows with empty maps");

    // Check that ids 3 and 4 are returned
    let ids: Vec<i32> = results
        .iter()
        .map(|row| match row.get("id").unwrap() {
            Value::I32(id) => *id,
            Value::I64(id) => *id as i32,
            other => panic!("Expected integer for id, got: {:?}", other),
        })
        .collect();

    assert!(ids.contains(&3), "Should contain id 3");
    assert!(ids.contains(&4), "Should contain id 4");

    ctx.commit();
}

#[test]
fn test_is_empty_map_returns_false() {
    let mut ctx = setup_test();
    setup_is_empty_table(&mut ctx);

    ctx.assert_query_value(
        "SELECT IS_EMPTY(map_items) as result FROM IsEmpty WHERE id=1",
        "result",
        Value::Bool(false),
    );

    ctx.commit();
}

#[test]
fn test_is_empty_non_collection_should_error() {
    let mut ctx = setup_test();
    setup_is_empty_table(&mut ctx);

    // The error message differs from GlueSQL
    ctx.assert_error_contains(
        "SELECT id FROM IsEmpty WHERE IS_EMPTY(id)",
        "string or collection",
    );

    ctx.commit();
}

#[test]
fn test_is_empty_function_signature() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Test").create_simple("id INTEGER");

    // Test that IS_EMPTY requires exactly 1 argument
    ctx.assert_error_contains("SELECT IS_EMPTY() FROM Test", "takes exactly 1 argument");

    ctx.commit();
}

#[test]
fn test_is_empty_with_null_values() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "NullTest")
        .create_simple("id INTEGER, list_items INT[], map_items MAP(VARCHAR, VARCHAR)")
        .insert_values("(1, NULL, NULL)");

    // IS_EMPTY should handle NULL values (likely returns NULL)
    let results = ctx.query("SELECT IS_EMPTY(list_items) as list_result, IS_EMPTY(map_items) as map_result FROM NullTest WHERE id=1");

    assert_eq!(results.len(), 1, "Should return 1 row");

    ctx.commit();
}
