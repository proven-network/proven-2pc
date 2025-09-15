//! LIST data type tests (variable-length arrays)
//! Lists can have different lengths in each row, following DuckDB's model

mod common;

use common::setup_test;

#[test]
fn test_create_table_with_list_column() {
    let mut ctx = setup_test();

    // LIST is variable-length, each row can have different number of elements
    ctx.exec("CREATE TABLE ListData (id INT, items LIST)");
    ctx.commit();
}

#[test]
fn test_insert_simple_list() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE ListData (id INT, items LIST)");

    // Different rows can have different list lengths
    ctx.exec("INSERT INTO ListData VALUES (1, '[1, 2, 3]')");
    ctx.exec("INSERT INTO ListData VALUES (2, '[4, 5]')");
    ctx.exec("INSERT INTO ListData VALUES (3, '[6, 7, 8, 9]')");

    let results = ctx.query("SELECT id, items FROM ListData ORDER BY id");
    assert_eq!(results.len(), 3);

    // Lists can have varying lengths
    assert!(results[0].get("items").unwrap().contains("List"));
    assert!(results[1].get("items").unwrap().contains("List"));
    assert!(results[2].get("items").unwrap().contains("List"));

    ctx.commit();
}

#[test]
#[ignore = "Mixed-type lists are not supported - lists must be homogeneous as in standard SQL"]
fn test_insert_mixed_type_list() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE MixedList (id INT, data LIST)");

    // Lists can contain mixed types
    ctx.exec(r#"INSERT INTO MixedList VALUES (1, '["hello", "world", 30, true]')"#);
    ctx.exec(r#"INSERT INTO MixedList VALUES (2, '[1, 2.5, "test", false]')"#);

    let results = ctx.query("SELECT data FROM MixedList ORDER BY id");
    assert_eq!(results.len(), 2);

    ctx.commit();
}

#[test]
fn test_nested_lists() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE NestedList (id INT, matrix INTEGER[][])");

    // Lists can contain other lists
    ctx.exec("INSERT INTO NestedList VALUES (1, '[[1, 2], [3, 4], [5, 6]]')");
    ctx.exec("INSERT INTO NestedList VALUES (2, '[[7], [8, 9]]')");

    let results = ctx.query("SELECT matrix FROM NestedList ORDER BY id");
    assert_eq!(results.len(), 2);

    ctx.commit();
}

#[test]
fn test_unwrap_list_elements() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE ListData (id INT, items LIST)");
    ctx.exec("INSERT INTO ListData VALUES (1, '[10, 20, 30]')");
    ctx.exec("INSERT INTO ListData VALUES (2, '[40, 50]')");

    // UNWRAP accesses list elements by index
    let results = ctx.query("SELECT id, UNWRAP(items, '0') AS first, UNWRAP(items, '1') AS second FROM ListData ORDER BY id");
    assert_eq!(results.len(), 2);

    assert_eq!(results[0].get("first").unwrap(), "I64(10)");
    assert_eq!(results[0].get("second").unwrap(), "I64(20)");
    assert_eq!(results[1].get("first").unwrap(), "I64(40)");
    assert_eq!(results[1].get("second").unwrap(), "I64(50)");

    ctx.commit();
}

#[test]
fn test_list_bracket_access() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE ListData (id INT, items LIST)");
    ctx.exec("INSERT INTO ListData VALUES (1, '[100, 200, 300]')");
    ctx.exec("INSERT INTO ListData VALUES (2, '[400, 500]')");

    // Bracket notation for list access
    let results =
        ctx.query("SELECT id, items[0] AS first, items[1] AS second FROM ListData ORDER BY id");
    assert_eq!(results.len(), 2);

    assert_eq!(results[0].get("first").unwrap(), "I64(100)");
    assert_eq!(results[0].get("second").unwrap(), "I64(200)");
    assert_eq!(results[1].get("first").unwrap(), "I64(400)");
    assert_eq!(results[1].get("second").unwrap(), "I64(500)");

    ctx.commit();
}

#[test]
fn test_list_with_nulls() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE ListData (id INT, items LIST)");

    ctx.exec("INSERT INTO ListData VALUES (1, '[1, 2, 3]')");
    ctx.exec("INSERT INTO ListData VALUES (2, NULL)");
    ctx.exec("INSERT INTO ListData VALUES (3, '[]')"); // Empty list

    let results = ctx.query("SELECT id, items FROM ListData WHERE items IS NOT NULL ORDER BY id");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap(), "I32(1)");
    assert_eq!(results[1].get("id").unwrap(), "I32(3)");

    let results = ctx.query("SELECT id FROM ListData WHERE items IS NULL");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), "I32(2)");

    ctx.commit();
}

#[test]
fn test_cast_to_list() {
    let mut ctx = setup_test();

    // CAST string literals to LIST
    let results = ctx.query("SELECT CAST('[1, 2, 3]' AS INTEGER[]) AS my_list");
    assert_eq!(results.len(), 1);
    assert!(results[0].get("my_list").unwrap().contains("List"));

    ctx.commit();
}

#[test]
fn test_list_in_where_clause() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE ListData (id INT, tags VARCHAR[])");

    ctx.exec(r#"INSERT INTO ListData VALUES (1, '["red", "blue"]')"#);
    ctx.exec(r#"INSERT INTO ListData VALUES (2, '["green", "yellow"]')"#);
    ctx.exec(r#"INSERT INTO ListData VALUES (3, '["red", "green"]')"#);

    // Find all rows that contain "red" in their list
    // This would require list functions like CONTAINS or IN operator support
    // For now, just test basic equality
    let results = ctx.query(r#"SELECT id FROM ListData WHERE tags = '["red", "blue"]'"#);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), "I32(1)");

    ctx.commit();
}

#[test]
#[should_panic(expected = "JsonArrayTypeRequired")]
fn test_insert_json_object_into_list_should_error() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE ListData (id INT, items LIST)");

    // JSON objects should not be allowed in LIST columns
    ctx.exec(r#"INSERT INTO ListData VALUES (1, '{"key": "value"}')"#);
}

#[test]
#[should_panic(expected = "InvalidJsonString")]
fn test_insert_invalid_json_should_error() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE ListData (id INT, items LIST)");

    // Invalid JSON should error
    ctx.exec("INSERT INTO ListData VALUES (1, '{{not valid json}}')");
}

#[test]
#[ignore = "GROUP BY with LIST not yet implemented"]
fn test_group_by_list() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE ListData (id INT, tags LIST)");

    ctx.exec(r#"INSERT INTO ListData VALUES (1, '["a", "b"]')"#);
    ctx.exec(r#"INSERT INTO ListData VALUES (2, '["c", "d"]')"#);
    ctx.exec(r#"INSERT INTO ListData VALUES (3, '["a", "b"]')"#); // Duplicate list

    // GROUP BY should work with LIST columns
    let results =
        ctx.query("SELECT tags, COUNT(*) as cnt FROM ListData GROUP BY tags ORDER BY cnt");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("cnt").unwrap(), "I64(1)");
    assert_eq!(results[1].get("cnt").unwrap(), "I64(2)");

    ctx.commit();
}
