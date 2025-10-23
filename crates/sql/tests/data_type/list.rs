//! LIST data type tests (variable-length arrays)
//! Lists can have different lengths in each row, following DuckDB's model

use crate::common::setup_test;
use proven_value::Value;
#[test]
fn test_create_table_with_list_column() {
    let mut ctx = setup_test();

    // LIST is variable-length, each row can have different number of elements
    ctx.exec("CREATE TABLE ListData (id INT, items INT[])");
    ctx.commit();
}

#[test]
fn test_insert_simple_list() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE ListData (id INT, items INT[])");

    // Different rows can have different list lengths
    ctx.exec("INSERT INTO ListData VALUES (1, '[1, 2, 3]')");
    ctx.exec("INSERT INTO ListData VALUES (2, '[4, 5]')");
    ctx.exec("INSERT INTO ListData VALUES (3, '[6, 7, 8, 9]')");

    let results = ctx.query("SELECT id, items FROM ListData ORDER BY id");
    assert_eq!(results.len(), 3);

    // Lists can have varying lengths
    assert!(
        results[0]
            .get("items")
            .unwrap()
            .to_string()
            .contains("List")
    );
    assert!(
        results[1]
            .get("items")
            .unwrap()
            .to_string()
            .contains("List")
    );
    assert!(
        results[2]
            .get("items")
            .unwrap()
            .to_string()
            .contains("List")
    );

    ctx.commit();
}

#[test]
fn test_nested_lists() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE NestedList (id INT, matrix INT[][])");

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

    ctx.exec("CREATE TABLE ListData (id INT, items INT[])");
    ctx.exec("INSERT INTO ListData VALUES (1, '[10, 20, 30]')");
    ctx.exec("INSERT INTO ListData VALUES (2, '[40, 50]')");

    // UNWRAP accesses list elements by index
    let results = ctx.query("SELECT id, UNWRAP(items, '0') AS first, UNWRAP(items, '1') AS second FROM ListData ORDER BY id");
    assert_eq!(results.len(), 2);

    assert_eq!(results[0].get("first").unwrap(), &Value::I32(10));
    assert_eq!(results[0].get("second").unwrap(), &Value::I32(20));
    assert_eq!(results[1].get("first").unwrap(), &Value::I32(40));
    assert_eq!(results[1].get("second").unwrap(), &Value::I32(50));

    ctx.commit();
}

#[test]
fn test_list_bracket_access() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE ListData (id INT, items INT[])");
    ctx.exec("INSERT INTO ListData VALUES (1, '[100, 200, 300]')");
    ctx.exec("INSERT INTO ListData VALUES (2, '[400, 500]')");

    // Bracket notation for list access
    let results =
        ctx.query("SELECT id, items[0] AS first, items[1] AS second FROM ListData ORDER BY id");
    assert_eq!(results.len(), 2);

    assert_eq!(results[0].get("first").unwrap(), &Value::I32(100));
    assert_eq!(results[0].get("second").unwrap(), &Value::I32(200));
    assert_eq!(results[1].get("first").unwrap(), &Value::I32(400));
    assert_eq!(results[1].get("second").unwrap(), &Value::I32(500));

    ctx.commit();
}

#[test]
fn test_list_with_nulls() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE ListData (id INT, items INT[])");

    ctx.exec("INSERT INTO ListData VALUES (1, '[1, 2, 3]')");
    ctx.exec("INSERT INTO ListData VALUES (2, NULL)");
    ctx.exec("INSERT INTO ListData VALUES (3, '[]')"); // Empty list

    let results = ctx.query("SELECT id, items FROM ListData WHERE items IS NOT NULL ORDER BY id");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(3));

    let results = ctx.query("SELECT id FROM ListData WHERE items IS NULL");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(2));

    ctx.commit();
}

#[test]
fn test_cast_to_list() {
    let mut ctx = setup_test();

    // CAST string literals to LIST
    let results = ctx.query("SELECT CAST('[1, 2, 3]' AS INT[]) AS my_list");
    assert_eq!(results.len(), 1);
    assert!(
        results[0]
            .get("my_list")
            .unwrap()
            .to_string()
            .contains("List")
    );

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
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));

    ctx.commit();
}

#[test]
#[should_panic(expected = "InvalidValue")]
fn test_insert_json_object_into_list_should_error() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE ListData (id INT, items INT[])");

    // JSON objects should not be allowed in LIST columns
    ctx.exec(r#"INSERT INTO ListData VALUES (1, '{"key": "value"}')"#);
}

#[test]
#[should_panic(expected = "InvalidValue")]
fn test_insert_invalid_json_should_error() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE ListData (id INT, items INT[])");

    // Invalid JSON should error
    ctx.exec("INSERT INTO ListData VALUES (1, '{{not valid json}}')");
}

#[test]
fn test_group_by_list() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE ListData (id INT, tags INT[])");

    ctx.exec(r#"INSERT INTO ListData VALUES (1, '[1, 2]')"#);
    ctx.exec(r#"INSERT INTO ListData VALUES (2, '[3, 4]')"#);
    ctx.exec(r#"INSERT INTO ListData VALUES (3, '[1, 2]')"#); // Duplicate list

    // GROUP BY should work with LIST columns
    let results = ctx.query("SELECT tags, COUNT(*) as cnt FROM ListData GROUP BY tags");
    assert_eq!(results.len(), 2);

    // Check that we have the right counts (one group with 1, one with 2)
    let counts: Vec<i64> = results
        .iter()
        .map(|r| match r.get("cnt").unwrap() {
            Value::I64(n) => *n,
            _ => panic!("Expected I64"),
        })
        .collect();
    assert!(counts.contains(&1));
    assert!(counts.contains(&2));

    ctx.commit();
}
