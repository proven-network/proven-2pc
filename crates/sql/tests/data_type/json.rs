//! JSON data type tests
//! Tests for schemaless JSON type support

use crate::common::setup_test;

#[test]
fn test_create_table_with_json_column() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE JsonData (id INT, data JSON)");
    ctx.commit();
}

#[test]
fn test_insert_json_values() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE JsonData (id INT, data JSON)");

    // Insert various JSON types via CAST
    ctx.exec("INSERT INTO JsonData VALUES (1, CAST('{\"name\": \"Alice\", \"age\": 30}' AS JSON))");
    ctx.exec("INSERT INTO JsonData VALUES (2, CAST('[1, 2, 3, 4, 5]' AS JSON))");
    ctx.exec("INSERT INTO JsonData VALUES (3, CAST('\"hello world\"' AS JSON))");
    ctx.exec("INSERT INTO JsonData VALUES (4, CAST('42' AS JSON))");
    ctx.exec("INSERT INTO JsonData VALUES (5, CAST('true' AS JSON))");
    ctx.exec("INSERT INTO JsonData VALUES (6, CAST('null' AS JSON))");

    let results = ctx.query("SELECT * FROM JsonData ORDER BY id");
    assert_eq!(results.len(), 6);

    ctx.commit();
}

#[test]
fn test_insert_json_implicit_conversion() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE JsonData (id INT, data JSON)");

    // Insert JSON without explicit CAST - should use implicit string → JSON coercion
    ctx.exec("INSERT INTO JsonData VALUES (1, '{\"name\": \"Alice\", \"age\": 30}')");
    ctx.exec("INSERT INTO JsonData VALUES (2, '[1, 2, 3, 4, 5]')");
    ctx.exec("INSERT INTO JsonData VALUES (3, '\"hello world\"')");
    ctx.exec("INSERT INTO JsonData VALUES (4, '42')");
    ctx.exec("INSERT INTO JsonData VALUES (5, 'true')");
    ctx.exec("INSERT INTO JsonData VALUES (6, 'null')");

    let results = ctx.query("SELECT * FROM JsonData ORDER BY id");
    assert_eq!(results.len(), 6);

    // Verify the JSON was parsed correctly
    assert!(
        results[0]
            .get("data")
            .unwrap()
            .to_string()
            .contains("Alice")
    );
    assert!(results[1].get("data").unwrap().to_string().contains("["));
    assert!(
        results[2]
            .get("data")
            .unwrap()
            .to_string()
            .contains("hello")
    );

    ctx.commit();
}

#[test]
fn test_json_round_trip() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE JsonData (id INT, data JSON)");

    let json_obj = r#"{"name":"Bob","nested":{"key":"value"},"array":[1,2,3]}"#;
    ctx.exec(&format!(
        "INSERT INTO JsonData VALUES (1, CAST('{}' AS JSON))",
        json_obj
    ));

    let results = ctx.query("SELECT data FROM JsonData WHERE id = 1");
    assert_eq!(results.len(), 1);

    // Verify the JSON is stored and retrieved
    let data = results[0].get("data").unwrap();
    assert!(data.to_string().contains("Bob"));
    assert!(data.to_string().contains("nested"));
    assert!(data.to_string().contains("array"));

    ctx.commit();
}

#[test]
fn test_json_with_nulls() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE JsonData (id INT, data JSON)");

    // Insert NULL JSON value (SQL NULL, not JSON null)
    ctx.exec("INSERT INTO JsonData VALUES (1, NULL)");

    // Insert JSON null (different from SQL NULL)
    ctx.exec("INSERT INTO JsonData VALUES (2, CAST('null' AS JSON))");

    let results = ctx.query("SELECT * FROM JsonData ORDER BY id");
    assert_eq!(results.len(), 2);

    ctx.commit();
}

#[test]
fn test_json_objects() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE JsonData (id INT, config JSON)");

    ctx.exec(
        r#"INSERT INTO JsonData VALUES
        (1, CAST('{"timeout": 30, "retries": 3, "enabled": true}' AS JSON))"#,
    );

    ctx.exec(
        r#"INSERT INTO JsonData VALUES
        (2, CAST('{"timeout": 60, "retries": 5, "enabled": false}' AS JSON))"#,
    );

    let results = ctx.query("SELECT * FROM JsonData ORDER BY id");
    assert_eq!(results.len(), 2);

    assert!(
        results[0]
            .get("config")
            .unwrap()
            .to_string()
            .contains("timeout")
    );
    assert!(
        results[1]
            .get("config")
            .unwrap()
            .to_string()
            .contains("retries")
    );

    ctx.commit();
}

#[test]
fn test_json_arrays() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE JsonData (id INT, tags JSON)");

    ctx.exec(r#"INSERT INTO JsonData VALUES (1, CAST('["tag1", "tag2", "tag3"]' AS JSON))"#);
    ctx.exec(r#"INSERT INTO JsonData VALUES (2, CAST('[1, 2, 3, 4, 5]' AS JSON))"#);
    ctx.exec(r#"INSERT INTO JsonData VALUES (3, CAST('[true, false, null]' AS JSON))"#);

    let results = ctx.query("SELECT * FROM JsonData ORDER BY id");
    assert_eq!(results.len(), 3);

    ctx.commit();
}

#[test]
fn test_nested_json() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE JsonData (id INT, data JSON)");

    let nested_json = r#"{
        "user": {
            "name": "Alice",
            "address": {
                "city": "Seattle",
                "zip": "98101"
            }
        },
        "orders": [
            {"id": 1, "total": 100},
            {"id": 2, "total": 200}
        ]
    }"#;

    ctx.exec(&format!(
        "INSERT INTO JsonData VALUES (1, CAST('{}' AS JSON))",
        nested_json.replace(['\n', ' '], "")
    ));

    let results = ctx.query("SELECT * FROM JsonData WHERE id = 1");
    assert_eq!(results.len(), 1);

    let data = results[0].get("data").unwrap();
    assert!(data.to_string().contains("Alice"));
    assert!(data.to_string().contains("Seattle"));
    assert!(data.to_string().contains("orders"));

    ctx.commit();
}

#[test]
fn test_json_invalid_syntax_should_error() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE JsonData (id INT, data JSON)");

    // Invalid JSON should error
    ctx.assert_error_contains(
        "INSERT INTO JsonData VALUES (1, CAST('{invalid json}' AS JSON))",
        "JSON",
    );

    ctx.abort();
}

#[test]
fn test_json_empty_structures() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE JsonData (id INT, data JSON)");

    // Empty object
    ctx.exec("INSERT INTO JsonData VALUES (1, CAST('{}' AS JSON))");

    // Empty array
    ctx.exec("INSERT INTO JsonData VALUES (2, CAST('[]' AS JSON))");

    let results = ctx.query("SELECT * FROM JsonData ORDER BY id");
    assert_eq!(results.len(), 2);

    assert!(results[0].get("data").unwrap().to_string().contains("{}"));
    assert!(results[1].get("data").unwrap().to_string().contains("[]"));

    ctx.commit();
}

#[test]
fn test_json_numbers() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE JsonData (id INT, value JSON)");

    // Various number types
    ctx.exec("INSERT INTO JsonData VALUES (1, CAST('42' AS JSON))");
    ctx.exec("INSERT INTO JsonData VALUES (2, CAST('-123' AS JSON))");
    ctx.exec("INSERT INTO JsonData VALUES (3, CAST('3.14159' AS JSON))");
    ctx.exec("INSERT INTO JsonData VALUES (4, CAST('1.23e10' AS JSON))");

    let results = ctx.query("SELECT * FROM JsonData ORDER BY id");
    assert_eq!(results.len(), 4);

    ctx.commit();
}

#[test]
fn test_json_strings() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE JsonData (id INT, text JSON)");

    // String values
    ctx.exec(r#"INSERT INTO JsonData VALUES (1, CAST('"hello"' AS JSON))"#);
    ctx.exec(r#"INSERT INTO JsonData VALUES (2, CAST('"with spaces"' AS JSON))"#);
    ctx.exec(r#"INSERT INTO JsonData VALUES (3, CAST('"with \"quotes\""' AS JSON))"#);

    let results = ctx.query("SELECT * FROM JsonData ORDER BY id");
    assert_eq!(results.len(), 3);

    ctx.commit();
}

#[test]
fn test_json_order_by() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE JsonData (id INT, data JSON)");

    ctx.exec("INSERT INTO JsonData VALUES (3, CAST('{\"a\": 2}' AS JSON))");
    ctx.exec("INSERT INTO JsonData VALUES (1, CAST('{\"a\": 1}' AS JSON))");
    ctx.exec("INSERT INTO JsonData VALUES (2, CAST('{\"a\": 1}' AS JSON))");

    // JSON can be used in ORDER BY
    let results = ctx.query("SELECT * FROM JsonData ORDER BY id");
    assert_eq!(results.len(), 3);

    ctx.commit();
}
