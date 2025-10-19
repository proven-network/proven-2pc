//! Array literal functionality tests
//! Based on gluesql/test-suite/src/array.rs

mod common;

use common::setup_test;
use proven_value::Value;

#[test]
fn test_create_table_with_list_column() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Test (id INTEGER DEFAULT 1, name TEXT[] NOT NULL)");

    ctx.commit();
}

#[test]
fn test_insert_array_literal_single_row() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Test (id INTEGER DEFAULT 1, name TEXT[] NOT NULL)");

    ctx.exec("INSERT INTO Test (id, name) VALUES (1, ['Seongbin','Bernie'])");

    assert_rows!(ctx, "SELECT * FROM Test", 1);

    let results = ctx.query("SELECT * FROM Test");
    assert_eq!(results.len(), 1);
    assert!(
        results[0]
            .get("name")
            .unwrap()
            .to_string()
            .contains("Seongbin")
    );
    assert!(
        results[0]
            .get("name")
            .unwrap()
            .to_string()
            .contains("Bernie")
    );

    ctx.commit();
}

#[test]
fn test_insert_array_literal_multiple_rows() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Test (id INTEGER DEFAULT 1, name TEXT[] NOT NULL)");
    ctx.exec("INSERT INTO Test (id, name) VALUES (3,Array['Seongbin','Bernie','Chobobdev']), (2,Array['devgony','Henry'])");

    assert_rows!(ctx, "SELECT * FROM Test", 2);

    let results = ctx.query("SELECT * FROM Test ORDER BY id");
    assert_eq!(results.len(), 2);

    assert!(results[0].get("id").unwrap().to_string().contains("2"));
    assert!(
        results[0]
            .get("name")
            .unwrap()
            .to_string()
            .contains("devgony")
    );
    assert!(
        results[0]
            .get("name")
            .unwrap()
            .to_string()
            .contains("Henry")
    );

    assert!(results[1].get("id").unwrap().to_string().contains("3"));
    assert!(
        results[1]
            .get("name")
            .unwrap()
            .to_string()
            .contains("Seongbin")
    );
    assert!(
        results[1]
            .get("name")
            .unwrap()
            .to_string()
            .contains("Bernie")
    );
    assert!(
        results[1]
            .get("name")
            .unwrap()
            .to_string()
            .contains("Chobobdev")
    );

    ctx.commit();
}

#[test]
fn test_insert_array_literal_simple() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Test (id INTEGER DEFAULT 1, name TEXT[] NOT NULL)");
    ctx.exec("INSERT INTO Test VALUES(5,['Jhon'])");

    assert_rows!(ctx, "SELECT * FROM Test", 1);
    ctx.assert_query_value("SELECT id FROM Test", "id", Value::I32(5));

    let results = ctx.query("SELECT * FROM Test");
    assert!(results[0].get("name").unwrap().to_string().contains("Jhon"));

    ctx.commit();
}

#[test]
fn test_insert_array_with_default_id() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Test (id INTEGER DEFAULT 1, name TEXT[] NOT NULL)");
    ctx.exec("INSERT INTO Test (name) VALUES (['Jane'])");

    assert_rows!(ctx, "SELECT * FROM Test", 1);
    ctx.assert_query_value("SELECT id FROM Test", "id", Value::I32(1));

    let results = ctx.query("SELECT * FROM Test");
    assert!(results[0].get("name").unwrap().to_string().contains("Jane"));

    ctx.commit();
}

#[test]
fn test_insert_another_array_with_default_id() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Test (id INTEGER DEFAULT 1, name TEXT[] NOT NULL)");
    ctx.exec("INSERT INTO Test (name) VALUES (['Proven'])");

    assert_rows!(ctx, "SELECT * FROM Test", 1);
    ctx.assert_query_value("SELECT id FROM Test", "id", Value::I32(1));

    let results = ctx.query("SELECT * FROM Test");
    assert!(
        results[0]
            .get("name")
            .unwrap()
            .to_string()
            .contains("Proven")
    );

    ctx.commit();
}

#[test]
fn test_select_all_array_data() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Test (id INTEGER DEFAULT 1, name TEXT[] NOT NULL)");

    ctx.exec("INSERT INTO Test (id, name) VALUES (1, ['Seongbin','Bernie'])");
    ctx.exec("INSERT INTO Test (id, name) VALUES (3,Array['Seongbin','Bernie','Chobobdev']), (2,Array['devgony','Henry'])");
    ctx.exec("INSERT INTO Test VALUES(5,['Jhon'])");

    assert_rows!(ctx, "SELECT * FROM Test", 4);

    let results = ctx.query("SELECT * FROM Test ORDER BY id, name");
    assert_eq!(results.len(), 4);

    assert!(results[0].get("id").unwrap().to_string().contains("1"));
    assert!(
        results[0]
            .get("name")
            .unwrap()
            .to_string()
            .contains("Seongbin")
    );
    assert!(
        results[0]
            .get("name")
            .unwrap()
            .to_string()
            .contains("Bernie")
    );

    assert!(results[1].get("id").unwrap().to_string().contains("2"));
    assert!(
        results[1]
            .get("name")
            .unwrap()
            .to_string()
            .contains("devgony")
    );
    assert!(
        results[1]
            .get("name")
            .unwrap()
            .to_string()
            .contains("Henry")
    );

    assert!(results[2].get("id").unwrap().to_string().contains("3"));
    assert!(
        results[2]
            .get("name")
            .unwrap()
            .to_string()
            .contains("Seongbin")
    );
    assert!(
        results[2]
            .get("name")
            .unwrap()
            .to_string()
            .contains("Bernie")
    );
    assert!(
        results[2]
            .get("name")
            .unwrap()
            .to_string()
            .contains("Chobobdev")
    );

    assert!(results[3].get("id").unwrap().to_string().contains("5"));
    assert!(results[3].get("name").unwrap().to_string().contains("Jhon"));

    ctx.commit();
}

#[test]
fn test_select_mixed_type_array_literal() {
    let mut ctx = setup_test();

    let result = ctx.exec_response("SELECT ['name', 1, True] AS arr");
    println!("Mixed type array result: {:?}", result);

    let results = ctx.query("SELECT ['name', 1, True] AS arr");
    assert_eq!(results.len(), 1);

    let list_val = results[0].get("arr").unwrap();
    println!("Mixed array value: {}", list_val);
    assert!(list_val.to_string().contains("name"));
    assert!(list_val.to_string().contains("1"));
    assert!(list_val.to_string().contains("true") || list_val.to_string().contains("Bool(true)"));

    ctx.commit();
}

#[test]
fn test_array_indexing_in_select() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT ['Proven', 1, True] [0] AS element");
    assert_eq!(results.len(), 1);
    ctx.assert_query_value(
        "SELECT ['Proven', 1, True] [0] AS element",
        "element",
        Value::Str("Proven".to_string()),
    );

    ctx.commit();
}

#[test]
fn test_array_literal_data_types() {
    let mut ctx = setup_test();

    // Mixed-type arrays ARE supported in SELECT statements
    let results = ctx.query("SELECT ['string', 42, True, Null] AS mixed");
    assert_eq!(results.len(), 1);

    let mixed_val = results[0].get("mixed").unwrap();
    assert!(mixed_val.to_string().contains("string"));
    assert!(mixed_val.to_string().contains("42"));
    assert!(mixed_val.to_string().contains("true") || mixed_val.to_string().contains("Bool(true)"));
    assert!(mixed_val.to_string().contains("null") || mixed_val.to_string().contains("Null"));

    // However, when storing in tables, arrays must have uniform types
    // that match the declared column type
    ctx.exec("CREATE TABLE TypedArrayTest (id INTEGER, str_data TEXT[], int_data INT[])");

    // Homogeneous arrays work fine
    ctx.exec("INSERT INTO TypedArrayTest VALUES (1, ['a', 'b', 'c'], [1, 2, 3])");

    // Mixed-type arrays cannot be inserted into typed columns
    // They would need a VARIANT or ANY type support which isn't implemented yet
    let mixed_result = ctx.exec_response("SELECT id, ['mixed', 42] as mixed FROM TypedArrayTest");
    assert!(matches!(
        mixed_result,
        proven_sql::SqlResponse::QueryResult { .. }
    ));

    ctx.commit();
}

#[test]
fn test_array_column_not_null_constraint() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Test (id INTEGER, name TEXT[] NOT NULL)");

    // The actual error message is "NullConstraintViolation"
    ctx.assert_error_contains(
        "INSERT INTO Test (id) VALUES (1)",
        "NullConstraintViolation",
    );

    ctx.commit();
}
