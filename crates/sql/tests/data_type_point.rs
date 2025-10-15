//! POINT data type tests
//! Based on gluesql/test-suite/src/data_type/point.rs

mod common;

use common::setup_test;
use proven_value::Value;

#[test]
fn test_create_table_with_point_column() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Location (coordinates POINT)");

    // Insert a point using CAST from string
    ctx.exec("INSERT INTO Location VALUES (CAST('POINT(1.5 2.3)' AS POINT))");

    let results = ctx.query("SELECT * FROM Location");
    assert_eq!(results.len(), 1);

    ctx.commit();
}

#[test]
fn test_insert_point_from_string() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Location (id INT, coordinates POINT)");

    // Insert points using CAST from string format
    ctx.exec("INSERT INTO Location VALUES (1, CAST('POINT(0.3134 0.156)' AS POINT))");
    ctx.exec("INSERT INTO Location VALUES (2, CAST('POINT(-71.064544 42.28787)' AS POINT))");

    let results = ctx.query("SELECT id, coordinates FROM Location ORDER BY id");
    assert_eq!(results.len(), 2);

    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    if let Value::Point(p) = results[0].get("coordinates").unwrap() {
        assert!((p.x - 0.3134).abs() < 0.0001);
        assert!((p.y - 0.156).abs() < 0.0001);
    } else {
        panic!("Expected Point value");
    }

    assert_eq!(results[1].get("id").unwrap(), &Value::I32(2));
    if let Value::Point(p) = results[1].get("coordinates").unwrap() {
        assert!((p.x - (-71.064544)).abs() < 0.0001);
        assert!((p.y - 42.28787).abs() < 0.0001);
    } else {
        panic!("Expected Point value");
    }

    ctx.commit();
}

#[test]
#[should_panic(expected = "Failed to parse")]
fn test_insert_invalid_point_should_error() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Location (coordinates POINT)");

    // Invalid POINT format
    ctx.exec("INSERT INTO Location VALUES (CAST('POINT(-71.06454t4 42.28787)' AS POINT))");
}

#[test]
#[should_panic(expected = "TypeMismatch")]
fn test_insert_number_into_point_should_error() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Location (coordinates POINT)");

    // Numbers cannot be inserted into POINT fields without CAST
    ctx.exec("INSERT INTO Location VALUES (0)");
}

#[test]
fn test_select_point_values() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Location (coordinates POINT)");

    ctx.exec("INSERT INTO Location VALUES (CAST('POINT(0.3134 0.156)' AS POINT))");
    ctx.exec("INSERT INTO Location VALUES (CAST('POINT(2.0 1.0)' AS POINT))");

    let results = ctx.query("SELECT coordinates FROM Location ORDER BY coordinates");
    assert_eq!(results.len(), 2);

    // Verify they're Point values
    for result in &results {
        let point_val = result.get("coordinates").unwrap();
        match point_val {
            Value::Point(_) => { /* Expected */ }
            _ => panic!("Expected Point value, got {:?}", point_val),
        }
    }

    ctx.commit();
}

#[test]
fn test_point_comparisons() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Location (id INT, coordinates POINT)");

    ctx.exec("INSERT INTO Location VALUES (1, CAST('POINT(0.3134 0.156)' AS POINT))");
    ctx.exec("INSERT INTO Location VALUES (2, CAST('POINT(2.0 1.0)' AS POINT))");
    ctx.exec("INSERT INTO Location VALUES (3, CAST('POINT(1.0 1.0)' AS POINT))");

    // Test equality with string
    let results =
        ctx.query("SELECT id FROM Location WHERE coordinates = CAST('POINT(2.0 1.0)' AS POINT)");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(2));

    // Test greater than
    let results = ctx.query(
        "SELECT id FROM Location WHERE coordinates > CAST('POINT(1.0 1.0)' AS POINT) ORDER BY id",
    );
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(2));

    ctx.commit();
}

#[test]
fn test_update_point_value() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Location (coordinates POINT)");

    ctx.exec("INSERT INTO Location VALUES (CAST('POINT(0.3134 0.156)' AS POINT))");

    ctx.exec(
        "UPDATE Location SET coordinates = CAST('POINT(2.0 1.0)' AS POINT) WHERE coordinates = CAST('POINT(0.3134 0.156)' AS POINT)",
    );

    let results = ctx.query("SELECT coordinates FROM Location");
    assert_eq!(results.len(), 1);

    if let Value::Point(p) = results[0].get("coordinates").unwrap() {
        assert!((p.x - 2.0).abs() < 0.0001);
        assert!((p.y - 1.0).abs() < 0.0001);
    }

    ctx.commit();
}

#[test]
fn test_delete_point_value() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Location (coordinates POINT)");

    ctx.exec("INSERT INTO Location VALUES (CAST('POINT(0.3134 0.156)' AS POINT))");
    ctx.exec("INSERT INTO Location VALUES (CAST('POINT(2.0 1.0)' AS POINT))");
    ctx.exec("INSERT INTO Location VALUES (CAST('POINT(1.0 1.0)' AS POINT))");

    ctx.exec("DELETE FROM Location WHERE coordinates = CAST('POINT(2.0 1.0)' AS POINT)");

    let results = ctx.query("SELECT coordinates FROM Location ORDER BY coordinates");
    assert_eq!(results.len(), 2);

    ctx.commit();
}

#[test]
fn test_point_with_null() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Location (id INT, coordinates POINT)");

    ctx.exec("INSERT INTO Location VALUES (1, CAST('POINT(1.0 2.0)' AS POINT))");
    ctx.exec("INSERT INTO Location VALUES (2, NULL)");
    ctx.exec("INSERT INTO Location VALUES (3, CAST('POINT(3.0 4.0)' AS POINT))");

    let results = ctx.query("SELECT id FROM Location WHERE coordinates IS NOT NULL ORDER BY id");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(3));

    let results = ctx.query("SELECT id FROM Location WHERE coordinates IS NULL");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(2));

    ctx.commit();
}

#[test]
fn test_cast_string_to_point() {
    let mut ctx = setup_test();

    // Test CAST from string to POINT
    let results = ctx.query("SELECT CAST('POINT(-71.064544 42.28787)' AS POINT) AS pt");
    assert_eq!(results.len(), 1);

    if let Value::Point(p) = results[0].get("pt").unwrap() {
        assert!((p.x - (-71.064544)).abs() < 0.0001);
        assert!((p.y - 42.28787).abs() < 0.0001);
    } else {
        panic!("Expected Point value");
    }

    ctx.commit();
}
