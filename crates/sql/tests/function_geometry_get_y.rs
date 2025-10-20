//! GET_Y geometry function tests
//! Based on gluesql/test-suite/src/function/geometry/get_y.rs

mod common;

use common::setup_test;
use proven_value::Value;

#[test]
fn test_create_table_with_point_column() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE PointGroup (point_field POINT)");

    ctx.commit();
}

#[test]
fn test_insert_point_value() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE PointGroup (point_field POINT)");
    ctx.exec("INSERT INTO PointGroup VALUES (CAST('POINT(0.3134 0.156)' AS POINT))");

    let results = ctx.query("SELECT * FROM PointGroup");
    assert_eq!(results.len(), 1);

    ctx.commit();
}

#[test]
fn test_get_y_from_point_column() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE PointGroup (point_field POINT)");
    ctx.exec("INSERT INTO PointGroup VALUES (CAST('POINT(0.3134 0.156)' AS POINT))");

    let results = ctx.query("SELECT GET_Y(point_field) AS point_field FROM PointGroup");
    assert_eq!(results.len(), 1);

    if let Value::F64(y) = results[0].get("point_field").unwrap() {
        assert!((y - 0.156).abs() < 0.0001);
    } else {
        panic!("Expected F64 value");
    }

    ctx.commit();
}

#[test]
fn test_get_y_from_cast_point_string() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT GET_Y(CAST('POINT(0.1 -0.2)' AS POINT)) AS pty");
    assert_eq!(results.len(), 1);

    if let Value::F64(y) = results[0].get("pty").unwrap() {
        assert!((y - (-0.2)).abs() < 0.0001);
    } else {
        panic!("Expected F64 value");
    }

    ctx.commit();
}

#[test]
fn test_get_y_from_point_literal() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT GET_Y(CAST('POINT(0.1 -0.2)' AS POINT)) AS pty");
    assert_eq!(results.len(), 1);

    if let Value::F64(y) = results[0].get("pty").unwrap() {
        assert!((y - (-0.2)).abs() < 0.0001);
    } else {
        panic!("Expected F64 value");
    }

    ctx.commit();
}

#[test]
fn test_get_y_non_point_argument_should_error() {
    let mut ctx = setup_test();

    ctx.assert_error_contains("SELECT GET_Y('cheese') AS pty", "Point");

    ctx.commit();
}

#[test]
fn test_get_y_extracts_correct_coordinate() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Locations (id INTEGER, coords POINT)");
    ctx.exec("INSERT INTO Locations VALUES (1, CAST('POINT(10.5 20.3)' AS POINT))");
    ctx.exec("INSERT INTO Locations VALUES (2, CAST('POINT(-5.2 15.7)' AS POINT))");
    ctx.exec("INSERT INTO Locations VALUES (3, CAST('POINT(0.0 0.0)' AS POINT))");

    let results = ctx.query("SELECT id, GET_Y(coords) AS y FROM Locations ORDER BY id");
    assert_eq!(results.len(), 3);

    if let Value::F64(y) = results[0].get("y").unwrap() {
        assert!((y - 20.3).abs() < 0.0001);
    } else {
        panic!("Expected F64 value");
    }

    if let Value::F64(y) = results[1].get("y").unwrap() {
        assert!((y - 15.7).abs() < 0.0001);
    } else {
        panic!("Expected F64 value");
    }

    if let Value::F64(y) = results[2].get("y").unwrap() {
        assert!((y - 0.0).abs() < 0.0001);
    } else {
        panic!("Expected F64 value");
    }

    ctx.commit();
}

#[test]
fn test_get_y_with_negative_coordinates() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE NegativePoints (coords POINT)");
    ctx.exec("INSERT INTO NegativePoints VALUES (CAST('POINT(-71.064544 42.28787)' AS POINT))");
    ctx.exec("INSERT INTO NegativePoints VALUES (CAST('POINT(-122.4194 37.7749)' AS POINT))");

    let results = ctx.query("SELECT GET_Y(coords) AS y FROM NegativePoints ORDER BY y");
    assert_eq!(results.len(), 2);

    if let Value::F64(y) = results[0].get("y").unwrap() {
        assert!((y - 37.7749).abs() < 0.0001);
    } else {
        panic!("Expected F64 value");
    }

    if let Value::F64(y) = results[1].get("y").unwrap() {
        assert!((y - 42.28787).abs() < 0.0001);
    } else {
        panic!("Expected F64 value");
    }

    ctx.commit();
}

#[test]
fn test_get_y_with_null() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE NullTest (id INTEGER, coords POINT)");
    ctx.exec("INSERT INTO NullTest VALUES (1, CAST('POINT(1.0 2.0)' AS POINT))");
    ctx.exec("INSERT INTO NullTest VALUES (2, NULL)");

    let results = ctx.query("SELECT id, GET_Y(coords) AS y FROM NullTest ORDER BY id");
    assert_eq!(results.len(), 2);

    if let Value::F64(y) = results[0].get("y").unwrap() {
        assert!((y - 2.0).abs() < 0.0001);
    } else {
        panic!("Expected F64 value");
    }

    assert_eq!(results[1].get("y").unwrap(), &Value::Null);

    ctx.commit();
}

#[test]
fn test_get_y_no_arguments_should_error() {
    let mut ctx = setup_test();

    ctx.assert_error_contains("SELECT GET_Y() AS y", "argument");

    ctx.commit();
}

#[test]
fn test_get_y_too_many_arguments_should_error() {
    let mut ctx = setup_test();

    ctx.assert_error_contains(
        "SELECT GET_Y(CAST('POINT(1.0 2.0)' AS POINT), CAST('POINT(3.0 4.0)' AS POINT)) AS y",
        "argument",
    );

    ctx.commit();
}
