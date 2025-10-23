//! GET_X geometry function tests
//! Based on gluesql/test-suite/src/function/geometry/get_x.rs

use crate::common::setup_test;
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
fn test_get_x_from_point_column() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE PointGroup (point_field POINT)");
    ctx.exec("INSERT INTO PointGroup VALUES (CAST('POINT(0.3134 0.156)' AS POINT))");

    let results = ctx.query("SELECT GET_X(point_field) AS point_field FROM PointGroup");
    assert_eq!(results.len(), 1);

    if let Value::F64(x) = results[0].get("point_field").unwrap() {
        assert!((x - 0.3134).abs() < 0.0001);
    } else {
        panic!("Expected F64 value");
    }

    ctx.commit();
}

#[test]
fn test_get_x_from_cast_point_string() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT GET_X(CAST('POINT(0.1 -0.2)' AS POINT)) AS ptx");
    assert_eq!(results.len(), 1);

    if let Value::F64(x) = results[0].get("ptx").unwrap() {
        assert!((x - 0.1).abs() < 0.0001);
    } else {
        panic!("Expected F64 value");
    }

    ctx.commit();
}

#[test]
fn test_get_x_from_point_literal() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT GET_X(CAST('POINT(0.1 -0.2)' AS POINT)) AS ptx");
    assert_eq!(results.len(), 1);

    if let Value::F64(x) = results[0].get("ptx").unwrap() {
        assert!((x - 0.1).abs() < 0.0001);
    } else {
        panic!("Expected F64 value");
    }

    ctx.commit();
}

#[test]
fn test_get_x_non_point_argument_should_error() {
    let mut ctx = setup_test();

    ctx.assert_error_contains("SELECT GET_X('cheese') AS ptx", "Point");

    ctx.commit();
}

#[test]
fn test_get_x_extracts_correct_coordinate() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Locations (id INTEGER, coords POINT)");
    ctx.exec("INSERT INTO Locations VALUES (1, CAST('POINT(10.5 20.3)' AS POINT))");
    ctx.exec("INSERT INTO Locations VALUES (2, CAST('POINT(-5.2 15.7)' AS POINT))");
    ctx.exec("INSERT INTO Locations VALUES (3, CAST('POINT(0.0 0.0)' AS POINT))");

    let results = ctx.query("SELECT id, GET_X(coords) AS x FROM Locations ORDER BY id");
    assert_eq!(results.len(), 3);

    if let Value::F64(x) = results[0].get("x").unwrap() {
        assert!((x - 10.5).abs() < 0.0001);
    } else {
        panic!("Expected F64 value");
    }

    if let Value::F64(x) = results[1].get("x").unwrap() {
        assert!((x - (-5.2)).abs() < 0.0001);
    } else {
        panic!("Expected F64 value");
    }

    if let Value::F64(x) = results[2].get("x").unwrap() {
        assert!((x - 0.0).abs() < 0.0001);
    } else {
        panic!("Expected F64 value");
    }

    ctx.commit();
}

#[test]
fn test_get_x_with_negative_coordinates() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE NegativePoints (coords POINT)");
    ctx.exec("INSERT INTO NegativePoints VALUES (CAST('POINT(-71.064544 42.28787)' AS POINT))");
    ctx.exec("INSERT INTO NegativePoints VALUES (CAST('POINT(-122.4194 37.7749)' AS POINT))");

    let results = ctx.query("SELECT GET_X(coords) AS x FROM NegativePoints ORDER BY x");
    assert_eq!(results.len(), 2);

    if let Value::F64(x) = results[0].get("x").unwrap() {
        assert!((x - (-122.4194)).abs() < 0.0001);
    } else {
        panic!("Expected F64 value");
    }

    if let Value::F64(x) = results[1].get("x").unwrap() {
        assert!((x - (-71.064544)).abs() < 0.0001);
    } else {
        panic!("Expected F64 value");
    }

    ctx.commit();
}

#[test]
fn test_get_x_with_null() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE NullTest (id INTEGER, coords POINT)");
    ctx.exec("INSERT INTO NullTest VALUES (1, CAST('POINT(1.0 2.0)' AS POINT))");
    ctx.exec("INSERT INTO NullTest VALUES (2, NULL)");

    let results = ctx.query("SELECT id, GET_X(coords) AS x FROM NullTest ORDER BY id");
    assert_eq!(results.len(), 2);

    if let Value::F64(x) = results[0].get("x").unwrap() {
        assert!((x - 1.0).abs() < 0.0001);
    } else {
        panic!("Expected F64 value");
    }

    assert_eq!(results[1].get("x").unwrap(), &Value::Null);

    ctx.commit();
}

#[test]
fn test_get_x_no_arguments_should_error() {
    let mut ctx = setup_test();

    ctx.assert_error_contains("SELECT GET_X() AS x", "argument");

    ctx.commit();
}

#[test]
fn test_get_x_too_many_arguments_should_error() {
    let mut ctx = setup_test();

    ctx.assert_error_contains(
        "SELECT GET_X(CAST('POINT(1.0 2.0)' AS POINT), CAST('POINT(3.0 4.0)' AS POINT)) AS x",
        "argument",
    );

    ctx.commit();
}
