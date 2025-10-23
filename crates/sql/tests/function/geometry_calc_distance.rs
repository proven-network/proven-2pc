//! CALC_DISTANCE geometry function tests
//! Based on gluesql/test-suite/src/function/geometry/calc_distance.rs

use crate::common::setup_test;
use proven_value::Value;

#[test]
fn test_create_table_with_point_columns() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Foo (geo1 POINT, geo2 POINT, bar FLOAT)");

    ctx.commit();
}

#[test]
fn test_insert_point_values() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Foo (geo1 POINT, geo2 POINT, bar FLOAT)");
    ctx.exec("INSERT INTO Foo VALUES (CAST('POINT(0.3134 3.156)' AS POINT), CAST('POINT(1.415 3.231)' AS POINT), 3.0)");

    let results = ctx.query("SELECT * FROM Foo");
    assert_eq!(results.len(), 1);

    ctx.commit();
}

#[test]
fn test_calc_distance_between_points() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Foo (geo1 POINT, geo2 POINT, bar FLOAT)");
    ctx.exec("INSERT INTO Foo VALUES (CAST('POINT(0.3134 3.156)' AS POINT), CAST('POINT(1.415 3.231)' AS POINT), 3.0)");

    let results = ctx.query("SELECT CALC_DISTANCE(geo1, geo2) AS georesult FROM Foo");
    assert_eq!(results.len(), 1);

    if let Value::F64(distance) = results[0].get("georesult").unwrap() {
        // Expected distance: sqrt((1.415 - 0.3134)^2 + (3.231 - 3.156)^2) = 1.104150152832485
        assert!((distance - 1.104150152832485).abs() < 0.0001);
    } else {
        panic!("Expected F64 value");
    }

    ctx.commit();
}

#[test]
fn test_calc_distance_wrong_arg_count_should_error() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Foo (geo1 POINT, geo2 POINT, bar FLOAT)");
    ctx.exec("INSERT INTO Foo VALUES (CAST('POINT(0.3134 3.156)' AS POINT), CAST('POINT(1.415 3.231)' AS POINT), 3.0)");

    ctx.assert_error_contains("SELECT CALC_DISTANCE(geo1) AS georesult FROM Foo", "2");

    ctx.commit();
}

#[test]
fn test_calc_distance_non_point_argument_should_error() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Foo (geo1 POINT, geo2 POINT, bar FLOAT)");
    ctx.exec("INSERT INTO Foo VALUES (CAST('POINT(0.3134 3.156)' AS POINT), CAST('POINT(1.415 3.231)' AS POINT), 3.0)");

    ctx.assert_error_contains(
        "SELECT CALC_DISTANCE(geo1, bar) AS georesult FROM Foo",
        "Point",
    );

    ctx.commit();
}

#[test]
fn test_calc_distance_with_null_returns_null() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Foo (geo1 POINT, geo2 POINT, bar FLOAT)");
    ctx.exec("INSERT INTO Foo VALUES (CAST('POINT(0.3134 3.156)' AS POINT), CAST('POINT(1.415 3.231)' AS POINT), 3.0)");

    let results = ctx.query("SELECT CALC_DISTANCE(geo1, NULL) AS georesult FROM Foo");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("georesult").unwrap(), &Value::Null);

    ctx.commit();
}

#[test]
fn test_calc_distance_function_signature() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Points (id INTEGER, p1 POINT, p2 POINT)");
    ctx.exec("INSERT INTO Points VALUES (1, CAST('POINT(0.0 0.0)' AS POINT), CAST('POINT(3.0 4.0)' AS POINT))");
    ctx.exec("INSERT INTO Points VALUES (2, CAST('POINT(1.0 1.0)' AS POINT), CAST('POINT(4.0 5.0)' AS POINT))");

    // Verify CALC_DISTANCE works with two Point arguments
    let results = ctx.query("SELECT id, CALC_DISTANCE(p1, p2) AS dist FROM Points ORDER BY id");
    assert_eq!(results.len(), 2);

    // First point: distance from (0,0) to (3,4) = 5.0
    if let Value::F64(distance) = results[0].get("dist").unwrap() {
        assert!((distance - 5.0).abs() < 0.0001);
    } else {
        panic!("Expected F64 value");
    }

    // Second point: distance from (1,1) to (4,5) = 5.0
    if let Value::F64(distance) = results[1].get("dist").unwrap() {
        assert!((distance - 5.0).abs() < 0.0001);
    } else {
        panic!("Expected F64 value");
    }

    ctx.commit();
}

#[test]
fn test_calc_distance_with_point_literals() {
    let mut ctx = setup_test();

    // Test with CAST point literals (direct inline calculation)
    let results = ctx.query(
        "SELECT CALC_DISTANCE(
            CAST('POINT(0.0 0.0)' AS POINT),
            CAST('POINT(3.0 4.0)' AS POINT)
        ) AS dist",
    );
    assert_eq!(results.len(), 1);

    if let Value::F64(distance) = results[0].get("dist").unwrap() {
        assert!((distance - 5.0).abs() < 0.0001);
    } else {
        panic!("Expected F64 value");
    }

    ctx.commit();
}

#[test]
fn test_calc_distance_same_point() {
    let mut ctx = setup_test();

    // Distance from a point to itself should be 0
    let results = ctx.query(
        "SELECT CALC_DISTANCE(
            CAST('POINT(5.5 7.3)' AS POINT),
            CAST('POINT(5.5 7.3)' AS POINT)
        ) AS dist",
    );
    assert_eq!(results.len(), 1);

    if let Value::F64(distance) = results[0].get("dist").unwrap() {
        assert!(distance.abs() < 0.0001);
    } else {
        panic!("Expected F64 value");
    }

    ctx.commit();
}

#[test]
fn test_calc_distance_negative_coordinates() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Coordinates (p1 POINT, p2 POINT)");
    ctx.exec("INSERT INTO Coordinates VALUES (CAST('POINT(-3.0 -4.0)' AS POINT), CAST('POINT(0.0 0.0)' AS POINT))");

    let results = ctx.query("SELECT CALC_DISTANCE(p1, p2) AS dist FROM Coordinates");
    assert_eq!(results.len(), 1);

    // Distance from (-3, -4) to (0, 0) = 5.0
    if let Value::F64(distance) = results[0].get("dist").unwrap() {
        assert!((distance - 5.0).abs() < 0.0001);
    } else {
        panic!("Expected F64 value");
    }

    ctx.commit();
}

#[test]
fn test_calc_distance_zero_arguments_should_error() {
    let mut ctx = setup_test();

    ctx.assert_error_contains("SELECT CALC_DISTANCE() AS dist", "2");

    ctx.commit();
}

#[test]
fn test_calc_distance_too_many_arguments_should_error() {
    let mut ctx = setup_test();

    ctx.assert_error_contains(
        "SELECT CALC_DISTANCE(
            CAST('POINT(0.0 0.0)' AS POINT),
            CAST('POINT(1.0 1.0)' AS POINT),
            CAST('POINT(2.0 2.0)' AS POINT)
        ) AS dist",
        "2",
    );

    ctx.commit();
}

#[test]
fn test_calc_distance_with_both_null() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT CALC_DISTANCE(NULL, NULL) AS dist");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("dist").unwrap(), &Value::Null);

    ctx.commit();
}

#[test]
fn test_calc_distance_horizontal_line() {
    let mut ctx = setup_test();

    // Points on a horizontal line (same y-coordinate)
    let results = ctx.query(
        "SELECT CALC_DISTANCE(
            CAST('POINT(0.0 5.0)' AS POINT),
            CAST('POINT(8.0 5.0)' AS POINT)
        ) AS dist",
    );
    assert_eq!(results.len(), 1);

    if let Value::F64(distance) = results[0].get("dist").unwrap() {
        assert!((distance - 8.0).abs() < 0.0001);
    } else {
        panic!("Expected F64 value");
    }

    ctx.commit();
}

#[test]
fn test_calc_distance_vertical_line() {
    let mut ctx = setup_test();

    // Points on a vertical line (same x-coordinate)
    let results = ctx.query(
        "SELECT CALC_DISTANCE(
            CAST('POINT(3.0 0.0)' AS POINT),
            CAST('POINT(3.0 6.0)' AS POINT)
        ) AS dist",
    );
    assert_eq!(results.len(), 1);

    if let Value::F64(distance) = results[0].get("dist").unwrap() {
        assert!((distance - 6.0).abs() < 0.0001);
    } else {
        panic!("Expected F64 value");
    }

    ctx.commit();
}
