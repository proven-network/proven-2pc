//! CALC_DISTANCE geometry function tests
//! Based on gluesql/test-suite/src/function/geometry/calc_distance.rs

#[ignore = "not yet implemented"]
#[test]
fn test_create_table_with_point_columns() {
    // TODO: Test CREATE TABLE Foo (geo1 Point, geo2 Point, bar Float) should return Payload::Create
}

#[ignore = "not yet implemented"]
#[test]
fn test_insert_point_values() {
    // TODO: Test INSERT INTO Foo VALUES (POINT(0.3134, 3.156), POINT(1.415, 3.231), 3)
    // Should return Payload::Insert(1)
}

#[ignore = "not yet implemented"]
#[test]
fn test_calc_distance_between_points() {
    // TODO: Test SELECT CALC_DISTANCE(geo1, geo2) AS georesult FROM Foo
    // Should return distance value: 1.104150152832485
}

#[ignore = "not yet implemented"]
#[test]
fn test_calc_distance_wrong_arg_count_should_error() {
    // TODO: Test SELECT CALC_DISTANCE(geo1) AS georesult FROM Foo
    // Should error: FunctionArgsLengthNotMatching (expected 2, found 1)
}

#[ignore = "not yet implemented"]
#[test]
fn test_calc_distance_non_point_argument_should_error() {
    // TODO: Test SELECT CALC_DISTANCE(geo1, bar) AS georesult FROM Foo
    // Should error: FunctionRequiresPointValue
}

#[ignore = "not yet implemented"]
#[test]
fn test_calc_distance_with_null_returns_null() {
    // TODO: Test SELECT CALC_DISTANCE(geo1, NULL) AS georesult FROM Foo
    // Should return NULL
}

#[ignore = "not yet implemented"]
#[test]
fn test_calc_distance_function_signature() {
    // TODO: Test that CALC_DISTANCE requires exactly 2 Point arguments
}

#[ignore = "not yet implemented"]
#[test]
fn test_calc_distance_with_point_literals() {
    // TODO: Test CALC_DISTANCE with POINT() literal expressions
}
