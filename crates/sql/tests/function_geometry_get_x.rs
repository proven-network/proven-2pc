//! GET_X geometry function tests
//! Based on gluesql/test-suite/src/function/geometry/get_x.rs

#[ignore = "not yet implemented"]
#[test]
fn test_create_table_with_point_column() {
    // TODO: Test CREATE TABLE PointGroup (point_field POINT) should return Payload::Create
}

#[ignore = "not yet implemented"]
#[test]
fn test_insert_point_value() {
    // TODO: Test INSERT INTO PointGroup VALUES (POINT(0.3134, 0.156)) should return Payload::Insert(1)
}

#[ignore = "not yet implemented"]
#[test]
fn test_get_x_from_point_column() {
    // TODO: Test SELECT GET_X(point_field) AS point_field FROM PointGroup
    // Should return x-coordinate: 0.3134
}

#[ignore = "not yet implemented"]
#[test]
fn test_get_x_from_cast_point_string() {
    // TODO: Test SELECT GET_X(CAST('POINT(0.1 -0.2)' AS POINT)) AS ptx
    // Should return x-coordinate: 0.1
}

#[ignore = "not yet implemented"]
#[test]
fn test_get_x_from_point_literal() {
    // TODO: Test SELECT GET_X(POINT(0.1, -0.2)) AS ptx
    // Should return x-coordinate: 0.1
}

#[ignore = "not yet implemented"]
#[test]
fn test_get_x_non_point_argument_should_error() {
    // TODO: Test SELECT GET_X('cheese') AS ptx
    // Should error: FunctionRequiresPointValue
}

#[ignore = "not yet implemented"]
#[test]
fn test_get_x_extracts_correct_coordinate() {
    // TODO: Test that GET_X returns the first (x) coordinate from POINT values
}

#[ignore = "not yet implemented"]
#[test]
fn test_get_x_with_negative_coordinates() {
    // TODO: Test GET_X with points that have negative x coordinates
}
