//! SERIES table-valued function tests
//! Based on gluesql/test-suite/src/series.rs

#[ignore = "not yet implemented"]
#[test]
fn test_series_basic() {
    // TODO: Test SELECT * FROM SERIES(3) returns rows with N column: 1, 2, 3
}

#[ignore = "not yet implemented"]
#[test]
fn test_series_case_insensitive() {
    // TODO: Test SELECT * FROM sErIeS(3) works (case insensitive)
}

#[ignore = "not yet implemented"]
#[test]
fn test_series_with_table_alias() {
    // TODO: Test SELECT S.* FROM SERIES(3) as S
}

#[ignore = "not yet implemented"]
#[test]
fn test_series_with_unary_plus() {
    // TODO: Test SELECT * FROM SERIES(+3) (unary plus is allowed)
}

#[ignore = "not yet implemented"]
#[test]
fn test_create_table_as_series() {
    // TODO: Test CREATE TABLE SeriesTable AS SELECT * FROM SERIES(3)
    // Should return Payload::Create
}

#[ignore = "not yet implemented"]
#[test]
fn test_select_from_series_table() {
    // TODO: Test SELECT * FROM SeriesTable returns the series data
}

#[ignore = "not yet implemented"]
#[test]
fn test_series_size_zero() {
    // TODO: Test SELECT * FROM SERIES(0) returns empty result set
}

#[ignore = "not yet implemented"]
#[test]
fn test_series_without_parentheses_should_error() {
    // TODO: Test SELECT * FROM SERIES should error: TableNotFound
}

#[ignore = "not yet implemented"]
#[test]
fn test_series_without_arguments_should_error() {
    // TODO: Test SELECT * FROM SERIES() should error: LackOfArgs
}

#[ignore = "not yet implemented"]
#[test]
fn test_series_negative_size_should_error() {
    // TODO: Test SELECT * FROM SERIES(-1) should error: SeriesSizeWrong
}

#[ignore = "not yet implemented"]
#[test]
fn test_select_without_table() {
    // TODO: Test SELECT 1, 'a', true, 1 + 2, 'a' || 'b' (no FROM clause)
}

#[ignore = "not yet implemented"]
#[test]
fn test_select_without_table_scalar_subquery() {
    // TODO: Test SELECT (SELECT 'Hello') (scalar subquery without table)
}

#[ignore = "not yet implemented"]
#[test]
fn test_select_without_table_with_aliases() {
    // TODO: Test SELECT 1 AS id, (SELECT MAX(N) FROM SERIES(3)) AS max
}

#[ignore = "not yet implemented"]
#[test]
fn test_select_without_table_in_derived() {
    // TODO: Test SELECT * FROM (SELECT 1) AS Derived
}

#[ignore = "not yet implemented"]
#[test]
fn test_select_star_without_table() {
    // TODO: Test SELECT * returns column N with value 1
}

#[ignore = "not yet implemented"]
#[test]
fn test_create_table_as_select_without_table() {
    // TODO: Test CREATE TABLE TargetTable AS SELECT 1
}

#[ignore = "not yet implemented"]
#[test]
fn test_select_from_created_target_table() {
    // TODO: Test SELECT * FROM TargetTable returns the created data
}
