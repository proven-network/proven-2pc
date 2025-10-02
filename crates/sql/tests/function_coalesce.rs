//! COALESCE function tests
//! Based on gluesql/test-suite/src/function/coalesce.rs

mod common;

use common::{TableBuilder, setup_test};

#[test]
fn test_coalesce_no_arguments() {
    let mut ctx = setup_test();

    ctx.assert_error_contains("SELECT COALESCE() AS coalesce", "argument");

    ctx.abort();
}

#[test]
fn test_coalesce_single_null() {
    let mut ctx = setup_test();

    ctx.assert_query_contains("SELECT COALESCE(NULL) AS coalesce", "coalesce", "Null");

    ctx.commit();
}

#[test]
fn test_coalesce_null_and_value() {
    let mut ctx = setup_test();

    ctx.assert_query_contains(
        "SELECT COALESCE(NULL, 42) AS coalesce",
        "coalesce",
        "I32(42)",
    );

    ctx.commit();
}

#[test]
#[ignore = "subqueries in COALESCE not yet supported"]
fn test_coalesce_with_subqueries() {
    let mut ctx = setup_test();

    ctx.assert_query_contains(
        "SELECT COALESCE((SELECT NULL), (SELECT 42)) as coalesce",
        "coalesce",
        "I32(42)",
    );

    ctx.commit();
}

#[test]
fn test_coalesce_nested() {
    let mut ctx = setup_test();

    ctx.assert_query_contains(
        "SELECT COALESCE(COALESCE(NULL), COALESCE(NULL, 'Answer to the Ultimate Question of Life')) as coalesce",
        "coalesce",
        "Str(Answer to the Ultimate Question of Life)",
    );

    ctx.commit();
}

#[test]
fn test_coalesce_non_null_first() {
    let mut ctx = setup_test();

    ctx.assert_query_contains(
        "SELECT COALESCE('Hitchhiker', NULL) AS coalesce",
        "coalesce",
        "Str(Hitchhiker)",
    );

    ctx.commit();
}

#[test]
fn test_coalesce_all_nulls() {
    let mut ctx = setup_test();

    ctx.assert_query_contains(
        "SELECT COALESCE(NULL, NULL, NULL) AS coalesce",
        "coalesce",
        "Null",
    );

    ctx.commit();
}

#[test]
fn test_coalesce_multiple_integers() {
    let mut ctx = setup_test();

    ctx.assert_query_contains(
        "SELECT COALESCE(NULL, 42, 84) AS coalesce",
        "coalesce",
        "I32(42)",
    );

    ctx.commit();
}

#[test]
fn test_coalesce_multiple_floats() {
    let mut ctx = setup_test();

    ctx.assert_query_contains(
        "SELECT COALESCE(NULL, 1.23, 4.56) AS coalesce",
        "coalesce",
        "F64(1.23)",
    );

    ctx.commit();
}

#[test]
fn test_coalesce_multiple_booleans() {
    let mut ctx = setup_test();

    ctx.assert_query_contains(
        "SELECT COALESCE(NULL, TRUE, FALSE) AS coalesce",
        "coalesce",
        "Bool(true)",
    );

    ctx.commit();
}

#[test]
fn test_coalesce_invalid_nested_expression() {
    let mut ctx = setup_test();

    ctx.assert_error_contains("SELECT COALESCE(NULL, COALESCE())", "argument");

    ctx.abort();
}

#[test]
fn test_coalesce_with_table_columns() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "TestCoalesce")
        .create_simple("id INTEGER, text_value TEXT NULL, integer_value INTEGER NULL, float_value REAL NULL, boolean_value BOOLEAN NULL")
        .insert_values("(1, 'Hitchhiker', NULL, NULL, NULL), (2, NULL, 42, NULL, NULL), (3, NULL, NULL, 1.11, NULL), (4, NULL, NULL, NULL, TRUE), (5, 'Universe', 84, 2.22, FALSE)");

    let results = ctx.query(
        "SELECT id,
            COALESCE(text_value, 'Default') AS coalesce_text,
            COALESCE(integer_value, 0) AS coalesce_integer,
            COALESCE(float_value, 0.1) AS coalesce_float,
            COALESCE(boolean_value, FALSE) AS coalesce_boolean
        FROM TestCoalesce ORDER BY id ASC",
    );

    assert_eq!(results.len(), 5);
    assert!(
        results[0]
            .get("coalesce_text")
            .unwrap()
            .contains("Hitchhiker")
    );
    assert!(results[1].get("coalesce_text").unwrap().contains("Default"));
    assert!(results[1].get("coalesce_integer").unwrap().contains("42"));

    ctx.commit();
}

#[test]
fn test_coalesce_multiple_table_columns() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "TestCoalesce")
        .create_simple("id INTEGER, text_value TEXT NULL, integer_value INTEGER NULL, float_value REAL NULL, boolean_value BOOLEAN NULL")
        .insert_values("(1, 'Hitchhiker', NULL, NULL, NULL), (2, NULL, 42, NULL, NULL), (3, NULL, NULL, 1.11, NULL), (4, NULL, NULL, NULL, TRUE), (5, 'Universe', 84, 2.22, FALSE)");

    let results = ctx.query(
        "SELECT id, COALESCE(text_value, integer_value, float_value, boolean_value) AS coalesce FROM TestCoalesce ORDER BY id ASC",
    );

    assert_eq!(results.len(), 5);
    assert!(results[0].get("coalesce").unwrap().contains("Hitchhiker"));
    assert!(results[1].get("coalesce").unwrap().contains("42"));
    assert!(results[2].get("coalesce").unwrap().contains("1.11"));
    assert!(results[3].get("coalesce").unwrap().contains("true"));
    assert!(results[4].get("coalesce").unwrap().contains("Universe"));

    ctx.commit();
}
