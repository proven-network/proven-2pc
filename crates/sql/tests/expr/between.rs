//! BETWEEN expression tests
//! Based on gluesql/test-suite/src/expr/between.rs

use crate::common::setup_test;
use proven_value::Value;

#[test]
fn test_between_basic() {
    let mut ctx = setup_test();

    // Test basic BETWEEN with integers
    let test_cases = vec![
        (0, 0, 0, true),
        (1, 1, 1, true),
        (2, 1, 3, true),
        (-1, -1, 1, true),
        (1, 2, 3, false),
    ];

    for (target, lhs, rhs, expected) in test_cases {
        let results = ctx.query(&format!(
            "SELECT ({} BETWEEN {} AND {}) as res",
            target, lhs, rhs
        ));
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].get("res").unwrap(),
            &Value::Bool(expected),
            "{} BETWEEN {} AND {} should be {}",
            target,
            lhs,
            rhs,
            expected
        );
    }

    ctx.commit();
}

#[test]
fn test_between_boundary_cases() {
    let mut ctx = setup_test();

    // Test i128 boundary cases
    let min_i128 = i128::MIN.to_string();
    let max_i128 = i128::MAX.to_string();

    let results = ctx.query(&format!(
        "SELECT ({} BETWEEN {} AND {}) as res",
        min_i128, min_i128, max_i128
    ));
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("res").unwrap(), &Value::Bool(true));

    let results = ctx.query(&format!(
        "SELECT ({} BETWEEN {} AND {}) as res",
        max_i128, min_i128, max_i128
    ));
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("res").unwrap(), &Value::Bool(true));

    ctx.commit();
}

#[test]
fn test_between_with_null_target() {
    let mut ctx = setup_test();

    // NULL BETWEEN any values should return NULL
    let results = ctx.query(&format!(
        "SELECT (NULL BETWEEN {} AND {}) as res",
        i128::MIN,
        i128::MAX
    ));
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("res").unwrap(), &Value::Null);

    let results = ctx.query("SELECT (NULL BETWEEN NULL AND NULL) as res");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("res").unwrap(), &Value::Null);

    ctx.commit();
}

#[test]
fn test_between_with_null_lower_bound() {
    let mut ctx = setup_test();

    // Any value BETWEEN NULL AND x should return NULL
    let test_cases = vec![
        (1, 1),   // target same as upper bound
        (0, 0),   // both zero
        (-1, -1), // both negative
        (1, 2),   // target less than upper bound
        (2, 1),   // target greater than upper bound
    ];

    for (target, rhs) in test_cases {
        let results = ctx.query(&format!(
            "SELECT ({} BETWEEN NULL AND {}) as res",
            target, rhs
        ));
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].get("res").unwrap(),
            &Value::Null,
            "{} BETWEEN NULL AND {} should be NULL",
            target,
            rhs
        );
    }

    ctx.commit();
}

#[test]
fn test_between_with_null_upper_bound() {
    let mut ctx = setup_test();

    // Any value BETWEEN x AND NULL should return NULL
    let test_cases = vec![
        (1, 1),   // target same as lower bound
        (0, 0),   // both zero
        (-1, -1), // both negative
        (1, 2),   // target less than lower bound
        (2, 1),   // target greater than lower bound
    ];

    for (target, lhs) in test_cases {
        let results = ctx.query(&format!(
            "SELECT ({} BETWEEN {} AND NULL) as res",
            target, lhs
        ));
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].get("res").unwrap(),
            &Value::Null,
            "{} BETWEEN {} AND NULL should be NULL",
            target,
            lhs
        );
    }

    ctx.commit();
}

#[test]
fn test_not_between() {
    let mut ctx = setup_test();

    // Test NOT BETWEEN
    let test_cases = vec![
        (1, 2, 3, true),  // outside range
        (2, 1, 3, false), // inside range
        (1, 1, 1, false), // equal to bounds
    ];

    for (target, lhs, rhs, expected) in test_cases {
        let results = ctx.query(&format!(
            "SELECT ({} NOT BETWEEN {} AND {}) as res",
            target, lhs, rhs
        ));
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].get("res").unwrap(),
            &Value::Bool(expected),
            "{} NOT BETWEEN {} AND {} should be {}",
            target,
            lhs,
            rhs,
            expected
        );
    }

    ctx.commit();
}

#[test]
fn test_between_with_columns() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE BetweenTest (id INTEGER, value INTEGER, lower INTEGER, upper INTEGER)");
    ctx.exec("INSERT INTO BetweenTest VALUES (1, 5, 1, 10), (2, 15, 1, 10), (3, 10, 1, 10), (4, 0, 1, 10)");

    // Test BETWEEN with column references
    let results = ctx
        .query("SELECT id, value BETWEEN lower AND upper as in_range FROM BetweenTest ORDER BY id");
    assert_eq!(results.len(), 4);
    assert_eq!(results[0].get("in_range").unwrap(), &Value::Bool(true)); // 5 is between 1 and 10
    assert_eq!(results[1].get("in_range").unwrap(), &Value::Bool(false)); // 15 is not between 1 and 10
    assert_eq!(results[2].get("in_range").unwrap(), &Value::Bool(true)); // 10 is between 1 and 10 (inclusive)
    assert_eq!(results[3].get("in_range").unwrap(), &Value::Bool(false)); // 0 is not between 1 and 10

    ctx.commit();
}

#[test]
fn test_between_with_expressions() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE ExprTest (a INTEGER, b INTEGER, c INTEGER)");
    ctx.exec("INSERT INTO ExprTest VALUES (5, 2, 3), (10, 1, 2), (15, 5, 5)");

    // Test BETWEEN with expressions
    let results =
        ctx.query("SELECT a, (a BETWEEN b * 2 AND c * 5) as in_range FROM ExprTest ORDER BY a");
    assert_eq!(results.len(), 3);
    // 5 BETWEEN (2*2=4) AND (3*5=15) -> true
    assert_eq!(results[0].get("in_range").unwrap(), &Value::Bool(true));
    // 10 BETWEEN (1*2=2) AND (2*5=10) -> true
    assert_eq!(results[1].get("in_range").unwrap(), &Value::Bool(true));
    // 15 BETWEEN (5*2=10) AND (5*5=25) -> true
    assert_eq!(results[2].get("in_range").unwrap(), &Value::Bool(true));

    ctx.commit();
}

#[test]
fn test_between_boundary_inclusive() {
    let mut ctx = setup_test();

    // Test that BETWEEN is inclusive on both bounds
    let results = ctx.query("SELECT (5 BETWEEN 5 AND 10) as lower_bound");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("lower_bound").unwrap(), &Value::Bool(true));

    let results = ctx.query("SELECT (10 BETWEEN 5 AND 10) as upper_bound");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("upper_bound").unwrap(), &Value::Bool(true));

    let results = ctx.query("SELECT (5 BETWEEN 5 AND 5) as both_bounds");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("both_bounds").unwrap(), &Value::Bool(true));

    ctx.commit();
}

#[test]
fn test_between_with_strings() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE StringTest (value TEXT)");
    ctx.exec(
        "INSERT INTO StringTest VALUES ('apple'), ('banana'), ('cherry'), ('date'), ('elderberry')",
    );

    // Test BETWEEN with strings (lexicographic ordering)
    let results = ctx.query(
        "SELECT value FROM StringTest WHERE value BETWEEN 'banana' AND 'date' ORDER BY value",
    );
    assert_eq!(results.len(), 3);
    assert_eq!(
        results[0].get("value").unwrap(),
        &Value::Str("banana".to_string())
    );
    assert_eq!(
        results[1].get("value").unwrap(),
        &Value::Str("cherry".to_string())
    );
    assert_eq!(
        results[2].get("value").unwrap(),
        &Value::Str("date".to_string())
    );

    ctx.commit();
}

#[test]
fn test_between_in_where_clause() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE WhereTest (id INTEGER, value INTEGER)");
    ctx.exec("INSERT INTO WhereTest VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)");

    // Test BETWEEN in WHERE clause
    let results = ctx.query("SELECT id FROM WhereTest WHERE value BETWEEN 20 AND 40 ORDER BY id");
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(2));
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(3));
    assert_eq!(results[2].get("id").unwrap(), &Value::I32(4));

    ctx.commit();
}
