//! Arithmetic error handling tests
//! Based on gluesql/test-suite/src/arithmetic/error.rs

use crate::common::{TableBuilder, setup_test};

#[test]
fn test_division_by_zero() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Arith")
        .create_simple("id INTEGER, num INTEGER, name TEXT")
        .insert_values("(1, 6, 'A'), (2, 8, 'B'), (3, 4, 'C'), (4, 2, 'D'), (5, 3, 'E')");

    // Integer division by zero
    let err = ctx.exec_error("SELECT * FROM Arith WHERE id = 2 / 0");
    assert!(
        err.contains("zero") || err.contains("divisor") || err.contains("divide"),
        "Expected division by zero error, got: {}",
        err
    );

    // Float division by zero
    let err = ctx.exec_error("SELECT * FROM Arith WHERE id = 2 / 0.0");
    assert!(
        err.contains("zero") || err.contains("divisor") || err.contains("divide"),
        "Expected division by zero error, got: {}",
        err
    );

    ctx.commit();
}

#[test]
fn test_modulo_by_zero() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Arith")
        .create_simple("id INTEGER, num INTEGER, name TEXT")
        .insert_values("(1, 6, 'A'), (2, 8, 'B'), (3, 4, 'C'), (4, 2, 'D'), (5, 3, 'E')");

    // Integer modulo by zero
    let err = ctx.exec_error("SELECT * FROM Arith WHERE id = 2 % 0");
    assert!(
        err.contains("zero") || err.contains("divisor") || err.contains("modulo"),
        "Expected modulo by zero error, got: {}",
        err
    );

    // Float modulo by zero
    let err = ctx.exec_error("SELECT * FROM Arith WHERE id = 2 % 0.0");
    assert!(
        err.contains("zero") || err.contains("divisor") || err.contains("modulo"),
        "Expected modulo by zero error, got: {}",
        err
    );

    ctx.commit();
}

#[test]
fn test_string_arithmetic_errors() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Arith")
        .create_simple("id INTEGER, num INTEGER, name TEXT")
        .insert_values("(1, 6, 'A'), (2, 8, 'B'), (3, 4, 'C'), (4, 2, 'D'), (5, 3, 'E')");

    // String + Integer
    let err = ctx.exec_error("SELECT * FROM Arith WHERE name + id < 1");
    assert!(
        err.contains("type")
            || err.contains("numeric")
            || err.contains("operator")
            || err.contains("operation")
            || err.contains("add"),
        "Expected type error for string + int, got: {}",
        err
    );

    // String - Integer
    let err = ctx.exec_error("SELECT * FROM Arith WHERE name - id < 1");
    assert!(
        err.contains("type")
            || err.contains("numeric")
            || err.contains("operator")
            || err.contains("operation")
            || err.contains("subtract"),
        "Expected type error for string - int, got: {}",
        err
    );

    // String * Integer
    let err = ctx.exec_error("SELECT * FROM Arith WHERE name * id < 1");
    assert!(
        err.contains("type")
            || err.contains("numeric")
            || err.contains("operator")
            || err.contains("operation")
            || err.contains("multiply"),
        "Expected type error for string * int, got: {}",
        err
    );

    // String / Integer
    let err = ctx.exec_error("SELECT * FROM Arith WHERE name / id < 1");
    assert!(
        err.contains("type")
            || err.contains("numeric")
            || err.contains("operator")
            || err.contains("operation")
            || err.contains("divide"),
        "Expected type error for string / int, got: {}",
        err
    );

    // String % Integer
    let err = ctx.exec_error("SELECT * FROM Arith WHERE name % id < 1");
    assert!(
        err.contains("type")
            || err.contains("numeric")
            || err.contains("operator")
            || err.contains("operation")
            || err.contains("modulo")
            || err.contains("remainder"),
        "Expected type error for string % int, got: {}",
        err
    );

    ctx.commit();
}

#[test]
fn test_boolean_arithmetic_errors() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Arith")
        .create_simple("id INTEGER, num INTEGER, name TEXT")
        .insert_values("(1, 6, 'A'), (2, 8, 'B'), (3, 4, 'C'), (4, 2, 'D'), (5, 3, 'E')");

    // Boolean + Integer
    let err = ctx.exec_error("SELECT * FROM Arith WHERE TRUE + 1 = 1");
    assert!(
        err.contains("type")
            || err.contains("operation")
            || err.contains("boolean")
            || err.contains("add"),
        "Expected type error for boolean + int, got: {}",
        err
    );

    ctx.commit();
}

#[test]
fn test_column_not_found_in_update() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Arith")
        .create_simple("id INTEGER, num INTEGER, name TEXT")
        .insert_values("(1, 6, 'A'), (2, 8, 'B'), (3, 4, 'C'), (4, 2, 'D'), (5, 3, 'E')");

    // Try to update non-existent column
    let err = ctx.exec_error("UPDATE Arith SET aaa = 1");
    assert!(
        err.contains("column") || err.contains("aaa") || err.contains("not found"),
        "Expected column not found error, got: {}",
        err
    );

    ctx.commit();
}

#[test]
fn test_boolean_type_required() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Arith")
        .create_simple("id INTEGER, num INTEGER, name TEXT")
        .insert_values("(1, 6, 'A'), (2, 8, 'B'), (3, 4, 'C'), (4, 2, 'D'), (5, 3, 'E')");

    // String in boolean context
    let err = ctx.exec_error("SELECT * FROM Arith WHERE TRUE AND 'hello'");
    assert!(
        err.contains("boolean") || err.contains("type") || err.contains("expected"),
        "Expected boolean type error, got: {}",
        err
    );

    // Column value (string) in boolean context
    let err = ctx.exec_error("SELECT * FROM Arith WHERE name AND id");
    assert!(
        err.contains("boolean") || err.contains("type") || err.contains("expected"),
        "Expected boolean type error, got: {}",
        err
    );

    ctx.commit();
}
