//! Arithmetic projection tests
//! Based on gluesql/test-suite/src/arithmetic/project.rs

mod common;

use common::{TableBuilder, setup_test};
use proven_value::Value;

#[test]
fn test_complex_arithmetic_expression() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Arith")
        .create_simple("id INTEGER, num INTEGER")
        .insert_values("(1, 6), (2, 8), (3, 4), (4, 2), (5, 3)");

    // Test complex arithmetic: 1 * 2 + 1 - 3 / 1 = 2 + 1 - 3 = 0
    let results = ctx.query("SELECT 1 * 2 + 1 - 3 / 1 FROM Arith LIMIT 1");

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("column_0"), Some(&Value::I32(0)));

    ctx.commit();
}

#[test]
fn test_arithmetic_with_columns() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Arith")
        .create_simple("id INTEGER, num INTEGER")
        .insert_values("(1, 6), (2, 8), (3, 4), (4, 2), (5, 3)");

    // Test arithmetic with column values
    let results = ctx.query("SELECT id, id + 1, id + num, 1 + 1 FROM Arith");

    assert_eq!(results.len(), 5);

    // Check first row: id=1, id+1=2 (column_1), id+num=7 (column_2), 1+1=2 (column_3)
    assert_eq!(results[0].get("id"), Some(&Value::I32(1)));
    assert_eq!(results[0].get("column_1"), Some(&Value::I32(2)));
    assert_eq!(results[0].get("column_2"), Some(&Value::I32(7)));
    assert_eq!(results[0].get("column_3"), Some(&Value::I32(2)));

    // Check second row: id=2, id+1=3, id+num=10, 1+1=2
    assert_eq!(results[1].get("id"), Some(&Value::I32(2)));
    assert_eq!(results[1].get("column_1"), Some(&Value::I32(3)));
    assert_eq!(results[1].get("column_2"), Some(&Value::I32(10)));
    assert_eq!(results[1].get("column_3"), Some(&Value::I32(2)));

    // Check third row: id=3, id+1=4, id+num=7, 1+1=2
    assert_eq!(results[2].get("id"), Some(&Value::I32(3)));
    assert_eq!(results[2].get("column_1"), Some(&Value::I32(4)));
    assert_eq!(results[2].get("column_2"), Some(&Value::I32(7)));
    assert_eq!(results[2].get("column_3"), Some(&Value::I32(2)));

    // Check fourth row: id=4, id+1=5, id+num=6, 1+1=2
    assert_eq!(results[3].get("id"), Some(&Value::I32(4)));
    assert_eq!(results[3].get("column_1"), Some(&Value::I32(5)));
    assert_eq!(results[3].get("column_2"), Some(&Value::I32(6)));
    assert_eq!(results[3].get("column_3"), Some(&Value::I32(2)));

    // Check fifth row: id=5, id+1=6, id+num=8, 1+1=2
    assert_eq!(results[4].get("id"), Some(&Value::I32(5)));
    assert_eq!(results[4].get("column_1"), Some(&Value::I32(6)));
    assert_eq!(results[4].get("column_2"), Some(&Value::I32(8)));
    assert_eq!(results[4].get("column_3"), Some(&Value::I32(2)));

    ctx.commit();
}

#[test]
fn test_arithmetic_with_join() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Arith")
        .create_simple("id INTEGER, num INTEGER")
        .insert_values("(1, 6), (2, 8), (3, 4), (4, 2), (5, 3)");

    // Test arithmetic across joined tables
    let results = ctx.query("SELECT a.id + b.id FROM Arith a JOIN Arith b ON a.id = b.id + 1");

    assert_eq!(results.len(), 4);

    // a.id=2, b.id=1: 2+1=3
    assert_eq!(results[0].get("column_0"), Some(&Value::I32(3)));
    // a.id=3, b.id=2: 3+2=5
    assert_eq!(results[1].get("column_0"), Some(&Value::I32(5)));
    // a.id=4, b.id=3: 4+3=7
    assert_eq!(results[2].get("column_0"), Some(&Value::I32(7)));
    // a.id=5, b.id=4: 5+4=9
    assert_eq!(results[3].get("column_0"), Some(&Value::I32(9)));

    ctx.commit();
}

#[test]
fn test_xor_operation() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Arith")
        .create_simple("id INTEGER, num INTEGER")
        .insert_values("(1, 6)");

    let results = ctx.query(
        "SELECT TRUE XOR TRUE, FALSE XOR FALSE, TRUE XOR FALSE, FALSE XOR TRUE FROM Arith LIMIT 1",
    );

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("column_0"), Some(&Value::Bool(false)));
    assert_eq!(results[0].get("column_1"), Some(&Value::Bool(false)));
    assert_eq!(results[0].get("column_2"), Some(&Value::Bool(true)));
    assert_eq!(results[0].get("column_3"), Some(&Value::Bool(true)));

    ctx.commit();
}
