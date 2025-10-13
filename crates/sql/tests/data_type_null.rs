//! Tests for NULL value handling in data types
//! Based on gluesql/test-suite/src/data_type/null.rs

mod common;

use common::setup_test;
use proven_value::Value;
#[test]
fn test_null_is_null_condition() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT NULL IS NULL as res");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("res").unwrap(), &Value::Bool(true));

    ctx.commit();
}

#[test]
fn test_null_binary_operators_equality() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT NULL = NULL as res");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("res").unwrap(), &Value::Null);

    ctx.commit();
}

#[test]
fn test_null_binary_operators_comparison() {
    let mut ctx = setup_test();

    let comparison_ops = vec![">", "<", ">=", "<=", "<>"];

    for op in comparison_ops {
        let sql = format!("SELECT NULL {} NULL as res", op);
        let results = ctx.query(&sql);
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].get("res").unwrap(),
            &Value::Null,
            "NULL {} NULL should return NULL",
            op
        );
    }

    ctx.commit();
}

#[test]
#[ignore = "Bitwise operators not yet implemented"]
fn test_null_binary_operators_bitwise() {
    let mut ctx = setup_test();

    // Note: These operators are not yet implemented in our SQL parser
    // & (bitwise AND), || (string concatenation conflicts with OR),
    // << (left shift), >> (right shift)
    let bitwise_ops = vec!["&", "||", "<<", ">>"];

    for op in bitwise_ops {
        let sql = format!("SELECT NULL {} NULL as res", op);
        let results = ctx.query(&sql);
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].get("res").unwrap(),
            &Value::Null,
            "NULL {} NULL should return NULL",
            op
        );
    }

    ctx.commit();
}

#[test]
fn test_null_binary_operators_arithmetic() {
    let mut ctx = setup_test();

    let arithmetic_ops = vec!["+", "-", "*", "/", "%"];

    for op in arithmetic_ops {
        let sql = format!("SELECT NULL {} NULL as res", op);
        let results = ctx.query(&sql);
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].get("res").unwrap(),
            &Value::Null,
            "NULL {} NULL should return NULL",
            op
        );
    }

    ctx.commit();
}

#[test]
fn test_null_unary_operators_arithmetic() {
    let mut ctx = setup_test();

    let unary_ops = vec!["-", "+"];

    for op in unary_ops {
        let sql = format!("SELECT {} NULL as res", op);
        let results = ctx.query(&sql);
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].get("res").unwrap(),
            &Value::Null,
            "{} NULL should return NULL",
            op
        );
    }

    ctx.commit();
}

#[test]
fn test_null_unary_operators_logical() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT NOT NULL as res");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("res").unwrap(), &Value::Null);

    ctx.commit();
}

#[test]
fn test_null_propagation_in_expressions() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE TestTable (id INT, val INT)");
    ctx.exec("INSERT INTO TestTable VALUES (1, 10), (2, NULL), (3, 30)");

    let results = ctx.query("SELECT id, val + 5 as result FROM TestTable ORDER BY id");
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].get("result").unwrap(), &Value::I32(15));
    assert_eq!(results[1].get("result").unwrap(), &Value::Null);
    assert_eq!(results[2].get("result").unwrap(), &Value::I32(35));

    let results = ctx.query("SELECT id, val * 2 as result FROM TestTable ORDER BY id");
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].get("result").unwrap(), &Value::I32(20));
    assert_eq!(results[1].get("result").unwrap(), &Value::Null);
    assert_eq!(results[2].get("result").unwrap(), &Value::I32(60));

    let results = ctx.query("SELECT id, val - 3 as result FROM TestTable ORDER BY id");
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].get("result").unwrap(), &Value::I32(7));
    assert_eq!(results[1].get("result").unwrap(), &Value::Null);
    assert_eq!(results[2].get("result").unwrap(), &Value::I32(27));

    let results = ctx.query("SELECT id, val > 15 as result FROM TestTable ORDER BY id");
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].get("result").unwrap(), &Value::Bool(false));
    assert_eq!(results[1].get("result").unwrap(), &Value::Null);
    assert_eq!(results[2].get("result").unwrap(), &Value::Bool(true));

    let results = ctx.query("SELECT id, val = NULL as result FROM TestTable ORDER BY id");
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].get("result").unwrap(), &Value::Null);
    assert_eq!(results[1].get("result").unwrap(), &Value::Null);
    assert_eq!(results[2].get("result").unwrap(), &Value::Null);

    let results = ctx.query("SELECT id, val IS NULL as result FROM TestTable ORDER BY id");
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].get("result").unwrap(), &Value::Bool(false));
    assert_eq!(results[1].get("result").unwrap(), &Value::Bool(true));
    assert_eq!(results[2].get("result").unwrap(), &Value::Bool(false));

    ctx.commit();
}
