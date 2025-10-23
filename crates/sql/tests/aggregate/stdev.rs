//! Standard deviation aggregate function tests
//! Based on gluesql/test-suite/src/aggregate/stdev.rs

use crate::common::{TableBuilder, setup_test};
use proven_value::Value;

#[test]
fn test_stdev_with_null_values() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, quantity INTEGER, age INTEGER, total INTEGER")
        .insert_values(
            "(1, 10, 11, 1), \
             (2, 0, 90, 2), \
             (3, 9, NULL, 3), \
             (4, 3, 3, 1), \
             (5, 25, NULL, 1)",
        );

    // STDEV(age) with only 3 non-null values should still work
    // Values are 11, 90, 3
    // Mean = (11+90+3)/3 = 34.667
    // Variance = ((11-34.667)^2 + (90-34.667)^2 + (3-34.667)^2) / 2
    //          = (559.111 + 3062.778 + 1003.111) / 2 = 2312.5
    // STDEV = sqrt(2312.5) = 48.0885
    let results = ctx.query("SELECT STDEV(age) FROM Item");
    assert_eq!(results.len(), 1);
    let stdev_val = results[0].get("STDEV(age)").unwrap();

    // Check if it's approximately 48.0885
    if let Value::F64(val) = stdev_val {
        assert!(
            (val - 48.0885).abs() < 0.01,
            "Expected ~48.0885, got {}",
            val
        );
    } else {
        panic!("Expected F64 value, got {:?}", stdev_val);
    }

    ctx.commit();
}

#[test]
fn test_stdev_basic_calculation() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, quantity INTEGER, age INTEGER, total INTEGER")
        .insert_values(
            "(1, 10, 11, 1), \
             (2, 0, 90, 2), \
             (3, 9, NULL, 3), \
             (4, 3, 3, 1), \
             (5, 25, NULL, 1)",
        );

    // STDEV(total) = STDEV([1, 2, 3, 1, 1])
    // Mean = (1+2+3+1+1)/5 = 1.6
    // Variance = ((1-1.6)^2 + (2-1.6)^2 + (3-1.6)^2 + (1-1.6)^2 + (1-1.6)^2) / 4
    //          = (0.36 + 0.16 + 1.96 + 0.36 + 0.36) / 4 = 0.8
    // STDEV = sqrt(0.8) = 0.8944...
    let results = ctx.query("SELECT STDEV(total) FROM Item");
    assert_eq!(results.len(), 1);
    let stdev_val = results[0].get("STDEV(total)").unwrap();

    // Check if it's approximately 0.8944
    if let Value::F64(val) = stdev_val {
        assert!((val - 0.8944).abs() < 0.01, "Expected ~0.8944, got {}", val);
    } else {
        panic!("Expected F64 value, got {:?}", stdev_val);
    }

    ctx.commit();
}

#[test]
fn test_stdev_distinct_values() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, quantity INTEGER, age INTEGER, total INTEGER")
        .insert_values(
            "(1, 10, 11, 1), \
             (2, 0, 90, 2), \
             (3, 9, NULL, 3), \
             (4, 3, 3, 1), \
             (5, 25, NULL, 1)",
        );

    // STDEV(DISTINCT id) = STDEV([1, 2, 3, 4, 5])
    // Mean = 3
    // Variance = ((1-3)^2 + (2-3)^2 + (3-3)^2 + (4-3)^2 + (5-3)^2) / 4
    //          = (4 + 1 + 0 + 1 + 4) / 4 = 2.5
    // STDEV = sqrt(2.5) = 1.5811... ≈ sqrt(2.5)
    let results = ctx.query("SELECT STDEV(DISTINCT id) FROM Item");
    assert_eq!(results.len(), 1);
    let stdev_val = results[0].get("STDEV(DISTINCT id)").unwrap();

    // Check if it's approximately sqrt(2.5) = 1.5811
    if let Value::F64(val) = stdev_val {
        let expected = (2.5_f64).sqrt();
        assert!(
            (val - expected).abs() < 0.01,
            "Expected ~{}, got {}",
            expected,
            val
        );
    } else {
        panic!("Expected F64 value, got {:?}", stdev_val);
    }

    ctx.commit();
}

#[test]
fn test_stdev_distinct_with_nulls() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, quantity INTEGER, age INTEGER, total INTEGER")
        .insert_values(
            "(1, 10, 11, 1), \
             (2, 0, 90, 2), \
             (3, 9, NULL, 3), \
             (4, 3, 3, 1), \
             (5, 25, NULL, 1)",
        );

    // STDEV(DISTINCT age) - distinct non-null values are [11, 90, 3]
    // Mean = (11+90+3)/3 = 34.667
    // Variance = ((11-34.667)^2 + (90-34.667)^2 + (3-34.667)^2) / 2
    //          = 2312.5
    // STDEV = sqrt(2312.5) = 48.0885
    let results = ctx.query("SELECT STDEV(DISTINCT age) FROM Item");
    assert_eq!(results.len(), 1);
    let stdev_val = results[0].get("STDEV(DISTINCT age)").unwrap();

    // Check if it's approximately 48.0885
    if let Value::F64(val) = stdev_val {
        assert!(
            (val - 48.0885).abs() < 0.01,
            "Expected ~48.0885, got {}",
            val
        );
    } else {
        panic!("Expected F64 value, got {:?}", stdev_val);
    }

    ctx.commit();
}

#[test]
fn test_stdev_requires_multiple_values() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, value INTEGER")
        .insert_values("(1, 10)");

    // STDEV with only one value should return NULL
    ctx.assert_query_value("SELECT STDEV(value) FROM Item", "STDEV(value)", Value::Null);

    ctx.commit();
}

#[test]
fn test_stdev_all_nulls() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, value INTEGER")
        .insert_values(
            "(1, NULL), \
             (2, NULL), \
             (3, NULL)",
        );

    // STDEV of all NULLs should return NULL
    ctx.assert_query_value("SELECT STDEV(value) FROM Item", "STDEV(value)", Value::Null);

    ctx.commit();
}

#[test]
fn test_stdev_with_duplicates() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, value INTEGER")
        .insert_values(
            "(1, 10), \
             (2, 10), \
             (3, 10), \
             (4, 20), \
             (5, 20), \
             (6, 20)",
        );

    // STDEV([10, 10, 10, 20, 20, 20])
    // Mean = 15
    // Variance = 3 * (10-15)^2 + 3 * (20-15)^2 / 5 = 150/5 = 30
    // STDEV = sqrt(30) = 5.477...
    let results = ctx.query("SELECT STDEV(value) FROM Item");
    assert_eq!(results.len(), 1);
    let stdev_val = results[0].get("STDEV(value)").unwrap();

    if let Value::F64(val) = stdev_val {
        let expected = 30_f64.sqrt();
        assert!(
            (val - expected).abs() < 0.01,
            "Expected ~{}, got {}",
            expected,
            val
        );
    } else {
        panic!("Expected F64 value, got {:?}", stdev_val);
    }

    ctx.commit();
}
