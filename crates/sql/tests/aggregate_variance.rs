//! Variance aggregate function tests
//! Based on gluesql/test-suite/src/aggregate/variance.rs

mod common;

use common::{TableBuilder, setup_test};
use proven_value::Value;

#[test]
fn test_variance_with_null_values() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, quantity INTEGER, age INTEGER, total INTEGER")
        .insert_values(
            "(1, 10, 11, 1), \
             (2, 0, 90, 2), \
             (3, 9, NULL, 3), \
             (4, 3, 3, 1), \
             (5, 25, NULL, 1), \
             (6, 10, 11, 2), \
             (7, 25, 90, 1)",
        );

    // VARIANCE(age) with 5 non-null values [11, 90, 3, 11, 90]
    // Mean = (11+90+3+11+90)/5 = 41
    // Variance = ((11-41)^2 + (90-41)^2 + (3-41)^2 + (11-41)^2 + (90-41)^2) / 4
    //          = (900 + 2401 + 1444 + 900 + 2401) / 4 = 8046 / 4 = 2011.5
    let results = ctx.query("SELECT VARIANCE(age) FROM Item");
    assert_eq!(results.len(), 1);
    let var_val = results[0].get("VARIANCE(age)").unwrap();

    if let Value::F64(val) = var_val {
        assert!((val - 2011.5).abs() < 0.1, "Expected ~2011.5, got {}", val);
    } else {
        panic!("Expected F64 value, got {:?}", var_val);
    }

    ctx.commit();
}

#[test]
fn test_variance_multiple_columns() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, quantity INTEGER, age INTEGER, total INTEGER")
        .insert_values(
            "(1, 10, 11, 1), \
             (2, 0, 90, 2), \
             (3, 9, NULL, 3), \
             (4, 3, 3, 1), \
             (5, 25, NULL, 1), \
             (6, 10, 11, 2), \
             (7, 25, 90, 1)",
        );

    let results = ctx.query("SELECT VARIANCE(id), VARIANCE(quantity) FROM Item");
    assert_eq!(results.len(), 1);

    // VARIANCE(id) = VARIANCE([1,2,3,4,5,6,7])
    // Mean = 4
    // Variance = ((1-4)^2 + (2-4)^2 + (3-4)^2 + (4-4)^2 + (5-4)^2 + (6-4)^2 + (7-4)^2) / 6
    //          = (9 + 4 + 1 + 0 + 1 + 4 + 9) / 6 = 28/6 = 4.667
    let var_id = results[0].get("VARIANCE(id)").unwrap();
    if let Value::F64(val) = var_id {
        assert!(
            (val - 4.666667).abs() < 0.01,
            "Expected ~4.667, got {}",
            val
        );
    } else {
        panic!("Expected F64 value for VARIANCE(id), got {:?}", var_id);
    }

    // VARIANCE(quantity) = VARIANCE([10, 0, 9, 3, 25, 10, 25])
    // Mean = 82/7 = 11.714...
    // Sample variance = 96.57
    let var_qty = results[0].get("VARIANCE(quantity)").unwrap();
    if let Value::F64(val) = var_qty {
        // Allow some tolerance for floating point calculations
        assert!((val - 96.57).abs() < 0.1, "Expected ~96.57, got {}", val);
    } else {
        panic!(
            "Expected F64 value for VARIANCE(quantity), got {:?}",
            var_qty
        );
    }

    ctx.commit();
}

#[test]
fn test_variance_distinct_values() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, quantity INTEGER, age INTEGER, total INTEGER")
        .insert_values(
            "(1, 10, 11, 1), \
             (2, 0, 90, 2), \
             (3, 9, NULL, 3), \
             (4, 3, 3, 1), \
             (5, 25, NULL, 1), \
             (6, 10, 11, 2), \
             (7, 25, 90, 1)",
        );

    // VARIANCE(DISTINCT id) = same as VARIANCE(id) since all ids are distinct
    // = 4.667
    let results = ctx.query("SELECT VARIANCE(DISTINCT id) FROM Item");
    assert_eq!(results.len(), 1);
    let var_val = results[0].get("VARIANCE(DISTINCT id)").unwrap();

    if let Value::F64(val) = var_val {
        assert!(
            (val - 4.666667).abs() < 0.01,
            "Expected ~4.667, got {}",
            val
        );
    } else {
        panic!("Expected F64 value, got {:?}", var_val);
    }

    ctx.commit();
}

#[test]
fn test_variance_distinct_with_nulls() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, quantity INTEGER, age INTEGER, total INTEGER")
        .insert_values(
            "(1, 10, 11, 1), \
             (2, 0, 90, 2), \
             (3, 9, NULL, 3), \
             (4, 3, 3, 1), \
             (5, 25, NULL, 1), \
             (6, 10, 11, 2), \
             (7, 25, 90, 1)",
        );

    // VARIANCE(DISTINCT age) - distinct non-null values are [11, 90, 3]
    // Mean = (11+90+3)/3 = 34.667
    // Variance = ((11-34.667)^2 + (90-34.667)^2 + (3-34.667)^2) / 2
    //          = 2312.5
    let results = ctx.query("SELECT VARIANCE(DISTINCT age) FROM Item");
    assert_eq!(results.len(), 1);
    let var_val = results[0].get("VARIANCE(DISTINCT age)").unwrap();

    if let Value::F64(val) = var_val {
        assert!((val - 2312.5).abs() < 1.0, "Expected ~2312.5, got {}", val);
    } else {
        panic!("Expected F64 value, got {:?}", var_val);
    }

    ctx.commit();
}

#[test]
fn test_variance_comparison_distinct_vs_all() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, quantity INTEGER, age INTEGER, total INTEGER")
        .insert_values(
            "(1, 10, 11, 1), \
             (2, 0, 90, 2), \
             (3, 9, NULL, 3), \
             (4, 3, 3, 1), \
             (5, 25, NULL, 1), \
             (6, 10, 11, 2), \
             (7, 25, 90, 1)",
        );

    let results = ctx.query("SELECT VARIANCE(quantity), VARIANCE(DISTINCT quantity) FROM Item");
    assert_eq!(results.len(), 1);

    // VARIANCE(quantity) = VARIANCE([10, 0, 9, 3, 25, 10, 25])
    let var_all = results[0].get("VARIANCE(quantity)").unwrap();
    if let Value::F64(val) = var_all {
        // Sample variance = 96.57
        assert!(
            (val - 96.57).abs() < 0.1,
            "Expected ~96.57 for VARIANCE(quantity), got {}",
            val
        );
    } else {
        panic!(
            "Expected F64 value for VARIANCE(quantity), got {:?}",
            var_all
        );
    }

    // VARIANCE(DISTINCT quantity) = VARIANCE([10, 0, 9, 3, 25])
    // Mean = (10+0+9+3+25)/5 = 9.4
    // Variance = ((10-9.4)^2 + (0-9.4)^2 + (9-9.4)^2 + (3-9.4)^2 + (25-9.4)^2) / 4
    //          = (0.36 + 88.36 + 0.16 + 40.96 + 243.36) / 4 = 373.2 / 4 = 93.3
    let var_distinct = results[0].get("VARIANCE(DISTINCT quantity)").unwrap();
    if let Value::F64(val) = var_distinct {
        assert!(
            (val - 93.3).abs() < 0.1,
            "Expected ~93.3 for VARIANCE(DISTINCT quantity), got {}",
            val
        );
    } else {
        panic!(
            "Expected F64 value for VARIANCE(DISTINCT quantity), got {:?}",
            var_distinct
        );
    }

    ctx.commit();
}

#[test]
fn test_variance_requires_multiple_values() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, value INTEGER")
        .insert_values("(1, 10)");

    // VARIANCE with only one value should return NULL
    ctx.assert_query_value(
        "SELECT VARIANCE(value) FROM Item",
        "VARIANCE(value)",
        Value::Null,
    );

    ctx.commit();
}

#[test]
fn test_variance_all_nulls() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, value INTEGER")
        .insert_values(
            "(1, NULL), \
             (2, NULL), \
             (3, NULL)",
        );

    // VARIANCE of all NULLs should return NULL
    ctx.assert_query_value(
        "SELECT VARIANCE(value) FROM Item",
        "VARIANCE(value)",
        Value::Null,
    );

    ctx.commit();
}

#[test]
fn test_variance_basic() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, value INTEGER")
        .insert_values(
            "(1, 2), \
             (2, 4), \
             (3, 6), \
             (4, 8), \
             (5, 10)",
        );

    // VARIANCE([2, 4, 6, 8, 10])
    // Mean = 6
    // Variance = ((2-6)^2 + (4-6)^2 + (6-6)^2 + (8-6)^2 + (10-6)^2) / 4
    //          = (16 + 4 + 0 + 4 + 16) / 4 = 40 / 4 = 10
    let results = ctx.query("SELECT VARIANCE(value) FROM Item");
    assert_eq!(results.len(), 1);
    let var_val = results[0].get("VARIANCE(value)").unwrap();

    if let Value::F64(val) = var_val {
        assert!((val - 10.0).abs() < 0.01, "Expected 10.0, got {}", val);
    } else {
        panic!("Expected F64 value, got {:?}", var_val);
    }

    ctx.commit();
}
