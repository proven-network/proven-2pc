//! Tests for GROUP BY clause functionality
//! Based on gluesql/test-suite/src/aggregate/group_by.rs

use crate::common::{TableBuilder, setup_test};
use proven_value::Value;

#[test]
fn test_group_by_with_count() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, name TEXT, city TEXT")
        .insert_values(
            "(1, 'Item A', 'New York'), \
             (2, 'Item B', 'Boston'), \
             (1, 'Item C', 'New York'), \
             (3, 'Item D', 'Boston'), \
             (1, 'Item E', 'Chicago')",
        );

    let results = ctx.query("SELECT id, COUNT(*) FROM Item GROUP BY id");
    assert_eq!(results.len(), 3); // 3 distinct ids

    // Verify the counts for each id
    for row in &results {
        let id = row.get("id").unwrap(); // GROUP BY columns now keep their names
        let count = row.get("COUNT(*)").unwrap();

        if id == &Value::I32(1) {
            assert_eq!(count, &Value::I64(3)); // id 1 appears 3 times
        } else if id == &Value::I32(2) {
            assert_eq!(count, &Value::I64(1)); // id 2 appears 1 time
        } else if id == &Value::I32(3) {
            assert_eq!(count, &Value::I64(1)); // id 3 appears 1 time
        }
    }

    ctx.commit();
}

#[test]
fn test_group_by_select_column_only() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, name TEXT")
        .insert_values(
            "(1, 'Item A'), \
             (2, 'Item B'), \
             (1, 'Item C'), \
             (3, 'Item D'), \
             (1, 'Item E')",
        );

    let results = ctx.query("SELECT id FROM Item GROUP BY id");
    assert_eq!(results.len(), 3); // 3 distinct ids

    // Check that we have ids 1, 2, 3
    let mut ids: Vec<&Value> = results.iter().map(|row| row.get("id").unwrap()).collect();
    ids.sort();
    assert_eq!(ids, vec![&Value::I32(1), &Value::I32(2), &Value::I32(3)]);

    ctx.commit();
}

#[test]
fn test_group_by_with_multiple_aggregates() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, quantity INTEGER, city TEXT")
        .insert_values(
            "(1, 10, 'New York'), \
             (2, 20, 'Boston'), \
             (3, 15, 'New York'), \
             (4, 25, 'Boston'), \
             (5, 30, 'Chicago')",
        );

    let results = ctx.query("SELECT SUM(quantity), COUNT(*), city FROM Item GROUP BY city");
    assert_eq!(results.len(), 3); // 3 distinct cities

    for row in &results {
        // The city column position varies, find it by checking all columns
        let city = row
            .get("city")
            .or_else(|| row.get("column_0"))
            .or_else(|| row.get("column_2"))
            .or_else(|| {
                // Find the string column
                row.values()
                    .find(|&value| value.to_string().starts_with("Str("))
                    .map(|v| v as _)
            })
            .unwrap();
        let sum = row.get("SUM(quantity)").unwrap();
        let count = row.get("COUNT(*)").unwrap();

        if city == &Value::Str("New York".to_string()) {
            assert_eq!(sum, &Value::I32(25)); // 10 + 15
            assert_eq!(count, &Value::I64(2));
        } else if city == &Value::Str("Boston".to_string()) {
            assert_eq!(sum, &Value::I32(45)); // 20 + 25
            assert_eq!(count, &Value::I64(2));
        } else if city == &Value::Str("Chicago".to_string()) {
            assert_eq!(sum, &Value::I32(30));
            assert_eq!(count, &Value::I64(1));
        }
    }

    ctx.commit();
}

#[test]
fn test_group_by_numeric_column() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, ratio FLOAT")
        .insert_values(
            "(1, 1.5), \
             (2, 2.5), \
             (3, 1.5), \
             (4, 3.0), \
             (5, 2.5)",
        );

    let results = ctx.query("SELECT ratio, COUNT(*) FROM Item GROUP BY ratio");
    assert_eq!(results.len(), 3); // 3 distinct ratios

    for row in &results {
        // The ratio column should now have its proper name
        let ratio = row.get("ratio").unwrap();
        let count = row.get("COUNT(*)").unwrap();

        match ratio {
            Value::F64(1.5) | Value::F64(2.5) => assert_eq!(count, &Value::I64(2)),
            Value::F64(3.0) => assert_eq!(count, &Value::I64(1)),
            _ => panic!("Unexpected ratio value: {:?}", ratio),
        }
    }

    ctx.commit();
}

#[test]
fn test_group_by_multiple_columns() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, city TEXT, ratio FLOAT")
        .insert_values(
            "(1, 'New York', 1.5), \
             (1, 'Boston', 2.5), \
             (1, 'New York', 1.5), \
             (2, 'Boston', 2.5), \
             (2, 'Boston', 3.0)",
        );

    let results = ctx.query("SELECT id, city, COUNT(*) FROM Item GROUP BY id, city");
    assert_eq!(results.len(), 3); // 3 distinct (id, city) combinations

    for row in &results {
        let id = row.get("id").unwrap(); // First GROUP BY column keeps its name
        let city = row.get("city").unwrap(); // Second GROUP BY column keeps its name
        let count = row.get("COUNT(*)").unwrap();

        if id == &Value::I32(1) && city == &Value::Str("New York".to_string()) {
            assert_eq!(count, &Value::I64(2));
        } else if id == &Value::I32(1) && city == &Value::Str("Boston".to_string()) {
            assert_eq!(count, &Value::I64(1));
        } else if id == &Value::I32(2) && city == &Value::Str("Boston".to_string()) {
            assert_eq!(count, &Value::I64(2));
        }
    }

    ctx.commit();
}

#[test]
fn test_group_by_having_with_aggregate() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, quantity INTEGER, city TEXT")
        .insert_values(
            "(1, 10, 'New York'), \
             (2, 20, 'Boston'), \
             (3, 15, 'New York'), \
             (4, 25, 'Boston'), \
             (5, 30, 'Chicago')",
        );

    // Only cities with more than 1 item
    let results = ctx
        .query("SELECT SUM(quantity), COUNT(*), city FROM Item GROUP BY city HAVING COUNT(*) > 1");
    assert_eq!(results.len(), 2); // Only New York and Boston

    ctx.commit();
}

#[test]
fn test_group_by_with_subquery_context() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, quantity INTEGER")
        .insert_values(
            "(1, 10), \
             (2, 20), \
             (3, 30)",
        );

    // Subquery with GROUP BY
    let results = ctx.query(
        "SELECT * FROM (SELECT id, SUM(quantity) as total FROM Item GROUP BY id) AS sub WHERE total > 15"
    );
    assert_eq!(results.len(), 2); // Only ids 2 and 3

    ctx.commit();
}

#[test]
fn test_group_by_handles_nulls() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, city TEXT")
        .insert_values(
            "(1, 'New York'), \
             (2, NULL), \
             (3, 'Boston'), \
             (4, NULL), \
             (5, 'Boston')",
        );

    let results = ctx.query("SELECT city, COUNT(*) FROM Item GROUP BY city");
    assert_eq!(results.len(), 3); // NULL, New York, Boston

    for row in &results {
        let city = row.get("city").unwrap(); // GROUP BY column keeps its name
        let count = row.get("COUNT(*)").unwrap();

        if city == &Value::Null {
            assert_eq!(count, &Value::I64(2)); // 2 NULL values
        } else if city == &Value::Str("New York".to_string()) {
            assert_eq!(count, &Value::I64(1));
        } else if city == &Value::Str("Boston".to_string()) {
            assert_eq!(count, &Value::I64(2));
        }
    }

    ctx.commit();
}
