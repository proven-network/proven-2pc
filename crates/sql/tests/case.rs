//! CASE expression tests
//! Based on gluesql/test-suite/src/case.rs

mod common;

use common::{TableBuilder, setup_test};

#[test]
fn test_create_table_for_case() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item").create_simple("id INTEGER, name TEXT");

    // Verify table was created
    assert_rows!(ctx, "SELECT * FROM Item", 0);

    ctx.commit();
}

#[test]
fn test_insert_data_for_case() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, name TEXT")
        .insert_values("(1, 'Harry'), (2, 'Ron'), (3, 'Hermione')");

    // Verify 3 rows were inserted
    assert_rows!(ctx, "SELECT * FROM Item", 3);
    ctx.assert_query_contains("SELECT name FROM Item WHERE id = 1", "name", "Str(Harry)");
    ctx.assert_query_contains("SELECT name FROM Item WHERE id = 2", "name", "Str(Ron)");
    ctx.assert_query_contains(
        "SELECT name FROM Item WHERE id = 3",
        "name",
        "Str(Hermione)",
    );

    ctx.commit();
}

#[test]
fn test_case_with_value_expression_and_else() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, name TEXT")
        .insert_values("(1, 'Harry'), (2, 'Ron'), (3, 'Hermione')");

    // Test CASE with value expression and ELSE clause
    // For id=1 and id=2, returns name; for id=3 (no match), returns 'Malfoy'
    let results = ctx.query(
        "SELECT CASE id
            WHEN 1 THEN name
            WHEN 2 THEN name
            WHEN 4 THEN name
            ELSE 'Malfoy' END
        AS case FROM Item",
    );

    assert_eq!(results.len(), 3);
    assert!(results[0].get("case").unwrap().contains("Harry"));
    assert!(results[1].get("case").unwrap().contains("Ron"));
    assert!(results[2].get("case").unwrap().contains("Malfoy"));

    ctx.commit();
}

#[test]
fn test_case_with_value_expression_without_else() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, name TEXT")
        .insert_values("(1, 'Harry'), (2, 'Ron'), (3, 'Hermione')");

    // Test CASE with value expression without ELSE clause
    // For id=1 and id=2, returns name; for id=3 (no match), returns NULL
    let results = ctx.query(
        "SELECT CASE id
            WHEN 1 THEN name
            WHEN 2 THEN name
            WHEN 4 THEN name
            END
        AS case FROM Item",
    );

    assert_eq!(results.len(), 3);
    assert!(results[0].get("case").unwrap().contains("Harry"));
    assert!(results[1].get("case").unwrap().contains("Ron"));
    assert_eq!(results[2].get("case").unwrap(), "Null");

    ctx.commit();
}

#[test]
fn test_case_with_boolean_expressions_and_else() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, name TEXT")
        .insert_values("(1, 'Harry'), (2, 'Ron'), (3, 'Hermione')");

    // Test CASE with boolean expressions and ELSE clause
    let results = ctx.query(
        "SELECT CASE
            WHEN name = 'Harry' THEN id
            WHEN name = 'Ron' THEN id
            WHEN name = 'Hermione' THEN id
            ELSE 404 END
        AS case FROM Item",
    );

    assert_eq!(results.len(), 3);
    assert!(results[0].get("case").unwrap().contains("1"));
    assert!(results[1].get("case").unwrap().contains("2"));
    assert!(results[2].get("case").unwrap().contains("3"));

    ctx.commit();
}

#[test]
fn test_case_with_boolean_expressions_without_else() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, name TEXT")
        .insert_values("(1, 'Harry'), (2, 'Ron'), (3, 'Hermione')");

    // Test CASE with boolean expressions without ELSE clause
    // Note: 'Hermion' is a typo (missing 'e'), so the third row returns NULL
    let results = ctx.query(
        "SELECT CASE
            WHEN name = 'Harry' THEN id
            WHEN name = 'Ron' THEN id
            WHEN name = 'Hermion' THEN id
            END
        AS case FROM Item",
    );

    assert_eq!(results.len(), 3);
    assert!(results[0].get("case").unwrap().contains("1"));
    assert!(results[1].get("case").unwrap().contains("2"));
    assert_eq!(results[2].get("case").unwrap(), "Null");

    ctx.commit();
}

#[test]
fn test_case_with_complex_expressions() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, name TEXT")
        .insert_values("(1, 'Harry'), (2, 'Ron'), (3, 'Hermione')");

    // Test CASE with complex boolean expressions and arithmetic operations
    let results = ctx.query(
        "SELECT CASE
            WHEN (name = 'Harry') OR (name = 'Ron') THEN (id + 1)
            WHEN name = ('Hermi' || 'one') THEN (id + 2)
            ELSE 404 END
        AS case FROM Item",
    );

    assert_eq!(results.len(), 3);
    assert!(results[0].get("case").unwrap().contains("2")); // id=1, 1+1=2
    assert!(results[1].get("case").unwrap().contains("3")); // id=2, 2+1=3
    assert!(results[2].get("case").unwrap().contains("5")); // id=3, 3+2=5

    ctx.commit();
}

#[test]
fn test_case_with_collate_unsupported() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, name TEXT")
        .insert_values("(1, 'Harry'), (2, 'Ron'), (3, 'Hermione')");

    // Test that COLLATE in CASE expression returns a parse error
    // Note: COLLATE is not implemented, so it fails at parse time
    ctx.assert_error_contains(
        "SELECT CASE 1 COLLATE Item
            WHEN name = 'Harry' THEN id
            WHEN name = 'Ron' THEN id
            WHEN 'Hermione' THEN id
            END
        AS case FROM Item",
        "Parse",
    );

    ctx.abort();
}

#[test]
fn test_collate_expression_unsupported() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, name TEXT")
        .insert_values("(1, 'Harry')");

    // Test that COLLATE expression in SELECT returns a parse error
    // Note: COLLATE is not implemented, so it fails at parse time
    ctx.assert_error_contains("SELECT 1 COLLATE Item FROM Item", "Parse");

    ctx.abort();
}
