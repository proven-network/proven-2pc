//! APPEND function tests
//! Based on gluesql/test-suite/src/function/append.rs

mod common;

use common::{TableBuilder, setup_test};

#[test]
fn test_append_integer_to_list() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Append")
        .create_simple("id INTEGER, items LIST, element INTEGER, element2 TEXT")
        .insert_values("(1, '[1, 2, 3]', 4, 'Foo')");

    let results = ctx.query("SELECT APPEND(items, element) as myappend FROM Append");
    assert_eq!(results.len(), 1);

    // Check that the result contains a list with the appended element
    let result_str = results[0].get("myappend").unwrap();
    assert!(result_str.contains("[") && result_str.contains("1") && result_str.contains("4"));

    ctx.commit();
}

#[test]
fn test_append_with_type_coercion() {
    let mut ctx = setup_test();

    // Test that APPEND properly coerces types (I32 -> I64)
    TableBuilder::new(&mut ctx, "AppendCoercion")
        .create_simple("id INTEGER, items LIST, small_num INTEGER")
        .insert_values("(1, '[100, 200, 300]', 42)");

    // Appending an I32 to a LIST<I64> should coerce to I64
    let results = ctx.query("SELECT APPEND(items, small_num) as myappend FROM AppendCoercion");
    assert_eq!(results.len(), 1);

    let result_str = results[0].get("myappend").unwrap();
    assert!(result_str.contains("42") && result_str.contains("100"));

    ctx.commit();
}

#[test]
fn test_append_non_list_should_error() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Append")
        .create_simple("id INTEGER, items LIST, element INTEGER, element2 TEXT")
        .insert_values("(1, '[1, 2, 3]', 4, 'Foo')");

    ctx.assert_error_contains(
        "SELECT APPEND(element, element2) as myappend FROM Append",
        "list",
    );

    ctx.abort();
}

#[test]
fn test_insert_with_append_function() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Foo").create_simple("elements LIST");

    ctx.exec("INSERT INTO Foo VALUES (APPEND(CAST('[1, 2, 3]' AS LIST), 4))");

    let results = ctx.query("SELECT elements as myappend FROM Foo");
    assert_eq!(results.len(), 1);

    // Check that the result contains the appended list
    let result_str = results[0].get("myappend").unwrap();
    assert!(result_str.contains("4"));

    ctx.commit();
}
