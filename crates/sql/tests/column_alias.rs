//! Column alias tests
//! Based on gluesql/test-suite/src/column_alias.rs

mod common;

use common::{TableBuilder, setup_test};

#[test]
fn test_create_tables_for_column_alias() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "InnerTable").create_simple("id INTEGER, name TEXT");
    TableBuilder::new(&mut ctx, "User").create_simple("id INTEGER, name TEXT");
    TableBuilder::new(&mut ctx, "EmptyTable").create_simple("");

    ctx.commit();
}

#[test]
fn test_insert_data_for_column_alias() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "InnerTable")
        .create_simple("id INTEGER, name TEXT")
        .insert_values("(1, 'PROVEN'), (2, 'SQL'), (3, 'SQL')");

    TableBuilder::new(&mut ctx, "User")
        .create_simple("id INTEGER, name TEXT")
        .insert_values("(1, 'Taehoon'), (2, 'Mike'), (3, 'Jorno')");

    assert_rows!(ctx, "SELECT * FROM InnerTable", 3);
    assert_rows!(ctx, "SELECT * FROM User", 3);

    ctx.commit();
}

#[test]
fn test_select_from_inner_table() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "InnerTable")
        .create_simple("id INTEGER, name TEXT")
        .insert_values("(1, 'PROVEN'), (2, 'SQL'), (3, 'SQL')");

    assert_rows!(ctx, "SELECT * FROM InnerTable", 3);
    ctx.assert_query_contains(
        "SELECT * FROM InnerTable WHERE id = 1",
        "name",
        "Str(PROVEN)",
    );
    ctx.assert_query_contains("SELECT * FROM InnerTable WHERE id = 2", "name", "Str(SQL)");

    ctx.commit();
}

#[test]
fn test_table_alias_with_full_column_aliases() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "User")
        .create_simple("id INTEGER, name TEXT")
        .insert_values("(1, 'Taehoon'), (2, 'Mike'), (3, 'Jorno')");

    // Column alias with wildcard - renames both columns to a, b
    let results = ctx.query("SELECT * FROM User AS Table(a, b)");
    assert_eq!(results.len(), 3);

    // Check that columns are now named 'a' and 'b'
    assert!(results[0].contains_key("a"));
    assert!(results[0].contains_key("b"));
    assert!(!results[0].contains_key("id"));
    assert!(!results[0].contains_key("name"));

    ctx.commit();
}

#[test]
fn test_table_alias_with_partial_column_aliases() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "User")
        .create_simple("id INTEGER, name TEXT")
        .insert_values("(1, 'Taehoon'), (2, 'Mike'), (3, 'Jorno')");

    // Partial column alias - only first column renamed
    let results = ctx.query("SELECT * FROM User AS Table(a)");
    assert_eq!(results.len(), 3);

    // Check that first column is 'a', second is still 'name'
    assert!(results[0].contains_key("a"));
    assert!(results[0].contains_key("name"));
    assert!(!results[0].contains_key("id"));

    ctx.commit();
}

#[test]
fn test_select_aliased_column() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "User")
        .create_simple("id INTEGER, name TEXT")
        .insert_values("(1, 'Taehoon'), (2, 'Mike'), (3, 'Jorno')");

    // Select using aliased column name
    let results = ctx.query("SELECT a FROM User AS Table(a, b)");
    assert_eq!(results.len(), 3);

    // Verify values
    assert!(results[0].get("a").unwrap().contains("1"));
    assert!(results[1].get("a").unwrap().contains("2"));
    assert!(results[2].get("a").unwrap().contains("3"));

    ctx.commit();
}

#[test]
fn test_table_alias_too_many_column_aliases_error() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "User").create_simple("id INTEGER, name TEXT");

    // Should error with TooManyColumnAliases (table has 2 columns, trying to alias 3)
    assert_error!(
        ctx,
        "SELECT * FROM User AS Table(a, b, c)",
        "TooManyColumnAliases"
    );

    ctx.abort();
}

#[ignore = "requires SELECT subquery column extraction"]
#[test]
fn test_inline_view_with_column_aliases() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "InnerTable")
        .create_simple("id INTEGER, name TEXT")
        .insert_values("(1, 'PROVEN'), (2, 'SQL'), (3, 'SQL')");

    // Alias columns in inline view
    let results = ctx.query("SELECT * FROM (SELECT * FROM InnerTable) AS InlineView(a, b)");
    assert_eq!(results.len(), 3);

    // Check aliased column names
    assert!(results[0].contains_key("a"));
    assert!(results[0].contains_key("b"));

    ctx.commit();
}

#[ignore = "requires SELECT subquery column extraction"]
#[test]
fn test_inline_view_select_aliased_columns() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "InnerTable")
        .create_simple("id INTEGER, name TEXT")
        .insert_values("(1, 'PROVEN'), (2, 'SQL'), (3, 'SQL')");

    // Select using aliased names from inline view
    let results = ctx.query("SELECT a, b FROM (SELECT * FROM InnerTable) AS InlineView(a, b)");
    assert_eq!(results.len(), 3);

    // Verify data
    assert!(results[0].get("a").unwrap().contains("1"));
    assert!(results[0].get("b").unwrap().contains("PROVEN"));

    ctx.commit();
}

#[ignore = "requires SELECT subquery column extraction"]
#[test]
fn test_inline_view_partial_column_aliases() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "InnerTable")
        .create_simple("id INTEGER, name TEXT")
        .insert_values("(1, 'PROVEN'), (2, 'SQL'), (3, 'SQL')");

    // Partial aliasing in inline view
    let results = ctx.query("SELECT * FROM (SELECT * FROM InnerTable) AS InlineView(a)");
    assert_eq!(results.len(), 3);

    // First column aliased, second retains original name
    assert!(results[0].contains_key("a"));
    assert!(results[0].contains_key("name"));

    ctx.commit();
}

#[ignore = "requires SELECT subquery column extraction"]
#[test]
fn test_inline_view_too_many_column_aliases_error() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "InnerTable").create_simple("id INTEGER, name TEXT");

    // Should error - inline view has 2 columns, trying to alias 3
    assert_error!(
        ctx,
        "SELECT * FROM (SELECT * FROM InnerTable) AS InlineView(a, b, c)",
        "TooManyColumnAliases"
    );

    ctx.abort();
}

#[test]
fn test_values_with_partial_column_aliases() {
    let mut ctx = setup_test();

    // Alias first column only, second should be 'column2'
    let results = ctx.query("SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS Derived(id)");
    assert_eq!(results.len(), 2);

    // Check column names
    assert!(results[0].contains_key("id"));
    assert!(results[0].contains_key("column2"));

    ctx.commit();
}

#[test]
fn test_values_with_full_column_aliases() {
    let mut ctx = setup_test();

    // Alias both columns
    let results = ctx.query("SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS Derived(id, name)");
    assert_eq!(results.len(), 2);

    // Check aliased column names
    assert!(results[0].contains_key("id"));
    assert!(results[0].contains_key("name"));

    // Verify values
    assert!(results[0].get("id").unwrap().contains("1"));
    assert!(results[0].get("name").unwrap().contains("a"));

    ctx.commit();
}

#[test]
fn test_values_select_qualified_aliased_columns() {
    let mut ctx = setup_test();

    // Use qualified column references with table alias
    let results = ctx.query(
        "SELECT Derived.id, Derived.name FROM (VALUES (1, 'a'), (2, 'b')) AS Derived(id, name)",
    );
    assert_eq!(results.len(), 2);

    // Verify data
    assert!(results[0].get("id").unwrap().contains("1"));
    assert!(results[0].get("name").unwrap().contains("a"));
    assert!(results[1].get("id").unwrap().contains("2"));
    assert!(results[1].get("name").unwrap().contains("b"));

    ctx.commit();
}

#[test]
fn test_values_too_many_column_aliases_error() {
    let mut ctx = setup_test();

    // Should error - VALUES has 2 columns, trying to alias 3
    assert_error!(
        ctx,
        "SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS Derived(id, name, dummy)",
        "TooManyColumnAliases"
    );

    ctx.abort();
}
