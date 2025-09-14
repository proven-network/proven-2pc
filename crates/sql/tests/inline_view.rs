//! Inline view (subquery in FROM clause) tests
//! Based on gluesql/test-suite/src/inline_view.rs

#[ignore = "not yet implemented"]
#[test]
fn test_create_tables_for_inline_view() {
    // TODO: Test CREATE TABLE InnerTable (id INTEGER, name TEXT)
    // TODO: Test CREATE TABLE OuterTable (id INTEGER, name TEXT)
}

#[ignore = "not yet implemented"]
#[test]
fn test_insert_data_for_inline_view() {
    // TODO: Test INSERT INTO InnerTable VALUES (1, 'GLUE'), (2, 'SQL'), (3, 'SQL') - 3 inserts
    // TODO: Test INSERT INTO OuterTable VALUES (1, 'WORKS!'), (2, 'EXTRA') - 2 inserts
}

#[ignore = "not yet implemented"]
#[test]
fn test_select_from_inner_table() {
    // TODO: Test SELECT * FROM InnerTable - verify 3 rows with GLUE, SQL, SQL
}

#[ignore = "not yet implemented"]
#[test]
fn test_inline_view_with_count_aggregate() {
    // TODO: Test SELECT * FROM (SELECT COUNT(*) AS cnt FROM InnerTable) AS InlineView - returns count of 3
}

#[ignore = "not yet implemented"]
#[test]
fn test_inline_view_with_where_clause() {
    // TODO: Test SELECT * FROM (SELECT COUNT(*) AS cnt FROM InnerTable WHERE id > 1) AS InlineView - returns count of 2
}

#[ignore = "not yet implemented"]
#[test]
fn test_inline_view_without_column_alias() {
    // TODO: Test SELECT * FROM (SELECT COUNT(*) FROM InnerTable) AS InlineView - using default "COUNT(*)" column name
}

#[ignore = "not yet implemented"]
#[test]
fn test_inline_view_without_table_alias_error() {
    // TODO: Test SELECT * FROM (SELECT COUNT(*) AS cnt FROM InnerTable) - should error with LackOfAlias
}

#[ignore = "not yet implemented"]
#[test]
fn test_nested_inline_views() {
    // TODO: Test SELECT * FROM (SELECT * FROM (SELECT COUNT(*) AS cnt FROM InnerTable) AS InlineView) AS InlineView2 - double nesting
}

#[ignore = "not yet implemented"]
#[test]
fn test_join_with_inline_view() {
    // TODO: Test SELECT * FROM OuterTable JOIN (SELECT id, name FROM InnerTable) AS InlineView ON OuterTable.id = InlineView.id - 2 results
}

#[ignore = "not yet implemented"]
#[test]
fn test_join_inline_view_missing_join_column_error() {
    // TODO: Test JOIN with inline view missing join column - should error with CompoundIdentifierNotFound
}

#[ignore = "not yet implemented"]
#[test]
fn test_join_inline_view_with_where_clause() {
    // TODO: Test JOIN with inline view containing WHERE id = 1 - 1 result
}

#[ignore = "not yet implemented"]
#[test]
fn test_join_inline_view_wildcard() {
    // TODO: Test JOIN with inline view using SELECT * - 2 results
}

#[ignore = "not yet implemented"]
#[test]
fn test_join_inline_view_qualified_wildcard_inner() {
    // TODO: Test JOIN with inline view using SELECT InnerTable.* - 2 results
}

#[ignore = "not yet implemented"]
#[test]
fn test_join_inline_view_qualified_wildcard_outer() {
    // TODO: Test SELECT InlineView.* FROM OuterTable JOIN inline view - 2 results with 3 columns
}

#[ignore = "not yet implemented"]
#[test]
fn test_join_nested_inline_views() {
    // TODO: Test JOIN with double-nested inline views - 2 results
}

#[ignore = "not yet implemented"]
#[test]
fn test_inline_view_with_group_by() {
    // TODO: Test SELECT * FROM (SELECT name, count(*) as cnt FROM InnerTable GROUP BY name) AS InlineView - 2 results (GLUE: 1, SQL: 2)
}

#[ignore = "not yet implemented"]
#[test]
fn test_inline_view_with_limit() {
    // TODO: Test SELECT * FROM (SELECT * FROM InnerTable LIMIT 1) AS InlineView - 1 result
}

#[ignore = "not yet implemented"]
#[test]
fn test_inline_view_with_offset() {
    // TODO: Test SELECT * FROM (SELECT * FROM InnerTable OFFSET 2) AS InlineView - 1 result (id=3)
}

#[ignore = "not yet implemented"]
#[test]
fn test_inline_view_with_order_by() {
    // TODO: Test SELECT * FROM (SELECT * FROM InnerTable ORDER BY id desc) AS InlineView - 3 results in desc order
}

#[ignore = "not yet implemented"]
#[test]
fn test_inline_view_unsupported_implicit_join() {
    // TODO: Test unsupported implicit join with inline view - should error with TooManyTables
}

#[ignore = "not yet implemented"]
#[test]
fn test_select_distinct() {
    // TODO: Test SELECT DISTINCT id FROM OuterTable - 2 results
}

#[ignore = "not yet implemented"]
#[test]
fn test_inline_view_as_left_side_of_join() {
    // TODO: Test SELECT * FROM (SELECT * FROM InnerTable) AS InlineView Join OuterTable - 2 results
}
