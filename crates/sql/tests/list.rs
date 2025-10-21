//! LIST type functionality tests
//! Tests for unbounded list types, as distinct from fixed-size arrays

mod common;

use common::setup_test;

#[test]
fn test_create_table_with_list_syntax_variations() {
    let mut ctx = setup_test();

    // Test various LIST declaration syntaxes
    ctx.exec("CREATE TABLE ListTest1 (id INTEGER, items TEXT[])");
    ctx.exec("CREATE TABLE ListTest2 (id INTEGER, items INT[])");
    ctx.exec("CREATE TABLE ListTest3 (id INTEGER, items INT[])");

    ctx.commit();
}

#[test]
fn test_list_vs_array_coercion() {
    let mut ctx = setup_test();

    // Create table with LIST column
    ctx.exec("CREATE TABLE ListTable (id INTEGER, items TEXT[])");

    // Array literals should coerce to LIST when inserting
    ctx.exec("INSERT INTO ListTable VALUES (1, ['a', 'b', 'c'])");
    ctx.exec("INSERT INTO ListTable VALUES (2, ['x', 'y'])");
    ctx.exec("INSERT INTO ListTable VALUES (3, ['single'])");

    // Lists can have different sizes unlike fixed arrays
    assert_rows!(ctx, "SELECT * FROM ListTable", 3);

    ctx.commit();
}

#[test]
fn test_empty_list() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE EmptyListTest (id INTEGER, items TEXT[])");

    // Test inserting empty array/list
    ctx.exec("INSERT INTO EmptyListTest VALUES (1, [])");

    assert_rows!(ctx, "SELECT * FROM EmptyListTest", 1);

    let results = ctx.query("SELECT * FROM EmptyListTest");
    assert!(
        results[0].get("items").unwrap().to_string().contains("[]")
            || results[0]
                .get("items")
                .unwrap()
                .to_string()
                .contains("Array[]")
    );

    ctx.commit();
}

#[test]
fn test_list_with_nulls() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE ListWithNulls (id INTEGER, items INT[])");

    // Lists can contain NULL values
    ctx.exec("INSERT INTO ListWithNulls VALUES (1, [1, NULL, 3])");

    assert_rows!(ctx, "SELECT * FROM ListWithNulls", 1);

    let results = ctx.query("SELECT * FROM ListWithNulls");
    assert!(
        results[0]
            .get("items")
            .unwrap()
            .to_string()
            .contains("Null")
    );

    ctx.commit();
}

#[test]
fn test_nested_lists() {
    let mut ctx = setup_test();

    // Nested list type declaration is supported!
    ctx.exec("CREATE TABLE NestedListTest (id INTEGER, matrix INT[][])");

    // And nested array literals work too!
    ctx.exec("INSERT INTO NestedListTest VALUES (1, [[1, 2], [3, 4]])");
    ctx.exec("INSERT INTO NestedListTest VALUES (2, [[5], [6, 7, 8]])");

    assert_rows!(ctx, "SELECT * FROM NestedListTest", 2);

    ctx.commit();
}

#[test]
fn test_list_operations_in_select() {
    let mut ctx = setup_test();

    // Note: || operator for arrays currently creates a string representation,
    // not actual array concatenation
    let results = ctx.query("SELECT [1, 2, 3] || [4, 5] as concatenated");
    assert_eq!(results.len(), 1);
    // Result is a string like "[1, 2, 3][4, 5]", not a concatenated array

    // LENGTH function has type restrictions
    // It currently doesn't support array types directly
    // Would need to be extended to support counting array elements

    ctx.commit();
}

#[test]
fn test_list_type_flexibility() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE FlexList (id INTEGER, data TEXT[])");

    // Since TEXT is flexible, we can coerce numbers to strings
    ctx.exec("INSERT INTO FlexList VALUES (1, ['text'])");

    // But can we coerce numbers to text? This depends on coercion rules
    let result = ctx.exec_response("INSERT INTO FlexList VALUES (2, [42])");
    println!("Number to text list coercion: {:?}", result);

    ctx.commit();
}

#[test]
fn test_list_aggregation() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Items (category TEXT, value INTEGER)");
    ctx.exec("INSERT INTO Items VALUES ('A', 1), ('A', 2), ('B', 3), ('B', 4)");

    // ARRAY_AGG aggregates values into lists
    let result = ctx.query("SELECT category, ARRAY_AGG(value) as values FROM Items GROUP BY category ORDER BY category");
    assert_eq!(result.len(), 2);

    // Category A should have [1, 2]
    assert_eq!(result[0].get("category").unwrap().to_string(), "A");
    let a_values = result[0].get("values").unwrap().to_string();
    assert!(a_values.contains("1") && a_values.contains("2"));

    // Category B should have [3, 4]
    assert_eq!(result[1].get("category").unwrap().to_string(), "B");
    let b_values = result[1].get("values").unwrap().to_string();
    assert!(b_values.contains("3") && b_values.contains("4"));

    ctx.commit();
}

#[test]
fn test_list_unnest() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE ListData (id INTEGER, items TEXT[])");
    ctx.exec("INSERT INTO ListData VALUES (1, ['a', 'b', 'c'])");
    ctx.exec("INSERT INTO ListData VALUES (2, ['x', 'y'])");

    // UNNEST expands arrays into rows - use in FROM clause with cross join
    let result = ctx.query("SELECT id, item FROM ListData CROSS JOIN UNNEST(items) AS t(item)");
    assert_eq!(result.len(), 5); // 3 items from first row + 2 from second row

    // Verify first row's items
    assert_eq!(result[0].get("id").unwrap().to_string(), "1");
    assert_eq!(result[0].get("item").unwrap().to_string(), "a");

    assert_eq!(result[1].get("id").unwrap().to_string(), "1");
    assert_eq!(result[1].get("item").unwrap().to_string(), "b");

    assert_eq!(result[2].get("id").unwrap().to_string(), "1");
    assert_eq!(result[2].get("item").unwrap().to_string(), "c");

    // Verify second row's items
    assert_eq!(result[3].get("id").unwrap().to_string(), "2");
    assert_eq!(result[3].get("item").unwrap().to_string(), "x");

    assert_eq!(result[4].get("id").unwrap().to_string(), "2");
    assert_eq!(result[4].get("item").unwrap().to_string(), "y");

    ctx.commit();
}

#[test]
fn test_list_unnest_with_order() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Numbers (id INTEGER, nums INT[])");
    ctx.exec("INSERT INTO Numbers VALUES (1, [3, 1, 2])");

    // UNNEST and order the results
    let result =
        ctx.query("SELECT num FROM Numbers CROSS JOIN UNNEST(nums) AS t(num) ORDER BY num");
    assert_eq!(result.len(), 3);

    assert_eq!(result[0].get("num").unwrap().to_string(), "1");
    assert_eq!(result[1].get("num").unwrap().to_string(), "2");
    assert_eq!(result[2].get("num").unwrap().to_string(), "3");

    ctx.commit();
}

#[test]
fn test_list_unnest_empty_array() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE EmptyArrays (id INTEGER, items TEXT[])");
    ctx.exec("INSERT INTO EmptyArrays VALUES (1, [])");
    ctx.exec("INSERT INTO EmptyArrays VALUES (2, ['a', 'b'])");

    // UNNEST should produce no rows for empty arrays
    let result = ctx.query("SELECT id, item FROM EmptyArrays CROSS JOIN UNNEST(items) AS t(item)");
    assert_eq!(result.len(), 2); // Only rows from id=2

    assert_eq!(result[0].get("id").unwrap().to_string(), "2");
    assert_eq!(result[0].get("item").unwrap().to_string(), "a");

    assert_eq!(result[1].get("id").unwrap().to_string(), "2");
    assert_eq!(result[1].get("item").unwrap().to_string(), "b");

    ctx.commit();
}

#[test]
fn test_list_comparison() {
    let mut ctx = setup_test();

    // Test list equality and comparison
    let eq_result = ctx.query("SELECT [1, 2, 3] = [1, 2, 3] as equal");
    println!("List equality: {:?}", eq_result);

    let neq_result = ctx.query("SELECT [1, 2] = [1, 2, 3] as equal");
    println!("List inequality: {:?}", neq_result);

    // Test if lists can be compared with <, > etc
    let cmp_result = ctx.exec_response("SELECT [1, 2] < [1, 3] as less_than");
    println!("List comparison: {:?}", cmp_result);

    ctx.commit();
}

#[test]
fn test_list_in_where_clause() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Products (id INTEGER, tags TEXT[])");
    ctx.exec("INSERT INTO Products VALUES (1, ['electronics', 'phone'])");
    ctx.exec("INSERT INTO Products VALUES (2, ['electronics', 'laptop'])");
    ctx.exec("INSERT INTO Products VALUES (3, ['books', 'fiction'])");

    // Array containment operators like @> (contains) or <@ (is contained by)
    // would be needed to query based on list contents - not yet implemented
    // PostgreSQL syntax: WHERE tags @> ARRAY['electronics']
    // or: WHERE 'electronics' = ANY(tags)

    ctx.commit();
}

#[test]
fn test_list_default_type() {
    let mut ctx = setup_test();

    // Using INT[] creates a list of integers
    ctx.exec("CREATE TABLE DefaultList (id INTEGER, nums INT[])");

    // Should accept integer arrays
    ctx.exec("INSERT INTO DefaultList VALUES (1, [1, 2, 3])");

    // Should reject non-integer arrays
    let result = ctx.exec_response("INSERT INTO DefaultList VALUES (2, ['a', 'b'])");
    assert!(matches!(result, proven_sql::SqlResponse::Error(_)));

    ctx.commit();
}

#[test]
fn test_array_literal_to_list_column() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE MixedSyntax (id INTEGER, list_col TEXT[], array_col TEXT[5])");

    // Both array literal syntaxes should work for LIST columns
    ctx.exec("INSERT INTO MixedSyntax VALUES (1, ['a', 'b'], ARRAY['x', 'y', 'z', 'w', 'v'])");
    ctx.exec("INSERT INTO MixedSyntax VALUES (2, ARRAY['c', 'd'], ['1', '2', '3', '4', '5'])");

    assert_rows!(ctx, "SELECT * FROM MixedSyntax", 2);

    ctx.commit();
}
