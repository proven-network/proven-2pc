//! LIKE and ILIKE pattern matching tests
//! Based on gluesql/test-suite/src/like_ilike.rs

mod common;

use common::{TableBuilder, setup_test};

#[test]
fn test_basic_like_operations() {
    let mut ctx = setup_test();

    // Create a test table with sample data
    ctx.exec("CREATE TABLE TestData (id INTEGER, value TEXT)");
    ctx.exec("INSERT INTO TestData VALUES (1, 'abc'), (2, 'HELLO'), (3, 'test')");

    // Test LIKE patterns
    assert_rows!(ctx, "SELECT id FROM TestData WHERE value LIKE '%c'", 1); // 'abc' ends with c
    assert_rows!(ctx, "SELECT id FROM TestData WHERE value LIKE '_b_'", 1); // 'abc' matches _b_
    // TODO: NOT LIKE needs to be implemented in parser

    ctx.commit();
}

#[test]
fn test_like_patterns_with_table_data() {
    let mut ctx = setup_test();

    // Create and populate test table
    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, name TEXT")
        .insert_values(
            "(1, 'Amelia'), (2, 'Doll'), (3, 'Gascoigne'), (4, 'Gehrman'), (5, 'Maria')",
        );

    // Test % wildcard patterns
    assert_rows!(ctx, "SELECT name FROM Item WHERE name LIKE 'A%'", 1); // Names starting with A (Amelia)
    assert_rows!(ctx, "SELECT name FROM Item WHERE name LIKE '%a'", 2); // Names ending with a (Amelia, Maria)
    assert_rows!(ctx, "SELECT name FROM Item WHERE name LIKE '%e%'", 3); // Names containing e (Amelia, Gascoigne, Gehrman)
    assert_rows!(ctx, "SELECT name FROM Item WHERE name LIKE '%%'", 5); // All names (% matches any string)

    // Test _ wildcard patterns
    assert_rows!(ctx, "SELECT name FROM Item WHERE name LIKE '____'", 1); // Exactly 4 chars (Doll)
    assert_rows!(ctx, "SELECT name FROM Item WHERE name LIKE '_____'", 1); // Exactly 5 chars (Maria)
    assert_rows!(ctx, "SELECT name FROM Item WHERE name LIKE 'M____'", 1); // Starting with M, 5 chars (Maria)
    assert_rows!(ctx, "SELECT name FROM Item WHERE name LIKE '___ia'", 1); // Ending with 'ia', 5 chars (Maria)

    // Test mixed patterns
    assert_rows!(ctx, "SELECT name FROM Item WHERE name LIKE '_a%'", 2); // Second char is 'a' (Gascoigne, Maria)
    assert_rows!(ctx, "SELECT name FROM Item WHERE name LIKE '%r%'", 2); // Contains 'r' (Gehrman, Maria)

    // TODO: NOT LIKE tests when parser supports it
    // assert_rows!(ctx, "SELECT name FROM Item WHERE name NOT LIKE 'G%'", 4); // Not starting with G
    // assert_rows!(ctx, "SELECT name FROM Item WHERE name NOT LIKE '%ll%'", 4); // Not containing 'll'
    // assert_rows!(ctx, "SELECT name FROM Item WHERE name NOT LIKE '%a%'", 1); // Not containing 'a' (only Doll)

    ctx.commit();
}

#[test]
fn test_ilike_case_insensitive_patterns() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, name TEXT")
        .insert_values(
            "(1, 'Amelia'), (2, 'Doll'), (3, 'Gascoigne'), (4, 'Gehrman'), (5, 'Maria')",
        );

    // ILIKE should be case insensitive
    assert_rows!(ctx, "SELECT name FROM Item WHERE name ILIKE 'amelia'", 1); // Matches 'Amelia'
    assert_rows!(ctx, "SELECT name FROM Item WHERE name ILIKE 'MARIA'", 1); // Matches 'Maria'
    assert_rows!(ctx, "SELECT name FROM Item WHERE name ILIKE '%DOLL%'", 1); // Matches 'Doll'

    // ILIKE with wildcards
    assert_rows!(ctx, "SELECT name FROM Item WHERE name ILIKE '_A%'", 2); // Second char is 'a' or 'A' (Gascoigne, Maria)
    assert_rows!(ctx, "SELECT name FROM Item WHERE name ILIKE 'g%'", 2); // Starting with g/G (Gascoigne, Gehrman)
    assert_rows!(ctx, "SELECT name FROM Item WHERE name ILIKE '%%'", 5); // All names

    // TODO: NOT ILIKE when parser supports it
    // assert_rows!(ctx, "SELECT name FROM Item WHERE name NOT ILIKE 'DOLL'", 4); // Not 'Doll' (case insensitive)
    // assert_rows!(ctx, "SELECT name FROM Item WHERE name NOT ILIKE '%A%'", 1); // Not containing 'a' or 'A' (only Doll)

    ctx.commit();
}

#[test]
fn test_like_with_expressions() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, name TEXT")
        .insert_values(
            "(1, 'Amelia'), (2, 'Doll'), (3, 'Gascoigne'), (4, 'Gehrman'), (5, 'Maria')",
        );

    // LIKE with SUBSTR
    assert_rows!(
        ctx,
        "SELECT name FROM Item WHERE SUBSTR(name, 1) LIKE '%a'",
        2
    ); // Substring ending with 'a' (Amelia, Maria)
    assert_rows!(
        ctx,
        "SELECT name FROM Item WHERE SUBSTR(name, 1) LIKE SUBSTR('%a', 1)",
        2
    );

    // LIKE with LOWER
    assert_rows!(ctx, "SELECT name FROM Item WHERE LOWER(name) LIKE '%a'", 2); // 'amelia' and 'maria' end with 'a' in lowercase
    assert_rows!(
        ctx,
        "SELECT name FROM Item WHERE LOWER(name) LIKE SUBSTR('%a', 1)",
        2
    );

    // LIKE with string concatenation
    assert_rows!(ctx, "SELECT name FROM Item WHERE name LIKE '%' || 'a'", 2); // Concatenation to form '%a' (Amelia, Maria)
    assert_rows!(
        ctx,
        "SELECT name FROM Item WHERE SUBSTR(name, 1) LIKE '%' || LOWER('A')",
        2
    );

    ctx.commit();
}

#[test]
fn test_like_with_literal_comparisons() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, name TEXT")
        .insert_values(
            "(1, 'Amelia'), (2, 'Doll'), (3, 'Gascoigne'), (4, 'Gehrman'), (5, 'Maria')",
        );

    // Literal string comparisons (always true/false for all rows)
    assert_rows!(ctx, "SELECT name FROM Item WHERE 'ABC' LIKE '_B_'", 5); // Always true for all rows
    assert_rows!(
        ctx,
        "SELECT name FROM Item WHERE 'name' LIKE SUBSTR('%a', 1)",
        0
    ); // 'name' doesn't end with 'a'

    ctx.commit();
}

#[test]
fn test_ilike_with_literal_comparisons() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, name TEXT")
        .insert_values(
            "(1, 'Amelia'), (2, 'Doll'), (3, 'Gascoigne'), (4, 'Gehrman'), (5, 'Maria')",
        );

    // ILIKE literal string comparisons (case insensitive)
    assert_rows!(ctx, "SELECT name FROM Item WHERE 'abc' ILIKE '_B_'", 5); // Case insensitive, always true
    assert_rows!(ctx, "SELECT name FROM Item WHERE 'ABC' ILIKE '_b_'", 5); // Always true

    ctx.commit();
}

#[test]
fn test_like_error_cases() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, name TEXT")
        .insert_values("(1, 'Amelia'), (2, 'Doll')");

    // LIKE with non-string types should error
    assert_error!(ctx, "SELECT name FROM Item WHERE 'ABC' LIKE 10");
    assert_error!(
        ctx,
        "SELECT name FROM Item WHERE name = 'Amelia' AND name LIKE 10"
    );

    // LIKE with NULL pattern
    assert_error!(ctx, "SELECT name FROM Item WHERE name LIKE NULL");

    ctx.commit();
}

#[test]
fn test_ilike_error_cases() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("id INTEGER, name TEXT")
        .insert_values("(1, 'Amelia'), (2, 'Doll')");

    // ILIKE with non-string types should error
    assert_error!(ctx, "SELECT name FROM Item WHERE True ILIKE '_B_'");
    assert_error!(
        ctx,
        "SELECT name FROM Item WHERE name = 'Amelia' AND name ILIKE 10"
    );

    // ILIKE with NULL pattern
    assert_error!(ctx, "SELECT name FROM Item WHERE name ILIKE NULL");

    ctx.commit();
}

#[test]
fn test_like_with_null_values() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "NullTest")
        .create_simple("id INTEGER, name TEXT")
        .insert_values("(1, 'Test'), (2, NULL), (3, 'Another')");

    // NULL values should not match any LIKE pattern
    assert_rows!(ctx, "SELECT id FROM NullTest WHERE name LIKE '%'", 2); // Only non-NULL values
    // TODO: NOT LIKE when parser supports it
    // assert_rows!(ctx, "SELECT id FROM NullTest WHERE name NOT LIKE '%'", 0); // NOT LIKE also doesn't match NULL

    // Explicit NULL checks
    assert_rows!(ctx, "SELECT id FROM NullTest WHERE name IS NULL", 1);
    assert_rows!(
        ctx,
        "SELECT id FROM NullTest WHERE name IS NOT NULL AND name LIKE 'T%'",
        1
    );

    ctx.commit();
}

#[test]
fn test_ilike_with_null_values() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "NullTest")
        .create_simple("id INTEGER, name TEXT")
        .insert_values("(1, 'Test'), (2, NULL), (3, 'Another')");

    // NULL values should not match any ILIKE pattern
    assert_rows!(ctx, "SELECT id FROM NullTest WHERE name ILIKE '%'", 2); // Only non-NULL values
    // TODO: NOT ILIKE when parser supports it
    // assert_rows!(ctx, "SELECT id FROM NullTest WHERE name NOT ILIKE '%'", 0);

    ctx.commit();
}

#[test]
fn test_complex_like_patterns() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "ComplexTest")
        .create_simple("id INTEGER, pattern TEXT")
        .insert_values("(1, 'abc123'), (2, '123abc'), (3, 'a1b2c3'), (4, 'ABC123'), (5, '___')");

    // Complex patterns
    assert_rows!(
        ctx,
        "SELECT id FROM ComplexTest WHERE pattern LIKE 'a%3'",
        2
    ); // Starts with 'a', ends with '3' (abc123, a1b2c3)
    assert_rows!(
        ctx,
        "SELECT id FROM ComplexTest WHERE pattern LIKE '%b%2%'",
        2
    ); // Contains 'b' then '2' (abc123, a1b2c3)
    assert_rows!(
        ctx,
        "SELECT id FROM ComplexTest WHERE pattern LIKE '___'",
        1
    ); // Exactly 3 characters (___)
    assert_rows!(
        ctx,
        "SELECT id FROM ComplexTest WHERE pattern LIKE '______'",
        4
    ); // Exactly 6 characters (abc123, 123abc, a1b2c3, ABC123)

    ctx.commit();
}

#[test]
fn test_complex_ilike_patterns() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "ComplexTest")
        .create_simple("id INTEGER, pattern TEXT")
        .insert_values("(1, 'abc123'), (2, '123abc'), (3, 'a1b2c3'), (4, 'ABC123'), (5, '___')");

    // ILIKE with complex patterns (case insensitive)
    assert_rows!(
        ctx,
        "SELECT id FROM ComplexTest WHERE pattern ILIKE 'a%3'",
        3
    ); // Case insensitive (abc123, a1b2c3, ABC123)
    assert_rows!(
        ctx,
        "SELECT id FROM ComplexTest WHERE pattern ILIKE 'ABC%'",
        2
    ); // Matches 'abc123' and 'ABC123'

    ctx.commit();
}

#[ignore = "VALUES not yet implemented as standalone statement"]
#[test]
fn test_values_with_like_ilike() {
    // This would test the VALUES statement like in GlueSQL:
    // VALUES
    //     ('abc' LIKE '%c'),
    //     ('abc' NOT LIKE '_c'),
    //     ('abc' LIKE '_b_'),
    //     ('HELLO' ILIKE '%el%'),
    //     ('HELLO' NOT ILIKE '_ELLE')
    // Should return: true, true, true, true, true

    // For now, we can't test this without VALUES statement support
}
