//! Nested SELECT and subquery tests
//! Based on gluesql/test-suite/src/nested_select.rs

mod common;

use common::setup_test;

/// Create standard test tables for nested select tests
fn setup_nested_tables(ctx: &mut common::TestContext) {
    // Create Player table (same as in join tests)
    ctx.exec("CREATE TABLE Player (id INTEGER, name TEXT)");

    // Create Request table (similar to Item but with different name)
    ctx.exec("CREATE TABLE Request (id INTEGER, quantity INTEGER, user_id INTEGER)");

    // Insert Player data
    ctx.exec(
        "INSERT INTO Player (id, name) VALUES
        (1, 'Taehoon'),
        (2, 'Mike'),
        (3, 'Jorno'),
        (4, 'Berry'),
        (5, 'Hwan')",
    );

    // Insert Request data (same values as Item table but with user_id instead of player_id)
    ctx.exec(
        "INSERT INTO Request (id, quantity, user_id) VALUES
        (101, 1, 1),
        (102, 4, 2),
        (103, 9, 3),
        (104, 2, 3),
        (105, 1, 3),
        (106, 5, 1),
        (107, 2, 1),
        (108, 1, 5),
        (109, 1, 5),
        (110, 3, 3),
        (111, 4, 2),
        (112, 8, 1),
        (113, 7, 1),
        (114, 1, 1),
        (115, 2, 1)",
    );
}

#[test]
fn test_where_in_with_literals() {
    let mut ctx = setup_test();
    setup_nested_tables(&mut ctx);

    // SELECT * FROM Request WHERE quantity IN (5, 1) - 6 results
    // Items with quantity 1: 101, 105, 108, 109, 114 (5 items)
    // Items with quantity 5: 106 (1 item)
    assert_rows!(ctx, "SELECT * FROM Request WHERE quantity IN (5, 1)", 6);

    ctx.commit();
}

#[test]
fn test_where_not_in_with_literals() {
    let mut ctx = setup_test();
    setup_nested_tables(&mut ctx);

    // SELECT * FROM Request WHERE quantity NOT IN (5, 1) - 9 results
    // Total 15 requests - 6 with quantity IN (5, 1) = 9 results
    assert_rows!(ctx, "SELECT * FROM Request WHERE quantity NOT IN (5, 1)", 9);

    ctx.commit();
}

#[test]
fn test_where_in_with_simple_subquery() {
    let mut ctx = setup_test();
    setup_nested_tables(&mut ctx);

    // SELECT * FROM Request WHERE user_id IN (SELECT id FROM Player WHERE id = 3) - 4 results
    // Player 3 is Jorno. Requests for user_id 3: 103, 104, 105, 110 (4 requests)
    assert_rows!(
        ctx,
        "SELECT * FROM Request WHERE user_id IN (SELECT id FROM Player WHERE id = 3)",
        4
    );

    ctx.commit();
}

#[test]
fn test_where_in_with_subquery_from_request() {
    let mut ctx = setup_test();
    setup_nested_tables(&mut ctx);

    // SELECT * FROM Player WHERE id IN (SELECT user_id FROM Request) - 4 results
    // Requests have user_ids: 1, 2, 3, 5 (Berry/4 has no requests)
    assert_rows!(
        ctx,
        "SELECT * FROM Player WHERE id IN (SELECT user_id FROM Request)",
        4
    );

    ctx.commit();
}

#[test]
fn test_where_in_with_correlated_subquery() {
    let mut ctx = setup_test();
    setup_nested_tables(&mut ctx);

    // SELECT * FROM Player WHERE id IN (SELECT user_id FROM Request WHERE user_id = Player.id) - 4 results
    // This is a correlated subquery - the inner query references the outer Player.id
    // Players 1, 2, 3, 5 have matching requests (Berry/4 doesn't)
    assert_rows!(
        ctx,
        "SELECT * FROM Player WHERE id IN (SELECT user_id FROM Request WHERE user_id = Player.id)",
        4
    );

    ctx.commit();
}

#[test]
fn test_where_in_with_nested_correlated_subquery() {
    let mut ctx = setup_test();
    setup_nested_tables(&mut ctx);

    // SELECT * FROM Player WHERE id IN (SELECT user_id FROM Request WHERE user_id IN (Player.id)) - 4 results
    // Similar to above but using IN instead of = for correlation
    assert_rows!(
        ctx,
        "SELECT * FROM Player WHERE id IN (SELECT user_id FROM Request WHERE user_id IN (Player.id))",
        4
    );

    ctx.commit();
}

#[test]
fn test_where_in_with_quantity_filter_subquery() {
    let mut ctx = setup_test();
    setup_nested_tables(&mut ctx);

    // SELECT * FROM Player WHERE id IN (SELECT user_id FROM Request WHERE quantity IN (6, 7, 8, 9)) - 2 results
    // Requests with quantity 6,7,8,9: 103(q=9,u=3), 112(q=8,u=1), 113(q=7,u=1)
    // User IDs: 1, 3 (2 players)
    assert_rows!(
        ctx,
        "SELECT * FROM Player WHERE id IN (SELECT user_id FROM Request WHERE quantity IN (6, 7, 8, 9))",
        2
    );

    ctx.commit();
}

#[test]
fn test_where_in_with_name_filter_subquery() {
    let mut ctx = setup_test();
    setup_nested_tables(&mut ctx);

    // SELECT * FROM Request WHERE user_id IN (SELECT id FROM Player WHERE name IN ('Taehoon', 'Hwan')) - 9 results
    // Players with names 'Taehoon'(1) and 'Hwan'(5)
    // Requests for user_id 1: 101, 106, 107, 112, 113, 114, 115 (7 requests)
    // Requests for user_id 5: 108, 109 (2 requests)
    // Total: 9 requests
    assert_rows!(
        ctx,
        "SELECT * FROM Request WHERE user_id IN (SELECT id FROM Player WHERE name IN ('Taehoon', 'Hwan'))",
        9
    );

    ctx.commit();
}

#[test]
fn test_where_equals_nonexistent_subquery() {
    let mut ctx = setup_test();
    setup_nested_tables(&mut ctx);

    // SELECT * FROM Player WHERE id = (SELECT id FROM Player WHERE id = 9) - empty result
    // No player with id = 9, so subquery returns NULL, WHERE id = NULL returns no results
    assert_rows!(
        ctx,
        "SELECT * FROM Player WHERE id = (SELECT id FROM Player WHERE id = 9)",
        0
    );

    ctx.commit();
}

#[test]
#[ignore = "SERIES function and scalar subqueries not yet implemented"]
fn test_select_subquery_with_series_nonexistent() {
    let mut ctx = setup_test();

    // SELECT (SELECT N FROM SERIES(3) WHERE N = 4) N - returns NULL
    // SERIES(3) generates 0,1,2 but we're looking for 4, so returns NULL
    let results = ctx.query("SELECT (SELECT N FROM SERIES(3) WHERE N = 4) N");

    assert_eq!(results.len(), 1, "Should return one row");
    assert!(
        results[0]
            .values()
            .next()
            .unwrap()
            .to_string()
            .contains("Null"),
        "Should return NULL"
    );

    ctx.commit();
}

// Additional test cases for EXISTS, ANY, ALL operators when implemented
#[test]
fn test_where_exists_subquery() {
    let mut ctx = setup_test();
    setup_nested_tables(&mut ctx);

    // SELECT * FROM Player WHERE EXISTS (SELECT 1 FROM Request WHERE user_id = Player.id)
    // Should return players who have at least one request (4 results)
    assert_rows!(
        ctx,
        "SELECT * FROM Player WHERE EXISTS (SELECT 1 FROM Request WHERE user_id = Player.id)",
        4
    );

    ctx.commit();
}

#[test]
fn test_where_not_exists_subquery() {
    let mut ctx = setup_test();
    setup_nested_tables(&mut ctx);

    // SELECT * FROM Player WHERE NOT EXISTS (SELECT 1 FROM Request WHERE user_id = Player.id)
    // Should return players with no requests (Berry only - 1 result)
    assert_rows!(
        ctx,
        "SELECT * FROM Player WHERE NOT EXISTS (SELECT 1 FROM Request WHERE user_id = Player.id)",
        1
    );

    ctx.commit();
}

#[test]
fn test_select_with_scalar_subquery() {
    let mut ctx = setup_test();
    setup_nested_tables(&mut ctx);

    // SELECT name, (SELECT COUNT(*) FROM Request WHERE user_id = Player.id) as request_count FROM Player
    // Should return all 5 players with their request counts
    let results = ctx.query(
        "SELECT name, (SELECT COUNT(*) FROM Request WHERE user_id = Player.id) as request_count FROM Player"
    );

    assert_eq!(results.len(), 5, "Should return all 5 players");

    ctx.commit();
}

#[test]
fn test_from_clause_subquery() {
    let mut ctx = setup_test();
    setup_nested_tables(&mut ctx);

    // SELECT * FROM (SELECT * FROM Player WHERE id < 3) AS sub WHERE sub.id = 1
    // Subquery returns players 1,2, then filter to just player 1
    assert_rows!(
        ctx,
        "SELECT * FROM (SELECT * FROM Player WHERE id < 3) AS sub WHERE sub.id = 1",
        1
    );

    ctx.commit();
}

// Error cases
#[test]
fn test_scalar_subquery_returns_multiple_rows_error() {
    let mut ctx = setup_test();
    setup_nested_tables(&mut ctx);

    // SELECT * FROM Player WHERE id = (SELECT id FROM Player)
    // Scalar subquery returns multiple rows - should error
    assert_error!(
        ctx,
        "SELECT * FROM Player WHERE id = (SELECT id FROM Player)",
        "more than one row"
    );

    ctx.commit();
}
