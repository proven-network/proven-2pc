//! JOIN operation tests
//! Based on gluesql/test-suite/src/join.rs

mod common;

use common::setup_test;

/// Create standard test tables for join tests
fn setup_join_tables(ctx: &mut common::TestContext) {
    // Create Player table
    ctx.exec("CREATE TABLE Player (id INTEGER, name TEXT)");

    // Create Item table
    ctx.exec("CREATE TABLE Item (id INTEGER, quantity INTEGER, player_id INTEGER)");

    // Insert Player data
    ctx.exec(
        "INSERT INTO Player (id, name) VALUES
        (1, 'Taehoon'),
        (2, 'Mike'),
        (3, 'Jorno'),
        (4, 'Berry'),
        (5, 'Hwan')",
    );

    // Insert Item data
    ctx.exec(
        "INSERT INTO Item (id, quantity, player_id) VALUES
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
fn test_cross_join_without_condition() {
    let mut ctx = setup_test();
    setup_join_tables(&mut ctx);

    // CROSS JOIN (cartesian product) - 15 items × 5 players = 75 results
    assert_rows!(ctx, "SELECT * FROM Item JOIN Player", 75);

    ctx.commit();
}

#[test]
fn test_left_join_basic() {
    let mut ctx = setup_test();
    setup_join_tables(&mut ctx);

    // LEFT JOIN with ON condition - 15 results (one per item)
    assert_rows!(
        ctx,
        "SELECT * FROM Item LEFT JOIN Player ON Player.id = Item.player_id",
        15
    );

    ctx.commit();
}

#[test]
fn test_left_join_with_where_clause() {
    let mut ctx = setup_test();
    setup_join_tables(&mut ctx);

    // LEFT JOIN with WHERE quantity = 1 filter - 5 items have quantity = 1
    assert_rows!(
        ctx,
        "SELECT * FROM Item LEFT JOIN Player ON Player.id = Item.player_id WHERE quantity = 1",
        5
    );

    ctx.commit();
}

#[test]
fn test_left_join_with_player_id_filter() {
    let mut ctx = setup_test();
    setup_join_tables(&mut ctx);

    // LEFT JOIN with WHERE Player.id = 1 filter - 7 items belong to player 1
    assert_rows!(
        ctx,
        "SELECT * FROM Item LEFT JOIN Player ON Player.id = Item.player_id WHERE Player.id = 1",
        7
    );

    ctx.commit();
}

#[test]
fn test_inner_join_with_player_id_filter() {
    let mut ctx = setup_test();
    setup_join_tables(&mut ctx);

    // INNER JOIN with WHERE Player.id = 1 filter - 7 items belong to player 1
    assert_rows!(
        ctx,
        "SELECT * FROM Item INNER JOIN Player ON Player.id = Item.player_id WHERE Player.id = 1",
        7
    );

    ctx.commit();
}

#[test]
fn test_multiple_left_joins() {
    let mut ctx = setup_test();
    setup_join_tables(&mut ctx);

    // Multiple LEFT JOINs (p1-p9) with WHERE Player.id = 1 - still 7 results
    let sql = "SELECT * FROM Item
        LEFT JOIN Player ON Player.id = Item.player_id
        LEFT JOIN Player p1 ON p1.id = Item.player_id
        LEFT JOIN Player p2 ON p2.id = Item.player_id
        LEFT JOIN Player p3 ON p3.id = Item.player_id
        LEFT JOIN Player p4 ON p4.id = Item.player_id
        LEFT JOIN Player p5 ON p5.id = Item.player_id
        LEFT JOIN Player p6 ON p6.id = Item.player_id
        LEFT JOIN Player p7 ON p7.id = Item.player_id
        LEFT JOIN Player p8 ON p8.id = Item.player_id
        LEFT JOIN Player p9 ON p9.id = Item.player_id
        WHERE Player.id = 1";

    assert_rows!(ctx, sql, 7);

    ctx.commit();
}

#[test]
fn test_mixed_left_and_inner_joins() {
    let mut ctx = setup_test();
    setup_join_tables(&mut ctx);

    // LEFT JOINs with one INNER JOIN and additional conditions - 6 results
    let sql = "SELECT * FROM Item
        LEFT JOIN Player ON Player.id = Item.player_id
        LEFT JOIN Player p1 ON p1.id = Item.player_id
        LEFT JOIN Player p2 ON p2.id = Item.player_id
        LEFT JOIN Player p3 ON p3.id = Item.player_id
        LEFT JOIN Player p4 ON p4.id = Item.player_id
        LEFT JOIN Player p5 ON p5.id = Item.player_id
        LEFT JOIN Player p6 ON p6.id = Item.player_id
        LEFT JOIN Player p7 ON p7.id = Item.player_id
        LEFT JOIN Player p8 ON p8.id = Item.player_id
        INNER JOIN Player p9 ON p9.id = Item.player_id AND Item.id > 101
        WHERE Player.id = 1";

    assert_rows!(ctx, sql, 6);

    ctx.commit();
}

#[test]
fn test_left_join_with_item_quantity_filter() {
    let mut ctx = setup_test();
    setup_join_tables(&mut ctx);

    // LEFT JOIN with WHERE Item.quantity = 1 - 5 items have quantity = 1
    assert_rows!(
        ctx,
        "SELECT * FROM Item LEFT JOIN Player ON Player.id = Item.player_id WHERE Item.quantity = 1",
        5
    );

    ctx.commit();
}

#[test]
fn test_left_join_with_table_aliases() {
    let mut ctx = setup_test();
    setup_join_tables(&mut ctx);

    // LEFT JOIN using table aliases (i, p) with quantity filter - 5 results
    assert_rows!(
        ctx,
        "SELECT * FROM Item i LEFT JOIN Player p ON p.id = i.player_id WHERE i.quantity = 1",
        5
    );

    ctx.commit();
}

#[test]
fn test_left_join_with_join_condition_filter() {
    let mut ctx = setup_test();
    setup_join_tables(&mut ctx);

    // LEFT JOIN ON condition with AND p.id = 1 - 15 results (all items, player only when id=1)
    assert_rows!(
        ctx,
        "SELECT * FROM Item i LEFT JOIN Player p ON p.id = i.player_id AND p.id = 1",
        15
    );

    ctx.commit();
}

#[test]
fn test_left_join_with_join_condition_quantity_filter() {
    let mut ctx = setup_test();
    setup_join_tables(&mut ctx);

    // LEFT JOIN ON condition with AND i.quantity = 1 - 15 results (all items)
    assert_rows!(
        ctx,
        "SELECT * FROM Item i LEFT JOIN Player p ON p.id = i.player_id AND i.quantity = 1",
        15
    );

    ctx.commit();
}

#[test]
fn test_left_join_with_join_condition_item_quantity_filter() {
    let mut ctx = setup_test();
    setup_join_tables(&mut ctx);

    // LEFT JOIN ON condition with AND Item.quantity = 1 - 15 results (all items)
    assert_rows!(
        ctx,
        "SELECT * FROM Item LEFT JOIN Player ON Player.id = Item.player_id AND Item.quantity = 1",
        15
    );

    ctx.commit();
}

#[test]
fn test_inner_join_with_join_condition_filter() {
    let mut ctx = setup_test();
    setup_join_tables(&mut ctx);

    // INNER JOIN with AND p.id = 1 in ON clause - 7 results
    assert_rows!(
        ctx,
        "SELECT * FROM Item i JOIN Player p ON p.id = i.player_id AND p.id = 1",
        7
    );

    ctx.commit();
}

#[test]
fn test_inner_join_with_join_condition_quantity_filter() {
    let mut ctx = setup_test();
    setup_join_tables(&mut ctx);

    // INNER JOIN with AND i.quantity = 1 in ON clause - 5 results
    assert_rows!(
        ctx,
        "SELECT * FROM Item i JOIN Player p ON p.id = i.player_id AND i.quantity = 1",
        5
    );

    ctx.commit();
}

#[test]
fn test_inner_join_with_impossible_condition() {
    let mut ctx = setup_test();
    setup_join_tables(&mut ctx);

    // INNER JOIN with impossible condition 1 = 2 - 0 results
    let sql = "SELECT * FROM Player
        INNER JOIN Item ON 1 = 2
        INNER JOIN Item i2 ON 1 = 2";

    assert_rows!(ctx, sql, 0);

    ctx.commit();
}

#[test]
fn test_left_join_with_subquery_limit_offset() {
    let mut ctx = setup_test();
    setup_join_tables(&mut ctx);

    // LEFT JOIN with WHERE Player.id = subquery with LIMIT/OFFSET - 7 results
    let sql = "SELECT * FROM Item
        LEFT JOIN Player ON Player.id = Item.player_id
        WHERE Player.id = (SELECT id FROM Player LIMIT 1 OFFSET 0)";

    assert_rows!(ctx, sql, 7);

    ctx.commit();
}

#[test]
fn test_left_join_with_correlated_subquery_no_results() {
    let mut ctx = setup_test();
    setup_join_tables(&mut ctx);

    // LEFT JOIN with correlated subquery using i1.id = i2.id - 0 results
    let sql = "SELECT * FROM Item i1
        LEFT JOIN Player ON Player.id = i1.player_id
        WHERE Player.id = (SELECT id FROM Item i2 WHERE i2.id = i1.id)";

    assert_rows!(ctx, sql, 0);

    ctx.commit();
}

#[test]
fn test_left_join_with_complex_correlated_subquery() {
    let mut ctx = setup_test();
    setup_join_tables(&mut ctx);

    // LEFT JOIN with complex JOIN in subquery and multiple correlations - 0 results
    let sql = "SELECT * FROM Item i1
        LEFT JOIN Player ON Player.id = i1.player_id
        WHERE Player.id =
            (SELECT i2.id FROM Item i2
             JOIN Item i3 ON i3.id = i2.id
             WHERE
                 i2.id = i1.id AND
                 i3.id = i2.id AND
                 i1.id = i3.id)";

    assert_rows!(ctx, sql, 0);

    ctx.commit();
}

#[test]
fn test_left_join_with_in_subquery() {
    let mut ctx = setup_test();
    setup_join_tables(&mut ctx);

    // LEFT JOIN with WHERE Player.id IN subquery with JOIN - 4 results
    let sql = "SELECT * FROM Item i1
        LEFT JOIN Player ON Player.id = i1.player_id
        WHERE Player.id IN
            (SELECT i2.player_id FROM Item i2
             JOIN Item i3 ON i3.id = i2.id
             WHERE Player.name = 'Jorno')";

    assert_rows!(ctx, sql, 4);

    ctx.commit();
}

#[test]
fn test_inner_join_player_item() {
    let mut ctx = setup_test();
    setup_join_tables(&mut ctx);

    // SELECT * FROM Player INNER JOIN Item ON Player.id = Item.player_id - 15 results
    assert_rows!(
        ctx,
        "SELECT * FROM Player INNER JOIN Item ON Player.id = Item.player_id",
        15
    );

    ctx.commit();
}

#[test]
fn test_left_join_cartesian_product() {
    let mut ctx = setup_test();
    setup_join_tables(&mut ctx);

    // LEFT JOIN with condition 1 = 1 for cartesian product - 25 results (5 players × 5 players)
    assert_rows!(
        ctx,
        "SELECT * FROM Player p1 LEFT JOIN Player p2 ON 1 = 1",
        25
    );

    ctx.commit();
}

#[test]
fn test_inner_join_with_in_condition() {
    let mut ctx = setup_test();
    setup_join_tables(&mut ctx);

    // INNER JOIN with condition using IN clause - 30 results
    assert_rows!(
        ctx,
        "SELECT * FROM Item INNER JOIN Item i2 ON i2.id IN (101, 103)",
        30
    );

    ctx.commit();
}

#[test]
fn test_join_project_with_table_aliases() {
    let mut ctx = setup_test();
    setup_join_tables(&mut ctx);

    // SELECT p.id, i.id FROM Player p LEFT JOIN Item i with proper column projection
    let results =
        ctx.query("SELECT p.id, i.id FROM Player p LEFT JOIN Item i ON p.id = i.player_id");

    // Should have 16 results (15 items + 1 NULL for player with no items)
    assert_eq!(
        results.len(),
        16,
        "Expected 16 results from JOIN with projection"
    );

    ctx.commit();
}

#[test]
fn test_join_project_without_table_qualifier() {
    let mut ctx = setup_test();
    setup_join_tables(&mut ctx);

    // SELECT p.id, player_id from LEFT JOIN without table qualifier on player_id
    let results =
        ctx.query("SELECT p.id, player_id FROM Player p LEFT JOIN Item i ON p.id = i.player_id");

    // Should have 16 results (15 items + 1 NULL for player with no items)
    assert_eq!(
        results.len(),
        16,
        "Expected 16 results from JOIN with unqualified column"
    );

    ctx.commit();
}

#[test]
fn test_join_project_with_wildcard() {
    let mut ctx = setup_test();
    setup_join_tables(&mut ctx);

    // SELECT Item.* FROM Player p LEFT JOIN Item with wildcard projection
    let results = ctx.query("SELECT Item.* FROM Player p LEFT JOIN Item ON p.id = Item.player_id");

    // Should have 16 results (15 items + 1 NULL row for Berry who has no items)
    assert_eq!(
        results.len(),
        16,
        "Expected 16 results from JOIN with wildcard projection"
    );

    ctx.commit();
}

#[test]
fn test_join_project_all_columns() {
    let mut ctx = setup_test();
    setup_join_tables(&mut ctx);

    // SELECT * FROM Player p LEFT JOIN Item with full result set
    let results = ctx.query("SELECT * FROM Player p LEFT JOIN Item ON p.id = Item.player_id");

    // Should have 16 results with all columns from both tables (15 items + 1 NULL for player with no items)
    assert_eq!(
        results.len(),
        16,
        "Expected 16 results from JOIN with full projection"
    );

    ctx.commit();
}

#[test]
fn test_join_using_constraint_error() {
    let mut ctx = setup_test();
    setup_join_tables(&mut ctx);

    // JOIN USING constraint should error with appropriate message
    assert_error!(
        ctx,
        "SELECT * FROM Player JOIN Item USING (id)",
        "USING clause not yet supported"
    );

    ctx.commit();
}

#[test]
fn test_cross_join_explicit() {
    let mut ctx = setup_test();
    setup_join_tables(&mut ctx);

    // CROSS JOIN should work and produce cartesian product (5 players × 15 items = 75 results)
    assert_rows!(ctx, "SELECT * FROM Player CROSS JOIN Item", 75);

    ctx.commit();
}

#[test]
fn test_join_ambiguous_column_reference() {
    let mut ctx = setup_test();
    setup_join_tables(&mut ctx);

    // Ambiguous column reference error in SELECT id from joined tables
    assert_error!(
        ctx,
        "SELECT id FROM Player JOIN Item ON Player.id = Item.player_id",
        "Ambiguous"
    );

    ctx.commit();
}

#[test]
fn test_join_ambiguous_column_self_join() {
    let mut ctx = setup_test();
    setup_join_tables(&mut ctx);

    // Ambiguous column in self-join scenario
    assert_error!(
        ctx,
        "SELECT id FROM Player p1 JOIN Player p2 ON p1.id = p2.id",
        "Ambiguous"
    );

    ctx.commit();
}

#[test]
#[ignore = "INSERT doesn't validate ambiguous columns in SELECT subqueries yet"]
fn test_join_ambiguous_column_in_insert() {
    let mut ctx = setup_test();
    setup_join_tables(&mut ctx);

    // Create target table
    ctx.exec("CREATE TABLE Target (id INTEGER)");

    // Ambiguous column error in INSERT with JOIN subquery
    assert_error!(
        ctx,
        "INSERT INTO Target SELECT id FROM Player JOIN Item ON Player.id = Item.player_id",
        "Ambiguous"
    );

    ctx.commit();
}

#[test]
fn test_join_ambiguous_column_in_create_table() {
    let mut ctx = setup_test();
    setup_join_tables(&mut ctx);

    // Ambiguous column error in CREATE TABLE AS with JOIN
    assert_error!(
        ctx,
        "CREATE TABLE NewTable AS SELECT id FROM Player JOIN Item ON Player.id = Item.player_id",
        "Ambiguous"
    );

    ctx.commit();
}

#[test]
fn test_join_too_many_tables_error() {
    let mut ctx = setup_test();
    setup_join_tables(&mut ctx);

    // Comma-separated table list should error
    assert_error!(ctx, "SELECT * FROM Player, Item", "Multiple tables");

    ctx.commit();
}
