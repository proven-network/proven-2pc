//! Arithmetic operations in WHERE clause tests
//! Based on gluesql/test-suite/src/arithmetic/on_where.rs

mod common;

use common::{TableBuilder, setup_test};

#[test]
fn test_add_on_where() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Arith")
        .create_simple("id INTEGER, num INTEGER, name TEXT")
        .insert_values("(1, 6, 'A'), (2, 8, 'B'), (3, 4, 'C'), (4, 2, 'D'), (5, 3, 'E')");

    // WHERE id = 1 + 1 (id = 2)
    ctx.assert_row_count("SELECT * FROM Arith WHERE id = 1 + 1", 1);

    // WHERE id < id + 1 (always true for positive numbers)
    ctx.assert_row_count("SELECT * FROM Arith WHERE id < id + 1", 5);

    // WHERE id < num + id
    ctx.assert_row_count("SELECT * FROM Arith WHERE id < num + id", 5);

    // WHERE id + 1 < 5 (id < 4, so id in [1,2,3])
    ctx.assert_row_count("SELECT * FROM Arith WHERE id + 1 < 5", 3);

    ctx.commit();
}

#[test]
fn test_subtract_on_where() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Arith")
        .create_simple("id INTEGER, num INTEGER, name TEXT")
        .insert_values("(1, 6, 'A'), (2, 8, 'B'), (3, 4, 'C'), (4, 2, 'D'), (5, 3, 'E')");

    // WHERE id = 2 - 1 (id = 1)
    ctx.assert_row_count("SELECT * FROM Arith WHERE id = 2 - 1", 1);

    // WHERE 2 - 1 = id (id = 1)
    ctx.assert_row_count("SELECT * FROM Arith WHERE 2 - 1 = id", 1);

    // WHERE id > id - 1 (always true for any number)
    ctx.assert_row_count("SELECT * FROM Arith WHERE id > id - 1", 5);

    // WHERE id > id - num (when num > 0, always true)
    ctx.assert_row_count("SELECT * FROM Arith WHERE id > id - num", 5);

    // WHERE 5 - id < 3 (id > 2, so id in [3,4,5])
    ctx.assert_row_count("SELECT * FROM Arith WHERE 5 - id < 3", 3);

    ctx.commit();
}

#[test]
fn test_multiply_on_where() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Arith")
        .create_simple("id INTEGER, num INTEGER, name TEXT")
        .insert_values("(1, 6, 'A'), (2, 8, 'B'), (3, 4, 'C'), (4, 2, 'D'), (5, 3, 'E')");

    // WHERE id = 2 * 2 (id = 4)
    ctx.assert_row_count("SELECT * FROM Arith WHERE id = 2 * 2", 1);

    // WHERE id > id * 2 (never true for positive numbers)
    ctx.assert_row_count("SELECT * FROM Arith WHERE id > id * 2", 0);

    // WHERE id > num * id (never true for positive num)
    ctx.assert_row_count("SELECT * FROM Arith WHERE id > num * id", 0);

    // WHERE 3 * id < 4 (id < 4/3, so id = 1)
    ctx.assert_row_count("SELECT * FROM Arith WHERE 3 * id < 4", 1);

    ctx.commit();
}

#[test]
fn test_divide_on_where() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Arith")
        .create_simple("id INTEGER, num INTEGER, name TEXT")
        .insert_values("(1, 6, 'A'), (2, 8, 'B'), (3, 4, 'C'), (4, 2, 'D'), (5, 3, 'E')");

    // WHERE id = 5 / 2 (integer division: 5/2 = 2, so id = 2)
    ctx.assert_row_count("SELECT * FROM Arith WHERE id = 5 / 2", 1);

    // WHERE id > id / 2 (always true for positive numbers >= 1)
    ctx.assert_row_count("SELECT * FROM Arith WHERE id > id / 2", 5);

    // WHERE id > num / id
    ctx.assert_row_count("SELECT * FROM Arith WHERE id > num / id", 3);

    // WHERE 10 / id = 2 (id = 5)
    ctx.assert_row_count("SELECT * FROM Arith WHERE 10 / id = 2", 2);

    ctx.commit();
}

#[test]
fn test_modulo_on_where() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Arith")
        .create_simple("id INTEGER, num INTEGER, name TEXT")
        .insert_values("(1, 6, 'A'), (2, 8, 'B'), (3, 4, 'C'), (4, 2, 'D'), (5, 3, 'E')");

    // WHERE id = 5 % 2 (5 mod 2 = 1)
    ctx.assert_row_count("SELECT * FROM Arith WHERE id = 5 % 2", 1);

    // WHERE id > num % id
    ctx.assert_row_count("SELECT * FROM Arith WHERE id > num % id", 5);

    // WHERE num % id > 2
    ctx.assert_row_count("SELECT * FROM Arith WHERE num % id > 2", 1);

    // WHERE num % 3 < 2 % id
    ctx.assert_row_count("SELECT * FROM Arith WHERE num % 3 < 2 % id", 2);

    ctx.commit();
}

#[test]
fn test_update_with_arithmetic_where() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Arith")
        .create_simple("id INTEGER, num INTEGER, name TEXT")
        .insert_values("(1, 6, 'A'), (2, 8, 'B'), (3, 4, 'C'), (4, 2, 'D'), (5, 3, 'E')");

    // WHERE 1 + 1 = id (id = 2)
    ctx.assert_row_count("SELECT * FROM Arith WHERE 1 + 1 = id", 1);

    // UPDATE with arithmetic in SET
    ctx.exec("UPDATE Arith SET id = id + 1");
    // Now no row has id = 1
    ctx.assert_row_count("SELECT * FROM Arith WHERE id = 1", 0);

    // UPDATE with arithmetic in WHERE
    ctx.exec("UPDATE Arith SET id = id - 1 WHERE id != 6");
    ctx.assert_row_count("SELECT * FROM Arith WHERE id <= 2", 2);

    // UPDATE with multiplication
    ctx.exec("UPDATE Arith SET id = id * 2");
    ctx.exec("UPDATE Arith SET id = id / 2");
    ctx.assert_row_count("SELECT * FROM Arith WHERE id <= 2", 2);

    ctx.commit();
}
