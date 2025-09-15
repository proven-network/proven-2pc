//! Index ORDER BY tests
//! Based on gluesql/test-suite/src/index/order_by.rs

mod common;

use common::setup_test;

#[test]
fn test_order_by_single_table() {
    let mut ctx = setup_test();

    // Create table
    ctx.exec("CREATE TABLE Test (id INTEGER, num INTEGER NULL, name TEXT)");

    // Insert data
    ctx.exec("INSERT INTO Test (id, num, name) VALUES (1, 2, 'Hello')");
    ctx.exec("INSERT INTO Test (id, num, name) VALUES (1, 9, 'Wild')");
    ctx.exec("INSERT INTO Test (id, num, name) VALUES (3, NULL, 'World')");
    ctx.exec("INSERT INTO Test (id, num, name) VALUES (4, 7, 'Monday')");

    // Create index on name column
    ctx.exec("CREATE INDEX idx_name ON Test (name)");

    // Test ORDER BY name - should use idx_name index
    let results = ctx.query("SELECT * FROM Test ORDER BY name");
    assert_eq!(results.len(), 4);

    // Check order - alphabetical by name: Hello, Monday, Wild, World
    assert_eq!(results[0].get("name").unwrap(), "Str(Hello)");
    assert_eq!(results[1].get("name").unwrap(), "Str(Monday)");
    assert_eq!(results[2].get("name").unwrap(), "Str(Wild)");
    assert_eq!(results[3].get("name").unwrap(), "Str(World)");

    ctx.commit();
}

#[test]
fn test_order_by_expression_index() {
    let mut ctx = setup_test();

    // Create table
    ctx.exec("CREATE TABLE Test (id INTEGER, num INTEGER NULL, name TEXT)");

    // Insert data
    ctx.exec("INSERT INTO Test (id, num, name) VALUES (1, 2, 'Hello')");
    ctx.exec("INSERT INTO Test (id, num, name) VALUES (1, 9, 'Wild')");
    ctx.exec("INSERT INTO Test (id, num, name) VALUES (3, NULL, 'World')");
    ctx.exec("INSERT INTO Test (id, num, name) VALUES (4, 7, 'Monday')");

    // Create expression index on (id + num)
    ctx.exec("CREATE INDEX idx_id_num_asc ON Test (id + num ASC)");

    // Test ORDER BY id + num
    let results = ctx.query("SELECT * FROM Test ORDER BY id + num");
    assert_eq!(results.len(), 4);

    // Check order by id + num: 1+2=3, 1+9=10, 4+7=11, 3+NULL=NULL
    // NULLs should come last in ASC order
    assert_eq!(results[0].get("id").unwrap(), "I32(1)");
    assert_eq!(results[0].get("num").unwrap(), "I32(2)");
    assert_eq!(results[1].get("id").unwrap(), "I32(1)");
    assert_eq!(results[1].get("num").unwrap(), "I32(9)");
    assert_eq!(results[2].get("id").unwrap(), "I32(4)");
    assert_eq!(results[2].get("num").unwrap(), "I32(7)");
    assert_eq!(results[3].get("id").unwrap(), "I32(3)");
    assert_eq!(results[3].get("num").unwrap(), "Null");

    ctx.commit();
}

#[test]
fn test_order_by_desc_index() {
    let mut ctx = setup_test();

    // Create table
    ctx.exec("CREATE TABLE Test (id INTEGER, num INTEGER NULL, name TEXT)");

    // Insert data
    ctx.exec("INSERT INTO Test (id, num, name) VALUES (1, 2, 'Hello')");
    ctx.exec("INSERT INTO Test (id, num, name) VALUES (1, 9, 'Wild')");
    ctx.exec("INSERT INTO Test (id, num, name) VALUES (3, NULL, 'World')");
    ctx.exec("INSERT INTO Test (id, num, name) VALUES (4, 7, 'Monday')");

    // Create index with DESC order
    ctx.exec("CREATE INDEX idx_num_desc ON Test (num DESC)");

    // Test ORDER BY num DESC with WHERE clause
    let results = ctx.query("SELECT * FROM Test WHERE id < 4 ORDER BY num DESC");
    assert_eq!(results.len(), 3);

    // Check DESC order with NULL: NULL, 9, 2
    assert_eq!(results[0].get("num").unwrap(), "Null");
    assert_eq!(results[1].get("num").unwrap(), "I32(9)");
    assert_eq!(results[2].get("num").unwrap(), "I32(2)");

    ctx.commit();
}

#[test]
fn test_order_by_multi_column() {
    let mut ctx = setup_test();

    // Create table
    ctx.exec("CREATE TABLE Multi (id INTEGER, num INTEGER)");

    // Insert data
    ctx.exec("INSERT INTO Multi VALUES (3, 50), (3, 10), (3, 40), (3, 30), (3, 20)");
    ctx.exec("INSERT INTO Multi VALUES (4, 10), (4, 30), (4, 20), (4, 40), (4, 50)");
    ctx.exec("INSERT INTO Multi VALUES (2, 20), (2, 10), (2, 30), (2, 40), (2, 50)");
    ctx.exec("INSERT INTO Multi VALUES (5, 40), (5, 50), (5, 10), (5, 20), (5, 30)");
    ctx.exec("INSERT INTO Multi VALUES (1, 30), (1, 40), (1, 20), (1, 50), (1, 10)");

    // Test ORDER BY id ASC, num ASC
    let results = ctx.query("SELECT * FROM Multi ORDER BY id ASC, num ASC");
    assert_eq!(results.len(), 25);

    // Check first few rows - should be id=1 with num in ascending order
    assert_eq!(results[0].get("id").unwrap(), "I32(1)");
    assert_eq!(results[0].get("num").unwrap(), "I32(10)");
    assert_eq!(results[1].get("id").unwrap(), "I32(1)");
    assert_eq!(results[1].get("num").unwrap(), "I32(20)");
    assert_eq!(results[2].get("id").unwrap(), "I32(1)");
    assert_eq!(results[2].get("num").unwrap(), "I32(30)");
    assert_eq!(results[3].get("id").unwrap(), "I32(1)");
    assert_eq!(results[3].get("num").unwrap(), "I32(40)");
    assert_eq!(results[4].get("id").unwrap(), "I32(1)");
    assert_eq!(results[4].get("num").unwrap(), "I32(50)");

    // Check last few rows - should be id=5 with num in ascending order
    assert_eq!(results[20].get("id").unwrap(), "I32(5)");
    assert_eq!(results[20].get("num").unwrap(), "I32(10)");
    assert_eq!(results[24].get("id").unwrap(), "I32(5)");
    assert_eq!(results[24].get("num").unwrap(), "I32(50)");

    ctx.commit();
}

#[test]
fn test_order_by_with_index() {
    let mut ctx = setup_test();

    // Create table
    ctx.exec("CREATE TABLE Multi (id INTEGER, num INTEGER)");

    // Insert data
    ctx.exec("INSERT INTO Multi VALUES (3, 50), (3, 10), (3, 40), (3, 30), (3, 20)");
    ctx.exec("INSERT INTO Multi VALUES (4, 10), (4, 30), (4, 20), (4, 40), (4, 50)");
    ctx.exec("INSERT INTO Multi VALUES (2, 20), (2, 10), (2, 30), (2, 40), (2, 50)");
    ctx.exec("INSERT INTO Multi VALUES (5, 40), (5, 50), (5, 10), (5, 20), (5, 30)");
    ctx.exec("INSERT INTO Multi VALUES (1, 30), (1, 40), (1, 20), (1, 50), (1, 10)");

    // Create index on num column
    ctx.exec("CREATE INDEX idx_num ON Multi (num)");

    // Test ORDER BY num ASC, id ASC
    let results = ctx.query("SELECT * FROM Multi ORDER BY num ASC, id ASC");
    assert_eq!(results.len(), 25);

    // Check first few rows - should be num=10 with id in ascending order
    assert_eq!(results[0].get("num").unwrap(), "I32(10)");
    assert_eq!(results[0].get("id").unwrap(), "I32(1)");
    assert_eq!(results[1].get("num").unwrap(), "I32(10)");
    assert_eq!(results[1].get("id").unwrap(), "I32(2)");
    assert_eq!(results[2].get("num").unwrap(), "I32(10)");
    assert_eq!(results[2].get("id").unwrap(), "I32(3)");
    assert_eq!(results[3].get("num").unwrap(), "I32(10)");
    assert_eq!(results[3].get("id").unwrap(), "I32(4)");
    assert_eq!(results[4].get("num").unwrap(), "I32(10)");
    assert_eq!(results[4].get("id").unwrap(), "I32(5)");

    ctx.commit();
}

#[test]
fn test_order_by_mixed_asc_desc() {
    let mut ctx = setup_test();

    // Create table
    ctx.exec("CREATE TABLE Multi (id INTEGER, num INTEGER)");

    // Insert data
    ctx.exec("INSERT INTO Multi VALUES (1, 10), (1, 20), (1, 30)");
    ctx.exec("INSERT INTO Multi VALUES (2, 10), (2, 20), (2, 30)");
    ctx.exec("INSERT INTO Multi VALUES (3, 10), (3, 20), (3, 30)");

    // Test ORDER BY id DESC, num ASC
    let results = ctx.query("SELECT * FROM Multi ORDER BY id DESC, num ASC");
    assert_eq!(results.len(), 9);

    // Check order - id descending (3, 2, 1), within each id num ascending
    assert_eq!(results[0].get("id").unwrap(), "I32(3)");
    assert_eq!(results[0].get("num").unwrap(), "I32(10)");
    assert_eq!(results[1].get("id").unwrap(), "I32(3)");
    assert_eq!(results[1].get("num").unwrap(), "I32(20)");
    assert_eq!(results[2].get("id").unwrap(), "I32(3)");
    assert_eq!(results[2].get("num").unwrap(), "I32(30)");

    assert_eq!(results[3].get("id").unwrap(), "I32(2)");
    assert_eq!(results[3].get("num").unwrap(), "I32(10)");

    assert_eq!(results[6].get("id").unwrap(), "I32(1)");
    assert_eq!(results[6].get("num").unwrap(), "I32(10)");
    assert_eq!(results[8].get("id").unwrap(), "I32(1)");
    assert_eq!(results[8].get("num").unwrap(), "I32(30)");

    ctx.commit();
}

#[test]
fn test_order_by_expression_with_desc() {
    let mut ctx = setup_test();

    // Create table
    ctx.exec("CREATE TABLE Multi (id INTEGER, num INTEGER)");

    // Insert data
    ctx.exec("INSERT INTO Multi VALUES (1, 30), (1, 40), (1, 20), (1, 50), (1, 10)");
    ctx.exec("INSERT INTO Multi VALUES (2, 20), (2, 10), (2, 30), (2, 40), (2, 50)");

    // Create expression index with DESC
    ctx.exec("CREATE INDEX idx_id_num ON Multi (id + num DESC)");

    // Test ORDER BY id ASC, id + num DESC
    let results = ctx.query("SELECT * FROM Multi ORDER BY id ASC, id + num DESC");
    assert_eq!(results.len(), 10);

    // Check order - id ascending, within each id: id+num descending
    // For id=1: 1+50=51, 1+40=41, 1+30=31, 1+20=21, 1+10=11
    assert_eq!(results[0].get("id").unwrap(), "I32(1)");
    assert_eq!(results[0].get("num").unwrap(), "I32(50)");
    assert_eq!(results[1].get("id").unwrap(), "I32(1)");
    assert_eq!(results[1].get("num").unwrap(), "I32(40)");
    assert_eq!(results[4].get("id").unwrap(), "I32(1)");
    assert_eq!(results[4].get("num").unwrap(), "I32(10)");

    // For id=2: 2+50=52, 2+40=42, etc.
    assert_eq!(results[5].get("id").unwrap(), "I32(2)");
    assert_eq!(results[5].get("num").unwrap(), "I32(50)");
    assert_eq!(results[9].get("id").unwrap(), "I32(2)");
    assert_eq!(results[9].get("num").unwrap(), "I32(10)");

    ctx.commit();
}
