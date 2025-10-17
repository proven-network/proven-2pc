//! Transaction index DDL tests
//! Based on gluesql/test-suite/src/transaction/index.rs

mod common;

use common::TestContext;

#[ignore = "Index usage verification requires EXPLAIN support"]
#[test]
fn test_create_index_with_rollback() {
    let mut ctx = TestContext::new();

    // Setup: Create table and insert data
    ctx.begin();
    ctx.exec("CREATE TABLE IdxCreate (id INTEGER)");
    ctx.exec("INSERT INTO IdxCreate VALUES (1)");
    ctx.commit();

    // Test: Create index within transaction, then rollback
    ctx.begin();
    ctx.exec("CREATE INDEX idx_id ON IdxCreate (id)");

    // Verify query still works within transaction
    ctx.assert_row_count("SELECT id FROM IdxCreate WHERE id = 1", 1);
    // TODO: Verify index is used (requires EXPLAIN or query plan inspection)

    // Rollback
    ctx.abort();

    // Verify query still works after rollback
    ctx.assert_row_count("SELECT id FROM IdxCreate WHERE id = 1", 1);
    // TODO: Verify index is NOT used (requires EXPLAIN or query plan inspection)
}

#[ignore = "Index usage verification requires EXPLAIN support"]
#[test]
fn test_create_index_with_commit() {
    let mut ctx = TestContext::new();

    // Setup: Create table and insert data
    ctx.begin();
    ctx.exec("CREATE TABLE IdxCreate (id INTEGER)");
    ctx.exec("INSERT INTO IdxCreate VALUES (1)");
    ctx.commit();

    // Test: Create index within transaction, then commit
    ctx.begin();
    ctx.exec("CREATE INDEX idx_id ON IdxCreate (id)");

    // Verify query works within transaction
    ctx.assert_row_count("SELECT id FROM IdxCreate WHERE id = 1", 1);
    // TODO: Verify index is used (requires EXPLAIN or query plan inspection)

    // Commit
    ctx.commit();

    // Verify query still works after commit
    ctx.assert_row_count("SELECT id FROM IdxCreate WHERE id = 1", 1);
    // TODO: Verify index is used (requires EXPLAIN or query plan inspection)
}

#[ignore = "Index usage verification requires EXPLAIN support"]
#[test]
fn test_create_multiple_indexes_with_rollback() {
    let mut ctx = TestContext::new();

    // Setup: Create table with index and insert data
    ctx.begin();
    ctx.exec("CREATE TABLE IdxCreate (id INTEGER)");
    ctx.exec("INSERT INTO IdxCreate VALUES (1)");
    ctx.commit();

    ctx.begin();
    ctx.exec("CREATE INDEX idx_id ON IdxCreate (id)");
    ctx.commit();

    // Clear and insert new data
    ctx.begin();
    ctx.exec("DELETE FROM IdxCreate");
    ctx.exec("INSERT INTO IdxCreate VALUES (3)");
    ctx.commit();

    // Test: Create expression index, then rollback
    ctx.begin();
    ctx.exec("CREATE INDEX idx_id2 ON IdxCreate (id * 2)");

    // Verify both queries work within transaction
    ctx.assert_row_count("SELECT id FROM IdxCreate WHERE id = 3", 1);
    // TODO: Verify idx_id is used

    ctx.assert_row_count("SELECT id FROM IdxCreate WHERE id * 2 = 6", 1);
    // TODO: Verify idx_id2 is used

    // Rollback
    ctx.abort();

    // Verify original index still works
    ctx.assert_row_count("SELECT id FROM IdxCreate WHERE id = 3", 1);
    // TODO: Verify idx_id is used

    // Verify expression query works but without index
    ctx.assert_row_count("SELECT id FROM IdxCreate WHERE id * 2 = 6", 1);
    // TODO: Verify idx_id2 is NOT used
}

#[ignore = "Index usage verification requires EXPLAIN support"]
#[test]
fn test_drop_index_with_rollback() {
    let mut ctx = TestContext::new();

    // Setup: Create table with index and insert data
    ctx.begin();
    ctx.exec("CREATE TABLE IdxDrop (id INTEGER)");
    ctx.exec("INSERT INTO IdxDrop VALUES (1)");
    ctx.exec("CREATE INDEX idx_id ON IdxDrop (id)");
    ctx.commit();

    // Test: Drop index within transaction, then rollback
    ctx.begin();
    ctx.exec("DROP INDEX IdxDrop.idx_id");

    // Verify query still works within transaction
    ctx.assert_row_count("SELECT id FROM IdxDrop WHERE id = 1", 1);
    // TODO: Verify index is NOT used (requires EXPLAIN or query plan inspection)

    // Rollback
    ctx.abort();

    // Verify query still works after rollback
    ctx.assert_row_count("SELECT id FROM IdxDrop WHERE id = 1", 1);
    // TODO: Verify index is used again (requires EXPLAIN or query plan inspection)
}

#[ignore = "Index usage verification requires EXPLAIN support"]
#[test]
fn test_drop_index_with_commit() {
    let mut ctx = TestContext::new();

    // Setup: Create table with index and insert data
    ctx.begin();
    ctx.exec("CREATE TABLE IdxDrop (id INTEGER)");
    ctx.exec("INSERT INTO IdxDrop VALUES (1)");
    ctx.exec("CREATE INDEX idx_id ON IdxDrop (id)");
    ctx.commit();

    // Test: Drop index within transaction, then commit
    ctx.begin();
    ctx.exec("DROP INDEX IdxDrop.idx_id");

    // Verify query still works within transaction
    ctx.assert_row_count("SELECT id FROM IdxDrop WHERE id = 1", 1);
    // TODO: Verify index is NOT used (requires EXPLAIN or query plan inspection)

    // Commit
    ctx.commit();

    // Verify query still works after commit
    ctx.assert_row_count("SELECT id FROM IdxDrop WHERE id = 1", 1);
    // TODO: Verify index is NOT used (requires EXPLAIN or query plan inspection)
}
