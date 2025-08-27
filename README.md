# Proven SQL

A distributed SQL engine with Pessimistic Concurrency Control (PCC) designed for integration with Raft consensus.

## Overview

This crate provides the foundational components for a distributed SQL engine that:

- Uses **pessimistic locking** instead of MVCC for simpler distributed coordination
- Implements **wound-wait deadlock prevention** to guarantee no deadlocks
- Provides **full lock visibility** for debugging and monitoring
- Ensures **deterministic execution** for Raft consensus
- Operates on **in-memory storage** with single-version data

## Architecture

### Core Components

1. **Lock Manager** (`src/lock.rs`)
   - Wound-wait deadlock prevention algorithm
   - Hierarchical locking (row, range, table, schema)
   - Lock compatibility matrix (S, X, IS, IX modes)
   - Full visibility into lock state

2. **Storage Engine** (`src/storage.rs`)
   - Single-version in-memory tables
   - B-tree based row storage for efficient scans
   - Secondary indexes
   - Deterministic ID generation
   - Soft deletes

3. **Transaction Manager** (`src/transaction.rs`)
   - Automatic lock acquisition before data access
   - Two-phase commit support
   - Access logging for distributed coordination
   - Automatic lock release on commit/abort

4. **Type System** (`src/types.rs`)
   - Deterministic value types (using `rust_decimal` instead of floats)
   - SQL-compatible operations
   - Logical timestamps
   - Null handling

5. **Raft Integration** (`src/raft.rs`)
   - State machine interface
   - Deterministic operation replay
   - Snapshot/restore capabilities

## Key Design Decisions

### Why PCC over MVCC?

- **Simpler distributed coordination**: Lock state is explicit and visible
- **Predictable behavior**: Conflicts detected immediately, not at commit time
- **Easier debugging**: Can query lock state at any time
- **Natural fit for consensus**: Operations on inputs rather than outputs

### Wound-Wait Algorithm

Transactions are assigned priorities based on their start time (earlier = higher priority):

- Higher priority transactions can "wound" (abort) lower priority holders
- Lower priority transactions must wait for higher priority holders
- Guarantees no deadlocks while allowing high concurrency

## Usage Example

```rust
use proven_sql::{
    Storage, 
    LockManager, 
    TransactionManager,
    storage::{Schema, Column},
    types::{DataType, Value},
};
use std::sync::Arc;

// Set up the engine
let storage = Arc::new(Storage::new());
let lock_manager = Arc::new(LockManager::new());
let tx_manager = TransactionManager::new(lock_manager, storage.clone());

// Create a table
let schema = Schema::new(vec![
    Column::new("id".into(), DataType::Integer).primary_key(),
    Column::new("name".into(), DataType::String),
    Column::new("balance".into(), DataType::Integer),
])?;
storage.create_table("accounts".into(), schema)?;

// Start a transaction
let tx = tx_manager.begin()?;

// Insert data (automatically acquires locks)
tx.insert("accounts", vec![
    Value::Integer(1),
    Value::String("Alice".into()),
    Value::Integer(1000),
])?;

// Commit (releases all locks)
tx.commit()?;
```

## Running the Demo

```bash
cargo run
```

This runs a comprehensive demo showing:
1. Basic storage operations
2. Lock manager with wound-wait
3. Concurrent transactions
4. Raft state machine integration

## Running Tests

```bash
cargo test
```

## Project Status

This is a proof-of-concept implementation demonstrating the core data structures for a PCC-based SQL engine. Current features:

✅ Lock manager with wound-wait  
✅ Single-version storage  
✅ Transaction management  
✅ Deterministic types  
✅ Raft integration framework  
✅ Basic tests and demos  

## Next Steps

To build a production system, the following would be needed:

1. **SQL Parser Integration**: Adapt toydb's parser for SQL parsing
2. **Query Planning**: Add lock analysis phase before execution  
3. **Query Execution**: Implement operators (scan, filter, join, aggregate)
4. **Network Layer**: Add RPC for distributed coordination
5. **Persistence**: Add write-ahead logging and snapshots
6. **Optimization**: Add query optimization and execution improvements

## Design Document

See [DESIGN.md](DESIGN.md) for the full system design and architecture details.
