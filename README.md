# Proven 2PC

A distributed storage system implementing multiple data models (SQL, KV, Queue, Resource) with ACID transactions, two-phase commit, and streaming support.

## Overview

Proven SQL is a monorepo containing a suite of storage engines that work together to provide:

- **Multiple Data Models**: SQL, Key-Value, Queue, and Resource (token/fungible asset) storage
- **Distributed Transactions**: Coordinated transactions across different storage types
- **Two-Phase Commit**: Full 2PC support with prepare/commit/abort phases
- **Streaming Architecture**: Event-driven processing with deterministic replay
- **MVCC & Locking**: Different concurrency control strategies optimized for each storage type

## Crates

### Core Infrastructure

#### `proven-coordinator` - Transaction Coordinator
Orchestrates distributed transactions across multiple storage engines using two-phase commit.

#### `proven-runner` - Stream Processor Manager
Manages the lifecycle, placement, and health of stream processors across the cluster.

#### `proven-stream` - Stream Processing Framework
Generic transaction engine trait and stream processing infrastructure used by all storage engines.

#### `proven-engine` - Mock Consensus Engine
Test implementation simulating a consensus/streaming layer for development and testing.

#### `proven-mvcc` - Multi-Version Concurrency Control
Generic MVCC storage layer providing snapshot isolation, time-travel queries, and crash recovery.

#### `proven-value` - Value Types and Serialization
Common value types and encoding/decoding utilities shared across storage engines.

### Storage Engines

#### `proven-sql` - SQL Storage Engine
- Full SQL query execution with parser and planner
- Predicate-based locking for transaction isolation
- Table management with schemas and indexes
- Support for basic SQL operations (CREATE, INSERT, SELECT, UPDATE, DELETE)

#### `proven-kv` - Key-Value Storage Engine
- Simple key-value operations (Get, Put, Delete)
- Multiple value types (String, Integer, Float, Boolean, Map, List)
- MVCC for concurrent reads
- Shared/Exclusive locking for consistency

#### `proven-queue` - Queue Storage Engine
- Named queues with FIFO semantics
- Enqueue/Dequeue operations
- Peek functionality
- Transaction support for reliable message processing

#### `proven-resource` - Resource/Token Storage Engine
- Fungible resource management (like ERC20 tokens)
- Mint, burn, and transfer operations
- Configurable decimals for precision
- Reservation-based concurrency for high throughput
- Balance tracking with MVCC

### Client Libraries

Each storage engine has a corresponding client library that provides a high-level API:

#### `proven-sql-client`
SQL client for executing queries within distributed transactions.

#### `proven-kv-client`
Key-value client with typed operations for common data types.

#### `proven-queue-client`
Queue client for reliable message enqueue/dequeue operations.

#### `proven-resource-client`
Resource client for token management operations.

## Architecture

```
┌───────────────────────────────────────────────────────┐
│                  Client Applications                  │
│         (using SQL/KV/Queue/Resource clients)         │
└───────────────────────────────────────────────────────┘
                            ↕
┌───────────────────────────────────────────────────────┐
│                 Transaction Coordinator               │
│                    (2PC Protocol)                     │
└───────────────────────────────────────────────────────┘
                            ↕
┌───────────────────────────────────────────────────────┐
│                      Runner                           │
│            (Processor Lifecycle Manager)              │
└───────────────────────────────────────────────────────┘
                            ↓
┌───────────────────────────────────────────────────────┐
│                 Consensus/Log Layer                   │
│                 (Raft, Kafka, etc.)                   │
└───────────────────────────────────────────────────────┘
                            ↓
┌───────────────────────────────────────────────────────┐
│                Stream Processors                      │
│              (TransactionEngine trait)                │
└───────────────────────────────────────────────────────┘
                            ↓
┌─────────────┬─────────────┬─────────────┬─────────────┐
│     SQL     │     KV      │    Queue    │  Resource   │
│    Engine   │   Engine    │   Engine    │   Engine    │
└─────────────┴─────────────┴─────────────┴─────────────┘
```

## Key Features

### Transaction Support
All storage engines implement the `TransactionEngine` trait providing:
- `begin` - Start a new transaction
- `apply_operation` - Execute operations within a transaction
- `prepare` - Phase 1 of 2PC, validate and prepare to commit
- `commit` - Phase 2 of 2PC, make changes permanent
- `abort` - Rollback all changes

### Concurrency Control
- **SQL**: Predicate-based locking with retry on conflicts
- **KV**: Shared/Exclusive locks with MVCC for reads
- **Queue**: Queue-level locking
- **Resource**: Reservation-based system for better throughput

### Two-Phase Commit
The coordinator ensures atomicity across storage engines:
1. **Prepare Phase**: All participants validate and lock resources
2. **Commit Phase**: All participants make changes permanent
3. **Abort**: Any participant can cause global rollback

## Examples

### Running the Distributed Transaction Example

```bash
cargo run --example distributed_transaction
```

This demonstrates a transaction that:
- Creates a SQL table and inserts user data
- Stores user metadata in KV storage
- Enqueues notifications and audit events
- Manages loyalty points in the Resource engine

### Basic Usage

```rust
use proven_coordinator::{Coordinator, Executor};
use proven_kv_client::KvClient;
use std::time::Duration;

// Create a coordinator
let coordinator = Coordinator::new(
    "my-coordinator".to_string(),
    client,
    runner,
);

// Begin a distributed transaction
let executor = coordinator
    .begin_read_write(
        Duration::from_secs(60),
        vec![],
        "my-transaction".to_string(),
    )
    .await?;

// Create a KV client
let kv = KvClient::new(executor.clone());

// Put a value
kv.put("kv_stream", "user:123", "Alice").await?;

// Commit the transaction
executor.finish().await?;
```

## Testing

Run all tests:
```bash
cargo test
```

Run tests for a specific crate:
```bash
cargo test -p proven-kv
cargo test -p proven-resource
```

## Development Status

### Production Ready
- ✅ Stream processing framework with atomic persistence
- ✅ KV storage engine
- ✅ Queue storage engine
- ✅ Resource storage engine
- ✅ Two-phase commit coordinator
- ✅ Runner with storage management

### In Development
- 🚧 Production consensus integration (monorepo using a mock version of engine)
- 🚧 Client libraries and runtime integration
- 🚧 Performance optimizations
- 🚧 More integration/fuzz/chaos tests
