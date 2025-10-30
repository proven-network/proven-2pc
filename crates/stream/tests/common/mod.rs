//! Common test utilities for integration tests

use proven_common::{Operation, OperationType, Response, TransactionId};
use proven_stream::engine::{OperationResult, RetryOn, TransactionEngine};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Test engine with lock tracking for wound-wait tests
pub struct TestEngine<Op, Resp> {
    /// Current log index
    log_index: u64,
    /// Resource locks (resource_name -> holder_txn_id)
    locks: HashMap<String, TransactionId>,
    /// Key-value data
    data: HashMap<String, String>,
    /// Phantom data for operation and response types
    _phantom: std::marker::PhantomData<(Op, Resp)>,
}

impl<Op, Resp> TestEngine<Op, Resp> {
    pub fn new() -> Self {
        Self {
            log_index: 0,
            locks: HashMap::new(),
            data: HashMap::new(),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Get the current locks (for testing)
    #[allow(dead_code)]
    pub fn locks(&self) -> &HashMap<String, TransactionId> {
        &self.locks
    }

    /// Get the key-value data (for testing)
    #[allow(dead_code)]
    pub fn data(&self) -> &HashMap<String, String> {
        &self.data
    }

    /// Get the current log index
    #[allow(dead_code)]
    pub fn log_index(&self) -> u64 {
        self.log_index
    }
}

/// Lock operation for wound-wait tests
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LockOp {
    Lock { resource: String },
    Read { resource: String },
}

impl Operation for LockOp {
    fn operation_type(&self) -> OperationType {
        match self {
            LockOp::Lock { .. } => OperationType::Write,
            LockOp::Read { .. } => OperationType::Read,
        }
    }
}

/// Lock response for wound-wait tests
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum LockResponse {
    Success { message: String },
    Value { data: String },
}

impl Response for LockResponse {}

/// Basic CRUD operation
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BasicOp {
    Read { key: String },
    Write { key: String, value: String },
}

impl Operation for BasicOp {
    fn operation_type(&self) -> OperationType {
        match self {
            BasicOp::Read { .. } => OperationType::Read,
            BasicOp::Write { .. } => OperationType::Write,
        }
    }
}

/// Basic CRUD response
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum BasicResponse {
    Value(Option<String>),
    Success,
}

impl Response for BasicResponse {}

// TransactionEngine implementation for LockOp tests
impl TransactionEngine for TestEngine<LockOp, LockResponse> {
    type Operation = LockOp;
    type Response = LockResponse;

    fn read_at_timestamp(
        &mut self,
        operation: Self::Operation,
        _read_timestamp: TransactionId,
    ) -> OperationResult<Self::Response> {
        match operation {
            LockOp::Lock { .. } => {
                panic!("Lock operations not supported for read-only")
            }
            LockOp::Read { resource } => OperationResult::Complete(LockResponse::Value {
                data: format!("Data from {}", resource),
            }),
        }
    }

    fn apply_operation(
        &mut self,
        operation: Self::Operation,
        txn_id: TransactionId,
        log_index: u64,
    ) -> OperationResult<Self::Response> {
        self.log_index = self.log_index.max(log_index);

        match operation {
            LockOp::Lock { resource } => {
                // Check if resource is locked by another transaction
                if let Some(&holder) = self.locks.get(&resource)
                    && holder != txn_id
                {
                    // Resource is locked - report conflict
                    return OperationResult::WouldBlock {
                        blockers: vec![proven_stream::BlockingInfo {
                            txn: holder,
                            retry_on: RetryOn::CommitOrAbort,
                        }],
                    };
                }

                // Acquire lock
                self.locks.insert(resource.clone(), txn_id);
                OperationResult::Complete(LockResponse::Success {
                    message: format!("Locked {}", resource),
                })
            }
            LockOp::Read { resource } => OperationResult::Complete(LockResponse::Value {
                data: format!("Data from {}", resource),
            }),
        }
    }

    fn begin(&mut self, _txn_id: TransactionId, log_index: u64) {
        self.log_index = self.log_index.max(log_index);
    }

    fn prepare(&mut self, _txn_id: TransactionId, log_index: u64) {
        self.log_index = self.log_index.max(log_index);
    }

    fn commit(&mut self, txn_id: TransactionId, log_index: u64) {
        self.log_index = self.log_index.max(log_index);
        // Release all locks held by this transaction
        self.locks.retain(|_, &mut holder| holder != txn_id);
    }

    fn abort(&mut self, txn_id: TransactionId, log_index: u64) {
        self.log_index = self.log_index.max(log_index);
        // Release all locks held by this transaction
        self.locks.retain(|_, &mut holder| holder != txn_id);
    }

    fn engine_name(&self) -> &'static str {
        "lock-test-engine"
    }
}

// TransactionEngine implementation for BasicOp tests
impl TransactionEngine for TestEngine<BasicOp, BasicResponse> {
    type Operation = BasicOp;
    type Response = BasicResponse;

    fn read_at_timestamp(
        &mut self,
        operation: Self::Operation,
        _read_timestamp: TransactionId,
    ) -> OperationResult<Self::Response> {
        match operation {
            BasicOp::Read { key } => {
                let value = self.data.get(&key).cloned();
                OperationResult::Complete(BasicResponse::Value(value))
            }
            BasicOp::Write { .. } => {
                panic!("Write operations not supported for read-only")
            }
        }
    }

    fn apply_operation(
        &mut self,
        operation: Self::Operation,
        _txn_id: TransactionId,
        log_index: u64,
    ) -> OperationResult<Self::Response> {
        self.log_index = self.log_index.max(log_index);

        match operation {
            BasicOp::Read { key } => {
                let value = self.data.get(&key).cloned();
                OperationResult::Complete(BasicResponse::Value(value))
            }
            BasicOp::Write { key, value } => {
                self.data.insert(key, value);
                OperationResult::Complete(BasicResponse::Success)
            }
        }
    }

    fn begin(&mut self, _txn_id: TransactionId, log_index: u64) {
        self.log_index = self.log_index.max(log_index);
    }

    fn prepare(&mut self, _txn_id: TransactionId, log_index: u64) {
        self.log_index = self.log_index.max(log_index);
    }

    fn commit(&mut self, _txn_id: TransactionId, log_index: u64) {
        self.log_index = self.log_index.max(log_index);
    }

    fn abort(&mut self, _txn_id: TransactionId, log_index: u64) {
        self.log_index = self.log_index.max(log_index);
    }

    fn engine_name(&self) -> &'static str {
        "basic-test-engine"
    }
}

/// Helper to generate deterministic transaction IDs for tests
/// Smaller timestamp_ms = older transaction (lexicographically smaller UUID)
pub fn txn_id(timestamp_ms: u64) -> proven_common::TransactionId {
    // UUIDv7 format: timestamp (48 bits) split into high (32 bits) and mid (16 bits)
    let high = ((timestamp_ms >> 16) & 0xFFFFFFFF) as u32;
    let mid = (timestamp_ms & 0xFFFF) as u16;

    let uuid_str = format!("{:08x}-{:04x}-7000-8000-000000000000", high, mid);
    proven_common::TransactionId::from_uuid(uuid::Uuid::parse_str(&uuid_str).unwrap())
}
