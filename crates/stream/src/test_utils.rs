//! Common test utilities for stream processor tests

use crate::engine::{OperationResult, RetryOn, TransactionEngine};
use proven_common::{Operation, Response};
use proven_hlc::HlcTimestamp;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Generic test engine that can be used across different test scenarios
pub struct TestEngine<Op, Resp> {
    /// Current log index
    log_index: u64,
    /// Resource locks (for wound-wait tests)
    locks: HashMap<String, HlcTimestamp>,
    /// Key-value data (for basic tests)
    data: HashMap<String, String>,
    /// Active transactions
    active_txns: Vec<HlcTimestamp>,
    /// Phantom data for operation and response types
    _phantom: std::marker::PhantomData<(Op, Resp)>,
}

impl<Op, Resp> TestEngine<Op, Resp> {
    pub fn new() -> Self {
        Self {
            log_index: 0,
            locks: HashMap::new(),
            data: HashMap::new(),
            active_txns: Vec::new(),
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn with_log_index(log_index: u64) -> Self {
        Self {
            log_index,
            locks: HashMap::new(),
            data: HashMap::new(),
            active_txns: Vec::new(),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Get the key-value data (for testing)
    pub fn data(&self) -> &HashMap<String, String> {
        &self.data
    }
}

// Wound-wait test operation
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LockOp {
    Lock { resource: String },
    Read { resource: String },
}

impl Operation for LockOp {
    fn operation_type(&self) -> proven_common::OperationType {
        match self {
            LockOp::Lock { .. } => proven_common::OperationType::Write,
            LockOp::Read { .. } => proven_common::OperationType::Read,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LockResponse {
    Success { message: String },
    Value { data: String },
}

impl Response for LockResponse {}

// Basic test operation
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BasicOp {
    Read { key: String },
    Write { key: String, value: String },
}

impl Operation for BasicOp {
    fn operation_type(&self) -> proven_common::OperationType {
        match self {
            BasicOp::Read { .. } => proven_common::OperationType::Read,
            BasicOp::Write { .. } => proven_common::OperationType::Write,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BasicResponse {
    Value(Option<String>),
    Success,
}

impl Response for BasicResponse {}

// Implementation for LockOp tests
impl TransactionEngine for TestEngine<LockOp, LockResponse> {
    type Operation = LockOp;
    type Response = LockResponse;

    fn read_at_timestamp(
        &mut self,
        operation: Self::Operation,
        _read_timestamp: HlcTimestamp,
        _log_index: u64,
    ) -> OperationResult<Self::Response> {
        match operation {
            LockOp::Lock { .. } => {
                panic!("Lock operations not supported for read-only operations");
            }
            LockOp::Read { resource } => OperationResult::Complete(LockResponse::Value {
                data: format!("Data from {}", resource),
            }),
        }
    }

    fn apply_operation(
        &mut self,
        operation: Self::Operation,
        txn_id: HlcTimestamp,
        log_index: u64,
    ) -> OperationResult<Self::Response> {
        self.log_index = self.log_index.max(log_index);

        match operation {
            LockOp::Lock { resource } => {
                if let Some(&holder) = self.locks.get(&resource)
                    && holder != txn_id
                {
                    // Just report the conflict - stream processor handles wound-wait
                    return OperationResult::WouldBlock {
                        blockers: vec![crate::engine::BlockingInfo {
                            txn: holder,
                            retry_on: RetryOn::CommitOrAbort,
                        }],
                    };
                }
                self.locks.insert(resource, txn_id);
                OperationResult::Complete(LockResponse::Success {
                    message: "Lock acquired".to_string(),
                })
            }
            LockOp::Read { resource } => OperationResult::Complete(LockResponse::Value {
                data: format!("Data from {}", resource),
            }),
        }
    }

    fn prepare(&mut self, _txn_id: HlcTimestamp, log_index: u64) {
        self.log_index = self.log_index.max(log_index);
    }

    fn commit(&mut self, txn_id: HlcTimestamp, log_index: u64) {
        self.log_index = self.log_index.max(log_index);
        self.locks.retain(|_, &mut holder| holder != txn_id);
    }

    fn abort(&mut self, txn_id: HlcTimestamp, log_index: u64) {
        self.log_index = self.log_index.max(log_index);
        self.locks.retain(|_, &mut holder| holder != txn_id);
    }

    fn begin(&mut self, _txn_id: HlcTimestamp, log_index: u64) {
        self.log_index = self.log_index.max(log_index);
    }

    fn engine_name(&self) -> &'static str {
        "test"
    }

    fn get_log_index(&self) -> u64 {
        self.log_index
    }
}

// Implementation for BasicOp tests
impl TransactionEngine for TestEngine<BasicOp, BasicResponse> {
    type Operation = BasicOp;
    type Response = BasicResponse;

    fn read_at_timestamp(
        &mut self,
        operation: Self::Operation,
        _read_timestamp: HlcTimestamp,
        _log_index: u64,
    ) -> OperationResult<Self::Response> {
        match operation {
            BasicOp::Read { key } => {
                let value = self.data.get(&key).cloned();
                OperationResult::Complete(BasicResponse::Value(value))
            }
            BasicOp::Write { .. } => {
                panic!("Write operations not supported for read-only operations");
            }
        }
    }

    fn apply_operation(
        &mut self,
        operation: Self::Operation,
        _txn_id: HlcTimestamp,
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

    fn prepare(&mut self, _txn_id: HlcTimestamp, log_index: u64) {
        self.log_index = self.log_index.max(log_index);
    }

    fn commit(&mut self, txn_id: HlcTimestamp, log_index: u64) {
        self.log_index = self.log_index.max(log_index);
        self.active_txns.retain(|&id| id != txn_id);
    }

    fn abort(&mut self, txn_id: HlcTimestamp, log_index: u64) {
        self.log_index = self.log_index.max(log_index);
        self.active_txns.retain(|&id| id != txn_id);
    }

    fn begin(&mut self, txn_id: HlcTimestamp, log_index: u64) {
        self.log_index = self.log_index.max(log_index);
        self.active_txns.push(txn_id);
    }

    fn engine_name(&self) -> &'static str {
        "test-engine"
    }

    fn get_log_index(&self) -> u64 {
        self.log_index
    }
}
