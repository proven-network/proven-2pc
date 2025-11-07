//! Common test utilities for integration tests

use proven_common::{ChangeData, Operation, OperationType, ProcessorType, Response, TransactionId};
use proven_engine::MockClient;
use proven_stream::{
    BatchOperations, OperationResult, ResponseMode, RetryOn, StreamProcessingKernel,
    TransactionEngine,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::oneshot;

// Common test batch for all test engines
pub struct TestBatch;

impl BatchOperations for TestBatch {
    fn insert_transaction_metadata(&mut self, _txn_id: TransactionId, _value: Vec<u8>) {}
    fn remove_transaction_metadata(&mut self, _txn_id: TransactionId) {}
}

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

    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Kv
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

    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Kv
    }
}

/// Basic CRUD response
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum BasicResponse {
    Value(Option<String>),
    Success,
}

impl Response for BasicResponse {}

#[derive(Debug, Serialize, Deserialize)]
pub struct TestChangeData;
impl ChangeData for TestChangeData {
    fn merge(self, _other: Self) -> Self {
        self
    }
}

// TransactionEngine implementation for LockOp tests
impl TransactionEngine for TestEngine<LockOp, LockResponse> {
    type Operation = LockOp;
    type Response = LockResponse;
    type ChangeData = TestChangeData;
    type Batch = TestBatch;

    fn start_batch(&mut self) -> Self::Batch {
        TestBatch
    }

    fn commit_batch(&mut self, _batch: Self::Batch, log_index: u64) {
        self.log_index = self.log_index.max(log_index);
    }

    fn read_at_timestamp(
        &self,
        operation: Self::Operation,
        _read_timestamp: TransactionId,
    ) -> Self::Response {
        match operation {
            LockOp::Lock { .. } => {
                panic!("Lock operations not supported for read-only")
            }
            LockOp::Read { resource } => LockResponse::Value {
                data: format!("Data from {}", resource),
            },
        }
    }

    fn apply_operation(
        &mut self,
        _batch: &mut Self::Batch,
        operation: Self::Operation,
        txn_id: TransactionId,
    ) -> OperationResult<Self::Response> {
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

    fn begin(&mut self, _batch: &mut Self::Batch, _txn_id: TransactionId) {}

    fn prepare(&mut self, _batch: &mut Self::Batch, _txn_id: TransactionId) {}

    fn commit(&mut self, _batch: &mut Self::Batch, txn_id: TransactionId) -> Self::ChangeData {
        // Release all locks held by this transaction
        self.locks.retain(|_, &mut holder| holder != txn_id);
        TestChangeData
    }

    fn abort(&mut self, _batch: &mut Self::Batch, txn_id: TransactionId) {
        // Release all locks held by this transaction
        self.locks.retain(|_, &mut holder| holder != txn_id);
    }

    fn get_log_index(&self) -> Option<u64> {
        Some(self.log_index)
    }

    fn scan_transaction_metadata(&self) -> Vec<(TransactionId, Vec<u8>)> {
        vec![]
    }

    fn engine_name(&self) -> &str {
        "lock-test-engine"
    }
}

// TransactionEngine implementation for BasicOp tests
impl TransactionEngine for TestEngine<BasicOp, BasicResponse> {
    type Operation = BasicOp;
    type Response = BasicResponse;
    type ChangeData = TestChangeData;
    type Batch = TestBatch;

    fn start_batch(&mut self) -> Self::Batch {
        TestBatch
    }

    fn commit_batch(&mut self, _batch: Self::Batch, log_index: u64) {
        self.log_index = self.log_index.max(log_index);
    }

    fn read_at_timestamp(
        &self,
        operation: Self::Operation,
        _read_timestamp: TransactionId,
    ) -> Self::Response {
        match operation {
            BasicOp::Read { key } => {
                let value = self.data.get(&key).cloned();
                BasicResponse::Value(value)
            }
            BasicOp::Write { .. } => {
                panic!("Write operations not supported for read-only")
            }
        }
    }

    fn apply_operation(
        &mut self,
        _batch: &mut Self::Batch,
        operation: Self::Operation,
        _txn_id: TransactionId,
    ) -> OperationResult<Self::Response> {
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

    fn begin(&mut self, _batch: &mut Self::Batch, _txn_id: TransactionId) {}

    fn prepare(&mut self, _batch: &mut Self::Batch, _txn_id: TransactionId) {}

    fn commit(&mut self, _batch: &mut Self::Batch, _txn_id: TransactionId) -> Self::ChangeData {
        TestChangeData
    }

    fn abort(&mut self, _batch: &mut Self::Batch, _txn_id: TransactionId) {}

    fn get_log_index(&self) -> Option<u64> {
        Some(self.log_index)
    }

    fn scan_transaction_metadata(&self) -> Vec<(TransactionId, Vec<u8>)> {
        vec![]
    }

    fn engine_name(&self) -> &str {
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

/// Helper to run a test processor with the kernel + orchestration pattern
/// Returns a shutdown sender to stop the processor
pub fn run_test_processor<E: TransactionEngine + Send + 'static>(
    engine: E,
    client: Arc<MockClient>,
    stream_name: String,
) -> oneshot::Sender<()> {
    let kernel = StreamProcessingKernel::new(engine, client.clone(), stream_name.clone());
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    tokio::spawn(async move {
        if let Err(e) = run_stream_processor_simple(kernel, client, shutdown_rx).await {
            tracing::error!("[{}] Processor error: {:?}", stream_name, e);
        }
    });

    shutdown_tx
}

/// Simple orchestration for tests (mimics runner/orchestration.rs but simplified)
async fn run_stream_processor_simple<E: TransactionEngine>(
    mut kernel: StreamProcessingKernel<E>,
    client: Arc<MockClient>,
    mut shutdown_rx: oneshot::Receiver<()>,
) -> Result<(), String> {
    use std::time::Duration;

    // Step 1: Recover transaction state
    kernel
        .recover_transaction_state()
        .map_err(|e| format!("Recovery failed: {}", e))?;

    // Step 2: Perform replay
    let stream_name = kernel.stream_name().to_string();
    let start_offset = kernel.current_offset() + 1;

    kernel.set_response_mode(ResponseMode::Suppress);

    let mut replay_stream = client
        .stream_messages(stream_name.clone(), Some(start_offset))
        .await
        .map_err(|e| format!("Stream subscribe failed: {}", e))?;

    // Replay until caught up (1 second timeout for empty stream detection)
    loop {
        tokio::select! {
            result = replay_stream.recv() => {
                match result {
                    Some((message, timestamp, offset)) => {
                        kernel.process_ordered(message, timestamp, offset).await
                            .map_err(|e| format!("Process error: {}", e))?;
                    }
                    None => break,
                }
            }
            _ = tokio::time::sleep(Duration::from_secs(1)) => break,
        }
    }

    // Step 3: Transition to live (enable responses)
    kernel.set_response_mode(ResponseMode::Send);

    let start_offset = if kernel.current_offset() > 0 {
        Some(kernel.current_offset() + 1)
    } else {
        Some(0)
    };

    let mut ordered_stream = client
        .stream_messages(stream_name.clone(), start_offset)
        .await
        .map_err(|e| format!("Stream subscribe failed: {}", e))?;

    let mut readonly_stream = client
        .subscribe(&format!("stream.{}.readonly", stream_name), None)
        .await
        .map_err(|e| format!("Pubsub subscribe failed: {}", e))?;

    let mut idle_check = tokio::time::interval(Duration::from_millis(100));
    idle_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // Event loop
    loop {
        tokio::select! {
            biased;

            _ = &mut shutdown_rx => break,

            result = ordered_stream.recv() => {
                match result {
                    Some((message, timestamp, offset)) => {
                        let _ = kernel.process_ordered(message, timestamp, offset).await;
                    }
                    None => break,
                }
            }

            Some(message) = readonly_stream.recv() => {
                let _ = kernel.process_read_only(message);
            }

            _ = idle_check.tick() => {
                let now = proven_common::Timestamp::now();
                if kernel.needs_processing(now) {
                    let noop_msg = proven_protocol::OrderedMessage::<E::Operation>::Noop;
                    let _ = client.publish_to_stream(
                        stream_name.clone(),
                        vec![noop_msg.into_message()],
                    ).await;
                }
            }
        }
    }

    Ok(())
}
