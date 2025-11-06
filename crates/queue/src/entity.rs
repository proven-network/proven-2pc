//! Queue entities with separate storages for data and metadata
//!
//! Design:
//! - QueueEntity: Stores actual queue items (linked list nodes)
//! - HeadPointerEntity: Stores pointer to head of queue
//! - TailPointerEntity: Stores pointer to tail of queue
//!
//! Using separate MvccStorage instances allows:
//! - Different delta types for each concern
//! - Atomic commits across all three
//! - Efficient snapshot reads (just read head/tail pointers, no scanning)

use proven_mvcc::{Decode, Encode, Error as MvccError, MvccDelta, MvccEntity};
use proven_value::Value;

type Result<T> = std::result::Result<T, MvccError>;

/// Queue entity type
pub struct QueueEntity;

/// Simple sequence number key
/// No TransactionId needed - MVCC provides read-your-own-writes!
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct QueueKey(pub u64);

impl Encode for QueueKey {
    fn encode(&self) -> Result<Vec<u8>> {
        Ok(self.0.to_be_bytes().to_vec())
    }
}

impl Decode for QueueKey {
    fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 8 {
            return Err(MvccError::Encoding(format!(
                "QueueKey must be 8 bytes, got {}",
                bytes.len()
            )));
        }
        let mut buf = [0u8; 8];
        buf.copy_from_slice(bytes);
        Ok(QueueKey(u64::from_be_bytes(buf)))
    }
}

/// Queue value - actual data with linked list pointers
#[derive(Clone, Debug, PartialEq)]
pub struct QueueValue {
    /// The actual queue item
    pub value: Value,

    /// Next item in queue (None if this is the tail)
    pub next: Option<u64>,

    /// Previous item in queue (None if this is the head)
    pub prev: Option<u64>,
}

impl Encode for QueueValue {
    fn encode(&self) -> Result<Vec<u8>> {
        let mut output = Vec::new();

        // Encode value using proven_value codec
        let value_bytes = proven_value::codec::encode_value(&self.value);
        output.extend_from_slice(&(value_bytes.len() as u32).to_be_bytes());
        output.extend_from_slice(&value_bytes);

        // Encode next pointer
        match self.next {
            Some(next) => {
                output.push(1);
                output.extend_from_slice(&next.to_be_bytes());
            }
            None => output.push(0),
        }

        // Encode prev pointer
        match self.prev {
            Some(prev) => {
                output.push(1);
                output.extend_from_slice(&prev.to_be_bytes());
            }
            None => output.push(0),
        }

        Ok(output)
    }
}

impl Decode for QueueValue {
    fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 5 {
            return Err(MvccError::Encoding("QueueValue too short".to_string()));
        }

        let mut cursor = 0;

        // Decode value length
        let value_len = u32::from_be_bytes([
            bytes[cursor],
            bytes[cursor + 1],
            bytes[cursor + 2],
            bytes[cursor + 3],
        ]) as usize;
        cursor += 4;

        // Decode value
        if cursor + value_len > bytes.len() {
            return Err(MvccError::Encoding("Invalid value length".to_string()));
        }
        let value = proven_value::codec::decode_value(&bytes[cursor..cursor + value_len])
            .map_err(|e| MvccError::Encoding(e.to_string()))?;
        cursor += value_len;

        // Decode next pointer
        if cursor >= bytes.len() {
            return Err(MvccError::Encoding("Missing next pointer".to_string()));
        }
        let next = if bytes[cursor] == 1 {
            cursor += 1;
            if cursor + 8 > bytes.len() {
                return Err(MvccError::Encoding("Invalid next pointer".to_string()));
            }
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&bytes[cursor..cursor + 8]);
            cursor += 8;
            Some(u64::from_be_bytes(buf))
        } else {
            cursor += 1;
            None
        };

        // Decode prev pointer
        if cursor >= bytes.len() {
            return Err(MvccError::Encoding("Missing prev pointer".to_string()));
        }
        let prev = if bytes[cursor] == 1 {
            cursor += 1;
            if cursor + 8 > bytes.len() {
                return Err(MvccError::Encoding("Invalid prev pointer".to_string()));
            }
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&bytes[cursor..cursor + 8]);
            Some(u64::from_be_bytes(buf))
        } else {
            None
        };

        Ok(QueueValue { value, next, prev })
    }
}

impl MvccEntity for QueueEntity {
    type Key = QueueKey;
    type Value = QueueValue;
    type Delta = QueueDelta;

    fn entity_name() -> &'static str {
        "queue_new"
    }
}

/// Queue delta operations
#[derive(Clone, Debug, PartialEq)]
pub enum QueueDelta {
    /// Enqueue a new item
    Enqueue {
        /// Sequence number for this item
        seq: u64,
        /// The value being enqueued
        value: Value,
        /// If queue wasn't empty, this is the old tail that needs its next pointer updated
        old_tail_seq: Option<u64>,
        /// If there was an old tail, this was its value (needed for unapply)
        old_tail_value: Option<Value>,
        /// If there was an old tail, this was its prev pointer (needed for unapply)
        old_tail_prev: Option<u64>,
    },

    /// Dequeue an item from the head
    Dequeue {
        /// The sequence number being dequeued
        seq: u64,
        /// The value that was dequeued (for unapply)
        old_value: Value,
        /// What was the next pointer (becomes new head)
        old_next: Option<u64>,
        /// If there was a next item, we need to update its prev to None
        /// Store its old state for unapply
        next_item_seq: Option<u64>,
        next_item_value: Option<Value>,
        next_item_next: Option<u64>,
    },

    /// Update an existing item's pointers (used when items before/after change)
    UpdatePointers {
        seq: u64,
        value: Value,
        old_next: Option<u64>,
        new_next: Option<u64>,
        old_prev: Option<u64>,
        new_prev: Option<u64>,
    },
}

impl MvccDelta<QueueEntity> for QueueDelta {
    fn key(&self) -> QueueKey {
        match self {
            QueueDelta::Enqueue { seq, .. } => QueueKey(*seq),
            QueueDelta::Dequeue { seq, .. } => QueueKey(*seq),
            QueueDelta::UpdatePointers { seq, .. } => QueueKey(*seq),
        }
    }

    fn apply(&self, _current: Option<QueueValue>) -> Option<QueueValue> {
        match self {
            QueueDelta::Enqueue {
                value,
                old_tail_seq,
                ..
            } => {
                // New item with prev pointing to old tail, next = None (it's the new tail)
                Some(QueueValue {
                    value: value.clone(),
                    next: None,
                    prev: *old_tail_seq,
                })
            }

            QueueDelta::Dequeue { .. } => {
                // Item is removed
                None
            }

            QueueDelta::UpdatePointers {
                value,
                new_next,
                new_prev,
                ..
            } => Some(QueueValue {
                value: value.clone(),
                next: *new_next,
                prev: *new_prev,
            }),
        }
    }

    fn unapply(&self, _current: Option<QueueValue>) -> Option<QueueValue> {
        match self {
            QueueDelta::Enqueue { .. } => {
                // Item didn't exist before enqueue
                None
            }

            QueueDelta::Dequeue {
                old_value,
                old_next,
                ..
            } => {
                // Restore the dequeued item
                Some(QueueValue {
                    value: old_value.clone(),
                    next: *old_next,
                    prev: None, // Head item has no prev
                })
            }

            QueueDelta::UpdatePointers {
                value,
                old_next,
                old_prev,
                ..
            } => Some(QueueValue {
                value: value.clone(),
                next: *old_next,
                prev: *old_prev,
            }),
        }
    }

    fn merge(self, next: Self) -> Self {
        // For simplicity, last operation wins
        // A more sophisticated merge could combine operations on the same key
        next
    }
}

impl Encode for QueueDelta {
    fn encode(&self) -> Result<Vec<u8>> {
        let mut output = Vec::new();

        match self {
            QueueDelta::Enqueue {
                seq,
                value,
                old_tail_seq,
                old_tail_value,
                old_tail_prev,
            } => {
                output.push(0); // Enqueue tag
                output.extend_from_slice(&seq.to_be_bytes());

                let value_bytes = proven_value::codec::encode_value(value);
                output.extend_from_slice(&(value_bytes.len() as u32).to_be_bytes());
                output.extend_from_slice(&value_bytes);

                encode_option_u64(&mut output, old_tail_seq);

                match old_tail_value {
                    Some(v) => {
                        output.push(1);
                        let v_bytes = proven_value::codec::encode_value(v);
                        output.extend_from_slice(&(v_bytes.len() as u32).to_be_bytes());
                        output.extend_from_slice(&v_bytes);
                    }
                    None => output.push(0),
                }

                encode_option_u64(&mut output, old_tail_prev);
            }
            QueueDelta::Dequeue {
                seq,
                old_value,
                old_next,
                next_item_seq,
                next_item_value,
                next_item_next,
            } => {
                output.push(1); // Dequeue tag
                output.extend_from_slice(&seq.to_be_bytes());

                let value_bytes = proven_value::codec::encode_value(old_value);
                output.extend_from_slice(&(value_bytes.len() as u32).to_be_bytes());
                output.extend_from_slice(&value_bytes);

                encode_option_u64(&mut output, old_next);
                encode_option_u64(&mut output, next_item_seq);

                match next_item_value {
                    Some(v) => {
                        output.push(1);
                        let v_bytes = proven_value::codec::encode_value(v);
                        output.extend_from_slice(&(v_bytes.len() as u32).to_be_bytes());
                        output.extend_from_slice(&v_bytes);
                    }
                    None => output.push(0),
                }

                encode_option_u64(&mut output, next_item_next);
            }
            QueueDelta::UpdatePointers {
                seq,
                value,
                old_next,
                new_next,
                old_prev,
                new_prev,
            } => {
                output.push(2); // UpdatePointers tag
                output.extend_from_slice(&seq.to_be_bytes());

                let value_bytes = proven_value::codec::encode_value(value);
                output.extend_from_slice(&(value_bytes.len() as u32).to_be_bytes());
                output.extend_from_slice(&value_bytes);

                encode_option_u64(&mut output, old_next);
                encode_option_u64(&mut output, new_next);
                encode_option_u64(&mut output, old_prev);
                encode_option_u64(&mut output, new_prev);
            }
        }

        Ok(output)
    }
}

impl Decode for QueueDelta {
    fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.is_empty() {
            return Err(MvccError::Encoding("Empty delta".to_string()));
        }

        let tag = bytes[0];
        let mut cursor = 1;

        match tag {
            0 => {
                // Enqueue
                let seq = decode_u64(bytes, &mut cursor)?;
                let value = decode_value(bytes, &mut cursor)?;
                let old_tail_seq = decode_option_u64(bytes, &mut cursor)?;
                let old_tail_value = decode_option_value(bytes, &mut cursor)?;
                let old_tail_prev = decode_option_u64(bytes, &mut cursor)?;

                Ok(QueueDelta::Enqueue {
                    seq,
                    value,
                    old_tail_seq,
                    old_tail_value,
                    old_tail_prev,
                })
            }
            1 => {
                // Dequeue
                let seq = decode_u64(bytes, &mut cursor)?;
                let old_value = decode_value(bytes, &mut cursor)?;
                let old_next = decode_option_u64(bytes, &mut cursor)?;
                let next_item_seq = decode_option_u64(bytes, &mut cursor)?;
                let next_item_value = decode_option_value(bytes, &mut cursor)?;
                let next_item_next = decode_option_u64(bytes, &mut cursor)?;

                Ok(QueueDelta::Dequeue {
                    seq,
                    old_value,
                    old_next,
                    next_item_seq,
                    next_item_value,
                    next_item_next,
                })
            }
            2 => {
                // UpdatePointers
                let seq = decode_u64(bytes, &mut cursor)?;
                let value = decode_value(bytes, &mut cursor)?;
                let old_next = decode_option_u64(bytes, &mut cursor)?;
                let new_next = decode_option_u64(bytes, &mut cursor)?;
                let old_prev = decode_option_u64(bytes, &mut cursor)?;
                let new_prev = decode_option_u64(bytes, &mut cursor)?;

                Ok(QueueDelta::UpdatePointers {
                    seq,
                    value,
                    old_next,
                    new_next,
                    old_prev,
                    new_prev,
                })
            }
            _ => Err(MvccError::Encoding(format!("Invalid delta tag: {}", tag))),
        }
    }
}

// Helper functions for encoding/decoding
fn encode_option_u64(output: &mut Vec<u8>, opt: &Option<u64>) {
    match opt {
        Some(v) => {
            output.push(1);
            output.extend_from_slice(&v.to_be_bytes());
        }
        None => output.push(0),
    }
}

fn decode_u64(bytes: &[u8], cursor: &mut usize) -> Result<u64> {
    if *cursor + 8 > bytes.len() {
        return Err(MvccError::Encoding(
            "Insufficient bytes for u64".to_string(),
        ));
    }
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&bytes[*cursor..*cursor + 8]);
    *cursor += 8;
    Ok(u64::from_be_bytes(buf))
}

fn decode_option_u64(bytes: &[u8], cursor: &mut usize) -> Result<Option<u64>> {
    if *cursor >= bytes.len() {
        return Err(MvccError::Encoding(
            "Insufficient bytes for Option<u64>".to_string(),
        ));
    }
    if bytes[*cursor] == 1 {
        *cursor += 1;
        Ok(Some(decode_u64(bytes, cursor)?))
    } else {
        *cursor += 1;
        Ok(None)
    }
}

fn decode_value(bytes: &[u8], cursor: &mut usize) -> Result<Value> {
    if *cursor + 4 > bytes.len() {
        return Err(MvccError::Encoding(
            "Insufficient bytes for value length".to_string(),
        ));
    }
    let len = u32::from_be_bytes([
        bytes[*cursor],
        bytes[*cursor + 1],
        bytes[*cursor + 2],
        bytes[*cursor + 3],
    ]) as usize;
    *cursor += 4;

    if *cursor + len > bytes.len() {
        return Err(MvccError::Encoding(
            "Insufficient bytes for value".to_string(),
        ));
    }
    let value = proven_value::codec::decode_value(&bytes[*cursor..*cursor + len])
        .map_err(|e| MvccError::Encoding(e.to_string()))?;
    *cursor += len;
    Ok(value)
}

fn decode_option_value(bytes: &[u8], cursor: &mut usize) -> Result<Option<Value>> {
    if *cursor >= bytes.len() {
        return Err(MvccError::Encoding(
            "Insufficient bytes for Option<Value>".to_string(),
        ));
    }
    if bytes[*cursor] == 1 {
        *cursor += 1;
        Ok(Some(decode_value(bytes, cursor)?))
    } else {
        *cursor += 1;
        Ok(None)
    }
}

// ============================================================================
// Head Pointer Entity
// ============================================================================

/// Head pointer entity - stores pointer to first item in queue
pub struct HeadPointerEntity;

/// Single key for head pointer (there's only one head)
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HeadPointerKey;

impl Encode for HeadPointerKey {
    fn encode(&self) -> Result<Vec<u8>> {
        Ok(b"head".to_vec())
    }
}

impl Decode for HeadPointerKey {
    fn decode(_bytes: &[u8]) -> Result<Self> {
        Ok(HeadPointerKey)
    }
}

/// Head pointer value - just the sequence number of the head item
#[derive(Clone, Debug, PartialEq)]
pub struct HeadPointerValue {
    /// Sequence number of head item (None if queue is empty)
    pub seq: Option<u64>,
}

impl Encode for HeadPointerValue {
    fn encode(&self) -> Result<Vec<u8>> {
        let mut output = Vec::new();
        encode_option_u64(&mut output, &self.seq);
        Ok(output)
    }
}

impl Decode for HeadPointerValue {
    fn decode(bytes: &[u8]) -> Result<Self> {
        let mut cursor = 0;
        let seq = decode_option_u64(bytes, &mut cursor)?;
        Ok(HeadPointerValue { seq })
    }
}

impl MvccEntity for HeadPointerEntity {
    type Key = HeadPointerKey;
    type Value = HeadPointerValue;
    type Delta = HeadPointerDelta;

    fn entity_name() -> &'static str {
        "queue_new_head"
    }
}

/// Head pointer delta
#[derive(Clone, Debug, PartialEq)]
pub enum HeadPointerDelta {
    /// Set the head pointer
    Set {
        new_seq: Option<u64>,
        old_seq: Option<u64>,
    },
}

impl MvccDelta<HeadPointerEntity> for HeadPointerDelta {
    fn key(&self) -> HeadPointerKey {
        HeadPointerKey
    }

    fn apply(&self, _current: Option<HeadPointerValue>) -> Option<HeadPointerValue> {
        match self {
            HeadPointerDelta::Set { new_seq, .. } => Some(HeadPointerValue { seq: *new_seq }),
        }
    }

    fn unapply(&self, _current: Option<HeadPointerValue>) -> Option<HeadPointerValue> {
        match self {
            HeadPointerDelta::Set { old_seq, .. } => {
                if old_seq.is_some() {
                    Some(HeadPointerValue { seq: *old_seq })
                } else {
                    None // No previous head (queue was created)
                }
            }
        }
    }

    fn merge(self, next: Self) -> Self {
        next // Last operation wins
    }
}

impl Encode for HeadPointerDelta {
    fn encode(&self) -> Result<Vec<u8>> {
        let mut output = Vec::new();
        match self {
            HeadPointerDelta::Set { new_seq, old_seq } => {
                encode_option_u64(&mut output, new_seq);
                encode_option_u64(&mut output, old_seq);
            }
        }
        Ok(output)
    }
}

impl Decode for HeadPointerDelta {
    fn decode(bytes: &[u8]) -> Result<Self> {
        let mut cursor = 0;
        let new_seq = decode_option_u64(bytes, &mut cursor)?;
        let old_seq = decode_option_u64(bytes, &mut cursor)?;
        Ok(HeadPointerDelta::Set { new_seq, old_seq })
    }
}

// ============================================================================
// Tail Pointer Entity
// ============================================================================

/// Tail pointer entity - stores pointer to last item in queue
pub struct TailPointerEntity;

/// Single key for tail pointer (there's only one tail)
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TailPointerKey;

impl Encode for TailPointerKey {
    fn encode(&self) -> Result<Vec<u8>> {
        Ok(b"tail".to_vec())
    }
}

impl Decode for TailPointerKey {
    fn decode(_bytes: &[u8]) -> Result<Self> {
        Ok(TailPointerKey)
    }
}

/// Tail pointer value - just the sequence number of the tail item
#[derive(Clone, Debug, PartialEq)]
pub struct TailPointerValue {
    /// Sequence number of tail item (None if queue is empty)
    pub seq: Option<u64>,
}

impl Encode for TailPointerValue {
    fn encode(&self) -> Result<Vec<u8>> {
        let mut output = Vec::new();
        encode_option_u64(&mut output, &self.seq);
        Ok(output)
    }
}

impl Decode for TailPointerValue {
    fn decode(bytes: &[u8]) -> Result<Self> {
        let mut cursor = 0;
        let seq = decode_option_u64(bytes, &mut cursor)?;
        Ok(TailPointerValue { seq })
    }
}

impl MvccEntity for TailPointerEntity {
    type Key = TailPointerKey;
    type Value = TailPointerValue;
    type Delta = TailPointerDelta;

    fn entity_name() -> &'static str {
        "queue_new_tail"
    }
}

/// Tail pointer delta
#[derive(Clone, Debug, PartialEq)]
pub enum TailPointerDelta {
    /// Set the tail pointer
    Set {
        new_seq: Option<u64>,
        old_seq: Option<u64>,
    },
}

impl MvccDelta<TailPointerEntity> for TailPointerDelta {
    fn key(&self) -> TailPointerKey {
        TailPointerKey
    }

    fn apply(&self, _current: Option<TailPointerValue>) -> Option<TailPointerValue> {
        match self {
            TailPointerDelta::Set { new_seq, .. } => Some(TailPointerValue { seq: *new_seq }),
        }
    }

    fn unapply(&self, _current: Option<TailPointerValue>) -> Option<TailPointerValue> {
        match self {
            TailPointerDelta::Set { old_seq, .. } => {
                if old_seq.is_some() {
                    Some(TailPointerValue { seq: *old_seq })
                } else {
                    None // No previous tail (queue was created)
                }
            }
        }
    }

    fn merge(self, next: Self) -> Self {
        next // Last operation wins
    }
}

impl Encode for TailPointerDelta {
    fn encode(&self) -> Result<Vec<u8>> {
        let mut output = Vec::new();
        match self {
            TailPointerDelta::Set { new_seq, old_seq } => {
                encode_option_u64(&mut output, new_seq);
                encode_option_u64(&mut output, old_seq);
            }
        }
        Ok(output)
    }
}

impl Decode for TailPointerDelta {
    fn decode(bytes: &[u8]) -> Result<Self> {
        let mut cursor = 0;
        let new_seq = decode_option_u64(bytes, &mut cursor)?;
        let old_seq = decode_option_u64(bytes, &mut cursor)?;
        Ok(TailPointerDelta::Set { new_seq, old_seq })
    }
}
