use crate::prelude::*;

use std::collections::HashMap;

use futures::Stream;

// Represents a single partition that can be consumed from.
// This trait abstracts over different consumer implementations.
pub trait Partition {
    type Error;
    type Message: Message;
    type Stream: Stream<Item = Result<Self::Message, Self::Error>> + Unpin;

    // Creates a stream for consuming messages from the partition.
    // This may involve assigning the partition to a consumer.
    fn stream(
        &mut self,
    ) -> impl std::future::Future<Output = Result<Self::Stream, Self::Error>> + Send;
}

pub trait SeekablePartition: Partition {
    /// Seek to the specified offset.
    /// Any open streams will be affected by this seek.
    ///
    /// # Arguments
    ///
    /// * `offset` - The offset to seek to.
    fn seek_from_offset(&mut self, offset: Offset)
    -> impl Future<Output = Result<(), Self::Error>>;
}

// A message from a partition.
pub trait Message {
    fn partition(&self) -> i32;
    fn offset(&self) -> i64;
    fn timestamp(&self) -> i64;
    fn headers(&self) -> HashMap<String, Vec<u8>>;
    fn header(&self, key: impl Into<String>) -> Option<Vec<u8>>;
    // Additional methods for key and payload can be added later if needed.
    // At the moment, we focus on metadata only, so we avoid unnecessary data copying.
    // fn key(&self) -> Option<Vec<u8>>;
    // fn payload(&self) -> Option<Vec[u8]>;
}

// A simple struct that holds message metadata.
#[derive(Debug, PartialEq, Eq)]
pub struct PartitionRecord {
    partition: i32,
    offset: i64,
    timestamp: i64,
    headers: HashMap<String, Vec<u8>>,
}

impl Message for PartitionRecord {
    fn partition(&self) -> i32 {
        self.partition
    }

    fn offset(&self) -> i64 {
        self.offset
    }

    fn timestamp(&self) -> i64 {
        self.timestamp
    }

    fn headers(&self) -> HashMap<String, Vec<u8>> {
        self.headers.clone()
    }

    fn header(&self, key: impl Into<String>) -> Option<Vec<u8>> {
        let key = key.into();
        self.headers.get(&key).cloned()
    }
}

impl PartitionRecord {
    pub fn set_header(&mut self, key: impl Into<String>, value: Vec<u8>) {
        self.headers.insert(key.into(), value);
    }

    pub fn set_timestamp(&mut self, timestamp: i64) {
        self.timestamp = timestamp;
    }

    pub fn set_offset(&mut self, offset: i64) {
        self.offset = offset;
    }

    pub fn set_partition(&mut self, partition: i32) {
        self.partition = partition;
    }
}

impl<M> From<&M> for PartitionRecord
where
    M: Message,
{
    fn from(msg: &M) -> Self {
        let partition = msg.partition();
        let offset = msg.offset();
        let timestamp = msg.timestamp();
        let headers = msg.headers();

        PartitionRecord {
            partition,
            offset,
            timestamp,
            headers,
        }
    }
}
