//! Test utilities for building and producing records of a (restored)
//! `__consumer_offsets` topic.
//!
//! Tests describe records with the typed [`OffsetCommit`], [`Tombstone`] and
//! [`GroupMetadata`] structs; the raw `encode_*` functions remain available for
//! tests that deliberately exercise invalid or version-specific bytes.

use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;

/// Default key version used for offset commits; version 0 is identical except
/// for the version number itself.
const DEFAULT_KEY_VERSION: i16 = 1;

/// A committed offset for a consumer group on a partition of the original topic.
pub struct OffsetCommit<'a> {
    pub group: &'a str,
    pub topic: &'a str,
    pub partition: i32,
    pub offset: i64,
}

impl OffsetCommit<'_> {
    /// Encodes the commit with a specific key version (0 or 1)
    /// instead of the default.
    pub fn with_key_version(self, version: i16) -> ConsumerOffsetsRecord {
        ConsumerOffsetsRecord {
            key: encode_offset_commit_key(version, self.group, self.topic, self.partition),
            value: Some(encode_offset_commit_value(self.offset)),
        }
    }
}

/// A tombstone that removes the committed offset for a [group, topic, partition] key.
pub struct Tombstone<'a> {
    pub group: &'a str,
    pub topic: &'a str,
    pub partition: i32,
}

/// A group metadata record (key version 2), which offset readers must skip.
pub struct GroupMetadata<'a> {
    pub group: &'a str,
}

/// A record of the `__consumer_offsets` topic as raw key bytes and optional value bytes.
pub struct ConsumerOffsetsRecord {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

impl From<OffsetCommit<'_>> for ConsumerOffsetsRecord {
    fn from(commit: OffsetCommit<'_>) -> Self {
        commit.with_key_version(DEFAULT_KEY_VERSION)
    }
}

impl From<Tombstone<'_>> for ConsumerOffsetsRecord {
    fn from(tombstone: Tombstone<'_>) -> Self {
        ConsumerOffsetsRecord {
            key: encode_offset_commit_key(
                DEFAULT_KEY_VERSION,
                tombstone.group,
                tombstone.topic,
                tombstone.partition,
            ),
            value: None,
        }
    }
}

impl From<GroupMetadata<'_>> for ConsumerOffsetsRecord {
    fn from(metadata: GroupMetadata<'_>) -> Self {
        ConsumerOffsetsRecord {
            key: encode_group_metadata_key(metadata.group),
            // The value of a group metadata record is never decoded,
            // so placeholder bytes suffice
            value: Some(vec![0, 1]),
        }
    }
}

/// Produces records to a (restored) `__consumer_offsets` topic.
pub struct ConsumerOffsetsProducer {
    producer: FutureProducer,
    topic: String,
}

impl ConsumerOffsetsProducer {
    pub fn new(producer: FutureProducer, topic: impl Into<String>) -> Self {
        ConsumerOffsetsProducer {
            producer,
            topic: topic.into(),
        }
    }

    /// Sends a record to partition 0 of the offsets topic.
    pub async fn send(
        &self,
        record: impl Into<ConsumerOffsetsRecord>,
    ) -> Result<(), rdkafka::error::KafkaError> {
        self.send_to_partition(0, record).await
    }

    /// Sends a record to a specific partition of the offsets topic
    /// (not to be confused with the original topic partition inside the record).
    pub async fn send_to_partition(
        &self,
        partition: i32,
        record: impl Into<ConsumerOffsetsRecord>,
    ) -> Result<(), rdkafka::error::KafkaError> {
        let record = record.into();
        let mut future_record = FutureRecord::<Vec<u8>, Vec<u8>>::to(&self.topic)
            .partition(partition)
            .key(&record.key);
        if let Some(value) = &record.value {
            future_record = future_record.payload(value);
        }
        self.producer
            .send(future_record, Duration::from_secs(10))
            .await
            .map_err(|(err, _)| err)?;
        Ok(())
    }
}

/// Encodes an `OffsetCommitKey` (key version 0 or 1).
pub fn encode_offset_commit_key(version: i16, group: &str, topic: &str, partition: i32) -> Vec<u8> {
    let mut buf = version.to_be_bytes().to_vec();
    encode_string(&mut buf, group);
    encode_string(&mut buf, topic);
    buf.extend_from_slice(&partition.to_be_bytes());
    buf
}

/// Encodes a `GroupMetadataKey` (key version 2).
pub fn encode_group_metadata_key(group: &str) -> Vec<u8> {
    let mut buf = 2i16.to_be_bytes().to_vec();
    encode_string(&mut buf, group);
    buf
}

/// Encodes an `OffsetCommitValue` (value version 3):
/// offset, leader epoch, metadata and commit timestamp.
pub fn encode_offset_commit_value(offset: i64) -> Vec<u8> {
    let mut buf = 3i16.to_be_bytes().to_vec();
    buf.extend_from_slice(&offset.to_be_bytes());
    buf.extend_from_slice(&0i32.to_be_bytes());
    encode_string(&mut buf, "");
    buf.extend_from_slice(&0i64.to_be_bytes());
    buf
}

fn encode_string(buf: &mut Vec<u8>, value: &str) {
    buf.extend_from_slice(&(value.len() as i16).to_be_bytes());
    buf.extend_from_slice(value.as_bytes());
}
