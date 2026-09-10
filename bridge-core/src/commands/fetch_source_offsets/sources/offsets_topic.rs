//! Reads committed consumer group offsets from a restored copy of the internal
//! `__consumer_offsets` topic by natively decoding the `OffsetCommitKey` and
//! `OffsetCommitValue` wire format.

use crate::commands::fetch_source_offsets::errors::ReadOffsetsTopicError;
use crate::prelude::{ConsumerGroup, Offset, PartitionNumber, TopicName};
use crate::snapshot::{OffsetRecord, OffsetSnapshot};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::util::Timeout;
use rdkafka::{Message, TopicPartitionList};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::{Duration, Instant};
use tracing::{info, trace, warn};

/// A decoded key of a record on the `__consumer_offsets` topic.
#[derive(Debug, PartialEq, Eq)]
pub enum OffsetCommitKey {
    /// Key versions 0-1: a committed offset for a [group, topic, partition] combination.
    Offset {
        group: ConsumerGroup,
        topic: TopicName,
        partition: PartitionNumber,
    },
    /// Key versions 2 and above carry group metadata instead of offsets.
    GroupMetadata,
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum DecodeError {
    #[error("unexpected end of data at byte {0}")]
    UnexpectedEndOfData(usize),

    #[error("invalid string length {0}")]
    InvalidStringLength(i16),

    #[error("string is not valid UTF-8")]
    InvalidUtf8,

    #[error("unsupported version {0}")]
    UnsupportedVersion(i16),
}

struct Decoder<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Decoder<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Decoder { buf, pos: 0 }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], DecodeError> {
        let end = self
            .pos
            .checked_add(len)
            .ok_or(DecodeError::UnexpectedEndOfData(self.pos))?;
        let bytes = self
            .buf
            .get(self.pos..end)
            .ok_or(DecodeError::UnexpectedEndOfData(self.pos))?;
        self.pos = end;
        Ok(bytes)
    }

    fn read_i16(&mut self) -> Result<i16, DecodeError> {
        Ok(i16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }

    fn read_i32(&mut self) -> Result<i32, DecodeError> {
        Ok(i32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn read_i64(&mut self) -> Result<i64, DecodeError> {
        Ok(i64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn read_string(&mut self) -> Result<String, DecodeError> {
        let len = self.read_i16()?;
        if len < 0 {
            return Err(DecodeError::InvalidStringLength(len));
        }
        let bytes = self.take(len as usize)?;
        String::from_utf8(bytes.to_vec()).map_err(|_| DecodeError::InvalidUtf8)
    }
}

/// Decodes the key of a record on the `__consumer_offsets` topic.
pub fn decode_offset_commit_key(buf: &[u8]) -> Result<OffsetCommitKey, DecodeError> {
    let mut decoder = Decoder::new(buf);
    match decoder.read_i16()? {
        0 | 1 => Ok(OffsetCommitKey::Offset {
            group: decoder.read_string()?,
            topic: decoder.read_string()?,
            partition: decoder.read_i32()?,
        }),
        version if version >= 2 => Ok(OffsetCommitKey::GroupMetadata),
        version => Err(DecodeError::UnsupportedVersion(version)),
    }
}

/// Decodes the committed offset from an `OffsetCommitValue`.
/// The offset is the first field after the version in all known versions (0-4).
pub fn decode_offset_commit_value(buf: &[u8]) -> Result<Offset, DecodeError> {
    let mut decoder = Decoder::new(buf);
    let version = decoder.read_i16()?;
    if !(0..=4).contains(&version) {
        return Err(DecodeError::UnsupportedVersion(version));
    }
    decoder.read_i64()
}

/// Accumulates offset commit records, keeping only the last commit per
/// [group, topic, partition] key. A tombstone (null value) drops the key.
pub struct OffsetAccumulator {
    topics: Vec<TopicName>,
    offsets: BTreeMap<(ConsumerGroup, TopicName, PartitionNumber), Offset>,
}

impl OffsetAccumulator {
    /// Creates an accumulator that only keeps offsets for the given topics.
    /// If the list is empty, all topics are kept.
    pub fn new(topics: impl IntoIterator<Item = TopicName>) -> Self {
        OffsetAccumulator {
            topics: topics.into_iter().collect(),
            offsets: BTreeMap::new(),
        }
    }

    /// Applies a single record from the offsets topic.
    /// Group metadata records and records that fail to decode are skipped.
    pub fn apply(&mut self, key: Option<&[u8]>, value: Option<&[u8]>) {
        let Some(key) = key else {
            warn!("Skipping record without a key on the offsets topic");
            return;
        };

        let (group, topic, partition) = match decode_offset_commit_key(key) {
            Ok(OffsetCommitKey::Offset {
                group,
                topic,
                partition,
            }) => (group, topic, partition),
            Ok(OffsetCommitKey::GroupMetadata) => return,
            Err(err) => {
                warn!("Skipping record with undecodable key: {err}");
                return;
            }
        };

        if !self.topics.is_empty() && !self.topics.contains(&topic) {
            return;
        }

        match value {
            None => {
                // A tombstone removes the committed offset for the key
                self.offsets.remove(&(group, topic, partition));
            }
            Some(value) => match decode_offset_commit_value(value) {
                Ok(offset) => {
                    self.offsets.insert((group, topic, partition), offset);
                }
                Err(err) => {
                    warn!("Skipping record with undecodable value: {err}");
                }
            },
        }
    }

    /// Returns the number of accumulated [group, topic, partition] keys.
    pub fn len(&self) -> usize {
        self.offsets.len()
    }

    /// Returns the accumulated offsets, ordered by consumer group, topic and partition.
    pub fn into_snapshot(self) -> OffsetSnapshot {
        self.offsets
            .into_iter()
            .map(
                |((consumer_group, topic, partition), offset)| OffsetRecord {
                    consumer_group,
                    topic,
                    partition,
                    offset,
                },
            )
            .collect()
    }
}

/// Reads all records of `offsets_topic` up to the log-end offsets captured at start,
/// and returns the last committed offset per [group, topic, partition].
///
/// `progress` is called as `progress(read, total)` after each consumed record,
/// where `total` is an upper bound derived from the watermarks (compaction gaps
/// and transaction markers may keep `read` below it).
pub fn read_offsets_from_topic(
    consumer: &BaseConsumer,
    offsets_topic: &str,
    topics: impl IntoIterator<Item = TopicName>,
    client_timeout: Duration,
    mut progress: impl FnMut(u64, u64),
) -> Result<OffsetSnapshot, ReadOffsetsTopicError> {
    let metadata = consumer.fetch_metadata(Some(offsets_topic), Timeout::After(client_timeout))?;

    let partitions: Vec<PartitionNumber> = metadata
        .topics()
        .iter()
        .find(|topic| topic.name() == offsets_topic)
        .map(|topic| topic.partitions().iter().map(|p| p.id()).collect())
        .unwrap_or_default();

    if partitions.is_empty() {
        return Err(ReadOffsetsTopicError::TopicNotFound(
            offsets_topic.to_string(),
        ));
    }

    // Capture the log-end offsets up front so the end condition is deterministic
    let mut assignment = TopicPartitionList::new();
    let mut end_offsets: HashMap<PartitionNumber, Offset> = HashMap::new();
    let mut total: u64 = 0;
    for partition in partitions {
        let (low, high) =
            consumer.fetch_watermarks(offsets_topic, partition, Timeout::After(client_timeout))?;
        trace!("Watermarks for partition {partition}: {low}..{high}");
        if low >= high {
            // Empty partition, nothing to read
            continue;
        }
        assignment.add_partition_offset(offsets_topic, partition, rdkafka::Offset::Beginning)?;
        end_offsets.insert(partition, high);
        total += (high - low) as u64;
    }

    let mut accumulator = OffsetAccumulator::new(topics);
    let mut remaining: HashSet<PartitionNumber> = end_offsets.keys().copied().collect();

    if remaining.is_empty() {
        return Ok(accumulator.into_snapshot());
    }

    consumer.assign(&assignment)?;

    let started = Instant::now();
    let mut read: u64 = 0;

    while !remaining.is_empty() {
        match consumer.poll(Timeout::After(client_timeout)) {
            None => {
                return Err(ReadOffsetsTopicError::ReadTimeout(
                    offsets_topic.to_string(),
                ));
            }
            Some(Err(rdkafka::error::KafkaError::PartitionEOF(partition))) => {
                // The tail of a compacted partition may hold no consumable records
                // (e.g. transaction markers), so EOF also completes a partition
                remaining.remove(&partition);
            }
            Some(Err(err)) => return Err(err.into()),
            Some(Ok(message)) => {
                read += 1;
                progress(read, total);
                let partition = message.partition();
                if !remaining.contains(&partition) {
                    // Already past the log-end offset captured at start
                    continue;
                }
                accumulator.apply(message.key(), message.payload());
                if message.offset() + 1 >= end_offsets[&partition] {
                    remaining.remove(&partition);
                }
            }
        }
    }

    info!(
        "Read {read} records from '{offsets_topic}' in {:.2?}, accumulated {} unique offsets",
        started.elapsed(),
        accumulator.len(),
    );

    Ok(accumulator.into_snapshot())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::consumer_offsets::{
        ConsumerOffsetsRecord, GroupMetadata, OffsetCommit, Tombstone, encode_group_metadata_key,
        encode_offset_commit_key, encode_offset_commit_value,
    };

    #[test]
    fn decode_key_version_0() {
        let key = encode_offset_commit_key(0, "group-a", "orders", 3);
        assert_eq!(
            decode_offset_commit_key(&key).unwrap(),
            OffsetCommitKey::Offset {
                group: "group-a".to_string(),
                topic: "orders".to_string(),
                partition: 3,
            }
        );
    }

    #[test]
    fn decode_key_version_1() {
        let key = encode_offset_commit_key(1, "group-b", "payments", 0);
        assert_eq!(
            decode_offset_commit_key(&key).unwrap(),
            OffsetCommitKey::Offset {
                group: "group-b".to_string(),
                topic: "payments".to_string(),
                partition: 0,
            }
        );
    }

    #[test]
    fn decode_key_version_2_is_group_metadata() {
        let key = encode_group_metadata_key("group-a");
        assert_eq!(
            decode_offset_commit_key(&key).unwrap(),
            OffsetCommitKey::GroupMetadata
        );
    }

    #[test]
    fn decode_key_negative_version_is_unsupported() {
        let key = (-1i16).to_be_bytes().to_vec();
        assert_eq!(
            decode_offset_commit_key(&key),
            Err(DecodeError::UnsupportedVersion(-1))
        );
    }

    #[test]
    fn decode_key_truncated_returns_error() {
        let key = encode_offset_commit_key(1, "group-a", "orders", 0);
        assert_eq!(
            decode_offset_commit_key(&key[..key.len() - 2]),
            Err(DecodeError::UnexpectedEndOfData(key.len() - 4))
        );
    }

    #[test]
    fn decode_key_empty_returns_error() {
        assert_eq!(
            decode_offset_commit_key(&[]),
            Err(DecodeError::UnexpectedEndOfData(0))
        );
    }

    #[test]
    fn decode_key_invalid_utf8_returns_error() {
        let mut key = 0i16.to_be_bytes().to_vec();
        key.extend_from_slice(&2i16.to_be_bytes());
        key.extend_from_slice(&[0xff, 0xfe]);
        assert_eq!(
            decode_offset_commit_key(&key),
            Err(DecodeError::InvalidUtf8)
        );
    }

    #[test]
    fn decode_value_reads_offset_and_ignores_trailing_fields() {
        let value = encode_offset_commit_value(1042);
        assert_eq!(decode_offset_commit_value(&value).unwrap(), 1042);
    }

    #[test]
    fn decode_value_version_0_minimal() {
        let mut value = 0i16.to_be_bytes().to_vec();
        value.extend_from_slice(&77i64.to_be_bytes());
        assert_eq!(decode_offset_commit_value(&value).unwrap(), 77);
    }

    #[test]
    fn decode_value_unsupported_version_returns_error() {
        let mut value = 5i16.to_be_bytes().to_vec();
        value.extend_from_slice(&77i64.to_be_bytes());
        assert_eq!(
            decode_offset_commit_value(&value),
            Err(DecodeError::UnsupportedVersion(5))
        );
    }

    #[test]
    fn decode_value_truncated_returns_error() {
        let value = 3i16.to_be_bytes().to_vec();
        assert_eq!(
            decode_offset_commit_value(&value),
            Err(DecodeError::UnexpectedEndOfData(2))
        );
    }

    fn apply(accumulator: &mut OffsetAccumulator, record: impl Into<ConsumerOffsetsRecord>) {
        let record = record.into();
        accumulator.apply(Some(&record.key), record.value.as_deref());
    }

    #[test]
    fn accumulator_keeps_last_commit_per_key() {
        let mut accumulator = OffsetAccumulator::new(vec![]);
        apply(
            &mut accumulator,
            OffsetCommit {
                group: "group-a",
                topic: "orders",
                partition: 0,
                offset: 10,
            },
        );
        apply(
            &mut accumulator,
            OffsetCommit {
                group: "group-a",
                topic: "orders",
                partition: 0,
                offset: 25,
            },
        );
        apply(
            &mut accumulator,
            OffsetCommit {
                group: "group-a",
                topic: "orders",
                partition: 1,
                offset: 5,
            },
        );

        let expected: OffsetSnapshot = "group-a,orders,0,25\ngroup-a,orders,1,5\n".parse().unwrap();
        crate::test::snapshot::assert_eq(accumulator.into_snapshot(), expected);
    }

    #[test]
    fn accumulator_tombstone_drops_key() {
        let mut accumulator = OffsetAccumulator::new(vec![]);
        apply(
            &mut accumulator,
            OffsetCommit {
                group: "group-a",
                topic: "orders",
                partition: 0,
                offset: 10,
            },
        );
        apply(
            &mut accumulator,
            OffsetCommit {
                group: "group-b",
                topic: "orders",
                partition: 0,
                offset: 20,
            },
        );
        apply(
            &mut accumulator,
            Tombstone {
                group: "group-a",
                topic: "orders",
                partition: 0,
            },
        );

        let expected: OffsetSnapshot = "group-b,orders,0,20\n".parse().unwrap();
        crate::test::snapshot::assert_eq(accumulator.into_snapshot(), expected);
    }

    #[test]
    fn accumulator_commit_after_tombstone_restores_key() {
        let mut accumulator = OffsetAccumulator::new(vec![]);
        apply(
            &mut accumulator,
            OffsetCommit {
                group: "group-a",
                topic: "orders",
                partition: 0,
                offset: 10,
            },
        );
        apply(
            &mut accumulator,
            Tombstone {
                group: "group-a",
                topic: "orders",
                partition: 0,
            },
        );
        apply(
            &mut accumulator,
            OffsetCommit {
                group: "group-a",
                topic: "orders",
                partition: 0,
                offset: 30,
            },
        );

        let expected: OffsetSnapshot = "group-a,orders,0,30\n".parse().unwrap();
        crate::test::snapshot::assert_eq(accumulator.into_snapshot(), expected);
    }

    #[test]
    fn accumulator_skips_group_metadata_records() {
        let mut accumulator = OffsetAccumulator::new(vec![]);
        apply(&mut accumulator, GroupMetadata { group: "group-a" });
        apply(
            &mut accumulator,
            OffsetCommit {
                group: "group-a",
                topic: "orders",
                partition: 0,
                offset: 10,
            },
        );

        let expected: OffsetSnapshot = "group-a,orders,0,10\n".parse().unwrap();
        crate::test::snapshot::assert_eq(accumulator.into_snapshot(), expected);
    }

    #[test]
    fn accumulator_filters_on_decoded_topic() {
        let mut accumulator = OffsetAccumulator::new(vec!["orders".to_string()]);
        apply(
            &mut accumulator,
            OffsetCommit {
                group: "group-a",
                topic: "orders",
                partition: 0,
                offset: 10,
            },
        );
        apply(
            &mut accumulator,
            OffsetCommit {
                group: "group-a",
                topic: "payments",
                partition: 0,
                offset: 20,
            },
        );

        let expected: OffsetSnapshot = "group-a,orders,0,10\n".parse().unwrap();
        crate::test::snapshot::assert_eq(accumulator.into_snapshot(), expected);
    }

    #[test]
    fn accumulator_skips_undecodable_records() {
        let mut accumulator = OffsetAccumulator::new(vec![]);
        accumulator.apply(Some(&[0xff]), Some(&encode_offset_commit_value(10)));
        accumulator.apply(
            Some(&encode_offset_commit_key(1, "group-a", "orders", 0)),
            Some(&[0xff]),
        );
        accumulator.apply(None, Some(&encode_offset_commit_value(10)));
        apply(
            &mut accumulator,
            OffsetCommit {
                group: "group-a",
                topic: "orders",
                partition: 0,
                offset: 42,
            },
        );

        let expected: OffsetSnapshot = "group-a,orders,0,42\n".parse().unwrap();
        crate::test::snapshot::assert_eq(accumulator.into_snapshot(), expected);
    }
}
