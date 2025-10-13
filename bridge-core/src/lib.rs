use serde::Deserialize;
use std::{collections::HashSet, fmt::Display};

pub mod commands;
pub mod helpers;
pub mod kafka;
mod read_offsets;

#[derive(Debug, PartialEq, Eq, Clone, Deserialize)]
pub struct OffsetRecord {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub consumer_group: String,
}

pub type OffsetSnapshot = Vec<OffsetRecord>;

pub fn get_unique_topics_from_offset_snapshot(offset_snapshot: &OffsetSnapshot) -> Vec<&str> {
    offset_snapshot
        .iter()
        .map(|o| o.topic.as_str())
        // Filter out duplicates. source_offsets can contain duplicate topic names in case multiple
        // consumer groups are present
        .collect::<HashSet<_>>()
        .into_iter()
        .collect()
}

pub type Partition = i32;
pub type Topic = String;
pub type ConsumerGroup = String;
pub type Offset = i64;
pub type ConsumerGroupRecord = (ConsumerGroup, Topic, Partition, Offset);
pub type TransformationRecord = (Topic, Partition, Offset, Offset);
pub type ApplicationRecord = (Topic, Partition, Offset);

impl Display for OffsetRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{},{},{},{}",
            self.consumer_group, self.topic, self.partition, self.offset
        )
    }
}
