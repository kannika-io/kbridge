pub mod export;
pub mod import;
pub mod transform;

#[derive(Debug, PartialEq, Eq)]
pub struct OffsetRecord {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub consumer_group: String,
}

pub type OffsetSnapshot = Vec<OffsetRecord>;

pub type Partition = i32;
pub type Topic = String;
pub type ConsumerGroup = String;
pub type Offset = i64;
pub type ConsumerGroupRecord = (ConsumerGroup, Topic, Partition, Offset);
pub type TransformationRecord = (Topic, Partition, Offset, Offset);
