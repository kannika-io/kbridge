use crate::OffsetRecord;

pub type OffsetSnapshot = Vec<OffsetRecord>;

pub type Partition = i32;
pub type Topic = String;
pub type ConsumerGroup = String;
pub type Offset = i64;
pub type ConsumerGroupRecord = (ConsumerGroup, Topic, Partition, Offset);
pub type TransformationRecord = (Topic, Partition, Offset, Offset);
pub type ApplicationRecord = (Topic, Partition, Offset);

pub type Properties = std::collections::HashMap<String, String>;
