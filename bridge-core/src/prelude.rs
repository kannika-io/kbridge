pub type Partition = i32;
pub type Topic = String;
pub type ConsumerGroup = String;
pub type Offset = i64;
// TODO is there a difference between OffsetRecord and ConsumerGroupRecord?
pub type ConsumerGroupRecord = (ConsumerGroup, Topic, Partition, Offset);
pub type TransformationRecord = (Topic, Partition, Offset, Offset);
pub type ApplicationRecord = (Topic, Partition, Offset);
pub type Properties = std::collections::HashMap<String, String>;

pub use crate::BridgeClient;
pub use crate::snapshot::{OffsetRecord, OffsetSnapshot};
