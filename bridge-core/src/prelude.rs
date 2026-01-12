pub type PartitionNumber = i32;
pub type Topic = String;
pub type TopicName = String;
pub type ConsumerGroup = String;
pub type Offset = i64;
// TODO is there a difference between OffsetRecord and ConsumerGroupRecord?
pub type ConsumerGroupRecord = (ConsumerGroup, Topic, PartitionNumber, Offset);
pub type TransformationRecord = (Topic, PartitionNumber, Offset, Offset);
pub type ApplicationRecord = (Topic, PartitionNumber, Offset);
pub type Properties = std::collections::HashMap<String, String>;

pub use crate::client::BridgeClient;
pub use crate::client::kafka::{KafkaBridgeClient, KafkaBridgeConfig};
pub use crate::errors::BridgeError;
pub use crate::snapshot::{OffsetRecord, OffsetSnapshot};

pub use crate::partition::Message;

pub use crate::kafka::KafkaError;
