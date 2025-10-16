use serde::Deserialize;
use std::path::PathBuf;

pub mod client;
mod commands;
pub mod errors;
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

pub type Partition = i32;
pub type Topic = String;
pub type ConsumerGroup = String;
pub type Offset = i64;
pub type ConsumerGroupRecord = (ConsumerGroup, Topic, Partition, Offset);
pub type TransformationRecord = (Topic, Partition, Offset, Offset);
pub type ApplicationRecord = (Topic, Partition, Offset);

pub trait BridgeClient {
    type Error;

    fn fetch_source_offsets_from_cluster(&self) -> Result<OffsetSnapshot, Self::Error>;

    fn calculate_target_offsets(
        &self,
        legacy_offset_header: &str,
        offset_snapshot: OffsetSnapshot,
    ) -> impl Future<Output = Result<OffsetSnapshot, Self::Error>>;

    fn apply_target_offsets(
        &self,
        offset_snapshot: OffsetSnapshot,
        confirmation_request: &dyn Fn(&OffsetSnapshot) -> bool,
    ) -> impl Future<Output = Result<(), Self::Error>>;
}

impl From<BridgeConfig> for KafkaBridgeClient {
    fn from(config: BridgeConfig) -> Self {
        KafkaBridgeClient { config }
    }
}

pub struct KafkaBridgeClient {
    config: BridgeConfig,
}

pub struct BridgeConfig {
    bootstrap_server: String,

    optional_client_properties: Option<Vec<String>>,

    topics: Option<Vec<String>>,
}

impl BridgeConfig {
    pub fn new(bootstrap_server: String) -> Self {
        BridgeConfig {
            bootstrap_server,
            optional_client_properties: None,
            topics: None,
        }
    }

    pub fn set_optional_client_properties(
        mut self,
        optional_client_properties: Option<Vec<String>>,
    ) -> Self {
        self.optional_client_properties = optional_client_properties;
        self
    }

    pub fn set_topics(mut self, topics: Option<Vec<String>>) -> Self {
        self.topics = topics;
        self
    }

    pub fn bootstrap_server(&self) -> &str {
        self.bootstrap_server.as_str()
    }

    pub fn optional_client_properties(&self) -> &Option<Vec<String>> {
        &self.optional_client_properties
    }

    pub fn topics(&self) -> &Option<Vec<Topic>> {
        &self.topics
    }
}

#[derive(Debug, Clone)]
pub enum CsvInput {
    File(PathBuf),
    Stdin,
}
