use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;

use crate::kafka::properties::KafkaConsumerProperties;
use crate::prelude::*;

use crate::transform::ConsumerGroupMigrator;
use crate::{
    commands::{apply_target_offsets, fetch_source_offsets},
    kafka::source::RecordStreamConsumer,
};

#[derive(Debug, Clone)]
pub struct KafkaBridgeClient {
    config: KafkaBridgeConfig,
}

#[derive(Debug, Clone)]
pub struct KafkaBridgeConfig {
    properties: HashMap<String, String>,
}

impl AsRef<KafkaBridgeConfig> for KafkaBridgeConfig {
    fn as_ref(&self) -> &KafkaBridgeConfig {
        self
    }
}

#[async_trait]
impl BridgeClient for KafkaBridgeClient {
    type Error = BridgeError;

    async fn fetch_source_offsets_from_cluster(
        &self,
        topics: impl IntoIterator<Item = String> + Send,
        client_timeout: Duration,
    ) -> Result<OffsetSnapshot, BridgeError> {
        fetch_source_offsets::execute(
            self.config.bootstrap_server(),
            self.config.properties(),
            topics,
            client_timeout,
        )
        .map_err(|err| err.into())
    }

    async fn fetch_source_offsets_from_offsets_topic(
        &self,
        offsets_topic: impl Into<String> + Send,
        topics: impl IntoIterator<Item = String> + Send,
        client_timeout: Duration,
        progress: impl FnMut(u64, u64) + Send,
    ) -> Result<OffsetSnapshot, BridgeError> {
        fetch_source_offsets::execute_from_offsets_topic(
            self.config.bootstrap_server(),
            self.config.properties(),
            offsets_topic.into(),
            topics,
            client_timeout,
            progress,
        )
        .map_err(|err| err.into())
    }

    async fn apply_target_offsets(
        &self,
        topics: impl IntoIterator<Item = String> + Send,
        offset_snapshot: OffsetSnapshot,
        dry_run: bool,
    ) -> Result<(), BridgeError> {
        apply_target_offsets::execute(
            &self.config.bootstrap_server(),
            self.config.properties(),
            topics,
            offset_snapshot,
            dry_run,
        )
        .await
        .map_err(|err| err.into())
    }

    async fn calculate_target_offsets(
        &self,
        offset_header: impl Into<String> + Send,
        topics: impl IntoIterator<Item = String> + Send,
        snapshot: OffsetSnapshot,
    ) -> Result<OffsetSnapshot, Self::Error> {
        let snapshot = {
            let topics: Vec<String> = topics.into_iter().collect();
            snapshot.filter_by_topics(&topics)
        };

        let props: KafkaConsumerProperties = self.config.as_ref().into();
        let (consumer, task) = RecordStreamConsumer::new(props)?;
        tokio::spawn(task);

        let migrator = ConsumerGroupMigrator::new(consumer).search_offset_in_header(offset_header);

        let result = migrator.migrate(&snapshot).await?;

        Ok(result)
    }
}

impl From<KafkaBridgeConfig> for KafkaBridgeClient {
    fn from(config: KafkaBridgeConfig) -> Self {
        KafkaBridgeClient { config }
    }
}

impl KafkaBridgeConfig {
    pub fn new(bootstrap_server: impl Into<String>) -> Self {
        let mut properties = HashMap::new();
        properties.insert("bootstrap.servers".to_string(), bootstrap_server.into());
        KafkaBridgeConfig { properties }
    }

    pub fn set_properties(mut self, properties: HashMap<String, String>) -> Self {
        self.properties.extend(properties);
        self
    }

    pub fn bootstrap_server(&self) -> String {
        self.properties
            .get("bootstrap.servers")
            .cloned()
            .unwrap_or("localhost:9092".to_string())
    }

    pub fn properties(&self) -> &HashMap<String, String> {
        &self.properties
    }
}

impl From<&KafkaBridgeConfig> for KafkaConsumerProperties {
    fn from(config: &KafkaBridgeConfig) -> Self {
        KafkaConsumerProperties::from_iter(config.properties().clone())
    }
}
