use std::time::Duration;

use log::info;
use rdkafka::{
    ClientConfig,
    config::RDKafkaLogLevel,
    consumer::{Consumer, StreamConsumer},
    metadata::Metadata,
};

use crate::transform::transformation_errors::TransformationError;

pub async fn setup_consumer_and_metadata(
    brokers: &str,
    topics: &[&str],
) -> Result<(StreamConsumer, Metadata), TransformationError> {
    let consumer = initialize_consumer(brokers)?;
    manage_topic_subscriptions(&consumer, topics)?;

    let metadata = consumer
        .fetch_metadata(None, Duration::from_secs(5))
        .map_err(|e| TransformationError::MetadataFetchFailed(e.to_string()))?;

    Ok((consumer, metadata))
}

// TODO: map<String> of consumer properties to initialize consumer
// TODO: move to bridge-kafka
pub fn initialize_consumer(brokers: &str) -> Result<StreamConsumer, TransformationError> {
    if brokers.is_empty() {
        return Err(TransformationError::InvalidInput(
            "Brokers string cannot be empty".to_string(),
        ));
    }

    let mut config = ClientConfig::new();

    info!("Initializing consumer with brokers: {brokers}");
    config
        .set("bootstrap.servers", brokers)
        .set("group.id", "test")
        .set("auto.offset.reset", "earliest")
        .set("enable.partition.eof", "false")
        .set("enable.auto.commit", "false")
        .set_log_level(RDKafkaLogLevel::Debug);

    config.create().map_err(|kafka_error| {
        TransformationError::ConsumerInitializationFailed(format!(
            "Failed to create consumer: {kafka_error}"
        ))
    })
}

pub fn manage_topic_subscriptions(
    consumer: &StreamConsumer,
    topics: &[&str],
) -> Result<(), TransformationError> {
    if topics.is_empty() {
        return Err(TransformationError::InvalidInput(
            "Topics list cannot be empty".to_string(),
        ));
    }

    let topic_selector = topics.join(",");
    info!("Subscribing to topics: {topic_selector}");

    consumer
        .subscribe(topics)
        .map_err(|kafka_error| TransformationError::SubscribingFailed {
            message: format!("Failed to subscribe to topics: {kafka_error}"),
            topic_selector,
        })
}
