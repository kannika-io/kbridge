use std::time::Duration;

use crate::commands::calculate_target_offsets::errors::TransformationError;
use log::info;
use rdkafka::{
    ClientConfig,
    consumer::{Consumer, StreamConsumer},
    metadata::Metadata,
};

pub async fn setup_consumer_and_metadata(
    topics: &[&str],
    transformer_consumer_config: &mut ClientConfig,
) -> Result<(StreamConsumer, Metadata), TransformationError> {
    let consumer = initialize_consumer(transformer_consumer_config)?;
    manage_topic_subscriptions(&consumer, topics)?;

    let metadata = consumer
        .fetch_metadata(None, Duration::from_secs(5))
        .map_err(|e| TransformationError::MetadataFetchFailed(e.to_string()))?;

    Ok((consumer, metadata))
}

pub fn initialize_consumer(
    transformer_consumer_config: &mut ClientConfig,
) -> Result<StreamConsumer, TransformationError> {
    transformer_consumer_config.create().map_err(|kafka_error| {
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
