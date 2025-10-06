use log::info;
use rdkafka::{
    ClientConfig,
    config::RDKafkaLogLevel,
    consumer::{Consumer, StreamConsumer},
};

use crate::transform::transformation_errors::TransformationError;

pub fn initialize_consumer(brokers: &str) -> Result<StreamConsumer, TransformationError> {
    if brokers.is_empty() {
        return Err(TransformationError::InvalidInput(
            "Brokers string cannot be empty".to_string(),
        ));
    }

    let mut config = ClientConfig::new();

    info!("Initializing consumer with brokers: {}", brokers);
    config
        .set("bootstrap.servers", brokers)
        .set("group.id", "test")
        .set("auto.offset.reset", "earliest")
        .set("enable.partition.eof", "false")
        .set("enable.auto.commit", "false")
        .set_log_level(RDKafkaLogLevel::Debug);

    config.create().map_err(|kafka_error| {
        TransformationError::ConsumerInitializationFailed(format!(
            "Failed to create consumer: {}",
            kafka_error
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
    info!("Subscribing to topics: {}", topic_selector);
    
    consumer.subscribe(topics).map_err(|kafka_error| {
        TransformationError::SubscribingFailed {
            message: format!("Failed to subscribe to topics: {}", kafka_error),
            topic_selector,
        }
    })
}
