use log::info;
use rdkafka::{
    ClientConfig,
    config::RDKafkaLogLevel,
    consumer::{Consumer, StreamConsumer},
};

use crate::transform::transformation_errors::TransformationError;

pub fn initialize_consumer(brokers: &str) -> Result<StreamConsumer, TransformationError> {
    let mut config = ClientConfig::new();

    info!("Initializing");
    config
        .set("bootstrap.servers", brokers)
        .set("group.id", "test")
        .set("auto.offset.reset", "earliest")
        .set("enable.partition.eof", "false")
        .set("enable.auto.commit", "false")
        .set_log_level(RDKafkaLogLevel::Debug);

    match config.create() {
        Ok(consumer) => Ok(consumer),
        Err(kafka_error) => Err(TransformationError::ConsumerInitializationFailed(
            kafka_error.to_string(),
        )),
    }
}

pub fn manage_topic_subscriptions(
    consumer: &StreamConsumer,
    topics: &[&str],
) -> Result<(), TransformationError> {
    info!("subscribing");
    match consumer.subscribe(topics) {
        Ok(_) => Ok(()),
        Err(kafka_error) => Err(TransformationError::SubscribingFailed {
            message: kafka_error.to_string(),
            topic_selector: topics.join(","),
        }),
    }
}
