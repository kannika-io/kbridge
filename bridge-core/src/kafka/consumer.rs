use std::time::Duration;

use rdkafka::{
    ClientConfig,
    consumer::{Consumer, StreamConsumer},
    metadata::Metadata,
};

use crate::KafkaError;

pub async fn setup_consumer_and_metadata(
    transformer_consumer_config: &mut ClientConfig,
) -> Result<(StreamConsumer, Metadata), KafkaError> {
    let consumer = initialize_consumer(transformer_consumer_config)?;

    let metadata = consumer
        .fetch_metadata(None, Duration::from_secs(5))
        .map_err(KafkaError::MetadataFetchFailed)?;

    Ok((consumer, metadata))
}

pub fn initialize_consumer(
    transformer_consumer_config: &mut ClientConfig,
) -> Result<StreamConsumer, KafkaError> {
    transformer_consumer_config
        .create()
        .map_err(KafkaError::ClientCreationError)
}
