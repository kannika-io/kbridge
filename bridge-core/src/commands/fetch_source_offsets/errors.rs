use rdkafka::error::KafkaError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ImportOffsetsError {
    #[error("Kafka Error. Reason: {0}")]
    KafkaError(#[from] KafkaError),

    #[error("Consumer {0} not found.")]
    ConsumerNotFound(String),
}

#[derive(Debug, Error)]
pub enum FetchMetadataError {
    #[error("Kafka Error. Reason: {0}")]
    KafkaError(#[from] KafkaError),
}
