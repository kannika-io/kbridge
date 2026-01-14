use crate::kafka::KafkaError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ImportOffsetsError {
    #[error(transparent)]
    KafkaError(#[from] KafkaError),

    #[error("consumer {0} not found")]
    ConsumerNotFound(String),
}

impl From<rdkafka::error::KafkaError> for ImportOffsetsError {
    fn from(err: rdkafka::error::KafkaError) -> Self {
        ImportOffsetsError::KafkaError(err.into())
    }
}

#[derive(Debug, Error)]
pub enum FetchMetadataError {
    #[error(transparent)]
    KafkaError(#[from] KafkaError),
}

impl From<rdkafka::error::KafkaError> for FetchMetadataError {
    fn from(err: rdkafka::error::KafkaError) -> Self {
        FetchMetadataError::KafkaError(err.into())
    }
}
