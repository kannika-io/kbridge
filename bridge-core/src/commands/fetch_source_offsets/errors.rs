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

#[derive(Error, Debug)]
pub enum ImportError {
    #[error("Resource to import not found. Reason: {0}")]
    ResourceNotFound(String),
    #[error("Errors during parsing of input. Reason(s): {0:?}")]
    ParseErrors(ValidationErrors),
}

type ValidationErrors = Vec<String>;
