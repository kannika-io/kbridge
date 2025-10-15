use rdkafka::error::KafkaError;
use std::io;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ApplyOffsetsError {
    #[error("Kafka error occurred. Reason: {0}")]
    KafkaError(#[from] KafkaError),
    #[error("IO error occurred. Reason: {0}")]
    IoError(#[from] io::Error),

    #[error("The operation was cancelled")]
    Cancelled,
}
