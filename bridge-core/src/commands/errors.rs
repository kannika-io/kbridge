use crate::commands::fetch_source_offsets::errors::{FetchMetadataError, ImportOffsetsError};
use rdkafka::error::KafkaError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum FetchSourceOffsetsError {
    #[error("Kafka Error. Reason: {0}")]
    KafkaError(#[from] KafkaError),

    #[error("Error while fetching metadata. Reason: {0}")]
    FetchMetadataError(#[from] FetchMetadataError),

    #[error("Error while importing offsets. Reason: {0}")]
    ImportOffsetsError(#[from] ImportOffsetsError),
}
