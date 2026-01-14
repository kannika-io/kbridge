use crate::commands::fetch_source_offsets::errors::{FetchMetadataError, ImportOffsetsError};
use crate::kafka::KafkaError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum FetchSourceOffsetsError {
    #[error(transparent)]
    Kafka(#[from] KafkaError),

    #[error("failed to fetch metadata")]
    FetchMetadata(
        #[from]
        #[source]
        FetchMetadataError,
    ),

    #[error("failed to import offsets")]
    ImportOffsets(
        #[from]
        #[source]
        ImportOffsetsError,
    ),
}

impl From<rdkafka::error::KafkaError> for FetchSourceOffsetsError {
    fn from(err: rdkafka::error::KafkaError) -> Self {
        FetchSourceOffsetsError::Kafka(err.into())
    }
}
