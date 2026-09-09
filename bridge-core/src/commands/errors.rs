use crate::commands::fetch_source_offsets::errors::{
    FetchMetadataError, ImportOffsetsError, ReadOffsetsTopicError,
};
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

    #[error("failed to read offsets topic")]
    ReadOffsetsTopic(
        #[from]
        #[source]
        ReadOffsetsTopicError,
    ),
}

impl From<rdkafka::error::KafkaError> for FetchSourceOffsetsError {
    fn from(err: rdkafka::error::KafkaError) -> Self {
        FetchSourceOffsetsError::Kafka(err.into())
    }
}
