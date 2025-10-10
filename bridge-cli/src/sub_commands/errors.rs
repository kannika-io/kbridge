use rdkafka::error::KafkaError;
use thiserror::Error;

use crate::fetch_offsets::client::{FetchMetadataError, ImportOffsetsError};

#[derive(Error, Debug)]
pub enum FetchSourceOffsetsError {
    #[error("Kafka Error. Reason: {0}")]
    KafkaError(#[from] KafkaError),

    #[error("Error while fetching metadata. Reason: {0}")]
    FetchMetadataError(#[from] FetchMetadataError),

    #[error("Error while importing offsets. Reason: {0}")]
    ImportOffsetserror(#[from] ImportOffsetsError),
}
