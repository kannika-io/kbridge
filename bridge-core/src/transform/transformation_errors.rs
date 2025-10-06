use std::fmt::Display;

use rdkafka::error::KafkaError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum TransformationError {
    #[error("Consumer initialization failed. Reason: {0}")]
    ConsumerInitializationFailed(String),
    #[error("Consumer subscription failed. Reason: {message}, TopicSelector: {topic_selector}")]
    SubscribingFailed {
        message: String,
        topic_selector: String,
    },
    #[error("Kafka Error. Reason: {0}")]
    KafkaError(String),
    #[error("Failed to receive messages. Reason: {message}, TopicSelector: {topic_selector}")]
    FailedToReceiveMessages {
        message: String,
        topic_selector: String,
    },
    #[error("Retrieving offset header failed. Reason: {0}")]
    RetrievingOffsetHeaderValueFailed(String),
    #[error("Offset header from message could not be fetched. Reason: {0}")]
    FetchOffsetError(FetchOffsetError),
    #[error("Error during offset mapping transformation. Reason: {0}")]
    OffsetMappingTransformationError(OffsetMappingTransformationError),
    #[error("Failed to fetch metadata. Reason: {0}")]
    MetadataFetchFailed(String),
    #[error(
        "Failed to fetch watermarks for topic {topic}, partition {partition}. Reason: {reason}"
    )]
    WatermarkFetchFailed {
        topic: String,
        partition: i32,
        reason: String,
    },
    #[error("Invalid input parameters. Reason: {0}")]
    InvalidInput(String),
    #[error("Timeout occurred while waiting for messages")]
    Timeout,
    #[error("No valid partitions found for topics: {0:?}")]
    NoValidPartitions(Vec<String>),
}

#[derive(Error, Debug)]
pub enum OffsetMappingTransformationError {
    #[error(
        "Source offset {source_offset} already present in result. Target offset is {target_offset}. Previous target offset was {previous_target_offset}"
    )]
    SourceOffsetAlreadyPresent {
        source_offset: i64,
        target_offset: i64,
        previous_target_offset: i64,
    },
}

#[derive(Error, Debug)]
pub enum FetchOffsetError {
    #[error("Parsing header failed. Reason: {0}")]
    ErrorParsingHeader(String),
    #[error("No header found.")]
    HeaderNotFound,
    #[error("No headers in message")]
    NoHeadersInMessage,
}

#[derive(Debug)]
pub struct KafkaMessage {
    pub partition: i32,
    pub offset: i64,
    pub topic: String,
}

impl Display for KafkaMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Additional message Info: Topic: {}, Partition: {}, Offset: {}",
            self.topic, self.partition, self.offset
        )
    }
}

impl From<OffsetMappingTransformationError> for TransformationError {
    fn from(value: OffsetMappingTransformationError) -> Self {
        TransformationError::OffsetMappingTransformationError(value)
    }
}

impl From<KafkaError> for TransformationError {
    fn from(value: KafkaError) -> Self {
        TransformationError::KafkaError(value.to_string())
    }
}

impl From<FetchOffsetError> for TransformationError {
    fn from(value: FetchOffsetError) -> Self {
        TransformationError::FetchOffsetError(value)
    }
}
