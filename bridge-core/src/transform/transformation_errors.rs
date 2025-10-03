use std::fmt::Display;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum TransformationError {
    #[error("Consumer initialization failed. Reason: {0}")]
    ConsumerInitializationFailed(String),
    #[error("Consumer subscription failed. Reason: {0}")]
    SubscribingFailed(String),
    #[error("Retrieving offset header failed. Reason: {0}")]
    RetrievingOffsetHeaderValueFailed(String),
    #[error("Offset header from message could not be parsed. Message: {message}, Inner error: {error}")]
    ErrorParsingHeader {
        message: KafkaMessage,
        error: FetchOffsetError
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
