pub mod admin;
pub mod client_config;
pub mod consumer;
pub(crate) mod partition;
pub mod properties;
pub mod source;

use std::collections::HashMap;

use rdkafka::message::Headers;

use crate::prelude::*;

#[derive(Clone, Debug, thiserror::Error, strum::IntoStaticStr)]
pub enum KafkaError {
    #[error("generic Kafka error: {0}")]
    Generic(String),
    #[error("topic `{0}` could not be found on the remote server")]
    TopicNotFound(Topic),
    #[error("partition `{1}` of topic `{0}` could not be found on the remote server")]
    PartitionNotFound(Topic, PartitionNumber),
    #[error("detected invalid seek from rdkafka")]
    InvalidSeek,
    #[error("Kafka read error: {0}")]
    ReadError(#[source] rdkafka::error::KafkaError),
    #[error("Kafka config error: {0}")]
    ClientCreationError(#[source] rdkafka::error::KafkaError),
    #[error("Failed to fetch metadata: {0}")]
    MetadataFetchFailed(#[source] rdkafka::error::KafkaError),
}

impl From<rdkafka::error::KafkaError> for KafkaError {
    fn from(err: rdkafka::error::KafkaError) -> Self {
        use rdkafka::error::KafkaError as RDKafkaError;
        match err {
            RDKafkaError::AdminOp(err) => KafkaError::Generic(err.to_string()),
            RDKafkaError::AdminOpCreation(ref err) => KafkaError::Generic(err.to_string()),
            RDKafkaError::Canceled => KafkaError::Generic(err.to_string()),
            RDKafkaError::ClientConfig(_, ref desc, ref key, ref value) => {
                KafkaError::Generic(format!("Invalid client config: {} {} {}", desc, key, value))
            }
            RDKafkaError::ClientCreation(ref err) => KafkaError::Generic(err.to_string()),
            RDKafkaError::ConsumerCommit(err) => KafkaError::Generic(err.to_string()),
            RDKafkaError::Flush(err) => KafkaError::Generic(err.to_string()),
            RDKafkaError::Global(err) => KafkaError::Generic(err.to_string()),
            RDKafkaError::GroupListFetch(err) => KafkaError::Generic(err.to_string()),
            RDKafkaError::MessageConsumption(err) => KafkaError::Generic(err.to_string()),
            RDKafkaError::MessageProduction(err) => KafkaError::Generic(err.to_string()),
            RDKafkaError::MetadataFetch(err) => KafkaError::Generic(err.to_string()),
            RDKafkaError::NoMessageReceived => KafkaError::Generic(err.to_string()),
            RDKafkaError::Nul(_) => KafkaError::Generic(err.to_string()),
            RDKafkaError::OffsetFetch(err) => KafkaError::Generic(err.to_string()),
            RDKafkaError::PartitionEOF(part_n) => {
                KafkaError::Generic(format!("Partition {} EOF", part_n))
            }
            RDKafkaError::PauseResume(ref err) => KafkaError::Generic(err.to_string()),
            RDKafkaError::Seek(_) => KafkaError::InvalidSeek,
            RDKafkaError::SetPartitionOffset(err) => KafkaError::Generic(err.to_string()),
            RDKafkaError::StoreOffset(err) => KafkaError::Generic(err.to_string()),
            RDKafkaError::Subscription(ref err) => KafkaError::Generic(err.to_string()),
            RDKafkaError::Transaction(err) => KafkaError::Generic(err.to_string()),
            _ => {
                panic!("Unhandled KafkaError");
            }
        }
    }
}

impl<M> Message for M
where
    M: rdkafka::Message,
{
    fn partition(&self) -> i32 {
        M::partition(self)
    }

    fn offset(&self) -> i64 {
        M::offset(self)
    }

    fn timestamp(&self) -> i64 {
        match M::timestamp(self) {
            rdkafka::Timestamp::NotAvailable => -1,
            rdkafka::Timestamp::CreateTime(ts) => ts,
            rdkafka::Timestamp::LogAppendTime(ts) => ts,
        }
    }

    fn headers(&self) -> HashMap<String, Vec<u8>> {
        let mut map = HashMap::new();
        if let Some(headers) = M::headers(self) {
            for header in headers.iter() {
                if let Some(value) = header.value {
                    map.insert(header.key.to_owned(), value.to_vec());
                }
            }
        }
        map
    }

    fn header(&self, key: impl Into<String>) -> Option<Vec<u8>> {
        let key = key.into();
        M::headers(self).and_then(|headers| {
            headers
                .iter()
                .find(|h| h.key == key)
                .and_then(|h| h.value.map(|v| v.to_vec()))
        })
    }
}
