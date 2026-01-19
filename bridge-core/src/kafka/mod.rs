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
    #[error("{0}")]
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
    #[error("Unknown Kafka error: {0}")]
    Unknown(String),
}

impl From<rdkafka::error::KafkaError> for KafkaError {
    fn from(err: rdkafka::error::KafkaError) -> Self {
        use rdkafka::error::KafkaError as RDKafkaError;
        match err {
            RDKafkaError::AdminOp(err) => KafkaError::Generic(format_rdkafka_code(err)),
            RDKafkaError::AdminOpCreation(ref err) => KafkaError::Generic(err.to_string()),
            RDKafkaError::Canceled => KafkaError::Generic(err.to_string()),
            RDKafkaError::ClientConfig(_, ref desc, ref key, ref value) => {
                KafkaError::Generic(format!("invalid client config: {} {} {}", desc, key, value))
            }
            RDKafkaError::ClientCreation(ref err) => KafkaError::Generic(err.to_string()),
            RDKafkaError::ConsumerCommit(err) => KafkaError::Generic(format_rdkafka_code(err)),
            RDKafkaError::Flush(err) => KafkaError::Generic(format_rdkafka_code(err)),
            RDKafkaError::Global(err) => KafkaError::Generic(format_rdkafka_code(err)),
            RDKafkaError::GroupListFetch(err) => KafkaError::Generic(format_rdkafka_code(err)),
            RDKafkaError::MessageConsumption(err) => KafkaError::Generic(format_rdkafka_code(err)),
            RDKafkaError::MessageProduction(err) => KafkaError::Generic(format_rdkafka_code(err)),
            RDKafkaError::MetadataFetch(err) => KafkaError::Generic(format_rdkafka_code(err)),
            RDKafkaError::NoMessageReceived => KafkaError::Generic("no message received".into()),
            RDKafkaError::Nul(_) => KafkaError::Generic(err.to_string()),
            RDKafkaError::OffsetFetch(err) => KafkaError::Generic(format_rdkafka_code(err)),
            RDKafkaError::PartitionEOF(part_n) => {
                KafkaError::Generic(format!("partition {} EOF", part_n))
            }
            RDKafkaError::PauseResume(ref err) => KafkaError::Generic(err.to_string()),
            RDKafkaError::Seek(_) => KafkaError::InvalidSeek,
            RDKafkaError::SetPartitionOffset(err) => KafkaError::Generic(format_rdkafka_code(err)),
            RDKafkaError::StoreOffset(err) => KafkaError::Generic(format_rdkafka_code(err)),
            RDKafkaError::Subscription(ref err) => KafkaError::Generic(err.to_string()),
            RDKafkaError::Transaction(err) => KafkaError::Generic(err.to_string()),
            other => KafkaError::Unknown(format!("{:?}", other)),
        }
    }
}

/// Format rdkafka error codes into human-readable messages
fn format_rdkafka_code(code: rdkafka::types::RDKafkaErrorCode) -> String {
    use rdkafka::types::RDKafkaErrorCode::*;
    match code {
        OperationTimedOut => "operation timed out".into(),
        BrokerTransportFailure => "broker unavailable".into(),
        UnknownTopicOrPartition => "unknown topic or partition".into(),
        other => format!("{:?}", other),
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

#[cfg(test)]
mod tests {
    use super::*;
    use rdkafka::types::RDKafkaErrorCode;

    #[test]
    fn test_format_rdkafka_code_timeout() {
        assert_eq!(
            format_rdkafka_code(RDKafkaErrorCode::OperationTimedOut),
            "operation timed out"
        );
    }

    #[test]
    fn test_format_rdkafka_code_broker_unavailable() {
        assert_eq!(
            format_rdkafka_code(RDKafkaErrorCode::BrokerTransportFailure),
            "broker unavailable"
        );
    }

    #[test]
    fn test_format_rdkafka_code_unknown_topic() {
        assert_eq!(
            format_rdkafka_code(RDKafkaErrorCode::UnknownTopicOrPartition),
            "unknown topic or partition"
        );
    }

    #[test]
    fn test_kafka_error_from_rdkafka_group_list_fetch() {
        let rdkafka_err =
            rdkafka::error::KafkaError::GroupListFetch(RDKafkaErrorCode::OperationTimedOut);
        let err: KafkaError = rdkafka_err.into();
        assert_eq!(err.to_string(), "operation timed out");
    }

    #[test]
    fn test_kafka_error_from_rdkafka_metadata_fetch() {
        let rdkafka_err =
            rdkafka::error::KafkaError::MetadataFetch(RDKafkaErrorCode::BrokerTransportFailure);
        let err: KafkaError = rdkafka_err.into();
        assert_eq!(err.to_string(), "broker unavailable");
    }
}
