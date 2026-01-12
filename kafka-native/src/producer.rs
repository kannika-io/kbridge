//! Kafka Producer for sending messages.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use indexmap::IndexMap;

use crate::config::{CommonProperties, ProducerProperties};
use crate::connection::ConnectionPool;
use crate::error::Error;
use crate::types::message::Headers;

/// A record to be produced to Kafka.
#[derive(Debug, Clone)]
pub struct ProducerRecord<'a> {
    /// Topic to produce to.
    pub topic: &'a str,
    /// Partition (optional, -1 for auto-assignment).
    pub partition: i32,
    /// Message key (optional).
    pub key: Option<Bytes>,
    /// Message value.
    pub value: Option<Bytes>,
    /// Message headers.
    pub headers: Headers,
    /// Timestamp (optional, -1 for broker time).
    pub timestamp: i64,
}

impl<'a> ProducerRecord<'a> {
    /// Create a new producer record.
    pub fn new(topic: &'a str, value: impl Into<Bytes>) -> Self {
        Self {
            topic,
            partition: -1,
            key: None,
            value: Some(value.into()),
            headers: Headers::new(),
            timestamp: -1,
        }
    }

    /// Set the partition.
    pub fn partition(mut self, partition: i32) -> Self {
        self.partition = partition;
        self
    }

    /// Set the key.
    pub fn key(mut self, key: impl Into<Bytes>) -> Self {
        self.key = Some(key.into());
        self
    }

    /// Add a header.
    pub fn header(mut self, key: impl Into<String>, value: impl Into<Bytes>) -> Self {
        self.headers.insert(key, value);
        self
    }

    /// Set the timestamp.
    pub fn timestamp(mut self, timestamp: i64) -> Self {
        self.timestamp = timestamp;
        self
    }
}

/// Kafka Producer for sending messages.
pub struct Producer {
    /// Connection pool.
    pool: Arc<ConnectionPool>,
    /// Default request timeout.
    timeout: Duration,
}

impl Producer {
    /// Create a new producer from properties.
    pub(crate) fn new(props: ProducerProperties) -> Result<Self, Error> {
        let common: CommonProperties = props.into();
        let timeout = Duration::from_millis(common.request_timeout_ms());
        let pool = Arc::new(ConnectionPool::from_properties(&common)?);

        Ok(Self { pool, timeout })
    }

    /// Send a record to Kafka.
    ///
    /// Returns (partition, offset) on success.
    pub async fn send(&self, record: ProducerRecord<'_>) -> Result<(i32, i64), Error> {
        use kafka_protocol::messages::{ProduceRequest, produce_request};
        use kafka_protocol::protocol::StrBytes;
        use kafka_protocol::records::{Record, RecordBatchEncoder, RecordEncodeOptions};

        // Build the record batch
        let mut records = vec![];
        // Convert headers to IndexMap format expected by kafka-protocol
        let headers: IndexMap<StrBytes, Option<Bytes>> = record
            .headers
            .iter()
            .map(|(k, v)| (StrBytes::from_string(k.to_owned()), Some(Bytes::copy_from_slice(v))))
            .collect();

        let kafka_record = Record {
            transactional: false,
            control: false,
            partition_leader_epoch: -1,
            producer_id: -1,
            producer_epoch: -1,
            timestamp_type: kafka_protocol::records::TimestampType::Creation,
            offset: 0,
            sequence: -1,
            timestamp: record.timestamp,
            key: record.key.clone(),
            value: record.value.clone(),
            headers,
        };
        records.push(kafka_record);

        // Encode records
        let mut record_bytes = bytes::BytesMut::new();
        RecordBatchEncoder::encode(
            &mut record_bytes,
            records.iter(),
            &RecordEncodeOptions {
                version: 2,
                compression: kafka_protocol::records::Compression::None,
            },
        )
        .map_err(|e| Error::Config(format!("Failed to encode records: {}", e)))?;

        // Build produce request
        let mut request = ProduceRequest::default();
        request.acks = 1; // Wait for leader ack
        request.timeout_ms = self.timeout.as_millis() as i32;

        let mut topic_data = produce_request::TopicProduceData::default();
        topic_data.name = kafka_protocol::messages::TopicName::from(StrBytes::from_string(
            record.topic.to_owned(),
        ));

        let mut partition_data = produce_request::PartitionProduceData::default();
        partition_data.index = record.partition;
        partition_data.records = Some(record_bytes.freeze());
        topic_data.partition_data = vec![partition_data];

        request.topic_data = vec![topic_data];

        // Send request
        let conn = self.pool.get_any_connection().await?;
        let response = conn.send_request(request, self.timeout).await?;

        // Parse response
        for topic_resp in response.responses {
            for partition_resp in topic_resp.partition_responses {
                if partition_resp.error_code != 0 {
                    return Err(Error::kafka(
                        partition_resp.error_code,
                        format!(
                            "Failed to produce to {:?}[{}]",
                            topic_resp.name, partition_resp.index
                        ),
                    ));
                }
                return Ok((partition_resp.index, partition_resp.base_offset));
            }
        }

        Err(Error::Config("No response from produce request".to_owned()))
    }
}
