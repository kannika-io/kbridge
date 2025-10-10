use std::collections::HashMap;

use bridge_core::{OffsetRecord, OffsetSnapshot};
use log::{trace, warn};
use rdkafka::{
    TopicPartitionList,
    consumer::{BaseConsumer, Consumer},
    error::KafkaError,
    util::Timeout,
};
use thiserror::Error;

const NO_OFFSET: i64 = -1001;

pub struct Metadata {
    pub consumer_groups: Vec<String>,
    pub topics_and_partitions: HashMap<String, Vec<i32>>,
}

/// Fetches metadata from kafka cluster: All consumer groups and topic-partition combos
pub fn fetch_metadata(consumer: BaseConsumer) -> Result<Metadata, FetchMetadataError> {
    let group_list = consumer.fetch_group_list(None, Timeout::Never)?;

    let metadata = consumer.fetch_metadata(None, Timeout::Never)?;

    let mut topics_and_partitions: HashMap<String, Vec<i32>> = HashMap::new();

    metadata.topics().iter().for_each(|topic| {
        topic.partitions().iter().for_each(|part| {
            topics_and_partitions
                .entry(topic.name().to_string())
                .and_modify(|t| t.push(part.id()))
                .or_insert(vec![part.id()]);
        })
    });

    Ok(Metadata {
        consumer_groups: group_list
            .groups()
            .iter()
            .map(|g| g.name().to_string())
            .collect(),
        topics_and_partitions,
    })
}

/// Fetches all committed consumer group offsets from metadata provided, using consumers provided
pub fn fetch_all_committed_consumer_group_offsets(
    metadata: Metadata,
    consumers: HashMap<String, BaseConsumer>,
) -> Result<OffsetSnapshot, ImportOffsetsError> {
    let mut all_offsets: OffsetSnapshot = Vec::new();
    for group in &metadata.consumer_groups {
        trace!("Fetching committed offsets for group {}", group);
        let group_consumer = consumers
            .get(group)
            .ok_or(ImportOffsetsError::ConsumerNotFound(group.to_string()))?;

        let mut topic_partition_list = TopicPartitionList::new();

        for topic in &metadata.topics_and_partitions {
            for partition in topic.1 {
                topic_partition_list.add_partition(topic.0.as_str(), *partition);
            }
        }

        let committed_offsets =
            group_consumer.committed_offsets(topic_partition_list, Timeout::Never)?;

        for committed_offset in committed_offsets.elements() {
            trace!("Committed offset: {:?}", committed_offset);
            match committed_offset.offset().to_raw() {
                Some(value) => {
                    if value != NO_OFFSET {
                        let value = OffsetRecord {
                            topic: committed_offset.topic().to_string(),
                            partition: committed_offset.partition(),
                            offset: value,
                            consumer_group: group.to_string(),
                        };
                        all_offsets.push(value);
                    }
                }
                None => {
                    warn!("Error fetching offset. Ignoring.");
                }
            }
        }
    }

    Ok(all_offsets)
}

#[derive(Debug, Error)]
pub enum ImportOffsetsError {
    #[error("Kafka Error. Reason: {0}")]
    KafkaError(#[from] KafkaError),

    #[error("Consumer {0} not found.")]
    ConsumerNotFound(String),
}

#[derive(Debug, Error)]
pub enum FetchMetadataError {
    #[error("Kafka Error. Reason: {0}")]
    KafkaError(#[from] KafkaError),
}
