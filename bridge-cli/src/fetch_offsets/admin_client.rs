use bridge_core::{OffsetRecord, OffsetSnapshot};
use rdkafka::{
    ClientConfig, TopicPartitionList,
    consumer::{BaseConsumer, Consumer},
    error::KafkaError,
    util::Timeout,
};
use thiserror::Error;

pub fn fetch_all_consumer_group_offsets(
    config: ClientConfig,
) -> Result<OffsetSnapshot, ImportOffsetsError> {
    let consumer: BaseConsumer = config.create()?;

    // Fetch all consumer groups
    let group_list = consumer.fetch_group_list(None, Timeout::Never)?;
    let mut all_offsets = Vec::new();

    for group in group_list.groups() {
        let group_id = group.name();

        // Get committed offsets for this consumer group
        match fetch_consumer_group_offsets(&consumer, group_id) {
            Ok(mut offsets) => {
                all_offsets.append(&mut offsets);
            }
            Err(e) => {
                eprintln!(
                    "Warning: Failed to fetch offsets for group {}: {}",
                    group_id, e
                );
                continue;
            }
        }
    }

    Ok(all_offsets)
}

fn fetch_consumer_group_offsets(
    consumer: &BaseConsumer,
    group_id: &str,
) -> Result<OffsetSnapshot, ImportOffsetsError> {
    let mut offsets = Vec::new();

    // Get the metadata to find all topics and partitions
    let metadata = consumer.fetch_metadata(None, Timeout::Never)?;

    // Create a TopicPartitionList with all available topic-partitions
    let mut topic_partition_list = TopicPartitionList::new();

    for topic in metadata.topics() {
        let topic_name = topic.name();
        for partition in topic.partitions() {
            topic_partition_list.add_partition(topic_name, partition.id());
        }
    }

    // Fetch committed offsets for this consumer group
    // Use a shorter timeout and handle the case where the group doesn't exist
    match consumer.committed_offsets(topic_partition_list, Timeout::After(std::time::Duration::from_secs(5))) {
        Ok(committed_offsets) => {
            for element in committed_offsets.elements() {
                if let Some(offset) = element.offset().to_raw() {
                    // Only include partitions that have committed offsets (not -1001 which means no offset)
                    if offset >= 0 {
                        offsets.push(OffsetRecord {
                            consumer_group: group_id.to_string(),
                            topic: element.topic().to_string(),
                            partition: element.partition(),
                            offset,
                        });
                    }
                }
            }
        }
        Err(KafkaError::ConsumerCommit(rdkafka::error::RDKafkaErrorCode::UnknownGroup)) => {
            // This consumer group has no committed offsets, skip it
            return Ok(offsets);
        }
        Err(e) => return Err(ImportOffsetsError::KafkaError(e)),
    }

    Ok(offsets)
}

#[derive(Debug, Error)]
pub enum ImportOffsetsError {
    #[error("Kafka Error. Reason: {0}")]
    KafkaError(#[from] KafkaError),
}
