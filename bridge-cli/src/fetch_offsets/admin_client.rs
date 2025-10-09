use rdkafka::{
    admin::{AdminClient, AdminOptions},
    consumer::{BaseConsumer, Consumer},
    error::KafkaError,
    util::Timeout,
    ClientConfig, TopicPartitionList,
};
use std::collections::HashMap;
use thiserror::Error;
use bridge_core::{OffsetRecord, OffsetSnapshot};

pub fn fetch_all_consumer_group_offsets(config: ClientConfig) -> Result<OffsetSnapshot, ImportOffsetsError> {
    let consumer: BaseConsumer = config.create()?;
    let admin_client: AdminClient<_> = config.create()?;
    
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
                eprintln!("Warning: Failed to fetch offsets for group {}: {}", group_id, e);
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
    let committed_offsets = consumer.committed_offsets(topic_partition_list, Timeout::Never)?;
    
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
    
    Ok(offsets)
}

pub fn import(config: ClientConfig, topic: Option<&str>) -> Result<OffsetSnapshot, ImportOffsetsError> {
    if let Some(_topic_filter) = topic {
        // If a specific topic is provided, we could filter the results
        // For now, we'll fetch all and let the caller filter if needed
        fetch_all_consumer_group_offsets(config)
    } else {
        fetch_all_consumer_group_offsets(config)
    }
}

#[derive(Debug, Error)]
pub enum ImportOffsetsError {
    #[error("Kafka Error. Reason: {0}")]
    KafkaError(#[from] KafkaError),
}
