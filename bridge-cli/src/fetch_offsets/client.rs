use bridge_core::{OffsetRecord, OffsetSnapshot, client_config::ConfigBuilder};
use rdkafka::{
    ClientConfig, TopicPartitionList,
    consumer::{BaseConsumer, Consumer},
    error::KafkaError,
    util::Timeout,
};
use thiserror::Error;

pub fn fetch_all_consumer_group_offsets(
    consumer_config: &mut ClientConfig,
) -> Result<OffsetSnapshot, ImportOffsetsError> {
    let consumer: BaseConsumer = consumer_config.create()?;

    // Fetch all consumer groups
    let group_list = consumer.fetch_group_list(None, Timeout::Never)?;
    let metadata = consumer.fetch_metadata(None, Timeout::Never)?;

    let mut all_offsets = Vec::new();

    for group in group_list.groups() {
        let group_id = group.name();
        consumer_config.set_consumer_group_id(group_id);

        let group_consumer: BaseConsumer = consumer_config.create()?;
        let mut topic_partition_list = TopicPartitionList::new();

        for topic in metadata.topics() {
            for partition in topic.partitions() {
                topic_partition_list.add_partition(topic.name(), partition.id());
            }
        }
        let committed_offsets =
            group_consumer.committed_offsets(topic_partition_list, Timeout::Never)?;

        for committed_offset in committed_offsets.elements() {
            match committed_offset.offset().to_raw() {
                Some(value) => {
                    // -1001 means no offset is present
                    if value != -1001 {
                        let value = OffsetRecord {
                            topic: committed_offset.topic().to_string(),
                            partition: committed_offset.partition(),
                            offset: value,
                            consumer_group: group_id.to_string(),
                        };
                        all_offsets.push(value);
                    }
                }
                None => {
                    eprintln!("Error fetching offset");
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
}
