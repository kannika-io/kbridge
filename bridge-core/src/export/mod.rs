use std::collections::HashMap;
use std::io;

use log::error;
use rdkafka::{
    ClientConfig, Offset, TopicPartitionList,
    consumer::{BaseConsumer, CommitMode, Consumer},
    error::KafkaError,
};
use thiserror::Error;

use crate::{ApplicationRecord, ConsumerGroup};

pub async fn apply_target_offsets(
    consumer_config: &mut ClientConfig,
    target_offsets: &HashMap<ConsumerGroup, Vec<ApplicationRecord>>,
) -> Result<(), ApplyOffsetsError> {
    for offset in target_offsets {
        consumer_config.set("group.id", offset.0);
        let consumer: BaseConsumer = consumer_config.create()?;

        let mut topic_partition_list = TopicPartitionList::new();

        for transformation in offset.1 {
            topic_partition_list.add_partition_offset(
                transformation.0.as_str(),
                transformation.1,
                Offset::Offset(transformation.2),
            )?;
        }
        consumer.commit(&topic_partition_list, CommitMode::Sync)?;
    }
    Ok(())
}

#[derive(Error, Debug)]
pub enum ApplyOffsetsError {
    #[error("Kafka error occurred. Reason: {0}")]
    KafkaError(#[from] KafkaError),
    #[error("IO error occurred. Reason: {0}")]
    IoError(#[from] io::Error),
}
