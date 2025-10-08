use std::collections::HashMap;
use std::io;

use kafka::client::{CommitOffset, GroupOffsetStorage, KafkaClient};
use log::{error, info};
use rdkafka::{
    ClientConfig, Offset, TopicPartitionList,
    consumer::{BaseConsumer, Consumer},
    error::KafkaError,
};
use thiserror::Error;

use crate::{ConsumerGroup, TransformationRecord};

pub async fn apply_target_offsets(
    brokers: &str,
    target_offsets: &HashMap<ConsumerGroup, Vec<TransformationRecord>>,
) -> Result<(), ApplyOffsetsError> {
    info!("Initializing kafka client with brokers: {brokers}");

    for offset in target_offsets {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("enable.auto.commit", "false")
            .set("group.id", offset.0)
            .create()?;

        let mut topic_partition_list = TopicPartitionList::new();

        for transformation in offset.1 {
            topic_partition_list
                .add_partition_offset(
                    transformation.0.as_str(),
                    transformation.1,
                    Offset::Offset(transformation.3),
                )
                .unwrap();
        }
        consumer
            .commit(&topic_partition_list, rdkafka::consumer::CommitMode::Sync)
            .unwrap();
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
