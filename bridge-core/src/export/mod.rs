use std::collections::HashMap;

use kafka::client::{CommitOffset, GroupOffsetStorage, KafkaClient};
use log::info;
use rdkafka::error::KafkaError;
use thiserror::Error;

use crate::{ConsumerGroup, TransformationRecord};

pub async fn apply_target_offsets(
    brokers: &str,
    target_offsets: &HashMap<ConsumerGroup, Vec<TransformationRecord>>,
) -> Result<(), ApplyOffsetsError> {
    info!("Initializing kafka client with brokers: {brokers}",);
    let mut client = KafkaClient::new(vec![brokers.to_string()]);
    client.load_metadata_all().unwrap();
    client.set_group_offset_storage(Some(GroupOffsetStorage::Kafka));
    client.set_fetch_min_bytes(0);


    info!("Committing target offsets",);
    for element in target_offsets {
        let commit_offsets: Vec<CommitOffset> = element
            .1
            .iter()
            .map(|e| CommitOffset::new(e.0.as_str(), e.1, e.3))
            .collect();
        info!("Commiting {element:?}");
        client.commit_offsets(element.0, commit_offsets).unwrap();
    }

    Ok(())
}

#[derive(Error, Debug)]
pub enum ApplyOffsetsError {
    #[error("Failed during importing of source offsets. Reason: {0}")]
    KafkaError(#[from] KafkaError),
}
