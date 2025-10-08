use std::collections::HashMap;
use std::io;

use kafka::client::{CommitOffset, GroupOffsetStorage, KafkaClient};
use log::{info, error};
use rdkafka::error::KafkaError;
use thiserror::Error;

use crate::{ConsumerGroup, TransformationRecord};

pub async fn apply_target_offsets(
    brokers: &str,
    target_offsets: &HashMap<ConsumerGroup, Vec<TransformationRecord>>,
) -> Result<(), ApplyOffsetsError> {
    info!("Initializing kafka client with brokers: {brokers}");
    let mut client = KafkaClient::new(vec![brokers.to_string()]);
    
    // Load metadata with error handling
    client.load_metadata_all()
        .map_err(|e| {
            error!("Failed to load metadata: {:?}", e);
            ApplyOffsetsError::IoError(io::Error::new(io::ErrorKind::ConnectionRefused, format!("Failed to load metadata: {:?}", e)))
        })?;
    
    client.set_group_offset_storage(Some(GroupOffsetStorage::Kafka));
    client.set_fetch_min_bytes(0);

    info!("Committing target offsets");
    for (consumer_group, transformations) in target_offsets {
        let commit_offsets: Vec<CommitOffset> = transformations
            .iter()
            .map(|transformation| CommitOffset::new(&transformation.0, transformation.1, transformation.3))
            .collect();
        
        info!("Committing offsets for consumer group '{}': {:?}", consumer_group, commit_offsets);
        
        client.commit_offsets(consumer_group, commit_offsets)
            .map_err(|e| {
                error!("Failed to commit offsets for consumer group '{}': {:?}", consumer_group, e);
                ApplyOffsetsError::IoError(io::Error::new(io::ErrorKind::UnexpectedEof, format!("Failed to commit offsets: {:?}", e)))
            })?;
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
