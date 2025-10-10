use std::collections::HashMap;

use bridge_core::client_config::ConfigBuilder;
use rdkafka::{ClientConfig, consumer::BaseConsumer, error::KafkaError};
use thiserror::Error;

use crate::fetch_offsets::client::{
    FetchMetadataError, ImportOffsetsError, fetch_all_consumer_group_offsets, fetch_metadata,
};

pub fn execute(
    bootstrap_server: String,
    optional_client_properties: Option<Vec<String>>,
    topics: Option<Vec<String>>,
) -> Result<(), FetchSourceOffsetsError> {
    let mut config = ClientConfig::new();
    config
        .set_bootstrap_server(bootstrap_server.as_str())
        .set_optional_properties(optional_client_properties);

    let consumer: BaseConsumer = config.clone().create()?;
    let metadata = fetch_metadata(consumer)?;

    let mut consumers: HashMap<String, BaseConsumer> = HashMap::new();

    for consumer_group in &metadata.consumer_groups {
        let consumer_config = config.set_consumer_group_id(consumer_group);
        let consumer_for_group = consumer_config.create()?;
        consumers.insert(consumer_group.to_string(), consumer_for_group);
    }

    let result = fetch_all_consumer_group_offsets(metadata, consumers)?;
    result
        .iter()
        .filter(|r| topics.as_ref().is_none_or(|t| t.contains(&r.topic)))
        .for_each(|cg| {
            println!(
                "{},{},{},{}",
                cg.consumer_group, cg.topic, cg.partition, cg.offset
            )
        });
    Ok(())
}

#[derive(Error, Debug)]
enum FetchSourceOffsetsError {
    #[error("Kafka Error. Reason: {0}")]
    KafkaError(#[from] KafkaError),

    #[error("Error while fetching metadata. Reason: {0}")]
    FetchMetadataError(#[from] FetchMetadataError),

    #[error("Error while importing offsets. Reason: {0}")]
    ImportOffsetserror(#[from] ImportOffsetsError),
}
