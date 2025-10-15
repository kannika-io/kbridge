use super::errors::FetchSourceOffsetsError;
use crate::OffsetSnapshot;
use crate::commands::fetch_source_offsets::errors::ImportError;
use crate::commands::fetch_source_offsets::sources::client::{
    fetch_all_committed_consumer_group_offsets, fetch_metadata,
};
use crate::kafka::client_config::ConfigBuilder;
use rdkafka::ClientConfig;
use rdkafka::consumer::BaseConsumer;
use std::collections::HashMap;

pub mod errors;
mod sources;

pub fn execute(
    bootstrap_server: String,
    // TODO: to prevent API from breaking & too many arguments, use an `Options` struct for all
    // optional parameters, with a default implementation for those
    optional_client_properties: Option<Vec<String>>,
    topics: Option<Vec<String>>,
) -> Result<OffsetSnapshot, FetchSourceOffsetsError> {
    let mut config = ClientConfig::new();
    config
        .set_bootstrap_server(bootstrap_server.as_str())
        .set_reset_from_beginning()
        .set_optional_properties(optional_client_properties);

    let consumer: BaseConsumer = config.clone().create()?;
    let metadata = fetch_metadata(consumer, topics)?;

    let mut consumers: HashMap<String, BaseConsumer> = HashMap::new();

    for consumer_group in &metadata.consumer_groups {
        let consumer_config = config.set_consumer_group_id(consumer_group);
        let consumer_for_group = consumer_config.create()?;
        consumers.insert(consumer_group.to_string(), consumer_for_group);
    }

    Ok(fetch_all_committed_consumer_group_offsets(
        metadata, consumers,
    )?)
}
pub trait OffsetSnapshotImporter {
    fn import(&self) -> Result<OffsetSnapshot, ImportError>;
}
