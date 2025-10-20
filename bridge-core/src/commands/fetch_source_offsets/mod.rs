use super::errors::FetchSourceOffsetsError;
use crate::commands::fetch_source_offsets::errors::ImportError;
use crate::commands::fetch_source_offsets::sources::client::{
    fetch_all_committed_consumer_group_offsets, fetch_metadata,
};
use crate::kafka::client_config::{ConfigBuilder, GROUP_ID_KEY};
use crate::{OffsetSnapshot, Properties};
use rdkafka::ClientConfig;
use rdkafka::consumer::BaseConsumer;
use std::collections::HashMap;
use std::time::Duration;

pub mod errors;
mod sources;

pub fn execute(
    bootstrap_server: &str,
    // TODO: to prevent API from breaking & too many arguments, use an `Options` struct for all
    // optional parameters, with a default implementation for those
    optional_client_properties: &Option<Properties>,
    topics: &Option<Vec<String>>,
    client_timeout: Duration,
) -> Result<OffsetSnapshot, FetchSourceOffsetsError> {
    let config = ClientConfig::new()
        .set_bootstrap_server(bootstrap_server)
        .set_optional_properties(optional_client_properties);

    let consumer: BaseConsumer = config.clone().create()?;
    let metadata = fetch_metadata(consumer, topics, client_timeout)?;

    let mut consumers: HashMap<String, BaseConsumer> = HashMap::new();

    for consumer_group in &metadata.consumer_groups {
        let consumer_config = config
            .clone()
            .set_optional_properties(&Some(HashMap::from([(
                GROUP_ID_KEY.to_string(),
                consumer_group.to_string(),
            )])));
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
