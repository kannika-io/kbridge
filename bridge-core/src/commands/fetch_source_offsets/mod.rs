use super::errors::FetchSourceOffsetsError;
use crate::OffsetSnapshot;
use crate::commands::fetch_source_offsets::sources::client::{
    fetch_all_committed_consumer_group_offsets, fetch_metadata,
};
use crate::commands::fetch_source_offsets::sources::offsets_topic::read_offsets_from_topic;
use crate::kafka::client_config::{ConfigBuilder, GROUP_ID_KEY};
use rdkafka::ClientConfig;
use rdkafka::consumer::BaseConsumer;
use std::collections::HashMap;
use std::time::Duration;

pub mod errors;
mod sources;

pub fn execute(
    bootstrap_server: impl AsRef<str>,
    // TODO: to prevent API from breaking & too many arguments, use an `Options` struct for all
    // optional parameters, with a default implementation for those
    properties: &HashMap<String, String>,
    topics: impl IntoIterator<Item = String>,
    client_timeout: Duration,
) -> Result<OffsetSnapshot, FetchSourceOffsetsError> {
    let config = ClientConfig::new()
        .set_bootstrap_server(bootstrap_server.as_ref())
        .set_properties(properties);

    let consumer: BaseConsumer = config.clone().create()?;
    let metadata = fetch_metadata(consumer, topics, client_timeout)?;

    let mut consumers: HashMap<String, BaseConsumer> = HashMap::new();

    for consumer_group in &metadata.consumer_groups {
        let consumer_config = config.clone().set_properties(&HashMap::from([(
            GROUP_ID_KEY.to_string(),
            consumer_group.to_string(),
        )]));
        let consumer_for_group = consumer_config.create()?;
        consumers.insert(consumer_group.to_string(), consumer_for_group);
    }

    Ok(fetch_all_committed_consumer_group_offsets(
        metadata, consumers,
    )?)
}

/// Fetches committed consumer group offsets by decoding a restored copy of the
/// internal `__consumer_offsets` topic instead of querying consumer groups.
///
/// `progress` is called as `progress(read, total)` after each consumed record.
pub fn execute_from_offsets_topic(
    bootstrap_server: impl AsRef<str>,
    properties: &HashMap<String, String>,
    offsets_topic: impl AsRef<str>,
    topics: impl IntoIterator<Item = String>,
    client_timeout: Duration,
    progress: impl FnMut(u64, u64),
) -> Result<OffsetSnapshot, FetchSourceOffsetsError> {
    let mut config = ClientConfig::new()
        .set_bootstrap_server(bootstrap_server.as_ref())
        .set_properties(properties)
        .disable_auto_commit();

    // Partition EOF events complete partitions whose tail holds no consumable records
    config.set("enable.partition.eof", "true");

    let consumer: BaseConsumer = config.create()?;

    Ok(read_offsets_from_topic(
        &consumer,
        offsets_topic.as_ref(),
        topics,
        client_timeout,
        progress,
    )?)
}
