use super::errors::FetchSourceOffsetsError;
use crate::commands::fetch_source_offsets::sources::client::{
    fetch_all_committed_consumer_group_offsets, fetch_metadata,
};
use crate::kafka::client_config::ConfigBuilder;
use crate::OffsetSnapshot;
use rdkafka::ClientConfig;
use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::BaseConsumer;
use std::collections::HashMap;
use std::time::Duration;

pub mod errors;
mod sources;

pub async fn execute(
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

    let admin_client: AdminClient<DefaultClientContext> = config.create()?;

    Ok(fetch_all_committed_consumer_group_offsets(
        metadata,
        &admin_client,
        client_timeout,
    )
    .await?)
}
