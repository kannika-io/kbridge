use bridge_core::client_config::ConfigBuilder;
use rdkafka::ClientConfig;

use crate::{GeneralError, fetch_offsets::client::fetch_all_consumer_group_offsets};

pub fn execute(
    bootstrap_server: String,
    optional_client_properties: Option<Vec<String>>,
    topics: Option<Vec<String>>,
) -> Result<(), GeneralError> {
    let mut config = ClientConfig::new();
    config
        .set_bootstrap_server(bootstrap_server.as_str())
        .set_optional_properties(optional_client_properties);
    let result = fetch_all_consumer_group_offsets(&mut config)?;
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
