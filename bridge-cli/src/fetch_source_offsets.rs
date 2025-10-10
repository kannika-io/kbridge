use bridge_core::client_config::ConfigBuilder;
use rdkafka::ClientConfig;

use crate::{GeneralError, fetch_offsets::client::fetch_all_consumer_group_offsets};

pub fn execute(
    bootstrap_server: String,
    optional_client_properties: Option<Vec<String>>,
) -> Result<(), GeneralError> {
    let mut config = ClientConfig::new();
    config
        .set_bootstrap_server(bootstrap_server.as_str())
        .set_optional_properties(optional_client_properties);
    let result = fetch_all_consumer_group_offsets(&mut config)?;
    for consumer_group in result {
        println!(
            "{},{},{},{}",
            consumer_group.consumer_group,
            consumer_group.topic,
            consumer_group.partition,
            consumer_group.offset
        )
    }
    Ok(())
}
