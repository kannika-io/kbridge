use crate::commands::apply_target_offsets::errors::ApplyOffsetsError;
use crate::commands::apply_target_offsets::export::apply_target_offsets;
use crate::kafka::client_config::ConfigBuilder;
use crate::{ApplicationRecord, ConsumerGroup, OffsetSnapshot};
use rdkafka::ClientConfig;
use std::collections::HashMap;
use tracing::trace;

pub mod errors;
mod export;

pub async fn execute(
    bootstrap_server: &str,
    properties: &HashMap<String, String>,
    topics: impl IntoIterator<Item = String>,
    offset_snapshot: OffsetSnapshot,
    dry_run: bool,
) -> Result<(), ApplyOffsetsError> {
    trace!("apply target offsets");

    let topics = topics.into_iter().collect::<Vec<String>>();

    let mut exporter_base_config = ClientConfig::new()
        .set_properties(properties)
        .set_bootstrap_server(bootstrap_server)
        .set_reset_from_beginning()
        .disable_auto_commit();

    let mut mapped_intermediary_result: HashMap<ConsumerGroup, Vec<ApplicationRecord>> =
        HashMap::new();

    offset_snapshot
        .iter()
        .filter(|r| topics.contains(&r.topic))
        .for_each(|item| {
            let value = (item.topic.to_string(), item.partition, item.offset);
            mapped_intermediary_result
                .entry(item.consumer_group.clone())
                .and_modify(|list| list.push(value.clone()))
                .or_insert(vec![value.clone()]);
        });

    if !dry_run {
        apply_target_offsets(&mut exporter_base_config, &mapped_intermediary_result).await?;
    }
    Ok(())
}
