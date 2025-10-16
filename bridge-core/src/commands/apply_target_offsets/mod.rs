use crate::commands::apply_target_offsets::errors::ApplyOffsetsError;
use crate::commands::apply_target_offsets::export::apply_target_offsets;
use crate::kafka::client_config::ConfigBuilder;
use crate::{ApplicationRecord, ConsumerGroup, OffsetSnapshot};
use rdkafka::ClientConfig;
use std::collections::HashMap;

pub mod errors;
mod export;

pub async fn execute(
    bootstrap_server: &str,
    optional_client_properties: &Option<Vec<String>>,
    topics: &Option<Vec<String>>,
    offset_snapshot: OffsetSnapshot,
    confirmation: &dyn Fn(&OffsetSnapshot) -> bool,
) -> Result<(), ApplyOffsetsError> {
    let mut exporter_base_config = ClientConfig::new();
    exporter_base_config
        .set_bootstrap_server(bootstrap_server)
        .set_reset_from_beginning()
        .set_optional_properties(optional_client_properties)
        .disable_auto_commit();

    let mut mapped_intermediary_result: HashMap<ConsumerGroup, Vec<ApplicationRecord>> =
        HashMap::new();

    offset_snapshot
        .iter()
        .filter(|r| topics.as_ref().is_none_or(|t| t.contains(&r.topic)))
        .for_each(|item| {
            let value = (item.topic.to_string(), item.partition, item.offset);
            mapped_intermediary_result
                .entry(item.consumer_group.clone())
                .and_modify(|list| list.push(value.clone()))
                .or_insert(vec![value.clone()]);
        });

    if confirmation(&offset_snapshot) {
        apply_target_offsets(&mut exporter_base_config, &mapped_intermediary_result).await?;
        Ok(())
    } else {
        Err(ApplyOffsetsError::Cancelled)
    }
}
