use crate::OffsetSnapshot;
use crate::commands::apply_target_offsets::errors::ApplyOffsetsError;
use crate::commands::apply_target_offsets::export::apply_target_offsets;
use crate::kafka::client_config::ConfigBuilder;
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
    let filtered = offset_snapshot.filter_by_topics(&topics);

    if filtered.is_empty() {
        return Err(ApplyOffsetsError::NoOffsetsToApply);
    }

    if !dry_run {
        let mut config = ClientConfig::new()
            .set_properties(properties)
            .set_bootstrap_server(bootstrap_server)
            .set_reset_from_beginning()
            .disable_auto_commit();

        apply_target_offsets(&mut config, &filtered).await?;
    }

    Ok(())
}
