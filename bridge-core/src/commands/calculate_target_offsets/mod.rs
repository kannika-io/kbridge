use crate::commands::calculate_target_offsets::errors::TransformationError;
use crate::kafka::client_config::ConfigBuilder;
use crate::kafka::consumer::setup_consumer_and_metadata;
use crate::{OffsetRecord, OffsetSnapshot, TransformationRecord};
use log::trace;
use rdkafka::ClientConfig;
use std::collections::HashMap;
use transform::get_target_offsets;

pub mod errors;
mod transform;

pub async fn execute(
    bootstrap_server: &str,
    legacy_offset_header: impl Into<String>,
    properties: &HashMap<String, String>,
    topics: impl IntoIterator<Item = String>,
    snapshot: OffsetSnapshot,
) -> Result<OffsetSnapshot, TransformationError> {
    trace!("Initializing consumer and fetching metadata..");

    let legacy_offset_header = legacy_offset_header.into();
    let topics = topics.into_iter().collect::<Vec<String>>();

    let snapshot = snapshot.filter_by_topics(&topics);

    let mut transformer_consumer_config = ClientConfig::new()
        .set_bootstrap_server(bootstrap_server)
        .set_reset_from_beginning()
        .set_properties(properties)
        .disable_auto_commit();

    // Initialize consumer
    let (consumer, metadata) =
        setup_consumer_and_metadata(topics, &mut transformer_consumer_config).await?;

    trace!("Initialization completed.");

    let transformed_result =
        get_target_offsets(&legacy_offset_header, &snapshot, consumer, metadata).await?;

    let snapshot = OffsetSnapshot::from_iter(transformed_result.iter().flat_map(|o| {
        o.1.iter().map(|t: &TransformationRecord| OffsetRecord {
            topic: t.0.to_string(),
            partition: t.1,
            offset: t.3,
            consumer_group: o.0.to_string(),
        })
    }));

    Ok(snapshot)
}
