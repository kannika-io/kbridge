use crate::client_config::ConfigBuilder;
use crate::commands::calculate_target_offsets::errors::TransformationError;
use crate::commands::calculate_target_offsets::transform::consumer::setup_consumer_and_metadata;
use crate::commands::calculate_target_offsets::transform::get_target_offsets;
use crate::{
    OffsetRecord, OffsetSnapshot, TransformationRecord, get_unique_topics_from_offset_snapshot,
};
use log::trace;
use rdkafka::ClientConfig;

pub mod errors;
mod transform;

pub async fn execute(
    bootstrap_server: String,
    consumer_group_id: String,
    legacy_offset_header: String,
    optional_client_properties: Option<Vec<String>>,
    topics: Option<Vec<String>>,
    result: OffsetSnapshot,
) -> Result<OffsetSnapshot, TransformationError> {
    let result_filtered: Vec<OffsetRecord> = result
        .into_iter()
        .filter(|o| topics.as_ref().is_none_or(|t| t.contains(&o.topic)))
        .collect();

    trace!("Initializing consumer and fetching metadata..");

    let mut transformer_consumer_config = ClientConfig::new();
    transformer_consumer_config
        .set_bootstrap_server(bootstrap_server.as_str())
        .set_consumer_group_id(consumer_group_id.as_str())
        .set_optional_properties(optional_client_properties)
        .disable_auto_commit();

    let topics: Vec<&str> = get_unique_topics_from_offset_snapshot(&result_filtered);

    // Initialize consumer
    let (consumer, metadata) =
        setup_consumer_and_metadata(&topics, &mut transformer_consumer_config).await?;

    trace!("Initialization completed.");

    let transformed_result =
        get_target_offsets(&legacy_offset_header, &result_filtered, consumer, metadata).await?;

    Ok(transformed_result
        .iter()
        .flat_map(|o| {
            o.1.iter().map(|t: &TransformationRecord| OffsetRecord {
                topic: t.0.to_string(),
                partition: t.1,
                offset: 2,
                consumer_group: o.0.to_string(),
            })
        })
        .collect())
}
