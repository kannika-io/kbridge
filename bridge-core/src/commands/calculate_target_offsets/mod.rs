use crate::commands::calculate_target_offsets::errors::TransformationError;
use crate::helpers::get_unique_topics_from_offset_snapshot;
use crate::kafka::client_config::ConfigBuilder;
use crate::kafka::consumer::setup_consumer_and_metadata;
use crate::{OffsetRecord, OffsetSnapshot, TransformationRecord};
use log::trace;
use rdkafka::ClientConfig;
use transform::get_target_offsets;

pub mod errors;
mod transform;

pub async fn execute(
    bootstrap_server: &str,
    legacy_offset_header: &str,
    optional_client_properties: &Option<Vec<String>>,
    topics: &Option<Vec<String>>,
    result: OffsetSnapshot,
) -> Result<OffsetSnapshot, TransformationError> {
    let result_filtered: Vec<OffsetRecord> = result
        .into_iter()
        .filter(|o| topics.as_ref().is_none_or(|t| t.contains(&o.topic)))
        .collect();

    trace!("Initializing consumer and fetching metadata..");

    let mut transformer_consumer_config = ClientConfig::new()
        .set_bootstrap_server(bootstrap_server)
        .set_reset_from_beginning()
        .set_optional_properties(optional_client_properties)
        .disable_auto_commit();

    let topics: Vec<&str> = get_unique_topics_from_offset_snapshot(&result_filtered);

    // Initialize consumer
    let (consumer, metadata) =
        setup_consumer_and_metadata(&topics, &mut transformer_consumer_config).await?;

    trace!("Initialization completed.");

    let transformed_result =
        get_target_offsets(legacy_offset_header, &result_filtered, consumer, metadata).await?;

    Ok(transformed_result
        .iter()
        .flat_map(|o| {
            o.1.iter().map(|t: &TransformationRecord| OffsetRecord {
                topic: t.0.to_string(),
                partition: t.1,
                offset: t.3,
                consumer_group: o.0.to_string(),
            })
        })
        .collect())
}
