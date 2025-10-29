use crate::commands::calculate_target_offsets::errors::TransformationError;
use crate::kafka::client_config::ConfigBuilder;
use crate::kafka::consumer::setup_consumer_and_metadata;
use crate::{OffsetRecord, OffsetSnapshot, Properties, TransformationRecord};
use log::trace;
use rdkafka::ClientConfig;
use transform::get_target_offsets;

pub mod errors;
mod transform;

pub async fn execute(
    bootstrap_server: &str,
    legacy_offset_header: &str,
    optional_client_properties: &Option<Properties>,
    topics: &Option<Vec<String>>,
    snapshot: OffsetSnapshot,
) -> Result<OffsetSnapshot, TransformationError> {
    let snapshot = match topics {
        Some(topics) => snapshot.filter_by_topic(topics),
        None => snapshot,
    };

    trace!("Initializing consumer and fetching metadata..");

    let mut transformer_consumer_config = ClientConfig::new()
        .set_bootstrap_server(bootstrap_server)
        .set_reset_from_beginning()
        .set_optional_properties(optional_client_properties)
        .disable_auto_commit();

    let topics: Vec<&str> = snapshot.topics().into_iter().collect();

    // Initialize consumer
    let (consumer, metadata) =
        setup_consumer_and_metadata(&topics, &mut transformer_consumer_config).await?;

    trace!("Initialization completed.");

    let transformed_result =
        get_target_offsets(legacy_offset_header, &snapshot, consumer, metadata).await?;

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
