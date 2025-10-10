use std::path::PathBuf;

use bridge_core::{
    OffsetRecord,
    client_config::ConfigBuilder,
    get_unique_topics_from_offset_snapshot,
    transform::{consumer::setup_consumer_and_metadata, get_target_offsets},
};
use rdkafka::ClientConfig;

use crate::{
    helpers::fetch_offset_records, GeneralError
};

pub async fn execute(
    source_offsets_csv_file_location: Option<PathBuf>,
    bootstrap_server: String,
    consumer_group_id: String,
    legacy_offset_header: String,
    from_stdin: bool,
    optional_client_properties: Option<Vec<String>>,
    topics: Option<Vec<String>>,
) -> Result<(), GeneralError> {
    let result = fetch_offset_records(from_stdin, source_offsets_csv_file_location)?;

    let result_filtered: Vec<OffsetRecord> = result
        .into_iter()
        .filter(|o| topics.as_ref().is_none_or(|t| t.contains(&o.topic)))
        .collect();

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

    let transformed_result =
        get_target_offsets(&legacy_offset_header, &result_filtered, consumer, metadata).await?;

    for consumer_group in transformed_result {
        for item in consumer_group.1 {
            println!("{},{},{},{}", consumer_group.0, item.0, item.1, item.3)
        }
    }
    Ok(())
}
