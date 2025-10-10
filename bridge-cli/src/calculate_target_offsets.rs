use std::path::PathBuf;

use bridge_core::{
    OffsetRecord,
    client_config::ConfigBuilder,
    import::{ImportError, OffsetSnapshotImporter},
    transform::get_target_offsets,
};
use rdkafka::ClientConfig;

use crate::{
    GeneralError,
    fetch_offsets::csv::{CsvOffsetSnapshotImporter, get_from_stdin},
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
    let result: Vec<OffsetRecord> = match from_stdin {
        true => Ok(get_from_stdin()),
        false => {
            if let Some(path) = source_offsets_csv_file_location {
                let offset_snapshot_importer = CsvOffsetSnapshotImporter { file_path: path };
                offset_snapshot_importer.import()
            } else {
                Err(ImportError::ResourceNotFound(
                    "Import path is required if --from-stdin is not specified.".to_string(),
                ))
            }
        }
    }?;

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

    let transformed_result = get_target_offsets(
        &mut transformer_consumer_config,
        &legacy_offset_header,
        &result_filtered,
    )
    .await?;

    for consumer_group in transformed_result {
        for item in consumer_group.1 {
            println!("{},{},{},{}", consumer_group.0, item.0, item.1, item.3)
        }
    }
    Ok(())
}
