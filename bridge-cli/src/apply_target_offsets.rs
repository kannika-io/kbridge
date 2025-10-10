use std::{collections::HashMap, path::PathBuf};

use bridge_core::{
    ApplicationRecord, ConsumerGroup,
    client_config::ConfigBuilder,
    export,
    import::{ImportError, OffsetSnapshotImporter},
};
use comfy_table::Table;
use inquire::Text;
use rdkafka::ClientConfig;

use crate::{
    GeneralError,
    fetch_offsets::csv::{CsvOffsetSnapshotImporter, get_from_stdin},
};

pub async fn execute(
    bootstrap_server: String,
    consumer_group_id: String,
    intermediary_offsets_csv_file_location: Option<PathBuf>,
    from_stdin: bool,
    optional_client_properties: Option<Vec<String>>,
) -> Result<(), GeneralError> {
    let result = match from_stdin {
        true => Ok(get_from_stdin()),
        false => {
            if let Some(path) = intermediary_offsets_csv_file_location {
                let offset_snapshot_importer = CsvOffsetSnapshotImporter { file_path: path };
                offset_snapshot_importer.import()
            } else {
                Err(ImportError::ResourceNotFound(
                    "Import path is required if --from-stdin is not specified.".to_string(),
                ))
            }
        }
    }?;

    let mut exporter_base_config = ClientConfig::new();
    exporter_base_config
        .set_bootstrap_server(bootstrap_server.as_str())
        .set_consumer_group_id(consumer_group_id.as_str())
        .set_optional_properties(optional_client_properties)
        .disable_auto_commit();

    let mut mapped_intermediary_result: HashMap<ConsumerGroup, Vec<ApplicationRecord>> =
        HashMap::new();

    for item in &result {
        let value = (item.topic.to_string(), item.partition, item.offset);
        mapped_intermediary_result
            .entry(item.consumer_group.clone())
            .and_modify(|list| list.push(value.clone()))
            .or_insert(vec![value.clone()]);
    }

    let mut table = Table::new();

    table.set_header(vec![
        "Consumer Group",
        "Topic",
        "Partition",
        "Target Offset",
    ]);

    for intermediate_result_item in &result {
        table.add_row(vec![
            intermediate_result_item.consumer_group.clone(),
            intermediate_result_item.topic.clone(),
            intermediate_result_item.partition.to_string(),
            intermediate_result_item.offset.to_string(),
        ]);
    }

    println!("{table}");

    let prompt = Text::new("The offsets above will be applied. Are you sure? (Y/n)").prompt();

    match prompt {
        Ok(value) => {
            if value == "Y" {
                println!("Executing operation.");
                let result = export::apply_target_offsets(
                    &mut exporter_base_config,
                    &mapped_intermediary_result,
                )
                .await;
                println!("Operation executed");
                result?
            } else {
                println!("Doing nothing.");
            }
        }
        _ => println!("The operation has been cancelled."),
    };
    Ok(())
}
