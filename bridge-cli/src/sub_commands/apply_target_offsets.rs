use std::{collections::HashMap, path::PathBuf};

use bridge_core::{ApplicationRecord, ConsumerGroup, client_config::ConfigBuilder, export};
use comfy_table::Table;
use inquire::Text;
use rdkafka::ClientConfig;

use crate::{GeneralError, helpers::fetch_offset_records};

pub async fn execute(
    bootstrap_server: String,
    consumer_group_id: String,
    intermediary_offsets_csv_file_location: Option<PathBuf>,
    from_stdin: bool,
    optional_client_properties: Option<Vec<String>>,
    topics: Option<Vec<String>>,
) -> Result<(), GeneralError> {
    let result = fetch_offset_records(from_stdin, intermediary_offsets_csv_file_location)?;

    let mut exporter_base_config = ClientConfig::new();
    exporter_base_config
        .set_bootstrap_server(bootstrap_server.as_str())
        .set_consumer_group_id(consumer_group_id.as_str())
        .set_optional_properties(optional_client_properties)
        .disable_auto_commit();

    let mut mapped_intermediary_result: HashMap<ConsumerGroup, Vec<ApplicationRecord>> =
        HashMap::new();

    result
        .iter()
        .filter(|r| topics.as_ref().is_none_or(|t| t.contains(&r.topic)))
        .for_each(|item| {
            let value = (item.topic.to_string(), item.partition, item.offset);
            mapped_intermediary_result
                .entry(item.consumer_group.clone())
                .and_modify(|list| list.push(value.clone()))
                .or_insert(vec![value.clone()]);
        });

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
        Ok(value) if value == "Y" => {
            println!("Executing operation.");
            let result = export::apply_target_offsets(
                &mut exporter_base_config,
                &mapped_intermediary_result,
            )
            .await;
            println!("Operation executed");
            result?
        }
        _ => println!("The operation has been cancelled."),
    };
    Ok(())
}
