use args::{Args, Commands};
use clap::Parser;
use comfy_table::Table;
use inquire::Text;
use log::trace;
use bridge_core::commands::{apply_target_offsets, calculate_target_offsets, fetch_source_offsets};
use bridge_core::helpers::fetch_offset_records;
use bridge_core::OffsetSnapshot;
use crate::errors::GeneralError;

mod args;
mod errors;

#[tokio::main]
async fn main() -> Result<(), GeneralError> {
    let args = Args::parse();

    env_logger::init();
    trace!("Executing with following arguments: {:?}", args);

    let result : Result<(), GeneralError> = match args.command {
        Commands::FetchSource {
            bootstrap_server,
            optional_client_properties,
            topics,
        } => {
            let result = fetch_source_offsets::execute(bootstrap_server, optional_client_properties, topics).map_err(GeneralError::from)?;
            print_offset_snapshot(&result);
            Ok(())
        },
        Commands::CalculateTarget {
            source_offsets_csv_file_location,
            bootstrap_server,
            consumer_group_id,
            legacy_offset_header,
            from_stdin,
            optional_client_properties,
            topics,
        } => {
            let offset_snapshot =  fetch_offset_records(from_stdin, source_offsets_csv_file_location).map_err(GeneralError::from)?;
            let result = calculate_target_offsets::execute(
                bootstrap_server,
                consumer_group_id,
                legacy_offset_header,
                optional_client_properties,
                topics,
                offset_snapshot
            )
            .await.map_err(GeneralError::from)?;
            print_offset_snapshot(&result);
            Ok(())
        }
        Commands::ApplyTarget {
            bootstrap_server,
            consumer_group_id,
            intermediary_offsets_csv_file_location,
            from_stdin,
            optional_client_properties,
            topics,
        } => {
            let offset_snapshot =  fetch_offset_records(from_stdin, intermediary_offsets_csv_file_location).map_err(GeneralError::from)?;
            apply_target_offsets::execute(
                bootstrap_server,
                consumer_group_id,
                optional_client_properties,
                topics,
                offset_snapshot,
                &|offset_snapshot| { ask_for_confirmation(offset_snapshot) },
            )
            .await.map_err(|e| e.into())
        }
    };
    trace!("Execution finished.");
    result
}

fn ask_for_confirmation(offset_snapshot: &OffsetSnapshot) -> bool
{
    let mut table = Table::new();

    table.set_header(vec![
        "Consumer Group",
        "Topic",
        "Partition",
        "Target Offset",
    ]);

    for intermediate_result_item in offset_snapshot {
        table.add_row(vec![
            intermediate_result_item.consumer_group.clone(),
            intermediate_result_item.topic.clone(),
            intermediate_result_item.partition.to_string(),
            intermediate_result_item.offset.to_string(),
        ]);
    }
    println!("{table}");
    let prompt = Text::new("The offsets above will be applied. Are you sure? (Y/n)").prompt();
    matches!(prompt, Ok(value) if value == "Y")
}

fn print_offset_snapshot(offset_snapshot: &OffsetSnapshot) {
    for element in offset_snapshot {
        println!("{},{},{},{}", element.consumer_group, element.topic, element.partition, element.offset);
    }
}