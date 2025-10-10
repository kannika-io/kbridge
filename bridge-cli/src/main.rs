use args::{Args, Commands};
use clap::Parser;
use errors::GeneralError;
use log::trace;
use sub_commands::{apply_target_offsets, calculate_target_offsets, fetch_source_offsets};

mod args;
mod errors;
mod fetch_offsets;
mod helpers;
mod sub_commands;

#[tokio::main]
async fn main() -> Result<(), GeneralError> {
    let args = Args::parse();

    env_logger::init();
    trace!("Executing with following arguments: {:?}", args);

    let result = match args.command {
        Commands::FetchSource {
            bootstrap_server,
            optional_client_properties,
            topics,
        } => fetch_source_offsets::execute(bootstrap_server, optional_client_properties, topics),
        Commands::CalculateTarget {
            source_offsets_csv_file_location,
            bootstrap_server,
            consumer_group_id,
            legacy_offset_header,
            from_stdin,
            optional_client_properties,
            topics,
        } => {
            calculate_target_offsets::execute(
                source_offsets_csv_file_location,
                bootstrap_server,
                consumer_group_id,
                legacy_offset_header,
                from_stdin,
                optional_client_properties,
                topics,
            )
            .await
        }
        Commands::ApplyTarget {
            bootstrap_server,
            consumer_group_id,
            intermediary_offsets_csv_file_location,
            from_stdin,
            optional_client_properties,
            topics,
        } => {
            apply_target_offsets::execute(
                bootstrap_server,
                consumer_group_id,
                intermediary_offsets_csv_file_location,
                from_stdin,
                optional_client_properties,
                topics,
            )
            .await
        }
    };
    trace!("Execution finished.");
    result
}
