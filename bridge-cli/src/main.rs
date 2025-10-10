use args::{Args, Commands};
use clap::Parser;
use errors::GeneralError;

mod apply_target_offsets;
mod args;
mod calculate_target_offsets;
mod fetch_offsets;
mod fetch_source_offsets;
mod errors;
mod helpers;

#[tokio::main]
async fn main() -> Result<(), GeneralError> {
    let args = Args::parse();

    env_logger::init();

    match args.command {
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
    }
}

