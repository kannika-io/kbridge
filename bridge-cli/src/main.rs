use args::{Args, Commands};
use bridge_core::{
    export::ApplyOffsetsError, import::ImportError, transform::errors::TransformationError,
};
use clap::Parser;
use fetch_offsets::client::ImportOffsetsError;
use thiserror::Error;

mod apply_target_offsets;
mod args;
mod calculate_target_offsets;
mod fetch_offsets;
mod fetch_source_offsets;

#[tokio::main]
async fn main() -> Result<(), GeneralError> {
    let args = Args::parse();

    env_logger::init();

    match args.command {
        Commands::FetchSource {
            bootstrap_server,
            optional_client_properties,
        } => fetch_source_offsets::execute(bootstrap_server, optional_client_properties),
        Commands::CalculateTarget {
            source_offsets_csv_file_location,
            bootstrap_server,
            consumer_group_id,
            legacy_offset_header,
            from_stdin,
            optional_client_properties,
        } => {
            calculate_target_offsets::execute(
                source_offsets_csv_file_location,
                bootstrap_server,
                consumer_group_id,
                legacy_offset_header,
                from_stdin,
                optional_client_properties,
            )
            .await
        }
        Commands::ApplyTarget {
            bootstrap_server,
            consumer_group_id,
            intermediary_offsets_csv_file_location,
            from_stdin,
            optional_client_properties,
        } => apply_target_offsets::execute(
            bootstrap_server,
            consumer_group_id,
            intermediary_offsets_csv_file_location,
            from_stdin,
            optional_client_properties
        ).await,
    }
}

#[derive(Error, Debug)]
enum GeneralError {
    #[error("Failed during importing of source offsets. Reason: {0}")]
    Import(#[from] ImportError),
    #[error("Failed during offset transformation. Reason: {0}")]
    Transformation(#[from] TransformationError),
    #[error("Failed during offset transformation. Reason: {0}")]
    ApplyOffsets(#[from] ApplyOffsetsError),
    #[error("Failed during offset import. Reason: {0}")]
    ImportOffsets(#[from] ImportOffsetsError),
}
