use std::path::PathBuf;

use bridge_core::{
    export::{ApplyOffsetsError, apply_target_offsets},
    import::{ImportError, OffsetSnapshotImporter},
    transform::{errors::TransformationError, get_target_offsets},
};
use clap::{Parser, command};
use rdkafka::ClientConfig;
use thiserror::Error;

use crate::fetch_offsets::csv::CsvOffsetSnapshotImporter;

mod fetch_offsets;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    /// The bootstrap server URL for the Kafka Broker where we want to restore the offsets
    bootstrap_server: String,

    #[arg(short, long)]
    /// Path to CSV file containing the offsets
    offsets_csv_file_location: PathBuf,

    #[arg(short, long, default_value_t = String::from("bridge-consumer-group"))]
    /// Consumer group ID that will be used to fetch the records
    consumer_group_id: String,

    #[arg(short, long)]
    /// Header in target messages that contains the offsets of the source topic
    legacy_offset_header: String,
}

#[tokio::main]
async fn main() -> Result<(), GeneralError> {
    let args = Args::parse();

    simple_logger::SimpleLogger::new()
        .env()
        .init()
        .map_err(|e| GeneralError::InitializeLoggingFailed(format!("{e}")))?;

    let offset_snapshot_importer = CsvOffsetSnapshotImporter {
        file_path: args.offsets_csv_file_location,
        consumer_group: "console-consumer".to_string(),
    };

    let mut exporter_base_config = ClientConfig::new();
    exporter_base_config
        .set("bootstrap.servers", args.bootstrap_server.as_str())
        .set("enable.auto.commit", "false")
        .set("group.id", &args.consumer_group_id);

    let mut transformer_consumer_config = ClientConfig::new();
    transformer_consumer_config
        .set("bootstrap.servers", args.bootstrap_server.as_str())
        .set("group.id", &args.consumer_group_id)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false");

    let result = offset_snapshot_importer.import()?;

    let transformed_result = get_target_offsets(
        transformer_consumer_config,
        &args.legacy_offset_header,
        &result,
    )
    .await?;

    println!("{transformed_result:?}");
    apply_target_offsets(&mut exporter_base_config, &transformed_result).await?;

    Ok(())
}

#[derive(Error, Debug)]
enum GeneralError {
    #[error("Failed during importing of source offsets. Reason: {0}")]
    ImportError(#[from] ImportError),
    #[error("Failed during offset transformation. Reason: {0}")]
    TransformationError(#[from] TransformationError),
    #[error("Failed during offset transformation. Reason: {0}")]
    ApplyOffsetsError(#[from] ApplyOffsetsError),
    #[error("Failed to initialize logging. Reason: {0}")]
    InitializeLoggingFailed(String),
}
