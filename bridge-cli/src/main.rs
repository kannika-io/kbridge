use std::{collections::HashMap, path::PathBuf};

use bridge_core::{
    ApplicationRecord, ConsumerGroup,
    export::{ApplyOffsetsError, apply_target_offsets},
    import::{ImportError, OffsetSnapshotImporter},
    transform::{errors::TransformationError, get_target_offsets},
};
use clap::{Parser, Subcommand, command};
use rdkafka::ClientConfig;
use thiserror::Error;

use crate::fetch_offsets::csv::CsvOffsetSnapshotImporter;

mod fetch_offsets;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    CalculateIntermediaryCsv {
        #[arg(short, long)]
        /// The bootstrap server URL for the Kafka Broker
        bootstrap_server: String,

        #[arg(short, long, default_value_t = String::from("bridge-consumer-group"))]
        /// Consumer group ID that will be used to fetch the records
        consumer_group_id: String,

        #[arg(short, long)]
        /// Header in target messages that contains the offsets of the source topic
        legacy_offset_header: String,

        #[arg(short, long)]
        /// Path to CSV file containing the offsets
        source_offsets_csv_file_location: PathBuf,
    },
    ApplyIntermediaryCsv {
        #[arg(short, long)]
        /// The bootstrap server URL for the Kafka Broker
        bootstrap_server: String,

        #[arg(short, long, default_value_t = String::from("bridge-consumer-group"))]
        /// Consumer group ID that will be used to fetch the records
        consumer_group_id: String,

        #[arg(short, long)]
        /// Path to CSV file containing the offsets
        intermediary_offsets_csv_file_location: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<(), GeneralError> {
    let args = Args::parse();

    env_logger::init();

    match args.command {
        Commands::CalculateIntermediaryCsv {
            source_offsets_csv_file_location,
            bootstrap_server,
            consumer_group_id,
            legacy_offset_header,
        } => {
            let offset_snapshot_importer = CsvOffsetSnapshotImporter {
                file_path: source_offsets_csv_file_location,
            };

            let mut transformer_consumer_config = ClientConfig::new();
            transformer_consumer_config
                .set("bootstrap.servers", bootstrap_server.as_str())
                .set("group.id", consumer_group_id.as_str())
                .set("auto.offset.reset", "earliest")
                .set("enable.auto.commit", "false");

            let result = offset_snapshot_importer.import()?;

            let transformed_result =
                get_target_offsets(transformer_consumer_config, &legacy_offset_header, &result)
                    .await?;

            for consumer_group in transformed_result {
                for item in consumer_group.1 {
                    println!(
                        "{},{},{},{}",
                        consumer_group.0, item.0, item.1, item.3
                    )
                }
            }
        }
        Commands::ApplyIntermediaryCsv {
            bootstrap_server,
            consumer_group_id,
            intermediary_offsets_csv_file_location,
        } => {
            let mut exporter_base_config = ClientConfig::new();
            exporter_base_config
                .set("bootstrap.servers", bootstrap_server.as_str())
                .set("enable.auto.commit", "false")
                .set("group.id", consumer_group_id.as_str());

            let offset_snapshot_importer = CsvOffsetSnapshotImporter {
                file_path: intermediary_offsets_csv_file_location,
            };
            let intermediary_result = offset_snapshot_importer.import()?;

            let mut mapped_intermediary_result: HashMap<ConsumerGroup, Vec<ApplicationRecord>> =
                HashMap::new();

            for item in intermediary_result {
                let value = (item.topic, item.partition, item.offset);
                mapped_intermediary_result
                    .entry(item.consumer_group)
                    .and_modify(|list| list.push(value.clone()))
                    .or_insert(vec![value.clone()]);
            }

            apply_target_offsets(&mut exporter_base_config, &mapped_intermediary_result).await?;
        }
    };

    Ok(())
}

#[derive(Error, Debug)]
enum GeneralError {
    #[error("Failed during importing of source offsets. Reason: {0}")]
    Import(#[from] ImportError),
    #[error("Failed during offset transformation. Reason: {0}")]
    Transformation(#[from] TransformationError),
    #[error("Failed during offset transformation. Reason: {0}")]
    ApplyOffsets(#[from] ApplyOffsetsError),
}
