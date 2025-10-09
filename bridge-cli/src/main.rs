use std::{
    collections::HashMap,
    io::{BufRead, stdin},
    path::PathBuf,
};

use bridge_core::{
    ApplicationRecord, ConsumerGroup,
    export::{ApplyOffsetsError, apply_target_offsets},
    import::{ImportError, OffsetSnapshotImporter},
    transform::{errors::TransformationError, get_target_offsets},
};
use clap::{Parser, Subcommand, command};
use comfy_table::Table;
use fetch_offsets::{
    client::{ImportOffsetsError, fetch_all_consumer_group_offsets},
    csv::convert,
};
use inquire::Text;
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
    FetchSource {
        #[arg(short, long)]
        /// The bootstrap server URL for the Kafka Broker
        bootstrap_server: String,
    },
    CalculateIntermediary {
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
        source_offsets_csv_file_location: Option<PathBuf>,

        #[arg(short, long, action)]
        /// Whether to read CSV file from stdin
        from_stdin: bool,
    },
    ApplyIntermediary {
        #[arg(short, long)]
        /// The bootstrap server URL for the Kafka Broker
        bootstrap_server: String,

        #[arg(short, long, default_value_t = String::from("bridge-consumer-group"))]
        /// Consumer group ID that will be used to fetch the records
        consumer_group_id: String,

        #[arg(short, long)]
        /// Path to CSV file containing the offsets
        intermediary_offsets_csv_file_location: Option<PathBuf>,

        #[arg(short, long, action)]
        /// Whether to read CSV file from stdin
        from_stdin: bool,
    },
}

#[tokio::main]
async fn main() -> Result<(), GeneralError> {
    let args = Args::parse();

    env_logger::init();

    match args.command {
        Commands::FetchSource { bootstrap_server } => {
            let mut admin_config = ClientConfig::new();
            admin_config.set("bootstrap.servers", bootstrap_server.as_str());
            let result = fetch_all_consumer_group_offsets(&mut admin_config)?;
            for consumer_group in result {
                println!(
                    "{},{},{},{}",
                    consumer_group.consumer_group,
                    consumer_group.topic,
                    consumer_group.partition,
                    consumer_group.offset
                )
            }
        }
        Commands::CalculateIntermediary {
            source_offsets_csv_file_location,
            bootstrap_server,
            consumer_group_id,
            legacy_offset_header,
            from_stdin,
        } => {
            let result = match from_stdin {
                true => Ok(stdin()
                    .lock()
                    .lines()
                    .map_while(Result::ok)
                    .filter_map(|line| convert(line).ok())
                    .collect()),
                false => {
                    if let Some(path) = source_offsets_csv_file_location {
                        let offset_snapshot_importer =
                            CsvOffsetSnapshotImporter { file_path: path };
                        offset_snapshot_importer.import()
                    } else {
                        Err(ImportError::ResourceNotFound(
                            "Import path is required if --from-stdin is not specified.".to_string(),
                        ))
                    }
                }
            }?;

            let mut transformer_consumer_config = ClientConfig::new();
            transformer_consumer_config
                .set("bootstrap.servers", bootstrap_server.as_str())
                .set("group.id", consumer_group_id.as_str())
                .set("auto.offset.reset", "earliest")
                .set("enable.auto.commit", "false");

            let transformed_result =
                get_target_offsets(transformer_consumer_config, &legacy_offset_header, &result)
                    .await?;

            for consumer_group in transformed_result {
                for item in consumer_group.1 {
                    println!("{},{},{},{}", consumer_group.0, item.0, item.1, item.3)
                }
            }
        }
        Commands::ApplyIntermediary {
            bootstrap_server,
            consumer_group_id,
            intermediary_offsets_csv_file_location,
            from_stdin,
        } => {
            let result = match from_stdin {
                true => Ok(stdin()
                    .lock()
                    .lines()
                    .map_while(Result::ok)
                    .filter_map(|line| convert(line).ok())
                    .collect()),
                false => {
                    if let Some(path) = intermediary_offsets_csv_file_location {
                        let offset_snapshot_importer =
                            CsvOffsetSnapshotImporter { file_path: path };
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
                .set("bootstrap.servers", bootstrap_server.as_str())
                .set("enable.auto.commit", "false")
                .set("group.id", consumer_group_id.as_str());

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

            let prompt =
                Text::new("The offsets above will be applied. Are you sure? (Y/n)").prompt();

            match prompt {
                Ok(value) => {
                    if value == "Y" {
                        println!("Executing operation.");
                        let result = apply_target_offsets(
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
    #[error("Failed during offset import. Reason: {0}")]
    ImportOffsets(#[from] ImportOffsetsError),
}
