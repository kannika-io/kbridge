use std::path::PathBuf;

use clap::{Parser, Subcommand, arg, command};

#[derive(Parser)]
#[command(version, about, long_about = None)]
pub struct Args {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    FetchSource {
        #[arg(short, long)]
        /// The bootstrap server URL for the Kafka Broker
        bootstrap_server: String,

        /// Additional properties for the kafka client, separated by a '='. e.g.:
        /// ssl.key.password=test
        #[arg(short, long)]
        optional_client_properties: Option<Vec<String>>,

        /// Specify topics. If no topics specified, all topics will be used.
        #[arg(short, long)]
        topics: Option<Vec<String>>,
    },
    CalculateTarget {
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

        /// Additional properties for the kafka client, separated by a '='. e.g.:
        /// ssl.key.password=test
        #[arg(short, long)]
        optional_client_properties: Option<Vec<String>>,

        /// Specify topics. If no topics specified, all topics will be used.
        #[arg(short, long)]
        topics: Option<Vec<String>>,
    },
    ApplyTarget {
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

        /// Additional properties for the kafka client, separated by a comma. e.g.:
        /// ssl.key.password=test
        #[arg(short, long)]
        optional_client_properties: Option<Vec<String>>,

        /// Specify topics. If no topics specified, all topics will be used.
        #[arg(short, long)]
        topics: Option<Vec<String>>,
    },
}
