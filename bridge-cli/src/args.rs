use std::path::PathBuf;

use bridge_core::{BridgeConfig, CsvInput, KafkaBridgeClient};
use clap::{Parser, Subcommand, arg, builder::TypedValueParser, command};

impl From<KafkaConnection> for BridgeConfig {
    fn from(value: KafkaConnection) -> Self {
        BridgeConfig::new(value.bootstrap_server)
            .set_optional_client_properties(value.optional_client_properties)
    }
}

impl From<KafkaConnection> for KafkaBridgeClient {
    fn from(value: KafkaConnection) -> Self {
        let config: BridgeConfig = value.into();
        config.into()
    }
}

#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
pub struct Args {
    #[command(subcommand)]
    pub command: Commands,

    #[arg(short, long, action)]
    pub verbose: bool,
}

#[derive(Clone)]
struct CsvInputParser;

impl TypedValueParser for CsvInputParser {
    type Value = CsvInput;

    fn parse_ref(
        &self,
        _cmd: &clap::Command,
        _arg: Option<&clap::Arg>,
        value: &std::ffi::OsStr,
    ) -> Result<Self::Value, clap::Error> {
        if value == "-" {
            Ok(CsvInput::Stdin)
        } else {
            Ok(CsvInput::File(PathBuf::from(value)))
        }
    }
}

#[derive(Debug, Parser)]
pub struct KafkaConnection {
    /// The bootstrap server URL for the Kafka Broker
    #[arg(short, long)]
    pub bootstrap_server: String,

    /// Additional properties for the kafka client, separated by a '='. e.g.:
    /// ssl.key.password=test
    #[arg(short, long)]
    pub optional_client_properties: Option<Vec<String>>,

    /// Specify topics. If no topics specified, all topics will be used.
    #[arg(short, long)]
    pub topics: Option<Vec<String>>,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    /// Fetches the source offsets from a kafka cluster
    Fetch {
        #[command(flatten)]
        kafka_connection: KafkaConnection,
    },
    /// Calculates target offsets based on message header in target cluster
    Calculate {
        /// Header in target messages that contains the offsets of the source topic
        #[arg(short = 'H', long)]
        offset_header: String,

        #[arg(
            short = 'i',
            long,
            value_name = "FILE",
            value_parser = CsvInputParser,
            help = "Path to CSV file containing the offsets (use '-' for stdin)",
        )]
        input: Option<CsvInput>,

        #[command(flatten)]
        kafka_connection: KafkaConnection,
    },
    /// Restores consumer group(s) in target cluster based on calculated target offsets
    Apply {
        #[arg(
            short = 'i',
            long,
            value_name = "FILE",
            value_parser = CsvInputParser,
            help = "Path to CSV file containing the offsets (use '-' for stdin)",
        )]
        input: Option<CsvInput>,

        #[command(flatten)]
        kafka_connection: KafkaConnection,

        #[arg(short = 'y', help = "Apply offsets without asking for confirmation")]
        skip_confirmation: bool,
    },
}
