use std::path::PathBuf;
use std::{collections::HashMap, time::Duration};

use bridge_core::{BridgeConfig, CsvInput, KafkaBridgeClient, Properties};
use clap::{Parser, Subcommand, arg, builder::TypedValueParser, command};

impl From<KafkaConnection> for BridgeConfig {
    fn from(value: KafkaConnection) -> Self {
        let merged_properties = value.optional_client_properties.map(|props_vec| {
            props_vec
                .into_iter()
                .fold(HashMap::new(), |mut acc, props| {
                    acc.extend(props);
                    acc
                })
        });

        BridgeConfig::new(value.bootstrap_server).set_optional_client_properties(merged_properties)
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

#[derive(Clone)]
struct DurationParser;

impl TypedValueParser for DurationParser {
    type Value = Duration;

    fn parse_ref(
        &self,
        _cmd: &clap::Command,
        _arg: Option<&clap::Arg>,
        value: &std::ffi::OsStr,
    ) -> Result<Self::Value, clap::Error> {
        let value_str = value.to_str().ok_or_else(|| {
            clap::Error::raw(
                clap::error::ErrorKind::InvalidValue,
                "Duration value contains invalid UTF-8",
            )
        })?;

        // Try to parse as plain seconds first
        match value_str.parse::<u64>() {
            Ok(seconds) => Ok(Duration::from_secs(seconds)),
            Err(err) => Err(clap::Error::raw(
                clap::error::ErrorKind::InvalidValue,
                format!(
                    "{} could not be parsed to seconds. Reason: {}",
                    value_str, err
                ),
            )),
        }
    }
}

#[derive(Clone)]
struct PropertiesInputParser;

impl TypedValueParser for PropertiesInputParser {
    type Value = Properties;
    fn parse_ref(
        &self,
        _cmd: &clap::Command,
        _arg: Option<&clap::Arg>,
        value: &std::ffi::OsStr,
    ) -> Result<Self::Value, clap::Error> {
        let value_str = value.to_str().ok_or_else(|| {
            clap::Error::raw(
                clap::error::ErrorKind::InvalidValue,
                "Property value contains invalid UTF-8",
            )
        })?;

        let mut properties = HashMap::new();
        if let Some((key, val)) = value_str.split_once('=') {
            properties.insert(key.to_string(), val.to_string());
        } else {
            return Err(clap::Error::raw(
                clap::error::ErrorKind::InvalidValue,
                format!("Property '{}' must be in format 'key=value'", value_str),
            ));
        }

        Ok(properties)
    }
}

#[derive(Debug, Parser)]
pub struct KafkaConnection {
    /// The bootstrap server URL for the Kafka Broker
    #[arg(short, long)]
    pub bootstrap_server: String,

    /// Additional properties for the kafka client, separated by a '='. e.g.:
    /// ssl.key.password=test
    #[arg(
            short = 'p',
            long,
            value_parser = PropertiesInputParser,
            action = clap::ArgAction::Append,
        )]
    pub optional_client_properties: Option<Vec<Properties>>,

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

        /// Timeout for api requests to kafka server, in seconds
        #[arg(short = 'T', long, value_parser = DurationParser, default_value = "5")]
        timeout: Duration,
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
