use std::fmt::Display;
use std::io::{Cursor, Read, stdin};
use std::path::PathBuf;
use std::{collections::HashMap, io, time::Duration};

use bridge_core::kafka::properties::KafkaConsumerProperties;
use bridge_core::{KafkaBridgeClient, KafkaBridgeConfig};
use clap::{Parser, Subcommand, builder::TypedValueParser};

impl From<KafkaConnection> for KafkaBridgeConfig {
    fn from(conn: KafkaConnection) -> Self {
        KafkaBridgeConfig::new(conn.bootstrap_server).set_properties(conn.properties)
    }
}

impl From<KafkaConnection> for KafkaBridgeClient {
    fn from(value: KafkaConnection) -> Self {
        let config: KafkaBridgeConfig = value.into();
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
    type Value = HashMap<String, String>;
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

        // Handle empty default value
        if value_str.is_empty() {
            return Ok(HashMap::new());
        }

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
            required = false,
            default_value = "",
        )]
    pub properties: HashMap<String, String>,

    /// Specify topics. If no topics specified, all topics will be used.
    #[arg(short, long)]
    pub topics: Vec<String>,
}

impl KafkaConnection {
    pub fn to_consumer_properties(&self) -> KafkaConsumerProperties {
        let mut props = KafkaConsumerProperties::from_iter(self.properties.clone().into_iter());
        props.insert("bootstrap.servers", self.bootstrap_server.clone());
        props
    }
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
        input: CsvInput,

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
        input: CsvInput,

        #[command(flatten)]
        kafka_connection: KafkaConnection,

        #[arg(
            short = 'y',
            long,
            help = "Apply offsets without asking for confirmation"
        )]
        skip_confirmation: bool,

        #[arg(
            short = 'n',
            long,
            help = "Validate the operation without actually applying offsets"
        )]
        dry_run: bool,
    },
}

#[derive(Debug, Clone, Default)]
pub enum CsvInput {
    #[default]
    Stdin,
    File(PathBuf),
}

impl CsvInput {
    /// Returns a reader that reads all input into memory.
    /// Blocks until EOF for stdin (handles piped input correctly).
    pub fn into_reader(self) -> io::Result<Cursor<Vec<u8>>> {
        match self {
            CsvInput::Stdin => {
                let mut buf = Vec::new();
                stdin().lock().read_to_end(&mut buf)?;
                Ok(Cursor::new(buf))
            }
            CsvInput::File(path) => {
                let buf = std::fs::read(path)?;
                Ok(Cursor::new(buf))
            }
        }
    }
}

impl Display for CsvInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CsvInput::Stdin => f.write_str("stdin"),
            CsvInput::File(path) => write!(f, "{}", path.display()),
        }
    }
}
