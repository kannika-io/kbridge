use std::process::ExitCode;

use args::{Args, Commands, CsvInput};
use bridge_core::{
    BridgeClient, KafkaBridgeClient, OffsetSnapshot, OffsetSource, errors::BridgeError,
    snapshot::csv::FromCsv,
};

impl FromCsv<CsvInput> for OffsetSnapshot {
    type Err = BridgeError;

    fn from_csv(input: CsvInput) -> Result<Self, Self::Err> {
        let reader = input
            .into_reader()
            .map_err(|e| BridgeError::Message(e.to_string()))?;
        OffsetSnapshot::from_csv(reader).map_err(BridgeError::from)
    }
}
use clap::Parser;
use helpers::{ask_for_confirmation, print_offset_snapshot};
use tracing::trace;

mod args;
mod helpers;
mod logging;

#[tokio::main]
async fn main() -> ExitCode {
    let args = Args::parse();

    logging::init(args.verbose);

    trace!("Executing with following arguments: {:?}", args);

    let result = run(args).await;

    trace!("Execution finished.");

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            logging::print_error(&err);
            ExitCode::FAILURE
        }
    }
}

async fn run(args: Args) -> Result<(), BridgeError> {
    match args.command {
        Commands::Fetch {
            kafka_connection,
            timeout,
            offsets_topic,
        } => {
            let topics = kafka_connection.topics.clone();
            let client: KafkaBridgeClient = kafka_connection.into();

            let source = match offsets_topic {
                Some(offsets_topic) => OffsetSource::Topic(offsets_topic),
                None => OffsetSource::GroupCoordinator,
            };

            let result = client.fetch_offsets(topics, timeout, source).await?;
            print_offset_snapshot(&result);
            Ok(())
        }
        Commands::Calculate {
            offset_header,
            kafka_connection,
            input,
        } => {
            let topics: Vec<String> = kafka_connection.topics.clone();
            let client: KafkaBridgeClient = kafka_connection.into();

            let snapshot = OffsetSnapshot::from_csv(input)?;

            let result = client
                .calculate_target_offsets(offset_header, topics, snapshot)
                .await?;

            print_offset_snapshot(&result);
            Ok(())
        }
        Commands::Apply {
            kafka_connection,
            input,
            skip_confirmation,
            dry_run,
        } => {
            let topics = kafka_connection.topics.clone();
            let client: KafkaBridgeClient = kafka_connection.into();

            let snapshot = {
                let snapshot = OffsetSnapshot::from_csv(input)?;
                snapshot.filter_by_topics(&topics)
            };

            if snapshot.is_empty() {
                // TODO decouple from command error
                return Err(BridgeError::ApplyOffsets(
                    bridge_core::commands::apply_target_offsets::errors::ApplyOffsetsError::NoOffsetsToApply,
                ));
            }

            // Only show confirmation/dry-run output if snapshot has data
            // (core library validates if offsets remain after topic filtering)
            if !snapshot.is_empty() {
                if !dry_run && !skip_confirmation && !ask_for_confirmation(&snapshot) {
                    return Err(BridgeError::Message(
                        "Operation cancelled by user".to_string(),
                    ));
                }

                if dry_run {
                    eprintln!("DRY RUN: Would apply the following offsets:");
                    print_offset_snapshot(&snapshot);
                }
            }

            client.apply_target_offsets(topics, snapshot, dry_run).await
        }
    }
}
