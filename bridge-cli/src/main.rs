use std::process::ExitCode;

use args::{Args, Commands, CsvInput};
use bridge_core::{
    BridgeClient, KafkaBridgeClient, OffsetSnapshot, errors::BridgeError, snapshot::csv::FromCsv,
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
use helpers::{ask_for_confirmation, print_offset_snapshot, print_snapshot_summary};
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use std::io::IsTerminal;
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

            let result = match offsets_topic {
                Some(offsets_topic) => {
                    let progress = ProgressBar::with_draw_target(
                        None,
                        ProgressDrawTarget::stderr(),
                    )
                    .with_style(
                        ProgressStyle::with_template(
                            "{msg} [{bar:40}] {pos}/{len} ({percent}%) {per_sec}, ETA {eta}",
                        )
                        .expect("invalid progress bar template"),
                    )
                    .with_message("Reading offsets topic");
                    let result = client
                        .fetch_source_offsets_from_offsets_topic(
                            offsets_topic,
                            topics,
                            timeout,
                            |read, total| {
                                progress.set_length(total);
                                progress.set_position(read);
                            },
                        )
                        .await;
                    progress.finish_and_clear();
                    result?
                }
                None => {
                    client
                        .fetch_source_offsets_from_cluster(topics, timeout)
                        .await?
                }
            };
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

            if !dry_run && !skip_confirmation && !ask_for_confirmation(&snapshot) {
                return Err(BridgeError::Message(
                    "Operation cancelled by user".to_string(),
                ));
            }

            if dry_run {
                eprintln!("DRY RUN: Would apply the following offsets:");
                if std::io::stdout().is_terminal() {
                    print_snapshot_summary(&snapshot);
                } else {
                    print_offset_snapshot(&snapshot);
                }
            }

            client.apply_target_offsets(topics, snapshot, dry_run).await
        }
    }
}
