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
use tracing::{info, trace};

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
                    let progress = progress_bar(
                        "Scanning offsets topic",
                        "{msg} [{bar:40}] {pos}/{len} ({percent}%) {per_sec}, ETA {eta}",
                    );
                    let result = client
                        .fetch_source_offsets_from_offsets_topic(
                            &offsets_topic,
                            topics,
                            timeout,
                            |read, total| {
                                progress.set_length(total);
                                progress.set_position(read);
                            },
                        )
                        .await;
                    clear_progress_bar(&progress);
                    let result = result?;
                    info!(
                        "Scanned {} offsets from '{offsets_topic}' in {:.2?}, accumulated {} unique offsets",
                        progress.position(),
                        progress.elapsed(),
                        result.len(),
                    );
                    result
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

            let progress = progress_bar(
                "Mapping partitions",
                "{msg} [{bar:40}] {pos}/{len} ({percent}%), ETA {eta}",
            );
            let result = client
                .calculate_target_offsets(offset_header, topics, snapshot, |done, total| {
                    progress.set_length(total);
                    progress.set_position(done);
                })
                .await;
            clear_progress_bar(&progress);
            let result = result?;
            info!(
                "Mapped {} partitions in {:.2?}, calculated {} offsets",
                progress.position(),
                progress.elapsed(),
                result.len(),
            );

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

/// Creates a progress bar on stderr and registers it with the logger, so log
/// lines emitted while it is drawn don't corrupt it. Hidden when stderr is not
/// a terminal.
fn progress_bar(message: &'static str, template: &str) -> ProgressBar {
    let bar = ProgressBar::with_draw_target(None, ProgressDrawTarget::stderr())
        .with_style(ProgressStyle::with_template(template).expect("invalid progress bar template"))
        .with_message(message);
    logging::set_active_progress(Some(bar.clone()));
    bar
}

/// Clears `bar` from stderr and unregisters it from the logger.
fn clear_progress_bar(bar: &ProgressBar) {
    bar.finish_and_clear();
    logging::set_active_progress(None);
}
