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
use bridge_core::transform::{MigrationProgress, SearchProgress};
use clap::Parser;
use helpers::{ask_for_confirmation, print_offset_snapshot, print_snapshot_summary};
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
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

            // One bar for all partitions, one for the binary search in the current
            // partition. Labels share a column so both bars line up.
            const LABEL: &str = "Mapping partitions";
            let width = snapshot
                .iter()
                .map(|r| format!("{}/{}", r.topic, r.partition).len())
                .chain([LABEL.len()])
                .max()
                .unwrap_or_default()
                .min(MAX_LABEL_WIDTH);
            let bars = MultiProgress::with_draw_target(ProgressDrawTarget::stderr());
            let progress = bars.add(progress_bar(
                LABEL,
                &format!(
                    "{{msg:{width}!}} [{{bar:40}}] {{percent:>3}}% {{pos}}/{{len}} partitions"
                ),
            ));
            let search = bars.add(ProgressBar::new(SEARCH_STEPS).with_style(progress_style(
                &format!("{{prefix:{width}!}} [{{bar:40}}] {{percent:>3}}% {{msg}}"),
            )));
            let result = client
                .calculate_target_offsets(offset_header, topics, snapshot, |event| match event {
                    MigrationProgress::Mapped { done, total } => {
                        progress.set_length(total);
                        progress.set_position(done);
                    }
                    MigrationProgress::Partition { topic, partition } => {
                        search.reset();
                        search.set_prefix(format!("{topic}/{partition}"));
                        search.set_message("");
                    }
                    MigrationProgress::Search(step) => {
                        search.set_position((step.narrowed * SEARCH_STEPS as f64) as u64);
                        search.set_message(format_search(&step));
                    }
                })
                .await;
            search.finish_and_clear();
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
        .with_style(progress_style(template))
        .with_message(message);
    logging::set_active_progress(Some(bar.clone()));
    bar
}

fn progress_style(template: &str) -> ProgressStyle {
    ProgressStyle::with_template(template).expect("invalid progress bar template")
}

/// Widest label before a progress bar; longer `topic/partition` labels are truncated.
const MAX_LABEL_WIDTH: usize = 40;

/// Resolution of the binary search bar, which tracks a fraction.
const SEARCH_STEPS: u64 = 1000;

/// Describes a binary search step, e.g. `1/2 offsets, 9 seeks, window 1200..1450`.
fn format_search(step: &SearchProgress) -> String {
    let mut message = format!(
        "{}/{} offsets, {} seeks",
        step.mapped, step.offsets, step.seeks
    );
    if !step.window.is_empty() {
        message += &format!(", window {}..{}", step.window.low, step.window.high);
    }
    message
}

/// Clears `bar` from stderr and unregisters it from the logger.
fn clear_progress_bar(bar: &ProgressBar) {
    bar.finish_and_clear();
    logging::set_active_progress(None);
}
