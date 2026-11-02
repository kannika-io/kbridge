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
use bridge_core::PartitionNumber;
use bridge_core::transform::{MigrationProgress, SearchProgress};
use clap::Parser;
use helpers::{ask_for_confirmation, print_offset_snapshot, print_snapshot_summary};
use indicatif::{ProgressBar, ProgressStyle};
use logging::add_progress_bar;
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
                        "{msg} [{bar:40}] {pos}/{len} ({percent}%) {per_sec}, ETA {eta}",
                    )
                    .with_message("Scanning offsets topic");
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
                    // Clear the bar before the summary, so it doesn't redraw below it
                    progress.finish_and_clear();
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
                .filter(|r| topics.is_empty() || topics.contains(&r.topic))
                .map(|r| label_width(&r.topic, r.partition))
                .fold(LABEL.len(), usize::max)
                .min(MAX_LABEL_WIDTH);
            let progress = progress_bar(&format!(
                "{{msg:{width}!}} [{{bar:40}}] {{percent:>3}}% {{pos}}/{{len}} partitions"
            ))
            .with_message(LABEL);
            let search_style = progress_style(&format!(
                "{{prefix:{width}!}} [{{bar:40}}] {{percent:>3}}% {{msg}}"
            ));
            // Shown from the first search step on, so it doesn't sit blank at 0%
            let mut search: Option<ProgressBar> = None;
            let mut current = String::new();
            let mut searched = false;
            let result = client
                .calculate_target_offsets(offset_header, topics, snapshot, |event| match event {
                    MigrationProgress::Mapped { done, total } => {
                        progress.set_length(total);
                        progress.set_position(done);
                        // Empty target partitions are mapped without a search
                        if let (Some(bar), false, true) = (&search, searched, done > 0) {
                            bar.set_position(SEARCH_STEPS);
                            bar.set_message("empty partition, nothing to search");
                        }
                    }
                    MigrationProgress::Partition { topic, partition } => {
                        current = format!("{topic}/{partition}");
                        searched = false;
                        if let Some(bar) = &search {
                            bar.reset();
                            bar.set_prefix(current.clone());
                            bar.set_message("");
                        }
                    }
                    MigrationProgress::Search(step) => {
                        searched = true;
                        let bar = search.get_or_insert_with(|| {
                            add_progress_bar(
                                ProgressBar::new(SEARCH_STEPS)
                                    .with_style(search_style.clone())
                                    .with_prefix(current.clone()),
                            )
                        });
                        bar.set_position((step.narrowed * SEARCH_STEPS as f64) as u64);
                        bar.set_message(format_search(&step));
                    }
                })
                .await;
            // Clear the bars before the summary, so they don't redraw below it
            if let Some(bar) = search {
                bar.finish_and_clear();
            }
            progress.finish_and_clear();
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

/// Creates a progress bar on stderr that log lines are written around; see
/// [`logging::add_progress_bar`].
fn progress_bar(template: &str) -> ProgressBar {
    add_progress_bar(ProgressBar::no_length().with_style(progress_style(template)))
}

fn progress_style(template: &str) -> ProgressStyle {
    ProgressStyle::with_template(template).expect("invalid progress bar template")
}

/// Widest label before a progress bar; longer `topic/partition` labels are truncated.
const MAX_LABEL_WIDTH: usize = 40;

/// Length of the `topic/partition` label, without allocating it.
fn label_width(topic: &str, partition: PartitionNumber) -> usize {
    let digits = partition
        .unsigned_abs()
        .checked_ilog10()
        .map_or(1, |d| d as usize + 1);
    topic.len() + 1 + usize::from(partition < 0) + digits
}

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

#[cfg(test)]
mod tests {
    use super::label_width;

    #[test]
    fn label_width_matches_formatted_label() {
        for (topic, partition) in [
            ("orders", 0),
            ("orders", 9),
            ("orders", 10),
            ("a.b", 1234),
            ("", -7),
        ] {
            assert_eq!(
                label_width(topic, partition),
                format!("{topic}/{partition}").len(),
                "{topic}/{partition}"
            );
        }
    }
}
