use std::process::ExitCode;

use args::{Args, Commands};
use bridge_core::{
    BridgeClient, KafkaBridgeClient, OffsetSnapshot, errors::BridgeError, snapshot::csv::FromCsv,
};
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
        } => {
            let topics = kafka_connection.topics.clone();
            let client: KafkaBridgeClient = kafka_connection.into();

            let result = client
                .fetch_source_offsets_from_cluster(topics, timeout)
                .await?;
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
            trace!("applying target offsets");
            let snapshot = OffsetSnapshot::from_csv(input)?;

            // Handle confirmation in CLI before calling the core library
            if !dry_run && !skip_confirmation && !ask_for_confirmation(&snapshot) {
                return Err(BridgeError::Message(
                    "Operation cancelled by user".to_string(),
                ));
            }

            if dry_run {
                println!("DRY RUN: Would apply the following offsets:");
                print_offset_snapshot(&snapshot);
                Ok(())
            } else {
                client.apply_target_offsets(topics, snapshot, false).await
            }
        }
    }
}
