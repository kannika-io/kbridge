use args::{Args, Commands};
use bridge_core::{
    BridgeClient, KafkaBridgeClient, OffsetSnapshot, errors::BridgeError,
    helpers::get_offset_records,
};
use clap::Parser;
use helpers::{ask_for_confirmation, print_offset_snapshot};
use log::trace;

mod args;
mod helpers;

#[tokio::main]
async fn main() -> Result<(), BridgeError> {
    let args = Args::parse();

    if args.verbose {
        env_logger::builder()
            .filter_level(log::LevelFilter::Trace)
            .init();
    } else {
        env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .init();
    }

    trace!("Executing with following arguments: {:?}", args);

    let result: Result<(), BridgeError> = match args.command {
        Commands::Fetch { kafka_connection } => {
            let topics = &kafka_connection.topics.clone();
            let client: KafkaBridgeClient = kafka_connection.into();

            let result = client.fetch_source_offsets_from_cluster(topics)?;
            print_offset_snapshot(&result);
            Ok(())
        }
        Commands::Calculate {
            offset_header,
            kafka_connection,
            input,
        } => {
            let topics = &kafka_connection.topics.clone();
            let client: KafkaBridgeClient = kafka_connection.into();
            let offset_snapshot = get_offset_records(&input)?;
            let result = client
                .calculate_target_offsets(offset_header.as_str(), topics, offset_snapshot)
                .await?;
            print_offset_snapshot(&result);
            Ok(())
        }
        Commands::Apply {
            kafka_connection,
            input,
            skip_confirmation,
        } => {
            let topics = &kafka_connection.topics.clone();
            let client: KafkaBridgeClient = kafka_connection.into();
            let offset_snapshot = get_offset_records(&input)?;

            let confirmation_clojure = match skip_confirmation {
                true => |_: &OffsetSnapshot| true,
                false => |offset_snapshot: &OffsetSnapshot| ask_for_confirmation(offset_snapshot),
            };

            client
                .apply_target_offsets(topics, offset_snapshot, &confirmation_clojure)
                .await
        }
    };
    trace!("Execution finished.");
    result
}
