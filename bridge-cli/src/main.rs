use args::{Args, Commands};
use bridge_core::{
    BridgeClient, KafkaBridgeClient, OffsetSnapshot, errors::BridgeError,
    kafka::source::RecordStreamConsumer, snapshot::csv::FromCsv, transform::ConsumerGroupMigrator,
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
        Commands::Fetch {
            kafka_connection,
            timeout,
        } => {
            let topics = kafka_connection.topics.clone();
            let client: KafkaBridgeClient = kafka_connection.into();

            let result = client.fetch_source_offsets_from_cluster(topics, timeout)?;
            print_offset_snapshot(&result);
            Ok(())
        }
        Commands::Calculate {
            offset_header,
            kafka_connection,
            input,
        } => {
            let snapshot = {
                let topics = &kafka_connection.topics.clone();
                OffsetSnapshot::from_csv(input)?.filter_by_topics(topics)
            };

            let consumer = {
                let properties = kafka_connection.to_consumer_properties();
                let (consumer, task) = RecordStreamConsumer::new(properties)?;
                tokio::spawn(task);
                consumer
            };

            let result = {
                let migrator =
                    ConsumerGroupMigrator::new(consumer).search_offset_in_header(&offset_header);
                migrator.migrate(&snapshot).await?
            };

            print_offset_snapshot(&result);
            Ok(())
        }
        Commands::Apply {
            kafka_connection,
            input,
            skip_confirmation,
        } => {
            let topics = kafka_connection.topics.clone();
            let client: KafkaBridgeClient = kafka_connection.into();
            trace!("applying target offsets");
            let snapshot = OffsetSnapshot::from_csv(input)?;

            let confirmation_clojure = match skip_confirmation {
                true => |_: &OffsetSnapshot| true,
                false => |snapshot: &OffsetSnapshot| ask_for_confirmation(snapshot),
            };

            client
                .apply_target_offsets(topics, snapshot, &confirmation_clojure)
                .await
        }
    };
    trace!("Execution finished.");
    result
}
