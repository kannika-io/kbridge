use args::{Args, Commands};
use bridge_core::{BridgeClient, KafkaBridgeClient, OffsetSnapshot, errors::BridgeError, helpers};
use clap::Parser;
use comfy_table::Table;
use inquire::Text;
use log::trace;

mod args;

#[tokio::main]
async fn main() -> Result<(), BridgeError> {
    let args = Args::parse();

    env_logger::init();
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
            legacy_offset_header,
            kafka_connection,
            input,
        } => {
            let topics = &kafka_connection.topics.clone();
            let client: KafkaBridgeClient = kafka_connection.into();
            let offset_snapshot = helpers::get_offset_records(&input)?;
            let result = client
                .calculate_target_offsets(legacy_offset_header.as_str(), topics, offset_snapshot)
                .await?;
            print_offset_snapshot(&result);
            Ok(())
        }
        Commands::Apply {
            kafka_connection,
            input,
        } => {
            let topics = &kafka_connection.topics.clone();
            let client: KafkaBridgeClient = kafka_connection.into();
            let offset_snapshot = helpers::get_offset_records(&input)?;
            client
                .apply_target_offsets(topics, offset_snapshot, &|offset_snapshot| {
                    ask_for_confirmation(offset_snapshot)
                })
                .await
        }
    };
    trace!("Execution finished.");
    result
}

fn ask_for_confirmation(offset_snapshot: &OffsetSnapshot) -> bool {
    let mut table = Table::new();

    table.set_header(vec![
        "Consumer Group",
        "Topic",
        "Partition",
        "Target Offset",
    ]);

    for intermediate_result_item in offset_snapshot {
        table.add_row(vec![
            intermediate_result_item.consumer_group.clone(),
            intermediate_result_item.topic.clone(),
            intermediate_result_item.partition.to_string(),
            intermediate_result_item.offset.to_string(),
        ]);
    }
    println!("{table}");
    let prompt = Text::new("The offsets above will be applied. Are you sure? (Y/n)").prompt();
    matches!(prompt, Ok(value) if value == "Y")
}

fn print_offset_snapshot(offset_snapshot: &OffsetSnapshot) {
    for element in offset_snapshot {
        println!(
            "{},{},{},{}",
            element.consumer_group, element.topic, element.partition, element.offset
        );
    }
}
