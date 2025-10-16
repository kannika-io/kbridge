use args::{Args, Commands};
use bridge_core::{errors::BridgeError, helpers, BridgeClient, KafkaBridgeClient, OffsetSnapshot};
use clap::Parser;
use comfy_table::Table;
use inquire::Text;
use log::trace;

mod args;
mod errors;

#[tokio::main]
async fn main() -> Result<(), BridgeError> {
    let args = Args::parse();

    env_logger::init();
    trace!("Executing with following arguments: {:?}", args);

    let result: Result<(), BridgeError> = match args.command {
        Commands::FetchSource { kafka_connection } => {
            let client: KafkaBridgeClient = kafka_connection.into();

            let result = client
                .fetch_source_offsets_from_cluster()
                .map_err(BridgeError::from)?;
            print_offset_snapshot(&result);
            Ok(())
        }
        Commands::CalculateTarget {
            legacy_offset_header,
            kafka_connection,
            input,
        } => {
            let client: KafkaBridgeClient = kafka_connection.into();
            let offset_snapshot = helpers::get_offset_records(&input)?;
            let result = client
                .calculate_target_offsets(legacy_offset_header.as_str(), offset_snapshot)
                .await
                .map_err(BridgeError::from)?;
            print_offset_snapshot(&result);
            Ok(())
        }
        Commands::ApplyTarget {
            kafka_connection,
            input,
        } => {
            let client: KafkaBridgeClient = kafka_connection.into();
            let offset_snapshot = helpers::get_offset_records(&input)?;
            client
                .apply_target_offsets(offset_snapshot, &|offset_snapshot| {
                    ask_for_confirmation(offset_snapshot)
                })
                .await
                .map_err(|e| e.into())
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
