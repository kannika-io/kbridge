use anyhow::Result;
use assert_matches::assert_matches;
use bridge_core::OffsetRecord;
use bridge_core::commands::errors::FetchSourceOffsetsError::FetchMetadataError;
use bridge_core::commands::fetch_source_offsets;
use std::process::Command;

fn setup_test_environment() -> Result<()> {
    Command::new("just").args(["teardown"]).status()?;
    Command::new("just").args(["setup"]).status()?;
    Ok(())
}

fn teardown_test_environment() -> Result<()> {
    Command::new("just").args(["teardown"]).status()?;
    Ok(())
}

fn get_expected_offsets() -> [OffsetRecord; 6] {
    [
        OffsetRecord {
            topic: "orders-1".to_string(),
            partition: 0,
            offset: 563,
            consumer_group: "console-consumer-2".to_string(),
        },
        OffsetRecord {
            topic: "orders-2".to_string(),
            partition: 0,
            offset: 772,
            consumer_group: "console-consumer-2".to_string(),
        },
        OffsetRecord {
            topic: "orders-3".to_string(),
            partition: 0,
            offset: 802,
            consumer_group: "console-consumer-2".to_string(),
        },
        OffsetRecord {
            topic: "orders-1".to_string(),
            partition: 0,
            offset: 1000,
            consumer_group: "console-consumer".to_string(),
        },
        OffsetRecord {
            topic: "orders-2".to_string(),
            partition: 0,
            offset: 1300,
            consumer_group: "console-consumer".to_string(),
        },
        OffsetRecord {
            topic: "orders-3".to_string(),
            partition: 0,
            offset: 500,
            consumer_group: "console-consumer".to_string(),
        },
    ]
}

#[test]
pub fn fetch_source_offsets_should_return_correct_offsets() -> Result<()> {
    setup_test_environment()?;

    let invalid_broker_address = fetch_source_offsets::execute("".to_string(), None, None);
    assert_matches!(invalid_broker_address, Err(FetchMetadataError(_)));

    let result = fetch_source_offsets::execute("localhost:9092".to_string(), None, None)?;
    println!("{:#?}", result);

    let correct_result = [
        OffsetRecord {
            topic: "orders-1".to_string(),
            partition: 0,
            offset: 563,
            consumer_group: "console-consumer-2".to_string(),
        },
        OffsetRecord {
            topic: "orders-2".to_string(),
            partition: 0,
            offset: 772,
            consumer_group: "console-consumer-2".to_string(),
        },
        OffsetRecord {
            topic: "orders-3".to_string(),
            partition: 0,
            offset: 802,
            consumer_group: "console-consumer-2".to_string(),
        },
        OffsetRecord {
            topic: "orders-1".to_string(),
            partition: 0,
            offset: 1000,
            consumer_group: "console-consumer".to_string(),
        },
        OffsetRecord {
            topic: "orders-2".to_string(),
            partition: 0,
            offset: 1300,
            consumer_group: "console-consumer".to_string(),
        },
        OffsetRecord {
            topic: "orders-3".to_string(),
            partition: 0,
            offset: 500,
            consumer_group: "console-consumer".to_string(),
        },
    ];

    let result_filtered = fetch_source_offsets::execute(
        "localhost:9092".to_string(),
        None,
        Some(vec!["orders-1".to_string()]),
    )?;

    Command::new("just").args(["teardown"]).status()?;

    assert!(result.iter().all(|item| correct_result.contains(item)));

    assert!(
        result
            .iter()
            .all(|item| item.topic != "orders-1" || result_filtered.contains(item))
    );

    Ok(())
}
