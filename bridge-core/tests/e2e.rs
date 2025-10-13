use anyhow::Result;
use assert_matches::assert_matches;
use bridge_core::OffsetRecord;
use bridge_core::commands::errors::FetchSourceOffsetsError::FetchMetadataError;
use bridge_core::commands::fetch_source_offsets;
use std::process::Command;
use std::sync::Once;

static INIT: Once = Once::new();

fn setup_test_environment() -> Result<()> {
    INIT.call_once(|| {
        teardown_test_environment().expect("Failed to teardown test environment");
        Command::new("just")
            .args(["setup"])
            .status()
            .expect("Failed to setup test environment");
    });
    Ok(())
}

#[test]
pub fn fetch_source_offsets_with_multiple_topics_filter() -> Result<()> {
    setup_test_environment()?;

    let result = fetch_source_offsets::execute(
        "localhost:9092".to_string(),
        None,
        Some(vec!["orders-1".to_string(), "orders-2".to_string()]),
    )?;

    let expected_offsets = get_expected_offsets();
    
    teardown_test_environment()?;

    // Verify all returned offsets are from the expected topics
    assert!(result.iter().all(|item| item.topic == "orders-1" || item.topic == "orders-2"));
    
    // Verify we got the expected offsets for these topics
    assert!(result.iter().all(|item| expected_offsets.contains(item)));
    
    // Verify we don't have any orders-3 offsets
    assert!(!result.iter().any(|item| item.topic == "orders-3"));

    Ok(())
}

fn teardown_test_environment() -> Result<()> {
    // Note: teardown is now handled by the global INIT setup
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

    let correct_result = get_expected_offsets();

    let result_filtered = fetch_source_offsets::execute(
        "localhost:9092".to_string(),
        None,
        Some(vec!["orders-1".to_string()]),
    )?;

    // Note: teardown is now handled by the global INIT setup

    assert!(result.iter().all(|item| correct_result.contains(item)));

    assert!(
        result
            .iter()
            .all(|item| item.topic != "orders-1" || result_filtered.contains(item))
    );

    Ok(())
}
