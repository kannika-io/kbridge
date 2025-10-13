use anyhow::Result;
use assert_matches::assert_matches;
use bridge_core::OffsetRecord;
use bridge_core::commands::errors::FetchSourceOffsetsError::FetchMetadataError;
use bridge_core::commands::fetch_source_offsets;
use init::TestEnvironment;

mod init;

#[test]
pub fn fetch_source_offsets_with_multiple_topics_filter() -> Result<()> {
    let _env = TestEnvironment::new()?;

    let result = fetch_source_offsets::execute(
        "localhost:9092".to_string(),
        None,
        Some(vec!["orders-1".to_string(), "orders-2".to_string()]),
    )?;

    let expected_offsets = get_expected_offsets();

    // Verify all returned offsets are from the expected topics
    assert!(result.iter().all(|item| item.topic == "orders-1" || item.topic == "orders-2"));
    
    // Verify we got the expected offsets for these topics
    assert!(result.iter().all(|item| expected_offsets.contains(item)));
    
    // Verify we don't have any orders-3 offsets
    assert!(!result.iter().any(|item| item.topic == "orders-3"));

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
    let _env = TestEnvironment::new()?;

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
