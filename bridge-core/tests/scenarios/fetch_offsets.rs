use anyhow::Result;
use assert_matches::assert_matches;
use bridge_core::{BridgeClient, BridgeConfig, KafkaBridgeClient, errors::BridgeError};
use init::{init_logging, setup_test_environment};
use stubs::get_expected_source_offsets;

use crate::{
    init,
    stubs::{self, SOURCE_BOOTSTRAP_SERVER},
};

#[test]
pub fn fetch_source_offsets_when_invalid_broker_url_should_return_error() -> Result<()> {
    let source_config: BridgeConfig = BridgeConfig::new("".to_string(), None, None);
    let source_client: KafkaBridgeClient = source_config.into();
    let invalid_broker_address = source_client.fetch_source_offsets_from_cluster();

    assert_matches!(
        invalid_broker_address,
        Err(BridgeError::FetchSourceOffsetsError(_))
    );

    Ok(())
}

#[test]
pub fn fetch_source_offsets_with_multiple_topics_filter() -> Result<()> {
    init_logging()?;
    setup_test_environment()?;

    let config: BridgeConfig = BridgeConfig::new(
        SOURCE_BOOTSTRAP_SERVER.to_string(),
        None,
        Some(vec!["orders-1".to_string(), "orders-2".to_string()]),
    );
    let client: KafkaBridgeClient = config.into();

    let result = client.fetch_source_offsets_from_cluster()?;

    let expected_offsets = get_expected_source_offsets();

    // Verify all returned offsets are from the expected topics
    assert!(
        result
            .iter()
            .all(|item| item.topic == "orders-1" || item.topic == "orders-2")
    );

    // Verify we got the expected offsets for these topics
    assert!(result.iter().all(|item| expected_offsets.contains(item)));

    // Verify we don't have any orders-3 offsets
    assert!(!result.iter().any(|item| item.topic == "orders-3"));

    Ok(())
}

#[test]
pub fn fetch_source_offsets_should_return_correct_offsets() -> Result<()> {
    init_logging()?;
    setup_test_environment()?;

    let config: BridgeConfig = BridgeConfig::new(
        SOURCE_BOOTSTRAP_SERVER.to_string(),
        None,
        Some(vec!["orders-1".to_string(), "orders-2".to_string()]),
    );
    let client: KafkaBridgeClient = config.into();
    let result = client.fetch_source_offsets_from_cluster()?;
    println!("{:#?}", result);

    let correct_result = get_expected_source_offsets();

    assert!(result.iter().all(|item| correct_result.contains(item)));

    assert!(
        result
            .iter()
            .all(|item| item.topic != "orders-1" || result.contains(item))
    );

    Ok(())
}
