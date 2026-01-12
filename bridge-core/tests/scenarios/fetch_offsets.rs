use std::time::Duration;

use anyhow::Result;
use assert_matches::assert_matches;
use bridge_core::{BridgeClient, KafkaBridgeClient, KafkaBridgeConfig, errors::BridgeError};
use init::{init_logging, setup_test_environment};
use stubs::get_expected_source_offsets;

use crate::{
    init,
    stubs::{self, SOURCE_BOOTSTRAP_SERVER},
};

#[tokio::test]
pub async fn fetch_source_offsets_when_invalid_broker_url_should_return_error() -> Result<()> {
    let source_config: KafkaBridgeConfig = KafkaBridgeConfig::new("".to_string());
    let source_client: KafkaBridgeClient = source_config.into();
    let invalid_broker_address = source_client
        .fetch_source_offsets_from_cluster(Vec::<String>::new(), Duration::from_secs(5))
        .await;

    assert_matches!(
        invalid_broker_address,
        Err(BridgeError::FetchSourceOffsetsError(_))
    );

    Ok(())
}

#[tokio::test]
pub async fn fetch_source_offsets_with_multiple_topics_filter() -> Result<()> {
    init_logging()?;
    setup_test_environment()?;

    let config: KafkaBridgeConfig = KafkaBridgeConfig::new(SOURCE_BOOTSTRAP_SERVER.to_string());

    let client: KafkaBridgeClient = config.into();

    let result = client
        .fetch_source_offsets_from_cluster(
            vec!["orders-1".to_string(), "orders-2".to_string()],
            Duration::from_secs(5),
        )
        .await?;

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

#[tokio::test]
pub async fn fetch_source_offsets_should_return_correct_offsets() -> Result<()> {
    init_logging()?;
    setup_test_environment()?;

    let config: KafkaBridgeConfig = KafkaBridgeConfig::new(SOURCE_BOOTSTRAP_SERVER.to_string());
    let client: KafkaBridgeClient = config.into();
    let result = client
        .fetch_source_offsets_from_cluster(
            vec!["orders-1".to_string(), "orders-2".to_string()],
            Duration::from_secs(5),
        )
        .await?;

    let correct_result = get_expected_source_offsets();

    assert!(result.iter().all(|item| correct_result.contains(item)));

    assert!(
        result
            .iter()
            .all(|item| item.topic != "orders-1" || result.contains(item))
    );

    Ok(())
}
