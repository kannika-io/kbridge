use std::time::Duration;

use anyhow::Result;
use bridge_core::{BridgeClient, KafkaBridgeClient, KafkaBridgeConfig, OffsetSnapshot};
use log::info;

use bridge_core::test;

use crate::{
    init::{init_logging, setup_test_environment},
    stubs::{
        OFFSET_HEADER, ORDERS_1_TOPIC, ORDERS_2_TOPIC, SOURCE_BOOTSTRAP_SERVER,
        TARGET_BOOTSTRAP_SERVER, get_expected_stub_target_offsets,
    },
};

#[tokio::test]
pub async fn calculate_target_offsets_should_return_expected_offsets() -> Result<()> {
    init_logging()?;
    setup_test_environment()?;

    let topics: Vec<String> = vec![];

    let source_offsets: OffsetSnapshot = {
        let config: KafkaBridgeConfig = KafkaBridgeConfig::new(SOURCE_BOOTSTRAP_SERVER.to_string());
        let client: KafkaBridgeClient = config.into();

        client
            .fetch_source_offsets_from_cluster(topics.clone(), Duration::from_secs(5))
            .await?
    };

    let _ = source_offsets.print_csv(&mut std::io::stdout());

    let target_offsets: OffsetSnapshot = {
        let config: KafkaBridgeConfig = KafkaBridgeConfig::new(TARGET_BOOTSTRAP_SERVER.to_string());
        let client: KafkaBridgeClient = config.into();

        client
            .calculate_target_offsets(OFFSET_HEADER, topics, source_offsets)
            .await?
    };
    let _ = target_offsets.print_csv(&mut std::io::stdout());

    let expected_offsets = get_expected_stub_target_offsets();
    let _ = expected_offsets.print_csv(&mut std::io::stdout());

    test::snapshot::assert_eq(target_offsets, expected_offsets);

    Ok(())
}

#[tokio::test]
pub async fn calculate_target_offsets_with_filter_should_return_expected_offsets() -> Result<()> {
    init_logging()?;
    setup_test_environment()?;

    let topics = vec![String::from(ORDERS_1_TOPIC), String::from(ORDERS_2_TOPIC)];

    let source_offsets = {
        let config: KafkaBridgeConfig = KafkaBridgeConfig::new(SOURCE_BOOTSTRAP_SERVER.to_string());
        let client: KafkaBridgeClient = config.into();

        client
            .fetch_source_offsets_from_cluster(topics.clone(), Duration::from_secs(5))
            .await?
    };

    let target_offsets = {
        let config: KafkaBridgeConfig = KafkaBridgeConfig::new(TARGET_BOOTSTRAP_SERVER.to_string());
        let client: KafkaBridgeClient = config.into();

        client
            .calculate_target_offsets(OFFSET_HEADER, topics.clone(), source_offsets)
            .await?
    };

    let expected_offsets = get_expected_stub_target_offsets().filter_by_topics(&topics);

    test::snapshot::assert_eq(target_offsets, expected_offsets);

    Ok(())
}
