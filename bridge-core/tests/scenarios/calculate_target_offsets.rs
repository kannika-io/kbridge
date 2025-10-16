use anyhow::Result;
use bridge_core::{BridgeClient, BridgeConfig, KafkaBridgeClient};
use log::info;

use crate::{
    init::{init_logging, setup_test_environment},
    stubs::{
        OFFSET_HEADER, ORDERS_1_TOPIC, ORDERS_2_TOPIC, SOURCE_BOOTSTRAP_SERVER,
        TARGET_BOOTSTRAP_SERVER, get_expected_stub_offsets_filtered_by_topics,
        get_expected_stub_target_offsets,
    },
};

#[tokio::test]
#[ignore]
pub async fn calculate_target_offsets_should_return_expected_offsets() -> Result<()> {
    init_logging()?;
    setup_test_environment()?;
    info!("Fetching source offsets");

    let config: BridgeConfig = BridgeConfig::new(SOURCE_BOOTSTRAP_SERVER.to_string(), None, None);
    let client: KafkaBridgeClient = config.into();

    let result = client.fetch_source_offsets_from_cluster()?;

    info!("Fetching target offsets");
    let target_offsets = client
        .calculate_target_offsets(OFFSET_HEADER, result)
        .await?;

    let expected_offsets = get_expected_stub_target_offsets();

    info!("{:#?}", target_offsets);
    info!("{:#?}", expected_offsets);

    assert!(target_offsets.iter().all(|t| {
        expected_offsets.iter().any(|e| {
            e.topic == t.topic
                && e.offset == t.offset
                && e.consumer_group == t.consumer_group
                && e.partition == t.partition
        })
    }));

    Ok(())
}

#[tokio::test]
#[ignore]
pub async fn calculate_target_offsets_with_filter_should_return_expected_offsets() -> Result<()> {
    init_logging()?;
    setup_test_environment()?;

    let topics = vec![String::from(ORDERS_1_TOPIC), String::from(ORDERS_2_TOPIC)];

    let config: BridgeConfig = BridgeConfig::new(
        SOURCE_BOOTSTRAP_SERVER.to_string(),
        None,
        Some(topics.clone()),
    );
    let source_client: KafkaBridgeClient = config.into();

    info!("Fetching source offsets");
    let result = source_client.fetch_source_offsets_from_cluster()?;

    let config: BridgeConfig = BridgeConfig::new(
        TARGET_BOOTSTRAP_SERVER.to_string(),
        None,
        Some(topics.clone()),
    );
    let target_client: KafkaBridgeClient = config.into();
    info!("Fetching target offsets");
    let target_offsets = target_client
        .calculate_target_offsets(OFFSET_HEADER, result)
        .await?;

    let expected_offsets = get_expected_stub_offsets_filtered_by_topics(
        get_expected_stub_target_offsets().to_vec(),
        topics,
    );

    info!("{:#?}", target_offsets);
    info!("{:#?}", expected_offsets);

    assert!(target_offsets.iter().all(|t| {
        expected_offsets.iter().any(|e| {
            e.topic == t.topic
                && e.offset == t.offset
                && e.consumer_group == t.consumer_group
                && e.partition == t.partition
        })
    }));

    Ok(())
}
