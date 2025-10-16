use anyhow::Result;
use bridge_core::commands::{calculate_target_offsets, fetch_source_offsets};
use init::{init_logging, setup_test_environment};
use log::info;
use stubs::{
    OFFSET_HEADER, ORDERS_1_TOPIC, ORDERS_2_TOPIC, SOURCE_BOOTSTRAP_SERVER,
    TARGET_BOOTSTRAP_SERVER, get_expected_offsets_filtered_by_topics, get_expected_target_offsets,
};

use crate::{init, stubs};

#[tokio::test]
#[ignore]
pub async fn calculate_target_offsets_should_return_expected_offsets() -> Result<()> {
    init_logging()?;
    setup_test_environment()?;
    info!("Fetching source offsets");
    let result = fetch_source_offsets::execute(SOURCE_BOOTSTRAP_SERVER, &None, &None)?;

    info!("Fetching target offsets");
    let target_offsets = calculate_target_offsets::execute(
        TARGET_BOOTSTRAP_SERVER,
        OFFSET_HEADER,
        &None,
        &None,
        result,
    )
    .await?;

    let expected_offsets = get_expected_target_offsets();

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

    info!("Fetching source offsets");
    let result = fetch_source_offsets::execute(SOURCE_BOOTSTRAP_SERVER, &None, &None)?;

    info!("Fetching target offsets");
    let target_offsets = calculate_target_offsets::execute(
        TARGET_BOOTSTRAP_SERVER,
        OFFSET_HEADER,
        &None,
        &Some(topics.clone()),
        result,
    )
    .await?;

    let expected_offsets =
        get_expected_offsets_filtered_by_topics(get_expected_target_offsets().to_vec(), topics);

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
