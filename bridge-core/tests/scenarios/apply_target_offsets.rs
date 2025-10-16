use anyhow::Result;
use bridge_core::{
    commands::{apply_target_offsets::execute, calculate_target_offsets, fetch_source_offsets},
    kafka::{
        admin::initialize_admin,
        client_config::{ConfigBuilder, GROUP_ID_KEY},
        consumer::setup_consumer_and_metadata,
    },
};
use init::{init_logging, setup_test_environment};
use log::info;
use rdkafka::{
    ClientConfig, TopicPartitionList, admin::AdminOptions, consumer::Consumer, util::Timeout,
};
use stubs::{
    CONSUMER_GROUP_1, CONSUMER_GROUP_2, OFFSET_HEADER, ORDERS_1_TOPIC, SOURCE_BOOTSTRAP_SERVER,
    TARGET_BOOTSTRAP_SERVER, get_expected_offsets_filtered_by_topics, get_expected_target_offsets,
};

use crate::{init, stubs};

#[tokio::test]
pub async fn apply_target_offsets_with_filter_should_return_expected_offsets() -> Result<()> {
    init_logging()?;
    setup_test_environment()?;

    let consumer_group_id = String::from("testing");

    let topics: Vec<String> = vec![String::from(ORDERS_1_TOPIC)];

    let mut client_config = ClientConfig::new();
    client_config.set_bootstrap_server(TARGET_BOOTSTRAP_SERVER);

    let admin_client = initialize_admin(&mut client_config)?;

    // Delete previous results (if present)
    admin_client
        .delete_groups(&[consumer_group_id.as_str()], &AdminOptions::new())
        .await?;

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

    info!("{:#?}", target_offsets);

    execute(
        TARGET_BOOTSTRAP_SERVER,
        &None,
        &Some(topics.clone()),
        target_offsets,
        &|_| true,
    )
    .await?;

    verify_consumer(topics.clone(), CONSUMER_GROUP_1).await?;
    verify_consumer(topics.clone(), CONSUMER_GROUP_2).await?;

    Ok(())
}

async fn verify_consumer(topics: Vec<String>, consumer_group: &str) -> Result<()> {
    let mut consumer_client_config = ClientConfig::new();
    let topic_references: Vec<&str> = topics.iter().map(|t| t.as_str()).collect();

    consumer_client_config.set_bootstrap_server(TARGET_BOOTSTRAP_SERVER);
    consumer_client_config
        .set_optional_properties(&Some(vec![format!("{GROUP_ID_KEY}={consumer_group}")]));

    let (consumer, metadata) =
        setup_consumer_and_metadata(&topic_references, &mut consumer_client_config).await?;

    let mut tpl = TopicPartitionList::new();

    for item in metadata
        .topics()
        .iter()
        .flat_map(|t| (t.partitions().iter().map(|p| (p.id(), t.name()))))
    {
        tpl.add_partition(item.1, item.0);
    }

    let committed_offsets = consumer
        .committed_offsets(tpl, Timeout::Never)?
        .to_topic_map();

    let expected_targets = get_expected_offsets_filtered_by_topics(
        get_expected_target_offsets().to_vec(),
        topics.to_vec(),
    );

    info!("{:#?}", committed_offsets);
    info!("{:#?}", expected_targets);

    assert!(
        expected_targets
            .iter()
            .filter(|t| t.consumer_group == consumer_group)
            .all(|e| {
                committed_offsets
                    .get(&(e.topic.clone(), e.partition))
                    .is_some_and(|r| r.to_raw().is_some_and(|v| v == e.offset))
            })
    );
    Ok(())
}
