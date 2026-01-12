use std::{collections::HashMap, time::Duration};

use anyhow::Result;
use bridge_core::{
    BridgeClient, KafkaBridgeClient, KafkaBridgeConfig, TopicName,
    kafka::{
        client_config::{ConfigBuilder, GROUP_ID_KEY},
        consumer::setup_consumer_and_metadata,
        properties::KafkaConsumerProperties,
        source::RecordStreamConsumer,
    },
    transform::ConsumerGroupMigrator,
};
use init::{init_logging, setup_test_environment};
use log::info;
use rdkafka::{
    ClientConfig, TopicPartitionList,
    admin::{AdminClient, AdminOptions},
    client::DefaultClientContext,
    config::FromClientConfig,
    consumer::Consumer,
    error::KafkaError,
    util::Timeout,
};
use stubs::{
    CONSUMER_GROUP_1, CONSUMER_GROUP_2, OFFSET_HEADER, ORDERS_1_TOPIC, SOURCE_BOOTSTRAP_SERVER,
    TARGET_BOOTSTRAP_SERVER, get_expected_stub_offsets_filtered_by_topics,
    get_expected_stub_target_offsets,
};

use crate::{init, stubs};

fn initialize_admin(
    config: &mut ClientConfig,
) -> Result<AdminClient<DefaultClientContext>, KafkaError> {
    AdminClient::<DefaultClientContext>::from_config(config)
}

#[tokio::test]
pub async fn apply_target_offsets_with_filter_should_return_expected_offsets() -> Result<()> {
    init_logging()?;
    setup_test_environment()?;

    let consumer_group_id = String::from("testing");

    let topics: Vec<TopicName> = vec![ORDERS_1_TOPIC.to_string()];

    let mut client_config = ClientConfig::new().set_bootstrap_server(TARGET_BOOTSTRAP_SERVER);

    let admin_client = initialize_admin(&mut client_config)?;

    // Delete previous results (if present)
    admin_client
        .delete_groups(&[consumer_group_id.as_str()], &AdminOptions::new())
        .await?;

    let source_config: KafkaBridgeConfig =
        KafkaBridgeConfig::new(SOURCE_BOOTSTRAP_SERVER.to_string());
    let source_client: KafkaBridgeClient = source_config.into();

    info!("Fetching source offsets");
    let result = source_client
        .fetch_source_offsets_from_cluster(topics.clone(), Duration::from_secs(5))
        .await?;

    let target_config: KafkaBridgeConfig =
        KafkaBridgeConfig::new(TARGET_BOOTSTRAP_SERVER.to_string());
    let target_client: KafkaBridgeClient = target_config.into();
    info!("Fetching target offsets");
    let target_offsets = target_client
        .calculate_target_offsets(OFFSET_HEADER, topics.clone(), result)
        .await?;

    info!("{:#?}", target_offsets);

    target_client
        .apply_target_offsets(topics.clone(), target_offsets, false)
        .await?;

    verify_consumer(topics.clone(), CONSUMER_GROUP_1).await?;
    verify_consumer(topics.clone(), CONSUMER_GROUP_2).await?;

    Ok(())
}

async fn verify_consumer(
    topics: impl IntoIterator<Item = TopicName>,
    consumer_group: &str,
) -> Result<()> {
    let topics = topics.into_iter().collect::<Vec<_>>();

    let mut consumer_client_config = ClientConfig::new()
        .set_bootstrap_server(TARGET_BOOTSTRAP_SERVER)
        .set_properties(&HashMap::from([(
            GROUP_ID_KEY.to_string(),
            consumer_group.to_string(),
        )]));

    let (consumer, metadata) = setup_consumer_and_metadata(&mut consumer_client_config).await?;

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

    let expected_targets = get_expected_stub_offsets_filtered_by_topics(
        get_expected_stub_target_offsets().to_vec(),
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
