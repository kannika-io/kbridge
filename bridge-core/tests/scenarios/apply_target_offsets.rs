use std::{collections::HashMap, time::Duration};

use anyhow::Result;
use bridge_core::{
    BridgeClient, KafkaBridgeClient, KafkaBridgeConfig, OffsetSnapshot, OffsetSource, TopicName,
    kafka::{
        client_config::{ConfigBuilder, GROUP_ID_KEY},
        consumer::setup_consumer_and_metadata,
    },
};
use init::{init_logging, setup_test_environment};
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
    TARGET_BOOTSTRAP_SERVER,
};
use tokio::time::sleep;
use tracing::info;

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
        .fetch_offsets(
            topics.clone(),
            Duration::from_secs(5),
            OffsetSource::GroupCoordinator,
        )
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
        .apply_target_offsets(topics.clone(), target_offsets.clone(), false)
        .await?;

    verify_consumer_with_retry(topics.clone(), CONSUMER_GROUP_1, &target_offsets).await?;
    verify_consumer_with_retry(topics.clone(), CONSUMER_GROUP_2, &target_offsets).await?;

    Ok(())
}

async fn verify_consumer_with_retry(
    topics: impl IntoIterator<Item = TopicName>,
    consumer_group: &str,
    expected_offsets: &OffsetSnapshot,
) -> Result<()> {
    let topics: Vec<_> = topics.into_iter().collect();
    let expected_for_group: Vec<_> = expected_offsets
        .iter()
        .filter(|r| r.consumer_group == consumer_group)
        .filter(|r| topics.contains(&r.topic))
        .collect();

    let max_attempts = 10;
    let retry_delay = Duration::from_millis(500);

    for attempt in 1..=max_attempts {
        let mut consumer_client_config = ClientConfig::new()
            .set_bootstrap_server(TARGET_BOOTSTRAP_SERVER)
            .set_properties(&HashMap::from([(
                GROUP_ID_KEY.to_string(),
                consumer_group.to_string(),
            )]));

        let (consumer, metadata) = setup_consumer_and_metadata(&mut consumer_client_config).await?;

        let mut tpl = TopicPartitionList::new();
        for topic in metadata.topics().iter() {
            for partition in topic.partitions().iter() {
                tpl.add_partition(topic.name(), partition.id());
            }
        }

        let committed_offsets = consumer
            .committed_offsets(tpl, Timeout::Never)?
            .to_topic_map();

        let all_match = expected_for_group.iter().all(|expected| {
            let key = (expected.topic.clone(), expected.partition);
            committed_offsets
                .get(&key)
                .and_then(|r| r.to_raw())
                .is_some_and(|v| v == expected.offset)
        });

        if all_match {
            info!(
                "Offsets verified for consumer group '{}' after {} attempt(s)",
                consumer_group, attempt
            );
            return Ok(());
        }

        if attempt < max_attempts {
            info!(
                "Attempt {}/{}: Offsets not yet propagated for '{}', retrying...",
                attempt, max_attempts, consumer_group
            );
            sleep(retry_delay).await;
        } else {
            // Final attempt failed, show detailed error
            info!("Committed offsets: {:#?}", committed_offsets);
            info!("Expected offsets: {:#?}", expected_for_group);

            for expected in &expected_for_group {
                let key = (expected.topic.clone(), expected.partition);
                let committed_value = committed_offsets.get(&key).and_then(|r| r.to_raw());

                assert_eq!(
                    committed_value,
                    Some(expected.offset),
                    "Offset mismatch for topic '{}' partition {} in consumer group '{}': expected {}, got {:?}",
                    expected.topic,
                    expected.partition,
                    consumer_group,
                    expected.offset,
                    committed_value
                );
            }
        }
    }

    Ok(())
}
