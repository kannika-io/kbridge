use std::time::Duration;

use anyhow::Result;
use bridge_core::test::consumer_offsets::{
    ConsumerOffsetsProducer, GroupMetadata, OffsetCommit, Tombstone,
};
use bridge_core::test::kafka::cluster::{ContainerizedCluster, MockClusterExt};
use bridge_core::{
    BridgeClient, KafkaBridgeClient, KafkaBridgeConfig, OffsetSnapshot, OffsetSource, test,
};
use serial_test::serial;

const OFFSETS_TOPIC: &str = "consumer-offsets-restored";

/// Produces a mix of offset commits (including overwrites and a tombstone),
/// a group metadata record, and leaves the last partition empty.
async fn produce_offsets_topic_records(cluster: &ContainerizedCluster) -> Result<()> {
    cluster.create_topic(OFFSETS_TOPIC, 3).await?;
    let producer = ConsumerOffsetsProducer::new(cluster.producer().await?, OFFSETS_TOPIC);

    // group-a commits twice on orders-0: only the last commit must survive
    producer
        .send(OffsetCommit {
            group: "group-a",
            topic: "orders",
            partition: 0,
            offset: 10,
        })
        .await?;
    producer
        .send(OffsetCommit {
            group: "group-a",
            topic: "orders",
            partition: 0,
            offset: 25,
        })
        .await?;

    // group-b commits on orders-0, then the key is tombstoned
    producer
        .send(OffsetCommit {
            group: "group-b",
            topic: "orders",
            partition: 0,
            offset: 50,
        })
        .await?;
    producer
        .send(Tombstone {
            group: "group-b",
            topic: "orders",
            partition: 0,
        })
        .await?;

    // Group metadata records (key version 2) must be skipped
    producer
        .send_to_partition(1, GroupMetadata { group: "group-a" })
        .await?;

    // group-a commit on payments-1, with a version 0 key
    producer
        .send_to_partition(
            1,
            OffsetCommit {
                group: "group-a",
                topic: "payments",
                partition: 1,
                offset: 7,
            }
            .with_key_version(0),
        )
        .await?;

    // Partition 2 stays empty: the reader must still terminate

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
pub async fn fetch_from_offsets_topic_should_return_last_committed_offsets() -> Result<()> {
    let cluster = ContainerizedCluster::start().await?;
    let bootstrap = cluster.bootstrap_servers().await?;

    produce_offsets_topic_records(&cluster).await?;

    let config = KafkaBridgeConfig::new(bootstrap);
    let client: KafkaBridgeClient = config.into();

    let result = client
        .fetch_offsets(
            Vec::<String>::new(),
            Duration::from_secs(10),
            OffsetSource::Topic(OFFSETS_TOPIC.to_string()),
        )
        .await?;

    let expected: OffsetSnapshot = indoc::indoc! {
        r#"
        group-a,orders,0,25
        group-a,payments,1,7
        "#
    }
    .parse()
    .expect("failed to parse expected offsets");

    test::snapshot::assert_eq(result, expected);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
pub async fn fetch_from_offsets_topic_with_topic_filter_should_only_include_filtered_topics()
-> Result<()> {
    let cluster = ContainerizedCluster::start().await?;
    let bootstrap = cluster.bootstrap_servers().await?;

    produce_offsets_topic_records(&cluster).await?;

    let config = KafkaBridgeConfig::new(bootstrap);
    let client: KafkaBridgeClient = config.into();

    let result = client
        .fetch_offsets(
            vec!["payments".to_string()],
            Duration::from_secs(10),
            OffsetSource::Topic(OFFSETS_TOPIC.to_string()),
        )
        .await?;

    let expected: OffsetSnapshot = indoc::indoc! {
        r#"
        group-a,payments,1,7
        "#
    }
    .parse()
    .expect("failed to parse expected offsets");

    test::snapshot::assert_eq(result, expected);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
pub async fn fetch_from_offsets_topic_when_topic_missing_should_return_error() -> Result<()> {
    let cluster = ContainerizedCluster::start().await?;
    let bootstrap = cluster.bootstrap_servers().await?;

    let config = KafkaBridgeConfig::new(bootstrap);
    let client: KafkaBridgeClient = config.into();

    let result = client
        .fetch_offsets(
            Vec::<String>::new(),
            Duration::from_secs(10),
            OffsetSource::Topic("does-not-exist".to_string()),
        )
        .await;

    assert!(result.is_err());

    Ok(())
}
