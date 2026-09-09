use std::time::Duration;

use anyhow::Result;
use bridge_core::test::consumer_offsets::{
    encode_group_metadata_key, encode_offset_commit_key, encode_offset_commit_value,
};
use bridge_core::test::kafka::cluster::{ContainerizedCluster, MockClusterExt};
use bridge_core::{BridgeClient, KafkaBridgeClient, KafkaBridgeConfig, OffsetSnapshot, test};
use rdkafka::producer::{FutureProducer, FutureRecord};
use serial_test::serial;

const OFFSETS_TOPIC: &str = "consumer-offsets-restored";

async fn produce_record(
    producer: &FutureProducer,
    partition: i32,
    key: Vec<u8>,
    value: Option<Vec<u8>>,
) -> Result<()> {
    let mut record = FutureRecord::<Vec<u8>, Vec<u8>>::to(OFFSETS_TOPIC)
        .partition(partition)
        .key(&key);
    if let Some(value) = &value {
        record = record.payload(value);
    }
    producer
        .send(record, Duration::from_secs(10))
        .await
        .map_err(|(err, _)| err)?;
    Ok(())
}

/// Produces a mix of offset commits (including overwrites and a tombstone),
/// a group metadata record, and leaves the last partition empty.
async fn produce_offsets_topic_records(cluster: &ContainerizedCluster) -> Result<()> {
    cluster.create_topic(OFFSETS_TOPIC, 3).await?;
    let producer = cluster.producer().await?;

    // group-a commits twice on orders-0: only the last commit must survive
    produce_record(
        &producer,
        0,
        encode_offset_commit_key(1, "group-a", "orders", 0),
        Some(encode_offset_commit_value(10)),
    )
    .await?;
    produce_record(
        &producer,
        0,
        encode_offset_commit_key(1, "group-a", "orders", 0),
        Some(encode_offset_commit_value(25)),
    )
    .await?;

    // group-b commits on orders-0, then the key is tombstoned
    produce_record(
        &producer,
        0,
        encode_offset_commit_key(1, "group-b", "orders", 0),
        Some(encode_offset_commit_value(50)),
    )
    .await?;
    produce_record(
        &producer,
        0,
        encode_offset_commit_key(1, "group-b", "orders", 0),
        None,
    )
    .await?;

    // Group metadata records (key version 2) must be skipped
    produce_record(
        &producer,
        1,
        encode_group_metadata_key("group-a"),
        Some(vec![1, 2, 3]),
    )
    .await?;

    // group-a commit on payments-1, with a version 0 key
    produce_record(
        &producer,
        1,
        encode_offset_commit_key(0, "group-a", "payments", 1),
        Some(encode_offset_commit_value(7)),
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
        .fetch_source_offsets_from_offsets_topic(
            OFFSETS_TOPIC,
            Vec::<String>::new(),
            Duration::from_secs(10),
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
        .fetch_source_offsets_from_offsets_topic(
            OFFSETS_TOPIC,
            vec!["payments".to_string()],
            Duration::from_secs(10),
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
        .fetch_source_offsets_from_offsets_topic(
            "does-not-exist",
            Vec::<String>::new(),
            Duration::from_secs(10),
        )
        .await;

    assert!(result.is_err());

    Ok(())
}
