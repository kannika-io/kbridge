use anyhow::Result;
use bridge_core::test::kafka::cluster::{ContainerizedCluster, MockClusterExt};
use bridge_core::{BridgeClient, KafkaBridgeClient, KafkaBridgeConfig, OffsetSnapshot, test};

const OFFSET_HEADER: &str = "source-offset";
const TOPIC_ORDERS: &str = "orders";
const TOPIC_PAYMENTS: &str = "payments";

/// Creates a header factory that stores the given base offset plus the message index
fn header_fn_with_base(base: i64) -> impl Fn(i64) -> std::collections::HashMap<String, String> {
    move |i| {
        maplit::hashmap! {
            OFFSET_HEADER.to_string() => (base + i).to_string(),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
pub async fn calculate_target_offsets_should_map_source_to_target() -> Result<()> {
    // Start containerized Kafka cluster
    let cluster = ContainerizedCluster::start().await?;
    let bootstrap = cluster.bootstrap_servers().await?;

    // Create topics with single partition
    cluster.create_topic(TOPIC_ORDERS, 1).await?;
    cluster.create_topic(TOPIC_PAYMENTS, 1).await?;

    // Produce messages with headers containing source offsets
    // Source offsets start at 1000 for orders, 2000 for payments
    cluster
        .produce_with_headers(TOPIC_ORDERS, 0, 100, header_fn_with_base(1000))
        .await?;
    cluster
        .produce_with_headers(TOPIC_PAYMENTS, 0, 100, header_fn_with_base(2000))
        .await?;

    // Define source offsets we want to map to target offsets
    // Consumer groups had committed these offsets on the source cluster
    let source_offsets: OffsetSnapshot = indoc::indoc! {
        r#"
        consumer-group-a,orders,0,1050
        consumer-group-a,payments,0,2050
        consumer-group-b,orders,0,1025
        "#
    }
    .parse()
    .expect("failed to parse source offsets");

    // Calculate target offsets using the bridge client
    let config = KafkaBridgeConfig::new(bootstrap);
    let client: KafkaBridgeClient = config.into();

    let topics: Vec<String> = vec![];
    let target_offsets = client
        .calculate_target_offsets(OFFSET_HEADER, topics, source_offsets)
        .await?;

    // Verify the mappings
    // Source offset 1050 should map to target offset 50 (1050 - 1000 base)
    // Source offset 2050 should map to target offset 50 (2050 - 2000 base)
    // Source offset 1025 should map to target offset 25 (1025 - 1000 base)
    let expected: OffsetSnapshot = indoc::indoc! {
        r#"
        consumer-group-a,orders,0,50
        consumer-group-a,payments,0,50
        consumer-group-b,orders,0,25
        "#
    }
    .parse()
    .expect("failed to parse expected offsets");

    test::snapshot::assert_eq(target_offsets, expected);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
pub async fn calculate_target_offsets_with_filter_should_only_include_filtered_topics() -> Result<()>
{
    let cluster = ContainerizedCluster::start().await?;
    let bootstrap = cluster.bootstrap_servers().await?;

    // Create multiple topics
    cluster.create_topic(TOPIC_ORDERS, 1).await?;
    cluster.create_topic(TOPIC_PAYMENTS, 1).await?;

    // Produce messages to both topics
    cluster
        .produce_with_headers(TOPIC_ORDERS, 0, 50, header_fn_with_base(500))
        .await?;
    cluster
        .produce_with_headers(TOPIC_PAYMENTS, 0, 50, header_fn_with_base(600))
        .await?;

    // Source offsets include both topics
    let source_offsets: OffsetSnapshot = indoc::indoc! {
        r#"
        group-1,orders,0,520
        group-1,payments,0,620
        group-2,orders,0,510
        "#
    }
    .parse()
    .expect("failed to parse source offsets");

    let config = KafkaBridgeConfig::new(bootstrap);
    let client: KafkaBridgeClient = config.into();

    // Only filter on orders topic
    let topics = vec![String::from(TOPIC_ORDERS)];
    let target_offsets = client
        .calculate_target_offsets(OFFSET_HEADER, topics.clone(), source_offsets)
        .await?;

    // Should only contain orders topic entries
    let expected: OffsetSnapshot = indoc::indoc! {
        r#"
        group-1,orders,0,20
        group-2,orders,0,10
        "#
    }
    .parse()
    .expect("failed to parse expected offsets");

    test::snapshot::assert_eq(target_offsets, expected);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
pub async fn calculate_target_offsets_with_multiple_partitions() -> Result<()> {
    let cluster = ContainerizedCluster::start().await?;
    let bootstrap = cluster.bootstrap_servers().await?;

    // Create topic with multiple partitions
    cluster.create_topic(TOPIC_ORDERS, 3).await?;

    // Produce messages to each partition with different base offsets
    cluster
        .produce_with_headers(TOPIC_ORDERS, 0, 100, header_fn_with_base(0))
        .await?;
    cluster
        .produce_with_headers(TOPIC_ORDERS, 1, 100, header_fn_with_base(1000))
        .await?;
    cluster
        .produce_with_headers(TOPIC_ORDERS, 2, 100, header_fn_with_base(2000))
        .await?;

    let source_offsets: OffsetSnapshot = indoc::indoc! {
        r#"
        my-consumer,orders,0,50
        my-consumer,orders,1,1050
        my-consumer,orders,2,2050
        "#
    }
    .parse()
    .expect("failed to parse source offsets");

    let config = KafkaBridgeConfig::new(bootstrap);
    let client: KafkaBridgeClient = config.into();

    let topics: Vec<String> = vec![];
    let target_offsets = client
        .calculate_target_offsets(OFFSET_HEADER, topics, source_offsets)
        .await?;

    // Each partition should map to offset 50 since:
    // partition 0: source 50 - base 0 = 50
    // partition 1: source 1050 - base 1000 = 50
    // partition 2: source 2050 - base 2000 = 50
    let expected: OffsetSnapshot = indoc::indoc! {
        r#"
        my-consumer,orders,0,50
        my-consumer,orders,1,50
        my-consumer,orders,2,50
        "#
    }
    .parse()
    .expect("failed to parse expected offsets");

    test::snapshot::assert_eq(target_offsets, expected);

    Ok(())
}
