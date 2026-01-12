use anyhow::Result;
use bridge_core::test::kafka::cluster::{ContainerizedCluster, MockClusterExt};
use bridge_core::{BridgeClient, KafkaBridgeClient, KafkaBridgeConfig, OffsetSnapshot, test};
use serial_test::serial;

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
#[serial]
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
#[serial]
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
#[serial]
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

/// Creates a header factory with gaps - only even indices get their actual value,
/// simulating scenarios where not every source offset is present.
fn header_fn_with_gaps(base: i64) -> impl Fn(i64) -> std::collections::HashMap<String, String> {
    move |i| {
        // Header value increases by 2 for each message (simulating gaps in source offsets)
        maplit::hashmap! {
            OFFSET_HEADER.to_string() => (base + i * 2).to_string(),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
pub async fn calculate_target_offsets_with_offset_gaps_should_map_to_next_available() -> Result<()>
{
    let cluster = ContainerizedCluster::start().await?;
    let bootstrap = cluster.bootstrap_servers().await?;

    cluster.create_topic(TOPIC_ORDERS, 1).await?;

    // Produce 50 messages with headers: 0, 2, 4, 6, ..., 98
    // This simulates gaps where odd source offsets don't exist
    cluster
        .produce_with_headers(TOPIC_ORDERS, 0, 50, header_fn_with_gaps(0))
        .await?;

    // Search for offsets that fall in gaps (odd numbers)
    let source_offsets: OffsetSnapshot = indoc::indoc! {
        r#"
        group-a,orders,0,5
        group-b,orders,0,11
        group-c,orders,0,0
        group-d,orders,0,98
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

    // Source offset 5 doesn't exist (gap between 4 and 6), should map to offset before 6 (which is at kafka offset 3)
    // Source offset 11 doesn't exist (gap between 10 and 12), should map to offset before 12 (which is at kafka offset 6)
    // Source offset 0 exists at kafka offset 0
    // Source offset 98 exists at kafka offset 49
    let expected: OffsetSnapshot = indoc::indoc! {
        r#"
        group-a,orders,0,2
        group-b,orders,0,5
        group-c,orders,0,0
        group-d,orders,0,49
        "#
    }
    .parse()
    .expect("failed to parse expected offsets");

    test::snapshot::assert_eq(target_offsets, expected);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
pub async fn calculate_target_offsets_at_boundaries() -> Result<()> {
    let cluster = ContainerizedCluster::start().await?;
    let bootstrap = cluster.bootstrap_servers().await?;

    cluster.create_topic(TOPIC_ORDERS, 1).await?;

    // Produce 100 messages with headers 0-99
    cluster
        .produce_with_headers(TOPIC_ORDERS, 0, 100, header_fn_with_base(0))
        .await?;

    // Test boundary conditions: first offset, last offset
    let source_offsets: OffsetSnapshot = indoc::indoc! {
        r#"
        group-first,orders,0,0
        group-last,orders,0,99
        group-mid,orders,0,50
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

    let expected: OffsetSnapshot = indoc::indoc! {
        r#"
        group-first,orders,0,0
        group-last,orders,0,99
        group-mid,orders,0,50
        "#
    }
    .parse()
    .expect("failed to parse expected offsets");

    test::snapshot::assert_eq(target_offsets, expected);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
pub async fn calculate_target_offsets_beyond_high_watermark_should_map_to_end() -> Result<()> {
    let cluster = ContainerizedCluster::start().await?;
    let bootstrap = cluster.bootstrap_servers().await?;

    cluster.create_topic(TOPIC_ORDERS, 1).await?;

    // Produce 50 messages with headers 100-149
    cluster
        .produce_with_headers(TOPIC_ORDERS, 0, 50, header_fn_with_base(100))
        .await?;

    // Search for offsets beyond what exists in the topic
    let source_offsets: OffsetSnapshot = indoc::indoc! {
        r#"
        group-beyond,orders,0,200
        group-way-beyond,orders,0,1000
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

    // Offsets beyond the available range should map to the last available offset (49)
    let expected: OffsetSnapshot = indoc::indoc! {
        r#"
        group-beyond,orders,0,49
        group-way-beyond,orders,0,49
        "#
    }
    .parse()
    .expect("failed to parse expected offsets");

    test::snapshot::assert_eq(target_offsets, expected);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
pub async fn calculate_target_offsets_before_low_watermark_should_map_to_start() -> Result<()> {
    let cluster = ContainerizedCluster::start().await?;
    let bootstrap = cluster.bootstrap_servers().await?;

    cluster.create_topic(TOPIC_ORDERS, 1).await?;

    // Produce 50 messages with headers starting at 500 (500-549)
    cluster
        .produce_with_headers(TOPIC_ORDERS, 0, 50, header_fn_with_base(500))
        .await?;

    // Search for offsets below what exists in the topic
    let source_offsets: OffsetSnapshot = indoc::indoc! {
        r#"
        group-below,orders,0,100
        group-way-below,orders,0,0
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

    // Offsets below the available range should map to offset 0 (start of partition)
    let expected: OffsetSnapshot = indoc::indoc! {
        r#"
        group-below,orders,0,0
        group-way-below,orders,0,0
        "#
    }
    .parse()
    .expect("failed to parse expected offsets");

    test::snapshot::assert_eq(target_offsets, expected);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
pub async fn calculate_target_offsets_with_multiple_consumer_groups_same_partition() -> Result<()> {
    let cluster = ContainerizedCluster::start().await?;
    let bootstrap = cluster.bootstrap_servers().await?;

    cluster.create_topic(TOPIC_ORDERS, 1).await?;

    // Produce 100 messages with headers 0-99
    cluster
        .produce_with_headers(TOPIC_ORDERS, 0, 100, header_fn_with_base(0))
        .await?;

    // Multiple consumer groups at different positions in the same partition
    let source_offsets: OffsetSnapshot = indoc::indoc! {
        r#"
        fast-consumer,orders,0,90
        medium-consumer,orders,0,50
        slow-consumer,orders,0,10
        very-slow-consumer,orders,0,5
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

    // Each consumer group should map to their exact offset
    let expected: OffsetSnapshot = indoc::indoc! {
        r#"
        fast-consumer,orders,0,90
        medium-consumer,orders,0,50
        slow-consumer,orders,0,10
        very-slow-consumer,orders,0,5
        "#
    }
    .parse()
    .expect("failed to parse expected offsets");

    test::snapshot::assert_eq(target_offsets, expected);

    Ok(())
}
