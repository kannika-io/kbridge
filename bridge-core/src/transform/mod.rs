use std::collections::HashMap;

use rdkafka::{ClientConfig, consumer::Consumer};

use crate::transform::consumer_group_offset::try_find_missing_offsets;
use crate::transform::consumer_group_offset_mapping::handle_missing_offsets;
use crate::transform::message_header::extract_source_offset_from_message;
use crate::transform::watermarks::update_partitions_to_check;
use crate::{
    ConsumerGroup, ConsumerGroupRecord, Offset, OffsetSnapshot, Partition, Topic,
    TransformationRecord,
    transform::{
        consumer::setup_consumer_and_metadata, errors::TransformationError,
        watermarks::get_high_watermark,
    },
};

mod consumer;
mod consumer_group_offset;
mod consumer_group_offset_mapping;
pub mod errors;
mod message_header;
mod watermarks;

/// Transforms source offsets to target offsets by consuming Kafka messages and matching header values.
///
/// This function consumes messages from specified Kafka topics, extracts source offset values from
/// message headers, and creates a mapping from source offsets to their corresponding target offsets
/// (the actual Kafka message offsets). This is useful for offset translation scenarios where you
/// need to map logical offsets stored in headers to physical Kafka offsets.
///
/// # Arguments
///
/// * `brokers` - Comma-separated list of Kafka broker addresses (e.g., "localhost:9092")
/// * `topics` - Array of topic names to consume from
/// * `offset_header_key` - The header key containing the source offset value in each message
/// * `source_offsets` - Vector of source offset values to find mappings for
///
/// # Returns
///
/// Returns a nested HashMap where:
/// - Outer key: consumer group name (String)
/// - Inner value: Vector of tuples containing (topic, partition, source_offset, target_offset)
///
/// # Errors
///
/// Returns `TransformationError` in the following cases:
/// - `InvalidInput` - If source_offsets is empty, offset_header_key is empty, or brokers is empty
/// - `ConsumerInitializationFailed` - If Kafka consumer setup fails
/// - `SubscribingFailed` - If topic subscription fails
/// - `MetadataFetchFailed` - If Kafka metadata cannot be retrieved
/// - `NoValidPartitions` - If no valid partitions are found for the specified topics
/// - `FailedToReceiveMessages` - If message consumption fails
/// - `FetchOffsetError` - If offset extraction from headers fails
/// - `OffsetMappingTransformationError` - If offset mapping conflicts occur
///
/// # Example
///
/// ```rust
/// use std::collections::HashMap;
/// use rdkafka::ClientConfig;
/// use bridge_core::transform::get_target_offsets;
/// use bridge_core::{OffsetRecord, OffsetSnapshot};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let topics = &["orders", "payments"];
/// let offset_header_key = "source-offset";
/// let mut transformer_consumer_config = ClientConfig::new();
/// transformer_consumer_config
///     .set("bootstrap.servers", "localhost:9092")
///     .set("group.id", "test")
///     .set("auto.offset.reset", "earliest")
///     .set("enable.partition.eof", "false")
///     .set("enable.auto.commit", "false");
///
/// let source_offsets: OffsetSnapshot = vec![
///     OffsetRecord {
///         topic: "orders".to_string(),
///         partition: 0,
///         offset: 100,
///         consumer_group: "my-consumer-group".to_string(),
///     },
///     OffsetRecord {
///         topic: "payments".to_string(),
///         partition: 1,
///         offset: 200,
///         consumer_group: "my-consumer-group".to_string(),
///     },
/// ];
///
/// let mappings = get_target_offsets(transformer_consumer_config, offset_header_key, &source_offsets).await?;
///
/// // mappings might look like:
/// // {
/// //   "my-consumer-group": [
/// //     ("orders".to_string(), 0, 100, 1523),
/// //     ("payments".to_string(), 1, 200, 1847)
/// //   ]
/// // }
/// # Ok(())
/// # }
/// ```
///
/// # Behavior
///
/// The function will:
/// 1. Validate input parameters
/// 2. Initialize a Kafka consumer and subscribe to the specified topics
/// 3. Fetch topic metadata and watermarks to determine consumption boundaries
/// 4. Consume messages until all source offsets are found or all partitions are exhausted
/// 5. For each message, extract the source offset from the specified header
/// 6. If the source offset matches one in the input list, record the mapping to the message's actual offset
/// 7. Stop consuming from a partition when its high watermark is reached
/// 8. Return the complete mapping of source offsets to target offsets
pub async fn get_target_offsets(
    transformer_consumer_config: ClientConfig,
    offset_header_key: &str,
    source_offsets: &OffsetSnapshot,
) -> Result<HashMap<ConsumerGroup, Vec<TransformationRecord>>, TransformationError> {
    validate_input_parameters(source_offsets, offset_header_key)?;

    let topics: Vec<&str> = source_offsets.iter().map(|o| o.topic.as_str()).collect();

    // Initialize consumer
    let (consumer, metadata) =
        setup_consumer_and_metadata(&topics, transformer_consumer_config).await?;

    let mut transformations = HashMap::new();

    // Fetch watermarks for topics and partitions
    // We need this to be able to exit the consumer loop
    let topic_partition_watermarks = get_high_watermark(&consumer, &metadata, &topics)?;

    let mut partitions_to_search: Vec<(Topic, Partition)> = topic_partition_watermarks
        .iter()
        .flat_map(|t| t.1.iter().map(|p| (t.0.to_string(), *p.0)))
        .collect();

    if partitions_to_search.is_empty() {
        return Err(TransformationError::NoPartitionsToSearch(
            topics.iter().map(|s| s.to_string()).collect(),
        ));
    }

    let mut missing_offsets: Vec<ConsumerGroupRecord> = source_offsets
        .iter()
        .map(|o| {
            (
                o.consumer_group.clone(),
                o.topic.clone(),
                o.partition,
                o.offset,
            )
        })
        .collect();

    let mut nearest_offsets = HashMap::new();

    // Consume all messages from the topics we are subscribed to
    while !partitions_to_search.is_empty() {
        let consume_result =
            consumer
                .recv()
                .await
                .map_err(|e| TransformationError::FailedToReceiveMessages {
                    message: e.to_string(),
                    topic_selector: topics.join(","),
                })?;

        let source_offset = extract_source_offset_from_message(&consume_result, offset_header_key)?;

        try_find_missing_offsets(
            &consume_result,
            source_offset,
            source_offsets,
            &mut transformations,
            &mut missing_offsets,
            &mut nearest_offsets,
        )?;

        update_partitions_to_check(
            &consume_result,
            &topic_partition_watermarks,
            &mut partitions_to_search,
        )?;
    }

    consumer.unassign()?;
    handle_missing_offsets(transformations, missing_offsets, nearest_offsets)
}

fn validate_input_parameters(
    source_offsets: &OffsetSnapshot,
    offset_header_key: &str,
) -> Result<(), TransformationError> {
    if source_offsets.is_empty() {
        return Err(TransformationError::InvalidInput(
            "Source offsets cannot be empty".to_string(),
        ));
    }

    if offset_header_key.is_empty() {
        return Err(TransformationError::InvalidInput(
            "Offset header key cannot be empty".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::OffsetRecord;

    #[tokio::test]
    async fn test_get_target_offsets_empty_source_offsets() {
        let offset_header_key = "source-offset";
        let source_offsets: OffsetSnapshot = vec![];

        let mut transformer_consumer_config = ClientConfig::new();
        transformer_consumer_config
            .set("bootstrap.servers", "localhost:9092")
            .set("group.id", "test")
            .set("auto.offset.reset", "earliest")
            .set("enable.partition.eof", "false")
            .set("enable.auto.commit", "false");

        let result = get_target_offsets(
            transformer_consumer_config,
            offset_header_key,
            &source_offsets,
        )
        .await;

        assert!(result.is_err());
        match result.unwrap_err() {
            TransformationError::InvalidInput(msg) => {
                assert_eq!(msg, "Source offsets cannot be empty");
            }
            _ => panic!("Expected InvalidInput error for empty source offsets"),
        }
    }

    #[tokio::test]
    async fn test_get_target_offsets_empty_offset_header_key() {
        let offset_header_key = "";
        let source_offsets: OffsetSnapshot = vec![OffsetRecord {
            topic: "test-topic".to_string(),
            partition: 1,
            offset: 200i64,
            consumer_group: "console-consumer".to_string(),
        }];

        let mut transformer_consumer_config = ClientConfig::new();
        transformer_consumer_config
            .set("bootstrap.servers", "localhost:9092")
            .set("group.id", "test")
            .set("auto.offset.reset", "earliest")
            .set("enable.partition.eof", "false")
            .set("enable.auto.commit", "false");

        let result = get_target_offsets(
            transformer_consumer_config,
            offset_header_key,
            &source_offsets,
        )
        .await;

        assert!(result.is_err());
        match result.unwrap_err() {
            TransformationError::InvalidInput(msg) => {
                assert_eq!(msg, "Offset header key cannot be empty");
            }
            _ => panic!("Expected InvalidInput error for empty offset header key"),
        }
    }

    #[tokio::test]
    async fn test_get_target_offsets_empty_brokers() {
        let offset_header_key = "source-offset";
        let source_offsets: OffsetSnapshot = vec![
            OffsetRecord {
                topic: "test-topic".to_string(),
                partition: 1,
                offset: 100i64,
                consumer_group: "console-consumer".to_string(),
            },
            OffsetRecord {
                topic: "test-topic-2".to_string(),
                partition: 2,
                offset: 200i64,
                consumer_group: "console-consumer".to_string(),
            },
        ];

        let mut transformer_consumer_config = ClientConfig::new();
        transformer_consumer_config
            .set("bootstrap.servers", "")
            .set("group.id", "test")
            .set("auto.offset.reset", "earliest")
            .set("enable.partition.eof", "false")
            .set("enable.auto.commit", "false");

        let result = get_target_offsets(
            transformer_consumer_config,
            offset_header_key,
            &source_offsets,
        )
        .await;

        assert!(result.is_err());
        match result.unwrap_err() {
            TransformationError::MetadataFetchFailed(msg) => {
                assert_eq!(
                    msg,
                    "Meta data fetch error: BrokerTransportFailure (Local: Broker transport failure)"
                );
            }
            other => panic!("{other:?}"),
        }
    }
}
