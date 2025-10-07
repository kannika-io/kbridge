use std::collections::HashMap;

use log::{info, trace};
use rdkafka::Message;

use crate::{
    OffsetSnapshot,
    transform::{
        consumer_initialization::setup_consumer_and_metadata,
        header::get_offset_from_header,
        offset_mapping::insert_offset_transformations,
        transformation_errors::{FetchOffsetError, TransformationError},
        watermarks::get_high_watermark,
    },
};

mod consumer_initialization;
mod header;
mod offset_mapping;
pub mod transformation_errors;
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
/// - Outer key: topic name (String)
/// - Inner key: source offset (i64)
/// - Inner value: target offset (i64) - the actual Kafka message offset
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
/// use bridge_core::transform::get_target_offsets;
/// use bridge_core::{ OffsetRecord, OffsetSnapshot };
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let brokers = "localhost:9092";
/// let topics = &["orders", "payments"];
/// let offset_header_key = "source-offset";
/// let source_offsets : OffsetSnapshot = vec![
///     OffsetRecord {
///         topic: "test-topic".to_string(),
///         partition: 1, offset: 200i64,
///         consumer_group: "console-consumer".to_string()
///     }
/// ];
///
/// let mappings = get_target_offsets(brokers, topics, offset_header_key, &source_offsets).await?;
///
/// // mappings might look like:
/// // {
/// //   "orders": { 100: 1523, 200: 1847 },
/// //   "payments": { 300: 892 }
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
    brokers: &str,
    topics: &[&str],
    offset_header_key: &str,
    source_offsets: &OffsetSnapshot,
) -> Result<HashMap<(String, Partition), HashMap<i64, i64>>, TransformationError> {
    validate_input_parameters(source_offsets, offset_header_key)?;
    let (consumer, metadata) = setup_consumer_and_metadata(brokers, topics).await?;

    let mut transformations = HashMap::new();

    // Fetch water marks for topics and partitions
    // We need this to be able to exit the consumer loop later
    let topic_partition_watermarks = get_high_watermark(&consumer, &metadata, topics)?;

    let mut topics_and_partitions_to_check: Vec<(String, i32)> = topic_partition_watermarks
        .iter()
        .flat_map(|t| t.1.iter().map(|p| (t.0.to_string(), *p.0)))
        .collect();

    if topics_and_partitions_to_check.is_empty() {
        return Err(TransformationError::NoValidPartitions(topics.iter().map(|s| s.to_string()).collect()));
    }

    info!("Topics to check: {topics_and_partitions_to_check:?}");
    info!("Source Offsets: {source_offsets:?}");

    // Consume all messages from the topics we are subscribed to
    while !source_offsets.is_empty() && !topics_and_partitions_to_check.is_empty() {
        let consume_result = consumer.recv().await.map_err(|e| TransformationError::FailedToReceiveMessages {
            message: e.to_string(),
            topic_selector: topics.join(","),
        })?;
        trace!("Processing offset {} for topic {}", consume_result.offset(), consume_result.topic());

        let source_offset = match consume_result.headers() {
            Some(headers) => get_offset_from_header(headers, offset_header_key),
            None => Err(FetchOffsetError::NoHeadersInMessage),
        }?;

        if source_offsets
            .iter()
            .any(|o| o.partition == consume_result.partition() && o.offset == source_offset && consume_result.topic() == o.topic)
        {
            insert_offset_transformations(
                &mut transformations,
                &source_offset,
                &consume_result.offset(),
                consume_result.topic().to_string(),
                consume_result.partition(),
            )?;
        }

        let water_mark = get_topic_partition_watermarks(&topic_partition_watermarks, consume_result.topic(), consume_result.partition())?;

        if water_mark == &(consume_result.offset() + 1) {
            topics_and_partitions_to_check.retain(|t| !(t.0 == consume_result.topic() && t.1 == consume_result.partition()));
        }
    }

    if topics_and_partitions_to_check.is_empty() {
        todo!()
    } else {
        Ok(transformations)
    }
}

pub type Partition = i32;
pub type Topic = String;
pub type SourceOffset = i64;
pub type TargetOffset = i64;

fn validate_input_parameters(source_offsets: &OffsetSnapshot, offset_header_key: &str) -> Result<(), TransformationError> {
    if source_offsets.is_empty() {
        return Err(TransformationError::InvalidInput("Source offsets cannot be empty".to_string()));
    }

    if offset_header_key.is_empty() {
        return Err(TransformationError::InvalidInput("Offset header key cannot be empty".to_string()));
    }

    Ok(())
}

fn get_topic_partition_watermarks<'a>(
    topic_partition_watermarks: &'a HashMap<String, HashMap<i32, i64>>,
    topic: &'a str,
    partition: i32,
) -> Result<&'a i64, TransformationError> {
    topic_partition_watermarks
        .get(topic)
        .and_then(|partitions| partitions.get(&partition))
        .ok_or_else(|| TransformationError::InvalidInput(format!("No watermark found for topic {topic} partition {partition}")))
}

#[cfg(test)]
mod tests {
    use crate::OffsetRecord;

    use super::*;

    #[test]
    fn test_get_topic_partition_watermarks_success() {
        let mut topic_partition_watermarks = HashMap::new();
        let mut partitions = HashMap::new();
        partitions.insert(0, 100i64);
        partitions.insert(1, 200i64);
        topic_partition_watermarks.insert("test-topic".to_string(), partitions);

        let result = get_topic_partition_watermarks(&topic_partition_watermarks, "test-topic", 0);

        assert!(result.is_ok());
        assert_eq!(*result.unwrap(), 100i64);
    }

    #[test]
    fn test_get_topic_partition_watermarks_topic_not_found() {
        let topic_partition_watermarks = HashMap::new();

        let result = get_topic_partition_watermarks(&topic_partition_watermarks, "nonexistent-topic", 0);

        assert!(result.is_err());
        match result.unwrap_err() {
            TransformationError::InvalidInput(msg) => {
                assert!(msg.contains("No watermark found for topic nonexistent-topic partition 0"));
            }
            _ => panic!("Expected InvalidInput error"),
        }
    }

    #[test]
    fn test_get_topic_partition_watermarks_partition_not_found() {
        let mut topic_partition_watermarks = HashMap::new();
        let mut partitions = HashMap::new();
        partitions.insert(0, 100i64);
        topic_partition_watermarks.insert("test-topic".to_string(), partitions);

        let result = get_topic_partition_watermarks(&topic_partition_watermarks, "test-topic", 1);

        assert!(result.is_err());
        match result.unwrap_err() {
            TransformationError::InvalidInput(msg) => {
                assert!(msg.contains("No watermark found for topic test-topic partition 1"));
            }
            _ => panic!("Expected InvalidInput error"),
        }
    }

    #[tokio::test]
    async fn test_get_target_offsets_empty_source_offsets() {
        let brokers = "localhost:9092";
        let topics = &["test-topic"];
        let offset_header_key = "source-offset";
        let source_offsets: OffsetSnapshot = vec![];

        let result = get_target_offsets(brokers, topics, offset_header_key, &source_offsets).await;

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
        let brokers = "localhost:9092";
        let topics = &["test-topic"];
        let offset_header_key = "";
        let source_offsets: OffsetSnapshot = vec![OffsetRecord {
            topic: "test-topic".to_string(),
            partition: 1,
            offset: 200i64,
            consumer_group: "console-consumer".to_string(),
        }];

        let result = get_target_offsets(brokers, topics, offset_header_key, &source_offsets).await;

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
        let brokers = "";
        let topics = &["test-topic"];
        let offset_header_key = "source-offset";
        let source_offsets: OffsetSnapshot = vec![
            OffsetRecord {
                topic: "test-topic".to_string(),
                partition: 1,
                offset: 100i64,
                consumer_group: "console-consumer".to_string(),
            },
            OffsetRecord {
                topic: "test-topic".to_string(),
                partition: 2,
                offset: 200i64,
                consumer_group: "console-consumer".to_string(),
            },
        ];

        let result = get_target_offsets(brokers, topics, offset_header_key, &source_offsets).await;

        assert!(result.is_err());
        match result.unwrap_err() {
            TransformationError::InvalidInput(msg) => {
                assert_eq!(msg, "Brokers string cannot be empty");
            }
            _ => panic!("Expected InvalidInput error for empty brokers"),
        }
    }
}
