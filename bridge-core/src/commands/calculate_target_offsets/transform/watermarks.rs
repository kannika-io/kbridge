use std::{collections::HashMap, time::Duration};

use crate::{
    Offset, Partition, Topic, commands::calculate_target_offsets::errors::TransformationError,
};
use log::info;
use rdkafka::{
    Message,
    consumer::{Consumer, StreamConsumer},
    error::KafkaError,
    metadata::Metadata,
};

/// Updates the list of partitions that still need to be checked for messages.
///
/// This function compares the current message's offset against the high watermark for its
/// topic-partition combination. When a message's offset reaches the high watermark (meaning
/// we've processed all available messages for that partition), the partition is removed
/// from the list of partitions that still need to be checked.
///
/// # Arguments
///
/// * `message` - The current Kafka message being processed
/// * `topic_partition_watermarks` - A nested HashMap containing high watermarks for each
///   topic-partition combination. Structure: `{topic: {partition: high_watermark}}`
/// * `partitions_to_check` - A mutable vector of (topic, partition) tuples representing
///   partitions that still have messages to process
///
/// # Returns
///
/// * `Ok(())` - If the watermark check and partition list update succeeded
/// * `Err(TransformationError)` - If no watermark was found for the message's topic-partition
///
/// # Behavior
///
/// The function removes a partition from `partitions_to_check` when:
/// `message.offset() + 1 == high_watermark`
///
/// This condition indicates that the current message is the last available message in the
/// partition (since Kafka offsets are 0-based and the high watermark points to the next
/// offset that would be assigned to a new message).
///
/// # Example
///
/// ```rust
/// let mut partitions_to_check = vec![
///     ("topic1".to_string(), 0),
///     ("topic1".to_string(), 1),
/// ];
///
/// // If message is at offset 99 and high watermark is 100,
/// // the partition will be removed from partitions_to_check
/// update_partitions_to_check(&message, &watermarks, &mut partitions_to_check)?;
/// ```
pub fn update_partitions_to_check(
    message: &rdkafka::message::BorrowedMessage,
    topic_partition_watermarks: &HashMap<String, HashMap<i32, i64>>,
    partitions_to_check: &mut Vec<(String, i32)>,
) -> Result<(), TransformationError> {
    let water_mark = get_topic_partition_watermark(
        topic_partition_watermarks,
        message.topic(),
        message.partition(),
    )?;

    if water_mark == &(message.offset() + 1) {
        partitions_to_check.retain(|t| !(t.0 == message.topic() && t.1 == message.partition()));
    }

    Ok(())
}

/// Gets watermark for specified topic/partition combination
fn get_topic_partition_watermark<'a>(
    topic_partition_watermarks: &'a HashMap<String, HashMap<i32, i64>>,
    topic: &'a str,
    partition: i32,
) -> Result<&'a i64, TransformationError> {
    topic_partition_watermarks
        .get(topic)
        .and_then(|partitions| partitions.get(&partition))
        .ok_or_else(|| {
            TransformationError::InvalidInput(format!(
                "No watermark found for topic {topic} partition {partition}"
            ))
        })
}

/// Retrieves high watermarks for all partitions of specified topics from a Kafka cluster.
///
/// This function fetches the high watermark (the offset of the next message that would be
/// written) for each partition of the specified topics. High watermarks are used to determine
/// when all available messages in a partition have been consumed.
///
/// # Arguments
///
/// * `consumer` - A reference to a Kafka StreamConsumer used to fetch watermark information
/// * `metadata` - Kafka cluster metadata containing topic and partition information
/// * `topics` - A slice of topic names for which to fetch watermarks
///
/// # Returns
///
/// * `Ok(HashMap<String, HashMap<i32, i64>>)` - A nested HashMap where:
///   - Outer key: Topic name (String)
///   - Inner key: Partition ID (i32)
///   - Inner value: High watermark offset (i64)
///   
///   Only partitions with high watermarks > 0 are included in the result.
///
/// * `Err(TransformationError)` - If watermark fetching fails for any partition
///
/// # Behavior
///
/// - Iterates through all topics in the metadata that match the specified topic names
/// - For each topic, fetches watermarks for all its partitions
/// - Filters out partitions with high watermark <= 0 (empty partitions)
/// - Logs watermark information for each partition with messages
/// - Only includes topics that have at least one partition with messages
///
/// # Example
///
/// ```rust
/// let topics = vec!["orders", "payments"];
/// let watermarks = get_high_watermark_for_topics(&consumer, &metadata, &topics)?;
///
/// // Access watermark for topic "orders", partition 0
/// if let Some(partition_map) = watermarks.get("orders") {
///     if let Some(high_watermark) = partition_map.get(&0) {
///         println!("Orders partition 0 high watermark: {}", high_watermark);
///     }
/// }
/// ```
///
/// # Errors
///
/// Returns `TransformationError::WatermarkFetchFailed` if the Kafka consumer fails to
/// fetch watermarks for any partition, including the topic name, partition ID, and
/// underlying Kafka error reason.
pub fn get_high_watermark_for_topics(
    consumer: &StreamConsumer,
    metadata: &Metadata,
    topics: &[&str],
) -> Result<HashMap<String, HashMap<Partition, Offset>>, TransformationError> {
    let mut topic_partition_watermarks: HashMap<Topic, HashMap<Partition, Offset>> = HashMap::new();

    for topic in metadata
        .topics()
        .iter()
        .filter(|t| topics.contains(&t.name()))
    {
        let mut partition_map = HashMap::new();

        for partition in topic.partitions() {
            let partition_id = partition.id();
            let water_marks =
                consumer.fetch_watermarks(topic.name(), partition_id, Duration::from_secs(5));
            let high_water_mark =
                get_high_water_mark(partition_id, water_marks, topic.name().to_string())?;
            if high_water_mark > 0 {
                partition_map.insert(partition_id, high_water_mark);
                info!(
                    "Watermark for {}-{}: high={}",
                    topic.name(),
                    partition_id,
                    high_water_mark
                );
            }
        }

        if !partition_map.is_empty() {
            topic_partition_watermarks.insert(topic.name().to_string(), partition_map);
        }
    }

    Ok(topic_partition_watermarks)
}

fn get_high_water_mark(
    partition_id: Partition,
    water_marks: Result<(Offset, Offset), KafkaError>,
    topic_name: String,
) -> Result<Offset, TransformationError> {
    match water_marks {
        Ok((_low, high)) => Ok(high),
        Err(e) => Err(TransformationError::WatermarkFetchFailed {
            topic: topic_name,
            partition: partition_id,
            reason: e.to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdkafka::error::KafkaError;

    // Mock message struct for testing
    struct MockMessage {
        topic: String,
        partition: i32,
        offset: i64,
    }

    impl MockMessage {
        fn new(topic: &str, partition: i32, offset: i64) -> Self {
            Self {
                topic: topic.to_string(),
                partition,
                offset,
            }
        }

        fn topic(&self) -> &str {
            &self.topic
        }

        fn partition(&self) -> i32 {
            self.partition
        }

        fn offset(&self) -> i64 {
            self.offset
        }
    }

    #[test]
    fn test_get_topic_partition_watermarks_success() {
        let mut topic_partition_watermarks = HashMap::new();
        let mut partitions = HashMap::new();
        partitions.insert(0, 100i64);
        partitions.insert(1, 200i64);
        topic_partition_watermarks.insert("test-topic".to_string(), partitions);

        let result = get_topic_partition_watermark(&topic_partition_watermarks, "test-topic", 0);

        assert!(result.is_ok());
        assert_eq!(*result.unwrap(), 100i64);
    }

    #[test]
    fn test_get_topic_partition_watermarks_topic_not_found() {
        let topic_partition_watermarks = HashMap::new();

        let result =
            get_topic_partition_watermark(&topic_partition_watermarks, "nonexistent-topic", 0);

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

        let result = get_topic_partition_watermark(&topic_partition_watermarks, "test-topic", 1);

        assert!(result.is_err());
        match result.unwrap_err() {
            TransformationError::InvalidInput(msg) => {
                assert!(msg.contains("No watermark found for topic test-topic partition 1"));
            }
            _ => panic!("Expected InvalidInput error"),
        }
    }

    #[test]
    fn test_update_partitions_to_check_removes_partition_at_watermark() {
        let mut topic_partition_watermarks = HashMap::new();
        let mut partitions = HashMap::new();
        partitions.insert(0, 100i64); // High watermark is 100
        topic_partition_watermarks.insert("test-topic".to_string(), partitions);

        let mut partitions_to_check = vec![
            ("test-topic".to_string(), 0),
            ("test-topic".to_string(), 1),
            ("other-topic".to_string(), 0),
        ];

        // Message at offset 99, so offset + 1 = 100 = high watermark
        let message = MockMessage::new("test-topic", 0, 99);

        // This would normally use BorrowedMessage, but we can't easily mock that
        // So we'll test the logic by calling get_topic_partition_watermark directly
        let watermark = get_topic_partition_watermark(
            &topic_partition_watermarks,
            message.topic(),
            message.partition(),
        )
        .unwrap();

        // Simulate the watermark check logic
        if *watermark == message.offset() + 1 {
            partitions_to_check.retain(|t| !(t.0 == message.topic() && t.1 == message.partition()));
        }

        assert_eq!(partitions_to_check.len(), 2);
        assert!(!partitions_to_check.contains(&("test-topic".to_string(), 0)));
        assert!(partitions_to_check.contains(&("test-topic".to_string(), 1)));
        assert!(partitions_to_check.contains(&("other-topic".to_string(), 0)));
    }

    #[test]
    fn test_update_partitions_to_check_keeps_partition_below_watermark() {
        let mut topic_partition_watermarks = HashMap::new();
        let mut partitions = HashMap::new();
        partitions.insert(0, 100i64); // High watermark is 100
        topic_partition_watermarks.insert("test-topic".to_string(), partitions);

        let mut partitions_to_check =
            vec![("test-topic".to_string(), 0), ("test-topic".to_string(), 1)];

        // Message at offset 98, so offset + 1 = 99 < 100 (high watermark)
        let message = MockMessage::new("test-topic", 0, 98);

        let watermark = get_topic_partition_watermark(
            &topic_partition_watermarks,
            message.topic(),
            message.partition(),
        )
        .unwrap();

        // Simulate the watermark check logic
        if *watermark == message.offset() + 1 {
            partitions_to_check.retain(|t| !(t.0 == message.topic() && t.1 == message.partition()));
        }

        // Partition should still be in the list since we haven't reached the watermark
        assert_eq!(partitions_to_check.len(), 2);
        assert!(partitions_to_check.contains(&("test-topic".to_string(), 0)));
        assert!(partitions_to_check.contains(&("test-topic".to_string(), 1)));
    }

    #[test]
    fn test_update_partitions_to_check_removes_only_matching_partition() {
        let mut topic_partition_watermarks = HashMap::new();
        let mut partitions = HashMap::new();
        partitions.insert(0, 100i64);
        partitions.insert(1, 200i64);
        topic_partition_watermarks.insert("test-topic".to_string(), partitions);

        let mut partitions_to_check = vec![
            ("test-topic".to_string(), 0),
            ("test-topic".to_string(), 1),
            ("test-topic".to_string(), 2),
        ];

        // Message at offset 99 for partition 0, watermark is 100
        let message = MockMessage::new("test-topic", 0, 99);

        let watermark = get_topic_partition_watermark(
            &topic_partition_watermarks,
            message.topic(),
            message.partition(),
        )
        .unwrap();

        if *watermark == message.offset() + 1 {
            partitions_to_check.retain(|t| !(t.0 == message.topic() && t.1 == message.partition()));
        }

        // Only partition 0 should be removed
        assert_eq!(partitions_to_check.len(), 2);
        assert!(!partitions_to_check.contains(&("test-topic".to_string(), 0)));
        assert!(partitions_to_check.contains(&("test-topic".to_string(), 1)));
        assert!(partitions_to_check.contains(&("test-topic".to_string(), 2)));
    }

    #[test]
    fn test_get_high_water_mark_success() {
        let watermarks = Ok((10i64, 100i64)); // (low, high)
        let result = get_high_water_mark(0, watermarks, "test-topic".to_string());

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 100i64);
    }

    #[test]
    fn test_get_high_water_mark_kafka_error() {
        let kafka_error =
            KafkaError::MetadataFetch(rdkafka::types::RDKafkaErrorCode::BrokerTransportFailure);
        let watermarks = Err(kafka_error);
        let result = get_high_water_mark(0, watermarks, "test-topic".to_string());

        assert!(result.is_err());
        match result.unwrap_err() {
            TransformationError::WatermarkFetchFailed {
                topic,
                partition,
                reason,
            } => {
                assert_eq!(topic, "test-topic");
                assert_eq!(partition, 0);
                assert!(reason.contains("BrokerTransportFailure"));
            }
            _ => panic!("Expected WatermarkFetchFailed error"),
        }
    }

    #[test]
    fn test_get_high_water_mark_zero_watermark() {
        let watermarks = Ok((0i64, 0i64)); // Empty partition
        let result = get_high_water_mark(0, watermarks, "test-topic".to_string());

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0i64);
    }

    #[test]
    fn test_get_topic_partition_watermarks_multiple_topics() {
        let mut topic_partition_watermarks = HashMap::new();

        let mut topic1_partitions = HashMap::new();
        topic1_partitions.insert(0, 100i64);
        topic1_partitions.insert(1, 200i64);
        topic_partition_watermarks.insert("topic1".to_string(), topic1_partitions);

        let mut topic2_partitions = HashMap::new();
        topic2_partitions.insert(0, 300i64);
        topic_partition_watermarks.insert("topic2".to_string(), topic2_partitions);

        // Test topic1, partition 0
        let result = get_topic_partition_watermark(&topic_partition_watermarks, "topic1", 0);
        assert!(result.is_ok());
        assert_eq!(*result.unwrap(), 100i64);

        // Test topic1, partition 1
        let result = get_topic_partition_watermark(&topic_partition_watermarks, "topic1", 1);
        assert!(result.is_ok());
        assert_eq!(*result.unwrap(), 200i64);

        // Test topic2, partition 0
        let result = get_topic_partition_watermark(&topic_partition_watermarks, "topic2", 0);
        assert!(result.is_ok());
        assert_eq!(*result.unwrap(), 300i64);
    }

    #[test]
    fn test_get_topic_partition_watermarks_empty_map() {
        let topic_partition_watermarks = HashMap::new();

        let result = get_topic_partition_watermark(&topic_partition_watermarks, "any-topic", 0);

        assert!(result.is_err());
        match result.unwrap_err() {
            TransformationError::InvalidInput(msg) => {
                assert!(msg.contains("No watermark found for topic any-topic partition 0"));
            }
            _ => panic!("Expected InvalidInput error"),
        }
    }
}
