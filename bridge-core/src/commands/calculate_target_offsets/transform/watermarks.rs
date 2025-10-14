use std::{collections::HashMap, time::Duration};

use crate::commands::calculate_target_offsets::errors::TransformationError;
use log::info;
use rdkafka::{
    Message,
    consumer::{Consumer, StreamConsumer},
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
    let water_mark = get_topic_partition_watermarks(
        topic_partition_watermarks,
        message.topic(),
        message.partition(),
    )?;

    if water_mark == &(message.offset() + 1) {
        partitions_to_check.retain(|t| !(t.0 == message.topic() && t.1 == message.partition()));
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
        .ok_or_else(|| {
            TransformationError::InvalidInput(format!(
                "No watermark found for topic {topic} partition {partition}"
            ))
        })
}

pub fn get_high_watermark(
    consumer: &StreamConsumer,
    metadata: &Metadata,
    topics: &[&str],
) -> Result<HashMap<String, HashMap<i32, i64>>, TransformationError> {
    let mut topic_partition_watermarks: HashMap<String, HashMap<i32, i64>> = HashMap::new();

    for topic in metadata
        .topics()
        .iter()
        .filter(|t| topics.contains(&t.name()))
    {
        let mut partition_map = HashMap::new();

        for partition in topic.partitions() {
            let partition_id = partition.id();

            match consumer.fetch_watermarks(topic.name(), partition_id, Duration::from_secs(5)) {
                Ok((_low, high)) => {
                    if high > 0 {
                        partition_map.insert(partition_id, high);
                        info!(
                            "Watermark for {}-{}: high={}",
                            topic.name(),
                            partition_id,
                            high
                        );
                    }
                }
                Err(e) => {
                    return Err(TransformationError::WatermarkFetchFailed {
                        topic: topic.name().to_string(),
                        partition: partition_id,
                        reason: e.to_string(),
                    });
                }
            }
        }

        if !partition_map.is_empty() {
            topic_partition_watermarks.insert(topic.name().to_string(), partition_map);
        }
    }

    Ok(topic_partition_watermarks)
}

#[cfg(test)]
mod tests {
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

        let result =
            get_topic_partition_watermarks(&topic_partition_watermarks, "nonexistent-topic", 0);

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
}
