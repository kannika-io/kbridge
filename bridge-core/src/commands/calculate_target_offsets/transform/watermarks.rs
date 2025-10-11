use std::{collections::HashMap, time::Duration};

use log::info;
use rdkafka::{
    Message,
    consumer::{Consumer, StreamConsumer},
    metadata::Metadata,
};
use crate::commands::calculate_target_offsets::errors::TransformationError;

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
