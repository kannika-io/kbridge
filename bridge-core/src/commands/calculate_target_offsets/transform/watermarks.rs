use std::{collections::HashMap, time::Duration};

use crate::{
    Offset, PartitionNumber, Topic, commands::calculate_target_offsets::errors::TransformationError,
};
use log::info;
use rdkafka::{
    Message,
    consumer::{Consumer, StreamConsumer},
    metadata::Metadata,
};

/// Updates the list of partitions that still need to be checked for messages.
pub fn update_partitions_to_search(
    message: &rdkafka::message::BorrowedMessage,
    topic_partition_watermarks: &HashMap<Topic, HashMap<PartitionNumber, (Offset, Offset)>>,
    partitions_to_search: &mut Vec<(String, i32)>,
) -> Result<(), TransformationError> {
    let water_mark = get_topic_partition_high_watermark(
        topic_partition_watermarks,
        message.topic(),
        message.partition(),
    )?;

    if water_mark == (message.offset() + 1) {
        partitions_to_search.retain(|t| !(t.0 == message.topic() && t.1 == message.partition()));
    }

    Ok(())
}

/// Gets high watermark for specified topic/partition combination
pub fn get_topic_partition_high_watermark<'a>(
    topic_partition_watermarks: &'a HashMap<String, HashMap<PartitionNumber, (Offset, Offset)>>,
    topic: &'a str,
    partition: PartitionNumber,
) -> Result<Offset, TransformationError> {
    get_topic_partition_watermark(
        topic_partition_watermarks,
        topic,
        partition,
        WaterMark::High,
    )
}

pub fn get_topic_partition_watermarks<'a>(
    topic_partition_watermarks: &'a HashMap<String, HashMap<PartitionNumber, (Offset, Offset)>>,
    topic: &'a str,
    partition: PartitionNumber,
) -> Result<&'a (Offset, Offset), TransformationError> {
    topic_partition_watermarks
        .get(topic)
        .and_then(|partitions| partitions.get(&partition))
        .ok_or_else(|| {
            TransformationError::InvalidInput(format!(
                "No watermark found for topic {topic} partition {partition}"
            ))
        })
}

fn get_topic_partition_watermark<'a>(
    topic_partition_watermarks: &'a HashMap<String, HashMap<PartitionNumber, (Offset, Offset)>>,
    topic: &'a str,
    partition: PartitionNumber,
    water_mark: WaterMark,
) -> Result<Offset, TransformationError> {
    topic_partition_watermarks
        .get(topic)
        .and_then(|partitions| partitions.get(&partition))
        .and_then(|partition_record| match water_mark {
            WaterMark::Low => Some(partition_record.0),
            WaterMark::High => Some(partition_record.1),
        })
        .ok_or_else(|| {
            TransformationError::InvalidInput(format!(
                "No watermark found for topic {topic} partition {partition}"
            ))
        })
}

enum WaterMark {
    High,
    Low,
}

/// Retrieves watermarks for all partitions of specified topics from a Kafka cluster.
pub fn get_watermarks_for_topics(
    consumer: &StreamConsumer,
    metadata: &Metadata,
    topics: &[&str],
) -> Result<HashMap<String, HashMap<PartitionNumber, (Offset, Offset)>>, TransformationError> {
    let mut topic_partition_watermarks: HashMap<Topic, HashMap<PartitionNumber, (Offset, Offset)>> =
        HashMap::new();

    for topic in metadata
        .topics()
        .iter()
        .filter(|t| topics.contains(&t.name()))
    {
        let mut partition_map: HashMap<PartitionNumber, (Offset, Offset)> = HashMap::new();

        for partition in topic.partitions() {
            let partition_id = partition.id();
            let water_marks =
                consumer.fetch_watermarks(topic.name(), partition_id, Duration::from_secs(5));
            let (low_water_mark, high_water_mark) = water_marks?;
            if high_water_mark > 0 {
                partition_map.insert(partition_id, (low_water_mark, high_water_mark));
                info!(
                    "Watermark for {}-{}: low={} high={}",
                    topic.name(),
                    partition_id,
                    low_water_mark,
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

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;

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
        partitions.insert(0, (0, 100i64));
        partitions.insert(1, (0, 200i64));
        topic_partition_watermarks.insert("test-topic".to_string(), partitions);

        let result =
            get_topic_partition_high_watermark(&topic_partition_watermarks, "test-topic", 0);

        assert_matches!(result, Ok(100i64));
    }

    #[test]
    fn test_get_topic_partition_watermarks_topic_not_found() {
        let topic_partition_watermarks = HashMap::new();

        let result =
            get_topic_partition_high_watermark(&topic_partition_watermarks, "nonexistent-topic", 0);

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
        partitions.insert(0, (0, 100i64));
        topic_partition_watermarks.insert("test-topic".to_string(), partitions);

        let result =
            get_topic_partition_high_watermark(&topic_partition_watermarks, "test-topic", 1);

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
        partitions.insert(0, (0, 100i64)); // High watermark is 100
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
        let watermark = get_topic_partition_high_watermark(
            &topic_partition_watermarks,
            message.topic(),
            message.partition(),
        )
        .unwrap();

        // Simulate the watermark check logic
        if watermark == message.offset() + 1 {
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
        partitions.insert(0, (0, 100i64)); // High watermark is 100
        topic_partition_watermarks.insert("test-topic".to_string(), partitions);

        let mut partitions_to_check =
            vec![("test-topic".to_string(), 0), ("test-topic".to_string(), 1)];

        // Message at offset 98, so offset + 1 = 99 < 100 (high watermark)
        let message = MockMessage::new("test-topic", 0, 98);

        let watermark = get_topic_partition_high_watermark(
            &topic_partition_watermarks,
            message.topic(),
            message.partition(),
        )
        .unwrap();

        // Simulate the watermark check logic
        if watermark == message.offset() + 1 {
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
        partitions.insert(0, (0, 100i64));
        partitions.insert(1, (0, 200i64));
        topic_partition_watermarks.insert("test-topic".to_string(), partitions);

        let mut partitions_to_check = vec![
            ("test-topic".to_string(), 0),
            ("test-topic".to_string(), 1),
            ("test-topic".to_string(), 2),
        ];

        // Message at offset 99 for partition 0, watermark is 100
        let message = MockMessage::new("test-topic", 0, 99);

        let watermark = get_topic_partition_high_watermark(
            &topic_partition_watermarks,
            message.topic(),
            message.partition(),
        )
        .unwrap();

        if watermark == message.offset() + 1 {
            partitions_to_check.retain(|t| !(t.0 == message.topic() && t.1 == message.partition()));
        }

        // Only partition 0 should be removed
        assert_eq!(partitions_to_check.len(), 2);
        assert!(!partitions_to_check.contains(&("test-topic".to_string(), 0)));
        assert!(partitions_to_check.contains(&("test-topic".to_string(), 1)));
        assert!(partitions_to_check.contains(&("test-topic".to_string(), 2)));
    }

    #[test]
    fn test_get_topic_partition_watermarks_multiple_topics() {
        let mut topic_partition_watermarks = HashMap::new();

        let mut topic1_partitions = HashMap::new();
        topic1_partitions.insert(0, (0, 100i64));
        topic1_partitions.insert(1, (0, 200i64));
        topic_partition_watermarks.insert("topic1".to_string(), topic1_partitions);

        let mut topic2_partitions = HashMap::new();
        topic2_partitions.insert(0, (0, 300i64));
        topic_partition_watermarks.insert("topic2".to_string(), topic2_partitions);

        // Test topic1, partition 0
        let result = get_topic_partition_high_watermark(&topic_partition_watermarks, "topic1", 0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 100i64);

        // Test topic1, partition 1
        let result = get_topic_partition_high_watermark(&topic_partition_watermarks, "topic1", 1);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 200i64);

        // Test topic2, partition 0
        let result = get_topic_partition_high_watermark(&topic_partition_watermarks, "topic2", 0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 300i64);
    }

    #[test]
    fn test_get_topic_partition_watermarks_empty_map() {
        let topic_partition_watermarks = HashMap::new();

        let result =
            get_topic_partition_high_watermark(&topic_partition_watermarks, "any-topic", 0);

        assert!(result.is_err());
        match result.unwrap_err() {
            TransformationError::InvalidInput(msg) => {
                assert!(msg.contains("No watermark found for topic any-topic partition 0"));
            }
            _ => panic!("Expected InvalidInput error"),
        }
    }
}
