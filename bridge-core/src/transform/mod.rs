use std::{collections::HashMap, time::Duration};

use log::info;
use rdkafka::{
    Message,
    consumer::{Consumer, StreamConsumer},
    metadata::Metadata,
};

use crate::transform::{
    consumer_initialization::{initialize_consumer, manage_topic_subscriptions}, header::get_offset_from_header, offset_mapping::insert_transformations, transformation_errors::{FetchOffsetError, TransformationError}, watermarks::get_high_watermark
};

mod consumer_initialization;
mod header;
mod offset_mapping;
pub mod transformation_errors;
mod watermarks;

pub async fn get_target_offsets(
    brokers: &str,
    topics: &[&str],
    offset_header_key: &str,
    source_offsets: Vec<&i64>,
) -> Result<HashMap<String,HashMap<i64, i64>>, TransformationError> {
    validate_input_parameters(&source_offsets, offset_header_key)?;
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
        return Err(TransformationError::NoValidPartitions(
            topics.iter().map(|s| s.to_string()).collect(),
        ));
    }

    info!("Topics to check: {topics_and_partitions_to_check:?}");
    info!("Initialization completed");

    // Consume all messages from the topics we are subscribed to
    while !source_offsets.is_empty() && !topics_and_partitions_to_check.is_empty() {
        let consume_result =
            consumer
                .recv()
                .await
                .map_err(|e| TransformationError::FailedToReceiveMessages {
                    message: e.to_string(),
                    topic_selector: topics.join(","),
                })?;

        let source_offset = match consume_result.headers() {
            Some(headers) => get_offset_from_header(headers, offset_header_key),
            None => Err(FetchOffsetError::NoHeadersInMessage),
        }?;

        if source_offsets.contains(&&source_offset) {
            insert_transformations(
                &mut transformations,
                &source_offset,
                &consume_result.offset(),
                &consume_result.topic()
            )?;
        }

        let water_mark = get_topic_partition_watermarks(
            &topic_partition_watermarks,
            consume_result.topic(),
            consume_result.partition(),
        )?;

        if water_mark == &(consume_result.offset() + 1) {
            topics_and_partitions_to_check
                .retain(|t| !(t.0 == consume_result.topic() && t.1 == consume_result.partition()));
        }
    }

    Ok(transformations)
}

fn validate_input_parameters(
    source_offsets: &[&i64],
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

async fn setup_consumer_and_metadata(
    brokers: &str,
    topics: &[&str],
) -> Result<(StreamConsumer, Metadata), TransformationError> {
    let consumer = initialize_consumer(brokers)?;
    manage_topic_subscriptions(&consumer, topics)?;

    let metadata = consumer
        .fetch_metadata(None, Duration::from_secs(5))
        .map_err(|e| TransformationError::MetadataFetchFailed(e.to_string()))?;

    Ok((consumer, metadata))
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

    #[tokio::test]
    async fn test_get_target_offsets_empty_source_offsets() {
        let brokers = "localhost:9092";
        let topics = &["test-topic"];
        let offset_header_key = "source-offset";
        let source_offsets = vec![];

        let result = get_target_offsets(brokers, topics, offset_header_key, source_offsets).await;

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
        let source_offsets = vec![&100i64, &200i64];

        let result = get_target_offsets(brokers, topics, offset_header_key, source_offsets).await;

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
        let source_offsets = vec![&100i64, &200i64];

        let result = get_target_offsets(brokers, topics, offset_header_key, source_offsets).await;

        assert!(result.is_err());
        match result.unwrap_err() {
            TransformationError::InvalidInput(msg) => {
                assert_eq!(msg, "Brokers string cannot be empty");
            }
            _ => panic!("Expected InvalidInput error for empty brokers"),
        }
    }
}
