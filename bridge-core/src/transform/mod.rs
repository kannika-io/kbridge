use std::{collections::HashMap, time::Duration};

use log::info;
use rdkafka::{Message, consumer::Consumer};

use crate::transform::{
    consumer_initialization::{initialize_consumer, manage_topic_subscriptions},
    header::get_offset_from_header,
    offset_mapping::handle,
    transformation_errors::{FetchOffsetError, KafkaMessage, TransformationError},
    watermarks::get_watermarks_from_metadata,
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
) -> Result<HashMap<String, HashMap<i64, i64>>, TransformationError> {
    // Validate input parameters
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

    // Initialize consumer + subscribe to topics
    let consumer = initialize_consumer(brokers)?;
    manage_topic_subscriptions(&consumer, topics)?;

    // Fetch metadata for all topics with proper error handling
    let metadata = consumer
        .fetch_metadata(None, Duration::from_secs(5))
        .map_err(|e| TransformationError::MetadataFetchFailed(e.to_string()))?;

    let mut transformations = HashMap::new();
    let topic_partition_watermarks = get_watermarks_from_metadata(&consumer, &metadata, topics)?;

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

    while !source_offsets.is_empty() && !topics_and_partitions_to_check.is_empty() {
        let consume_result =
            consumer
                .recv()
                .await
                .map_err(|e| TransformationError::FailedToReceiveMessages {
                    message: e.to_string(),
                    topic_selector: topics.join(","),
                })?;

        let message = KafkaMessage {
            partition: consume_result.partition(),
            offset: consume_result.offset(),
            topic: consume_result.topic().to_string(),
        };
        info!("Handling Kafka Message: {message}");

        let source_offset = match consume_result.headers() {
            Some(headers) => get_offset_from_header(headers, offset_header_key),
            None => Err(FetchOffsetError::NoHeadersInMessage),
        }?;

        info!("Source offset: {source_offset}");
        handle(
            &mut transformations,
            &source_offsets,
            &source_offset,
            &consume_result.offset(),
            consume_result.topic(),
        )?;

        let water_mark = topic_partition_watermarks
            .get(consume_result.topic())
            .and_then(|partitions| partitions.get(&consume_result.partition()))
            .ok_or_else(|| {
                TransformationError::InvalidInput(format!(
                    "No watermark found for topic {} partition {}",
                    consume_result.topic(),
                    consume_result.partition()
                ))
            })?;

        info!("Water mark: {water_mark}");
        if water_mark == &(consume_result.offset() + 1) {
            topics_and_partitions_to_check
                .retain(|t| !(t.0 == consume_result.topic() && t.1 == consume_result.partition()));
        }
        info!("Remaining topics to check: {topics_and_partitions_to_check:?}");
    }

    Ok(transformations)
}
