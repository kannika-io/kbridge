use calculate_new_offset::CalculateOffsetInput;
use log::{error, info, trace, warn};
use rdkafka::Message;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::metadata::Metadata;
use std::collections::HashMap;
use std::time::Duration;
use watermarks::{
    get_topic_partition_watermarks, get_watermarks_for_topics, update_partitions_to_search,
};

use crate::commands::calculate_target_offsets::errors::TransformationError;
use crate::commands::calculate_target_offsets::transform::consumer_group_offset::try_find_missing_offsets;
use crate::commands::calculate_target_offsets::transform::consumer_group_offset_mapping::handle_missing_offsets;
use crate::commands::calculate_target_offsets::transform::message_header::extract_source_offset_from_message_headers;
use crate::helpers::get_unique_topics_from_offset_snapshot;
use crate::{
    ConsumerGroup, ConsumerGroupRecord, Offset, OffsetSnapshot, Partition, Topic,
    TransformationRecord,
};

mod calculate_new_offset;
mod consumer_group_offset;
mod consumer_group_offset_mapping;
mod message_header;
mod watermarks;

/// Transforms source offsets to target offsets by consuming Kafka messages and matching offset headers.
///
/// This function consumes messages from Kafka topics and builds a mapping between source offsets
/// (stored in message headers) and target offsets (the actual message offsets in the target cluster).
/// It's designed to help with offset translation when migrating data between Kafka clusters.
///
/// # Arguments
///
/// * `offset_header_key` - The header key used to store source offsets in Kafka messages
/// * `source_offsets` - A snapshot of offsets from the source cluster that need to be transformed
/// * `consumer` - A Kafka StreamConsumer configured to read from the target cluster
/// * `metadata` - Kafka cluster metadata containing topic and partition information
///
/// # Returns
///
/// Returns a `HashMap` where:
/// - Key: Consumer group name
/// - Value: Vector of transformation records, each containing (topic, partition, source_offset, target_offset)
///
/// # Errors
///
/// This function will return an error if:
/// - `source_offsets` is empty
/// - `offset_header_key` is empty or invalid
/// - Consumer fails to receive messages from Kafka
/// - Message headers cannot be parsed
/// - No partitions are found to search
/// - Watermark fetching fails
///
/// # Behavior
///
/// 1. Validates input parameters
/// 2. Fetches high watermarks for all relevant topic partitions
/// 3. Consumes messages from all partitions until watermarks are reached
/// 4. For each message, extracts the source offset from headers and maps it to the current message offset
/// 5. Handles missing offsets by finding the nearest available offsets
/// 6. Returns the complete transformation mapping
/// ```
pub async fn get_target_offsets(
    offset_header_key: &str,
    source_offsets: &OffsetSnapshot,
    consumer: StreamConsumer,
    metadata: Metadata,
) -> Result<HashMap<ConsumerGroup, Vec<TransformationRecord>>, TransformationError> {
    validate_input_parameters(source_offsets, offset_header_key)?;

    let mut transformations = HashMap::new();
    let topics: Vec<&str> = get_unique_topics_from_offset_snapshot(source_offsets);

    let topic_partition_watermarks = get_watermarks_for_topics(&consumer, &metadata, &topics)?;

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

    trace!("Starting consumer loop");

    // TODO add timeout to consumer loop
    // If target topic without messages, will otherwise be stuck in an endless loop
    while !partitions_to_search.is_empty() {
        let consume_result =
            consumer
                .recv()
                .await
                .map_err(|e| TransformationError::FailedToReceiveMessages {
                    message: e.to_string(),
                    topic_selector: topics.join(","),
                })?;

        let source_offset_to_search = missing_offsets
            .iter()
            .find(|m| m.1 == consume_result.topic() && m.2 == consume_result.partition())
            .iter()
            .map(|o| o.3)
            .min();

        trace!("Source offset: {:?}", source_offset_to_search);

        if let Some(source_offset) = source_offset_to_search {
            let source_offset_current_message = extract_source_offset_from_message_headers(
                consume_result.headers(),
                offset_header_key,
            )?;

            let watermarks = *get_topic_partition_watermarks(
                &topic_partition_watermarks,
                consume_result.topic(),
                consume_result.partition(),
            )?;

            warn!(
                "Calculating new offset for topic {} and partition {}",
                consume_result.topic(),
                consume_result.partition()
            );

            let new_offset = calculate_new_offset::execute(CalculateOffsetInput {
                current_target: consume_result.offset(),
                current_source: source_offset_current_message,
                target_source: source_offset,
                high_water_mark: watermarks.1,
                low_water_mark: watermarks.0,
            });

            if let Some(offset) = new_offset {
                warn!(
                    "Setting new offset for topic {}, partition {} : {}",
                    consume_result.topic(),
                    consume_result.partition(),
                    offset
                );
                consumer.seek(
                    consume_result.topic(),
                    consume_result.partition(),
                    rdkafka::Offset::Offset(offset),
                    Duration::from_secs(5),
                )?;
            }

            try_find_missing_offsets(
                &consume_result,
                source_offset_current_message,
                source_offsets,
                &mut transformations,
                &mut missing_offsets,
                &mut nearest_offsets,
            )?;

            trace!("missing offsets: {:?}", missing_offsets);
            trace!("remaining partitions: {:?}", partitions_to_search);
        } else {
            trace!("Skipping finding missing offsets.");
        }
        update_partitions_to_search(
            &consume_result,
            &topic_partition_watermarks,
            &mut partitions_to_search,
        )?;
    }

    trace!("Ending consumer loop");
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

    #[test]
    fn test_validate_input_parameters_success() {
        let source_offsets = vec![OffsetRecord {
            topic: "test-topic".to_string(),
            partition: 0,
            offset: 100,
            consumer_group: "test-group".to_string(),
        }];
        let offset_header_key = "source-offset";

        let result = validate_input_parameters(&source_offsets, offset_header_key);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_input_parameters_empty_source_offsets() {
        let source_offsets = vec![];
        let offset_header_key = "source-offset";

        let result = validate_input_parameters(&source_offsets, offset_header_key);
        assert!(result.is_err());

        match result.unwrap_err() {
            TransformationError::InvalidInput(msg) => {
                assert_eq!(msg, "Source offsets cannot be empty");
            }
            _ => panic!("Expected InvalidInput error"),
        }
    }

    #[test]
    fn test_validate_input_parameters_empty_offset_header_key() {
        let source_offsets = vec![OffsetRecord {
            topic: "test-topic".to_string(),
            partition: 0,
            offset: 100,
            consumer_group: "test-group".to_string(),
        }];
        let offset_header_key = "";

        let result = validate_input_parameters(&source_offsets, offset_header_key);
        assert!(result.is_err());

        match result.unwrap_err() {
            TransformationError::InvalidInput(msg) => {
                assert_eq!(msg, "Offset header key cannot be empty");
            }
            _ => panic!("Expected InvalidInput error"),
        }
    }

    #[test]
    fn test_validate_input_parameters_whitespace_only_offset_header_key() {
        let source_offsets = vec![OffsetRecord {
            topic: "test-topic".to_string(),
            partition: 0,
            offset: 100,
            consumer_group: "test-group".to_string(),
        }];
        let offset_header_key = "   ";

        // This should pass validation since we only check for empty string, not whitespace
        let result = validate_input_parameters(&source_offsets, offset_header_key);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_input_parameters_multiple_source_offsets() {
        let source_offsets = vec![
            OffsetRecord {
                topic: "test-topic-1".to_string(),
                partition: 0,
                offset: 100,
                consumer_group: "test-group-1".to_string(),
            },
            OffsetRecord {
                topic: "test-topic-2".to_string(),
                partition: 1,
                offset: 200,
                consumer_group: "test-group-2".to_string(),
            },
        ];
        let offset_header_key = "source-offset";

        let result = validate_input_parameters(&source_offsets, offset_header_key);
        assert!(result.is_ok());
    }
}
