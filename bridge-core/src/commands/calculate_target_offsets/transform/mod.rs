use log::trace;
use rdkafka::consumer::StreamConsumer;
use rdkafka::metadata::Metadata;
use std::collections::HashMap;

use crate::commands::calculate_target_offsets::errors::TransformationError;
use crate::commands::calculate_target_offsets::transform::consumer_group_offset::try_find_missing_offsets;
use crate::commands::calculate_target_offsets::transform::consumer_group_offset_mapping::handle_missing_offsets;
use crate::commands::calculate_target_offsets::transform::message_header::extract_source_offset_from_message;
use crate::commands::calculate_target_offsets::transform::watermarks::{
    get_high_watermark, update_partitions_to_check,
};
use crate::get_unique_topics_from_offset_snapshot;
use crate::{
    ConsumerGroup, ConsumerGroupRecord, OffsetSnapshot, Partition, Topic, TransformationRecord,
};

pub mod consumer;
mod consumer_group_offset;
mod consumer_group_offset_mapping;
mod message_header;
mod watermarks;

pub async fn get_target_offsets(
    offset_header_key: &str,
    source_offsets: &OffsetSnapshot,
    consumer: StreamConsumer,
    metadata: Metadata,
) -> Result<HashMap<ConsumerGroup, Vec<TransformationRecord>>, TransformationError> {
    validate_input_parameters(source_offsets, offset_header_key)?;

    let mut transformations = HashMap::new();
    let topics: Vec<&str> = get_unique_topics_from_offset_snapshot(source_offsets);

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

    trace!("Starting consumer loop");
    // Consume all messages from the topics we are subscribed to

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
