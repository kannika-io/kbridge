use std::{collections::HashMap, time::Duration};

use log::info;
use rdkafka::{Message, consumer::Consumer, error::KafkaError};

use crate::transform::{
    consumer_initialization::{initialize_consumer, manage_topic_subscriptions},
    header::get_offset_from_header,
    transformation_errors::{
        FetchOffsetError, KafkaMessage, OffsetMappingTransformationError, TransformationError,
    },
};

mod consumer_initialization;
mod header;
pub mod transformation_errors;

pub async fn get_target_offsets(
    brokers: &str,
    topics: &[&str],
    offset_header_key: &str,
    source_offsets: Vec<&i64>,
) -> Result<HashMap<i64, i64>, TransformationError> {
    let consumer = initialize_consumer(brokers)?;
    manage_topic_subscriptions(&consumer, topics)?;

    // Fetch metadata for all topics
    let metadata = consumer
        .fetch_metadata(None, Duration::from_secs(5))
        .expect("Failed to fetch metadata");

    let mut transformations = HashMap::new();

    // Map: topic -> (partition -> high watermark)
    let mut topic_partition_watermarks: HashMap<String, HashMap<i32, i64>> = HashMap::new();

    for topic in metadata.topics() {
        let mut partition_map = HashMap::new();

        for partition in topic.partitions() {
            let partition_id = partition.id();

            match consumer.fetch_watermarks(topic.name(), partition_id, Duration::from_secs(5)) {
                Ok((_low, high)) => {
                    if high > 0 {
                        partition_map.insert(partition_id, high);
                    }
                }
                Err(e) => {
                    eprintln!(
                        "Failed to fetch watermarks for {}-{}: {:?}",
                        topic.name(),
                        partition_id,
                        e
                    );
                }
            }
        }

        topic_partition_watermarks.insert(topic.name().to_string(), partition_map);
    }

    let mut topics_to_check: Vec<(String, i32)> = topic_partition_watermarks
        .iter()
        .filter(|partition| topics.contains(&partition.0.as_str()))
        .flat_map(|t| t.1.iter().map(|p| (t.0.to_string(), *p.0)))
        .collect();

    info!("topics to check: {topics_to_check:?}");

    info!("Initialization completed");
    while !source_offsets.is_empty() && !topics_to_check.is_empty() {
        let consume_result = consumer.recv().await?;
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
        handle_offset_mapping(
            &mut transformations,
            &source_offsets,
            &source_offset,
            &consume_result.offset(),
        )?;

        let water_mark = topic_partition_watermarks
            .get(consume_result.topic())
            .unwrap()
            .get(&consume_result.partition())
            .unwrap();

        info!("Water mark: {water_mark}");
        if water_mark == &(consume_result.offset() + 1) {
            topics_to_check
                .retain(|t| !(t.0 == consume_result.topic() && t.1 == consume_result.partition()));
        }
        info!("Remaining topics to check: {topics_to_check:?}")
    }
    Ok(transformations)
}

fn handle_offset_mapping(
    transformations: &mut HashMap<i64, i64>,
    source_offsets: &Vec<&i64>,
    source_offset_from_message: &i64,
    current_offset_from_message: &i64,
) -> Result<(), OffsetMappingTransformationError> {
    if source_offsets.contains(&source_offset_from_message) {
        let insertion_result =
            transformations.insert(*source_offset_from_message, *current_offset_from_message);
        // This should never happen
        if let Some(target_offset) = insertion_result {
            // TODO: handle legacy offset that is not present anymore in topic
            return Err(
                OffsetMappingTransformationError::SourceOffsetAlreadyPresent {
                    source_offset: *source_offset_from_message,
                    target_offset: *current_offset_from_message,
                    previous_target_offset: target_offset,
                },
            );
        }
    }
    Ok(())
}

impl From<OffsetMappingTransformationError> for TransformationError {
    fn from(value: OffsetMappingTransformationError) -> Self {
        TransformationError::OffsetMappingTransformationError(value)
    }
}

impl From<KafkaError> for TransformationError {
    fn from(value: KafkaError) -> Self {
        TransformationError::KafkaError(value.to_string())
    }
}

impl From<FetchOffsetError> for TransformationError {
    fn from(value: FetchOffsetError) -> Self {
        TransformationError::FetchOffsetError(value)
    }
}
