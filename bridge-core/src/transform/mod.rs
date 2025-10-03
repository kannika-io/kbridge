use std::collections::HashMap;

use log::info;
use rdkafka::{Message, error::KafkaError};

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

    let mut transformations = HashMap::new();

    info!("Initialization completed");
    while !source_offsets.is_empty() {
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

        handle_offset_mapping(
            &mut transformations,
            &source_offsets,
            &source_offset,
            &consume_result.offset(),
        )?;
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
