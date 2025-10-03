use std::collections::HashMap;

use log::info;
use rdkafka::Message;

use crate::transform::{
    consumer_initialization::{initialize_consumer, manage_topic_subscriptions},
    header::get_offset_from_header,
    transformation_errors::{FetchOffsetError, KafkaMessage, TransformationError},
};

mod consumer_initialization;
mod header;
mod transformation_errors;

pub async fn transform(
    brokers: &str,
    topics: &[&str],
    offset_header_key: &str,
    source_offsets: Vec<usize>,
) -> Result<HashMap<usize, usize>, TransformationError> {
    let consumer = initialize_consumer(brokers)?;
    manage_topic_subscriptions(&consumer, topics)?;

    let mut transformations = HashMap::new();

    info!("Initialization completed");
    while !source_offsets.is_empty() {
        match consumer.recv().await {
            Err(e) => return Err(TransformationError::SubscribingFailed(e.to_string())),
            Ok(m) => {
                info!("Message Received");
                let result = match m.headers() {
                    Some(headers) => get_offset_from_header(headers, offset_header_key),
                    None => Err(FetchOffsetError::NoHeadersInMessage),
                };

                match result {
                    Ok(value) => transformations.insert(value, value),
                    Err(error) => {
                        return Err(TransformationError::ErrorParsingHeader {
                            message: KafkaMessage {
                                partition: m.partition(),
                                offset: m.offset(),
                                topic: m.topic().to_string(),
                            },
                            error,
                        });
                    }
                };
            }
        };
    }
    Ok(transformations)
}
