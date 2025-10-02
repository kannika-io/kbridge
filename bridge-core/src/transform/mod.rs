use std::collections::HashMap;

use rdkafka::{
    ClientConfig, Message,
    config::RDKafkaLogLevel,
    consumer::{Consumer, StreamConsumer},
    message::Headers,
};

fn initialize_consumer(brokers: &str) -> Result<StreamConsumer, TransformationError> {
    let mut config = ClientConfig::new();

    config
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("enable.auto.commit", "false")
        .set_log_level(RDKafkaLogLevel::Debug);

    match config.create() {
        Ok(consumer) => Ok(consumer),
        Err(kafka_error) => Err(TransformationError::ConsumerInitializationFailed(
            kafka_error.to_string(),
        )),
    }
}

fn manage_topic_subscriptions(
    consumer: &StreamConsumer,
    topics: &[&str],
) -> Result<(), TransformationError> {
    match consumer.subscribe(topics) {
        Ok(_) => Ok(()),
        Err(kafka_error) => Err(TransformationError::SubscribingFailed(
            kafka_error.to_string(),
        )),
    }
}

fn get_offset_from_header(
    headers: &rdkafka::message::BorrowedHeaders,
    offset_header_key: &str,
) -> Result<usize, TransformationError> {
    let header = headers.iter().find(|h| h.key == offset_header_key);
    if let Some(header_value) = header {
        match header_value.value {
            Some(value) => {
                if let Ok(parsed_from_utf8) = str::from_utf8(value)
                    && let Ok(result) = parsed_from_utf8.parse::<usize>()
                {
                    return Ok(result);
                }
                Err(TransformationError::ErrorParsingHeader(format!(
                    "Could not parse {value:?} to usize"
                )))
            }
            None => Err(TransformationError::ErrorParsingHeader(String::from(
                "No header found for message",
            ))),
        }
    } else {
        Err(TransformationError::ErrorParsingHeader(String::from(
            "No header found for message",
        )))
    }
}

pub async fn transform(
    brokers: &str,
    topics: &[&str],
    offset_header_key: &str,
    source_offsets: Vec<usize>,
) -> Result<HashMap<usize, usize>, TransformationError> {
    let consumer = initialize_consumer(brokers)?;
    manage_topic_subscriptions(&consumer, topics)?;

    let mut transformations = HashMap::new();

    while !source_offsets.is_empty() {
        match consumer.recv().await {
            Err(e) => return Err(TransformationError::SubscribingFailed(e.to_string())),
            Ok(m) => {
                let result = match m.headers() {
                    Some(headers) => get_offset_from_header(headers, offset_header_key),
                    None => Err(TransformationError::ErrorParsingHeader(String::from(
                        "No headers in message",
                    ))),
                };

                match result {
                    Ok(value) => transformations.insert(value, value),
                    Err(error) => return Err(error),
                };
            }
        };
    }
    Ok(transformations)
}

pub enum TransformationError {
    ConsumerInitializationFailed(String),
    SubscribingFailed(String),
    RetrievingOffsetHeaderValueFailed(String),
    ErrorParsingHeader(String),
}
