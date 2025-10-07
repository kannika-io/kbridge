use std::{collections::HashMap, time::Duration};

use log::info;
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    metadata::Metadata,
};

use crate::transform::transformation_errors::TransformationError;

pub fn get_high_watermark(consumer: &StreamConsumer, metadata: &Metadata, topics: &[&str]) -> Result<HashMap<String, HashMap<i32, i64>>, TransformationError> {
    let mut topic_partition_watermarks: HashMap<String, HashMap<i32, i64>> = HashMap::new();

    for topic in metadata.topics().iter().filter(|t| topics.contains(&t.name())) {
        let mut partition_map = HashMap::new();

        for partition in topic.partitions() {
            let partition_id = partition.id();

            match consumer.fetch_watermarks(topic.name(), partition_id, Duration::from_secs(5)) {
                Ok((_low, high)) => {
                    if high > 0 {
                        partition_map.insert(partition_id, high);
                        info!("Watermark for {}-{}: high={}", topic.name(), partition_id, high);
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
