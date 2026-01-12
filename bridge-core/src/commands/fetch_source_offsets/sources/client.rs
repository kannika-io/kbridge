use crate::commands::fetch_source_offsets::errors::{FetchMetadataError, ImportOffsetsError};
use crate::{OffsetRecord, OffsetSnapshot};
use log::{trace, warn};
use rdkafka::{
    admin::{AdminClient, AdminOptions, ListConsumerGroupOffsets},
    client::DefaultClientContext,
    consumer::{BaseConsumer, Consumer},
    util::Timeout,
};
use std::collections::HashMap;
use std::time::Duration;

const NO_OFFSET: i64 = -1001;

pub struct Metadata {
    pub consumer_groups: Vec<String>,
    pub topics_and_partitions: HashMap<String, Vec<i32>>,
}

/// Fetches metadata from kafka cluster: All consumer groups and topic-partition combos
pub fn fetch_metadata(
    consumer: BaseConsumer,
    topics: impl IntoIterator<Item = String>,
    client_timeout: Duration,
) -> Result<Metadata, FetchMetadataError> {
    let topics = topics.into_iter().collect::<Vec<String>>();

    let group_list = consumer.fetch_group_list(None, Timeout::After(client_timeout))?;

    let metadata = consumer.fetch_metadata(None, Timeout::After(client_timeout))?;

    let mut topics_and_partitions: HashMap<String, Vec<i32>> = HashMap::new();

    metadata
        .topics()
        .iter()
        // Optimization: filter topics when fetching metadata
        .filter(|mt| topics.is_empty() || topics.contains(&mt.name().to_string()))
        .for_each(|topic| {
            topic.partitions().iter().for_each(|part| {
                topics_and_partitions
                    .entry(topic.name().to_string())
                    .and_modify(|t| t.push(part.id()))
                    .or_insert(vec![part.id()]);
            })
        });

    Ok(Metadata {
        consumer_groups: group_list
            .groups()
            .iter()
            .map(|g| g.name().to_string())
            .collect(),
        topics_and_partitions,
    })
}

/// Fetches all committed consumer group offsets from metadata provided using AdminClient
pub async fn fetch_all_committed_consumer_group_offsets(
    metadata: Metadata,
    admin_client: &AdminClient<DefaultClientContext>,
    client_timeout: Duration,
) -> Result<OffsetSnapshot, ImportOffsetsError> {
    let mut all_offsets = OffsetSnapshot::new();

    if metadata.consumer_groups.is_empty() {
        warn!("No consumer groups found to fetch offsets for");
        return Ok(all_offsets);
    }

    trace!(
        "Fetching committed offsets for {} groups",
        metadata.consumer_groups.len()
    );

    let opts = AdminOptions::new().request_timeout(Some(client_timeout));

    // Fetch offsets for each group individually
    // Note: Calling one group at a time to work around potential librdkafka limitations
    for group_id in &metadata.consumer_groups {
        let group_request = ListConsumerGroupOffsets::new(group_id.as_str());

        let results = admin_client
            .list_consumer_group_offsets(&[group_request], &opts)
            .await?;

        for result in results {
            match result {
                Ok((group_name, offsets)) => {
                    trace!("Received offsets for group {}", group_name);
                    for elem in offsets.elements() {
                        // Filter by topics we care about
                        if !metadata.topics_and_partitions.is_empty()
                            && !metadata.topics_and_partitions.contains_key(elem.topic())
                        {
                            continue;
                        }

                        trace!("Committed offset: {:?}", elem);
                        match elem.offset().to_raw() {
                            Some(value) if value != NO_OFFSET => {
                                let record = OffsetRecord {
                                    topic: elem.topic().to_string(),
                                    partition: elem.partition(),
                                    offset: value,
                                    consumer_group: group_name.clone(),
                                };
                                all_offsets.push(record);
                            }
                            None => {
                                warn!("Error fetching offset for group {}. Ignoring.", group_name);
                            }
                            _ => {
                                // Offset not found
                            }
                        }
                    }
                }
                Err((group_name, error_code)) => {
                    warn!(
                        "Failed to fetch offsets for group {}: {:?}",
                        group_name, error_code
                    );
                }
            }
        }
    }

    if all_offsets.is_empty() {
        warn!(
            "No offsets were found on topics-partition combos {:?}",
            &metadata.topics_and_partitions
        );
    }

    Ok(all_offsets)
}
