//! Kafka Consumer for metadata, offset operations, and message streaming.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::RwLock;

use crate::broker::metadata::{ClusterMetadata, MetadataManager};
use crate::broker::coordinator::CoordinatorManager;
use crate::config::{CommonProperties, ConsumerProperties};
use crate::connection::ConnectionPool;
use crate::error::Error;
use crate::types::message::Message;
use crate::types::topic_partition::{CommitMode, TopicPartitionList};

/// Consumer group information.
#[derive(Debug, Clone)]
pub struct GroupInfo {
    /// Group ID.
    pub group_id: String,
    /// Protocol type (e.g., "consumer").
    pub protocol_type: String,
    /// Group state.
    pub state: String,
}

/// Kafka Consumer for metadata, offset operations, and message streaming.
pub struct Consumer {
    /// Group ID for this consumer.
    group_id: String,
    /// Connection pool.
    pool: Arc<ConnectionPool>,
    /// Metadata manager.
    metadata_manager: MetadataManager,
    /// Coordinator manager.
    coordinator_manager: CoordinatorManager,
    /// Currently assigned partitions.
    assignments: Arc<RwLock<TopicPartitionList>>,
}

impl Consumer {
    /// Create a new consumer from properties.
    pub(crate) fn new(props: ConsumerProperties) -> Result<Self, Error> {
        let common: CommonProperties = props.clone().into();
        let pool = Arc::new(ConnectionPool::from_properties(&common)?);

        let group_id = props
            .group_id()
            .ok_or_else(|| Error::MissingConfig("group.id".to_owned()))?
            .to_owned();

        let metadata_manager = MetadataManager::new(pool.clone());
        let coordinator_manager = CoordinatorManager::new(pool.clone());

        Ok(Self {
            group_id,
            pool,
            metadata_manager,
            coordinator_manager,
            assignments: Arc::new(RwLock::new(TopicPartitionList::new())),
        })
    }

    // === Metadata operations ===

    /// Fetch metadata for all topics.
    pub async fn fetch_metadata(&self, timeout: Duration) -> Result<ClusterMetadata, Error> {
        self.metadata_manager.fetch_metadata(timeout).await
    }

    /// Fetch metadata for specific topics.
    pub async fn fetch_metadata_for_topics(
        &self,
        topics: &[&str],
        timeout: Duration,
    ) -> Result<ClusterMetadata, Error> {
        self.metadata_manager
            .fetch_metadata_for_topics(topics, timeout)
            .await
    }

    /// List all consumer groups.
    pub async fn fetch_group_list(&self, timeout: Duration) -> Result<Vec<GroupInfo>, Error> {
        use kafka_protocol::messages::ListGroupsRequest;

        let conn = self.pool.get_any_connection().await?;
        let request = ListGroupsRequest::default();
        let response = conn.send_request(request, timeout).await?;

        // Check for errors
        if response.error_code != 0 {
            return Err(Error::kafka(
                response.error_code,
                "Failed to list consumer groups",
            ));
        }

        let groups = response
            .groups
            .iter()
            .map(|g| GroupInfo {
                group_id: g.group_id.to_string(),
                protocol_type: g.protocol_type.to_string(),
                state: g.group_state.to_string(),
            })
            .collect();

        Ok(groups)
    }

    /// Fetch watermarks (earliest/latest offsets) for a partition.
    pub async fn fetch_watermarks(
        &self,
        topic: &str,
        partition: i32,
        timeout: Duration,
    ) -> Result<(i64, i64), Error> {
        // Fetch earliest offset
        let earliest = self
            .fetch_offset_for_timestamp(topic, partition, -2, timeout)
            .await?;

        // Fetch latest offset
        let latest = self
            .fetch_offset_for_timestamp(topic, partition, -1, timeout)
            .await?;

        Ok((earliest, latest))
    }

    async fn fetch_offset_for_timestamp(
        &self,
        topic: &str,
        partition: i32,
        timestamp: i64,
        timeout: Duration,
    ) -> Result<i64, Error> {
        use kafka_protocol::messages::{ListOffsetsRequest, list_offsets_request};
        use kafka_protocol::protocol::StrBytes;

        let mut request = ListOffsetsRequest::default();
        request.replica_id = kafka_protocol::messages::BrokerId(-1);

        let mut topic_req = list_offsets_request::ListOffsetsTopic::default();
        topic_req.name = kafka_protocol::messages::TopicName::from(StrBytes::from_string(
            topic.to_owned(),
        ));

        let mut partition_req = list_offsets_request::ListOffsetsPartition::default();
        partition_req.partition_index = partition;
        partition_req.timestamp = timestamp;
        topic_req.partitions = vec![partition_req];

        request.topics = vec![topic_req];

        let conn = self.pool.get_any_connection().await?;
        let response = conn.send_request(request, timeout).await?;

        // Extract offset from response
        for topic_resp in response.topics {
            for partition_resp in topic_resp.partitions {
                if partition_resp.partition_index == partition {
                    if partition_resp.error_code != 0 {
                        return Err(Error::kafka(
                            partition_resp.error_code,
                            format!("Failed to list offsets for {}[{}]", topic, partition),
                        ));
                    }
                    return Ok(partition_resp.offset);
                }
            }
        }

        Err(Error::PartitionNotFound {
            topic: topic.to_owned(),
            partition,
        })
    }

    // === Offset operations ===

    /// Fetch committed offsets for this consumer's group.
    pub async fn committed_offsets(
        &self,
        topic_partitions: &TopicPartitionList,
        timeout: Duration,
    ) -> Result<TopicPartitionList, Error> {
        let mut result = self
            .committed_offsets_for_groups(&[&self.group_id], topic_partitions, timeout)
            .await?;

        result
            .remove(&self.group_id)
            .ok_or_else(|| Error::CoordinatorNotFound(self.group_id.clone()))
    }

    /// Fetch committed offsets for multiple groups at once.
    ///
    /// This is a key feature that allows efficient batch fetching of offsets
    /// for multiple consumer groups in a single operation.
    pub async fn committed_offsets_for_groups(
        &self,
        groups: &[&str],
        topic_partitions: &TopicPartitionList,
        timeout: Duration,
    ) -> Result<HashMap<String, TopicPartitionList>, Error> {
        use kafka_protocol::messages::{OffsetFetchRequest, offset_fetch_request};
        use kafka_protocol::protocol::StrBytes;

        // Build the request with multiple groups (Kafka 3.0+ batch API)
        let mut request = OffsetFetchRequest::default();

        // Build topics list using OffsetFetchRequestTopics (for batch API)
        let topics: Vec<offset_fetch_request::OffsetFetchRequestTopics> = {
            let mut topic_map: HashMap<&str, Vec<i32>> = HashMap::new();
            for elem in topic_partitions.elements() {
                topic_map
                    .entry(&elem.topic)
                    .or_default()
                    .push(elem.partition);
            }

            topic_map
                .into_iter()
                .map(|(topic, partitions)| {
                    let mut t = offset_fetch_request::OffsetFetchRequestTopics::default();
                    t.name = kafka_protocol::messages::TopicName::from(StrBytes::from_string(
                        topic.to_owned(),
                    ));
                    t.partition_indexes = partitions;
                    t
                })
                .collect()
        };

        // Use batch API (version 8+) for multiple groups
        request.groups = groups
            .iter()
            .map(|group_id| {
                let mut g = offset_fetch_request::OffsetFetchRequestGroup::default();
                g.group_id = kafka_protocol::messages::GroupId::from(StrBytes::from_string(
                    group_id.to_string(),
                ));
                g.topics = Some(topics.clone());
                g
            })
            .collect();

        let conn = self.pool.get_any_connection().await?;
        let response = conn.send_request(request, timeout).await?;

        // Parse response
        let mut result = HashMap::new();

        for group_resp in response.groups {
            let group_id = group_resp.group_id.to_string();
            let mut tpl = TopicPartitionList::new();

            for topic in group_resp.topics {
                let topic_name = topic.name.to_string();
                for partition in topic.partitions {
                    if partition.error_code == 0 {
                        let offset = if partition.committed_offset >= 0 {
                            crate::types::topic_partition::Offset::Offset(
                                partition.committed_offset,
                            )
                        } else {
                            crate::types::topic_partition::Offset::Invalid
                        };
                        tpl.add_partition_offset(&topic_name, partition.partition_index, offset)?;
                    }
                }
            }

            result.insert(group_id, tpl);
        }

        Ok(result)
    }

    /// Commit offsets for this consumer's group.
    pub async fn commit(
        &self,
        topic_partitions: &TopicPartitionList,
        _mode: CommitMode,
    ) -> Result<(), Error> {
        use kafka_protocol::messages::{OffsetCommitRequest, offset_commit_request};
        use kafka_protocol::protocol::StrBytes;

        let mut request = OffsetCommitRequest::default();
        request.group_id = kafka_protocol::messages::GroupId::from(StrBytes::from_string(
            self.group_id.clone(),
        ));

        // Build topics
        let mut topic_map: HashMap<&str, Vec<&crate::types::topic_partition::TopicPartitionElement>> =
            HashMap::new();
        for elem in topic_partitions.elements() {
            topic_map.entry(&elem.topic).or_default().push(elem);
        }

        request.topics = topic_map
            .into_iter()
            .map(|(topic, elems)| {
                let mut t = offset_commit_request::OffsetCommitRequestTopic::default();
                t.name = kafka_protocol::messages::TopicName::from(StrBytes::from_string(
                    topic.to_owned(),
                ));
                t.partitions = elems
                    .into_iter()
                    .map(|elem| {
                        let mut p = offset_commit_request::OffsetCommitRequestPartition::default();
                        p.partition_index = elem.partition;
                        p.committed_offset = elem.offset.to_raw();
                        p
                    })
                    .collect();
                t
            })
            .collect();

        // Get coordinator connection
        let conn = self
            .coordinator_manager
            .get_coordinator_connection(&self.group_id, Duration::from_secs(5))
            .await?;

        let response = conn
            .send_request(request, Duration::from_secs(5))
            .await?;

        // Check for errors
        for topic in response.topics {
            for partition in topic.partitions {
                if partition.error_code != 0 {
                    return Err(Error::kafka(
                        partition.error_code,
                        format!(
                            "Failed to commit offset for {:?}[{}]",
                            topic.name, partition.partition_index
                        ),
                    ));
                }
            }
        }

        Ok(())
    }

    // === Partition assignment & streaming ===

    /// Assign partitions to consume.
    pub fn assign(&self, partitions: &TopicPartitionList) -> Result<(), Error> {
        // For now, just store the assignments
        // Actual fetch will happen in stream()
        let assignments = self.assignments.clone();
        let partitions = partitions.clone();

        tokio::spawn(async move {
            let mut assignments = assignments.write().await;
            *assignments = partitions;
        });

        Ok(())
    }

    /// Unassign partitions.
    pub fn unassign(&self, partitions: &TopicPartitionList) -> Result<(), Error> {
        let assignments = self.assignments.clone();
        let to_remove: Vec<_> = partitions
            .elements()
            .iter()
            .map(|e| (e.topic.clone(), e.partition))
            .collect();

        tokio::spawn(async move {
            let mut assignments = assignments.write().await;
            let remaining: Vec<_> = assignments
                .elements()
                .iter()
                .filter(|e| !to_remove.contains(&(e.topic.clone(), e.partition)))
                .cloned()
                .collect();
            *assignments = remaining.into_iter().collect();
        });

        Ok(())
    }

    /// Seek to specific offsets.
    pub async fn seek(
        &self,
        partitions: TopicPartitionList,
        _timeout: Duration,
    ) -> Result<TopicPartitionList, Error> {
        // Update assignments with new offsets
        let mut assignments = self.assignments.write().await;

        for elem in partitions.elements() {
            if let Some(existing) = assignments.find_mut(&elem.topic, elem.partition) {
                existing.offset = elem.offset;
            } else {
                assignments.add_partition_offset(&elem.topic, elem.partition, elem.offset)?;
            }
        }

        Ok(partitions)
    }

    /// Get a message stream.
    ///
    /// Note: This is a simplified implementation. A full implementation would
    /// include proper fetch batching, offset management, and error handling.
    pub fn stream(&self) -> MessageStream {
        MessageStream::new(
            self.pool.clone(),
            self.assignments.clone(),
        )
    }
}

/// Message stream for consuming records.
pub struct MessageStream {
    pool: Arc<ConnectionPool>,
    assignments: Arc<RwLock<TopicPartitionList>>,
}

impl MessageStream {
    fn new(pool: Arc<ConnectionPool>, assignments: Arc<RwLock<TopicPartitionList>>) -> Self {
        Self { pool, assignments }
    }
}

// Note: Full Stream implementation would be more complex
// This is a placeholder for the streaming functionality
impl MessageStream {
    /// Poll for the next message (simplified).
    ///
    /// A full implementation would implement the futures::Stream trait properly.
    pub async fn next(&mut self) -> Option<Result<Message, Error>> {
        // Placeholder - actual implementation would fetch from Kafka
        None
    }
}
