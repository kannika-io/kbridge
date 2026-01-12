//! Cluster metadata caching.

use std::sync::Arc;
use std::time::Duration;

use kafka_protocol::messages::{MetadataRequest, MetadataResponse, TopicName};
use kafka_protocol::protocol::StrBytes;
use tokio::sync::RwLock;

use crate::connection::pool::{BrokerEndpoint, ConnectionPool};
use crate::error::Error;

/// Information about a broker.
#[derive(Debug, Clone)]
pub struct BrokerInfo {
    /// Broker node ID.
    pub node_id: i32,
    /// Host name.
    pub host: String,
    /// Port number.
    pub port: i32,
    /// Optional rack ID.
    pub rack: Option<String>,
}

/// Information about a partition.
#[derive(Debug, Clone)]
pub struct PartitionInfo {
    /// Partition number.
    pub partition: i32,
    /// Leader broker node ID.
    pub leader: i32,
    /// Replica broker node IDs.
    pub replicas: Vec<i32>,
    /// In-sync replica broker node IDs.
    pub isr: Vec<i32>,
}

/// Information about a topic.
#[derive(Debug, Clone)]
pub struct TopicInfo {
    /// Topic name.
    pub name: String,
    /// Partitions in the topic.
    pub partitions: Vec<PartitionInfo>,
    /// Whether the topic is internal.
    pub is_internal: bool,
}

/// Cluster metadata.
#[derive(Debug, Clone, Default)]
pub struct ClusterMetadata {
    /// Brokers in the cluster.
    pub brokers: Vec<BrokerInfo>,
    /// Topics in the cluster.
    pub topics: Vec<TopicInfo>,
    /// Controller broker node ID.
    pub controller_id: i32,
    /// Cluster ID.
    pub cluster_id: Option<String>,
}

impl ClusterMetadata {
    /// Get broker info by node ID.
    pub fn get_broker(&self, node_id: i32) -> Option<&BrokerInfo> {
        self.brokers.iter().find(|b| b.node_id == node_id)
    }

    /// Get topic info by name.
    pub fn get_topic(&self, name: &str) -> Option<&TopicInfo> {
        self.topics.iter().find(|t| t.name == name)
    }

    /// Get partition leader for a topic-partition.
    pub fn get_partition_leader(&self, topic: &str, partition: i32) -> Option<i32> {
        self.get_topic(topic)
            .and_then(|t| t.partitions.iter().find(|p| p.partition == partition))
            .map(|p| p.leader)
    }

    /// Get all topic names.
    pub fn topic_names(&self) -> Vec<&str> {
        self.topics.iter().map(|t| t.name.as_str()).collect()
    }

    /// Convert brokers to endpoints for connection pool.
    pub fn broker_endpoints(&self) -> Vec<BrokerEndpoint> {
        self.brokers
            .iter()
            .map(|b| BrokerEndpoint::new(b.node_id, &b.host, b.port as u16))
            .collect()
    }
}

impl From<MetadataResponse> for ClusterMetadata {
    fn from(response: MetadataResponse) -> Self {
        let brokers = response
            .brokers
            .iter()
            .map(|b| BrokerInfo {
                node_id: b.node_id.0,
                host: b.host.to_string(),
                port: b.port,
                rack: b.rack.as_ref().map(|s| s.to_string()),
            })
            .collect();

        let topics = response
            .topics
            .iter()
            .filter_map(|t| {
                // Skip topics with errors
                if t.error_code != 0 {
                    return None;
                }

                // Get topic name, skipping if None
                let topic_name = t.name.as_ref()?.to_string();

                let partitions = t
                    .partitions
                    .iter()
                    .filter_map(|p| {
                        // Skip partitions with errors
                        if p.error_code != 0 {
                            return None;
                        }

                        Some(PartitionInfo {
                            partition: p.partition_index,
                            leader: p.leader_id.0,
                            replicas: p.replica_nodes.iter().map(|n| n.0).collect(),
                            isr: p.isr_nodes.iter().map(|n| n.0).collect(),
                        })
                    })
                    .collect();

                Some(TopicInfo {
                    name: topic_name,
                    partitions,
                    is_internal: t.is_internal,
                })
            })
            .collect();

        Self {
            brokers,
            topics,
            controller_id: response.controller_id.0,
            cluster_id: response.cluster_id.as_ref().map(|s| s.to_string()),
        }
    }
}

/// Metadata manager that handles fetching and caching cluster metadata.
pub struct MetadataManager {
    /// Connection pool for fetching metadata.
    pool: Arc<ConnectionPool>,
    /// Cached metadata.
    metadata: RwLock<Option<ClusterMetadata>>,
}

impl MetadataManager {
    /// Create a new metadata manager.
    pub fn new(pool: Arc<ConnectionPool>) -> Self {
        Self {
            pool,
            metadata: RwLock::new(None),
        }
    }

    /// Fetch metadata for all topics.
    pub async fn fetch_metadata(&self, timeout: Duration) -> Result<ClusterMetadata, Error> {
        let conn = self.pool.get_any_connection().await?;

        let request = MetadataRequest::default();
        let response = conn.send_request(request, timeout).await?;

        let metadata = ClusterMetadata::from(response);

        // Update connection pool with new broker info
        self.pool.update_brokers(metadata.broker_endpoints()).await;

        // Cache the metadata
        {
            let mut cached = self.metadata.write().await;
            *cached = Some(metadata.clone());
        }

        Ok(metadata)
    }

    /// Fetch metadata for specific topics.
    pub async fn fetch_metadata_for_topics(
        &self,
        topics: &[&str],
        timeout: Duration,
    ) -> Result<ClusterMetadata, Error> {
        let conn = self.pool.get_any_connection().await?;

        let mut request = MetadataRequest::default();
        request.topics = Some(
            topics
                .iter()
                .map(|t| {
                    let mut topic = kafka_protocol::messages::metadata_request::MetadataRequestTopic::default();
                    topic.name = Some(TopicName::from(StrBytes::from_string(t.to_string())));
                    topic
                })
                .collect(),
        );

        let response = conn.send_request(request, timeout).await?;

        let metadata = ClusterMetadata::from(response);

        // Update connection pool with new broker info
        self.pool.update_brokers(metadata.broker_endpoints()).await;

        Ok(metadata)
    }

    /// Get cached metadata.
    pub async fn get_cached(&self) -> Option<ClusterMetadata> {
        self.metadata.read().await.clone()
    }
}
