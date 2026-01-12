//! Kafka AdminClient for cluster operations.

use std::sync::Arc;
use std::time::Duration;

use crate::broker::coordinator::CoordinatorManager;
use crate::broker::metadata::{ClusterMetadata, MetadataManager};
use crate::config::CommonProperties;
use crate::connection::ConnectionPool;
use crate::consumer::GroupInfo;
use crate::error::Error;

/// Detailed information about a consumer group member.
#[derive(Debug, Clone)]
pub struct GroupMemberInfo {
    /// Member ID.
    pub member_id: String,
    /// Client ID.
    pub client_id: String,
    /// Client host.
    pub client_host: String,
    /// Assigned partitions (topic -> partitions).
    pub assignments: Vec<(String, Vec<i32>)>,
}

/// Detailed information about a consumer group.
#[derive(Debug, Clone)]
pub struct GroupDescription {
    /// Group ID.
    pub group_id: String,
    /// Group state (e.g., "Stable", "PreparingRebalance", "Empty").
    pub state: String,
    /// Protocol type (usually "consumer").
    pub protocol_type: String,
    /// Protocol (e.g., "range", "roundrobin").
    pub protocol: String,
    /// Group members.
    pub members: Vec<GroupMemberInfo>,
}

/// Admin client for cluster operations.
pub struct AdminClient {
    /// Connection pool.
    pool: Arc<ConnectionPool>,
    /// Metadata manager.
    metadata_manager: MetadataManager,
    /// Coordinator manager.
    coordinator_manager: CoordinatorManager,
}

impl AdminClient {
    /// Create a new admin client from common properties.
    pub(crate) fn new(props: CommonProperties) -> Result<Self, Error> {
        let pool = Arc::new(ConnectionPool::from_properties(&props)?);
        let metadata_manager = MetadataManager::new(pool.clone());
        let coordinator_manager = CoordinatorManager::new(pool.clone());

        Ok(Self {
            pool,
            metadata_manager,
            coordinator_manager,
        })
    }

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
    pub async fn list_groups(&self, timeout: Duration) -> Result<Vec<GroupInfo>, Error> {
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

    /// Describe consumer groups in detail.
    ///
    /// This fetches detailed information about the specified consumer groups,
    /// including their members and partition assignments.
    pub async fn describe_groups(
        &self,
        group_ids: &[&str],
        timeout: Duration,
    ) -> Result<Vec<GroupDescription>, Error> {
        use kafka_protocol::messages::DescribeGroupsRequest;
        use kafka_protocol::protocol::StrBytes;

        if group_ids.is_empty() {
            return Ok(vec![]);
        }

        // Build request
        let mut request = DescribeGroupsRequest::default();
        request.groups = group_ids
            .iter()
            .map(|id| kafka_protocol::messages::GroupId::from(StrBytes::from_string(id.to_string())))
            .collect();

        // For now, send to any broker - ideally we'd send to each group's coordinator
        // but that would require multiple requests. The broker will forward as needed.
        let conn = self.pool.get_any_connection().await?;
        let response = conn.send_request(request, timeout).await?;

        let mut descriptions = Vec::new();

        for group in response.groups {
            if group.error_code != 0 {
                // Skip groups with errors, or we could collect errors
                tracing::warn!(
                    group_id = group.group_id.to_string(),
                    error_code = group.error_code,
                    "Error describing group"
                );
                continue;
            }

            let members = group
                .members
                .iter()
                .map(|m| {
                    // Parse member assignment to extract topic-partition mappings
                    let assignments = parse_member_assignment(&m.member_assignment);

                    GroupMemberInfo {
                        member_id: m.member_id.to_string(),
                        client_id: m.client_id.to_string(),
                        client_host: m.client_host.to_string(),
                        assignments,
                    }
                })
                .collect();

            descriptions.push(GroupDescription {
                group_id: group.group_id.to_string(),
                state: group.group_state.to_string(),
                protocol_type: group.protocol_type.to_string(),
                protocol: group.protocol_data.to_string(),
                members,
            });
        }

        Ok(descriptions)
    }
}

/// Parse member assignment bytes into topic-partition mappings.
///
/// The assignment format is:
/// - version: i16
/// - topic count: i32
/// - for each topic:
///   - topic name: string (i16 length + bytes)
///   - partition count: i32
///   - partitions: i32[]
fn parse_member_assignment(data: &bytes::Bytes) -> Vec<(String, Vec<i32>)> {
    use bytes::Buf;

    if data.len() < 2 {
        return vec![];
    }

    let mut cursor = data.as_ref();

    // Read version
    if cursor.remaining() < 2 {
        return vec![];
    }
    let _version = cursor.get_i16();

    // Read topic count
    if cursor.remaining() < 4 {
        return vec![];
    }
    let topic_count = cursor.get_i32();

    let mut assignments = Vec::new();

    for _ in 0..topic_count {
        // Read topic name length
        if cursor.remaining() < 2 {
            break;
        }
        let name_len = cursor.get_i16() as usize;

        if cursor.remaining() < name_len {
            break;
        }
        let topic_name = String::from_utf8_lossy(&cursor[..name_len]).to_string();
        cursor.advance(name_len);

        // Read partition count
        if cursor.remaining() < 4 {
            break;
        }
        let partition_count = cursor.get_i32();

        let mut partitions = Vec::new();
        for _ in 0..partition_count {
            if cursor.remaining() < 4 {
                break;
            }
            partitions.push(cursor.get_i32());
        }

        assignments.push((topic_name, partitions));
    }

    assignments
}
