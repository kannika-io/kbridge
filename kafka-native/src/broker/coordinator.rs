//! Consumer group coordinator management.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use kafka_protocol::messages::{FindCoordinatorRequest, FindCoordinatorResponse};
use kafka_protocol::protocol::StrBytes;
use tokio::sync::RwLock;

use crate::connection::pool::{BrokerEndpoint, ConnectionPool};
use crate::error::Error;

/// Coordinator type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoordinatorType {
    /// Group coordinator (for consumer groups).
    Group = 0,
    /// Transaction coordinator.
    Transaction = 1,
}

/// Coordinator information.
#[derive(Debug, Clone)]
pub struct CoordinatorInfo {
    /// Coordinator broker node ID.
    pub node_id: i32,
    /// Host name.
    pub host: String,
    /// Port number.
    pub port: i32,
}

impl From<&FindCoordinatorResponse> for CoordinatorInfo {
    fn from(response: &FindCoordinatorResponse) -> Self {
        Self {
            node_id: response.node_id.0,
            host: response.host.to_string(),
            port: response.port,
        }
    }
}

/// Manages coordinator discovery and caching.
pub struct CoordinatorManager {
    /// Connection pool.
    pool: Arc<ConnectionPool>,
    /// Cached group coordinators.
    group_coordinators: RwLock<HashMap<String, CoordinatorInfo>>,
}

impl CoordinatorManager {
    /// Create a new coordinator manager.
    pub fn new(pool: Arc<ConnectionPool>) -> Self {
        Self {
            pool,
            group_coordinators: RwLock::new(HashMap::new()),
        }
    }

    /// Find the coordinator for a consumer group.
    pub async fn find_group_coordinator(
        &self,
        group_id: &str,
        timeout: Duration,
    ) -> Result<CoordinatorInfo, Error> {
        // Check cache first
        {
            let cache = self.group_coordinators.read().await;
            if let Some(coordinator) = cache.get(group_id) {
                return Ok(coordinator.clone());
            }
        }

        // Fetch from cluster
        let coordinator = self
            .fetch_coordinator(group_id, CoordinatorType::Group, timeout)
            .await?;

        // Cache the result
        {
            let mut cache = self.group_coordinators.write().await;
            cache.insert(group_id.to_owned(), coordinator.clone());
        }

        Ok(coordinator)
    }

    /// Fetch coordinator from the cluster.
    async fn fetch_coordinator(
        &self,
        key: &str,
        coordinator_type: CoordinatorType,
        timeout: Duration,
    ) -> Result<CoordinatorInfo, Error> {
        let conn = self.pool.get_any_connection().await?;

        let mut request = FindCoordinatorRequest::default();
        request.key = StrBytes::from_string(key.to_owned());
        request.key_type = coordinator_type as i8;

        let response = conn.send_request(request, timeout).await?;

        // Check for errors
        if response.error_code != 0 {
            return Err(Error::kafka(
                response.error_code,
                format!(
                    "Failed to find coordinator for {}: error_code={}",
                    key, response.error_code
                ),
            ));
        }

        Ok(CoordinatorInfo::from(&response))
    }

    /// Invalidate cached coordinator for a group.
    pub async fn invalidate_group_coordinator(&self, group_id: &str) {
        let mut cache = self.group_coordinators.write().await;
        cache.remove(group_id);
    }

    /// Get coordinator connection for a consumer group.
    ///
    /// This will find the coordinator and ensure its broker endpoint is
    /// registered in the connection pool before getting a connection.
    pub async fn get_coordinator_connection(
        &self,
        group_id: &str,
        timeout: Duration,
    ) -> Result<Arc<crate::connection::BrokerConnection>, Error> {
        let coordinator = self.find_group_coordinator(group_id, timeout).await?;

        // Ensure the coordinator broker is in the pool
        let endpoint = BrokerEndpoint::new(
            coordinator.node_id,
            &coordinator.host,
            coordinator.port as u16,
        );
        self.pool.add_broker(endpoint).await;

        self.pool.get_connection(coordinator.node_id).await
    }
}
