//! Connection pool for managing connections to Kafka brokers.

use std::collections::HashMap;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::RwLock;

use super::tcp::BrokerConnection;
use super::tls::{connect_tls, TlsConfig};
use crate::config::CommonProperties;
use crate::error::{ConnectionError, Error};

/// A broker endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BrokerEndpoint {
    /// Broker node ID.
    pub node_id: i32,
    /// Host name.
    pub host: String,
    /// Port number.
    pub port: u16,
}

impl BrokerEndpoint {
    /// Create a new broker endpoint.
    pub fn new(node_id: i32, host: impl Into<String>, port: u16) -> Self {
        Self {
            node_id,
            host: host.into(),
            port,
        }
    }

    /// Resolve to socket addresses.
    pub fn to_socket_addrs(&self) -> Result<impl Iterator<Item = SocketAddr>, Error> {
        let addr_str = format!("{}:{}", self.host, self.port);
        addr_str.to_socket_addrs().map_err(|e| {
            ConnectionError::DnsResolution {
                host: self.host.clone(),
                message: e.to_string(),
            }
            .into()
        })
    }
}

/// Connection pool configuration.
#[derive(Debug, Clone)]
pub struct ConnectionPoolConfig {
    /// Client ID for connections.
    pub client_id: Option<String>,
    /// TLS configuration (None for plaintext).
    pub tls: Option<TlsConfig>,
    /// Connection timeout.
    pub connect_timeout: Duration,
}

impl Default for ConnectionPoolConfig {
    fn default() -> Self {
        Self {
            client_id: None,
            tls: None,
            connect_timeout: Duration::from_secs(10),
        }
    }
}

impl From<&CommonProperties> for ConnectionPoolConfig {
    fn from(props: &CommonProperties) -> Self {
        let tls = match props.inner.get("security.protocol").map(|s| s.as_str()) {
            Some("ssl") | Some("sasl_ssl") => {
                let mut config = TlsConfig::new();
                if let Some(ca_pem) = props.inner.get("ssl.ca.pem") {
                    config.ca_cert_pem = Some(ca_pem.as_bytes().to_vec());
                }
                if let (Some(cert_pem), Some(key_pem)) = (
                    props.inner.get("ssl.certificate.pem"),
                    props.inner.get("ssl.key.pem"),
                ) {
                    config.client_cert_pem = Some(cert_pem.as_bytes().to_vec());
                    config.client_key_pem = Some(key_pem.as_bytes().to_vec());
                }
                Some(config)
            }
            _ => None,
        };

        Self {
            client_id: props.client_id().map(|s| s.to_owned()),
            tls,
            connect_timeout: Duration::from_millis(props.request_timeout_ms()),
        }
    }
}

/// Connection pool for managing connections to Kafka brokers.
pub struct ConnectionPool {
    /// Configuration for creating new connections.
    config: ConnectionPoolConfig,
    /// Active connections by node ID.
    connections: RwLock<HashMap<i32, Arc<BrokerConnection>>>,
    /// Known brokers (node_id -> endpoint).
    brokers: RwLock<HashMap<i32, BrokerEndpoint>>,
    /// Bootstrap servers for initial connection.
    bootstrap_servers: Vec<String>,
}

impl ConnectionPool {
    /// Create a new connection pool.
    pub fn new(bootstrap_servers: Vec<String>, config: ConnectionPoolConfig) -> Self {
        Self {
            config,
            connections: RwLock::new(HashMap::new()),
            brokers: RwLock::new(HashMap::new()),
            bootstrap_servers,
        }
    }

    /// Create a connection pool from common properties.
    pub fn from_properties(props: &CommonProperties) -> Result<Self, Error> {
        let bootstrap_servers = props
            .bootstrap_servers()
            .ok_or_else(|| Error::MissingConfig("bootstrap.servers".to_owned()))?
            .split(',')
            .map(|s| s.trim().to_owned())
            .collect();

        let config = ConnectionPoolConfig::from(props);
        Ok(Self::new(bootstrap_servers, config))
    }

    /// Get or create a connection to a specific broker.
    pub async fn get_connection(&self, node_id: i32) -> Result<Arc<BrokerConnection>, Error> {
        // Check for existing connection
        {
            let connections = self.connections.read().await;
            if let Some(conn) = connections.get(&node_id) {
                if conn.is_alive() {
                    return Ok(conn.clone());
                }
            }
        }

        // Need to create a new connection
        let mut connections = self.connections.write().await;

        // Double-check after acquiring write lock
        if let Some(conn) = connections.get(&node_id) {
            if conn.is_alive() {
                return Ok(conn.clone());
            }
        }

        // Get broker endpoint
        let endpoint = {
            let brokers = self.brokers.read().await;
            brokers
                .get(&node_id)
                .cloned()
                .ok_or(Error::BrokerNotAvailable(node_id))?
        };

        // Create new connection
        let conn = self.connect_to_endpoint(&endpoint).await?;
        let conn = Arc::new(conn);
        connections.insert(node_id, conn.clone());

        Ok(conn)
    }

    /// Get a connection to any available broker (for bootstrap).
    pub async fn get_any_connection(&self) -> Result<Arc<BrokerConnection>, Error> {
        // Try existing connections first
        {
            let connections = self.connections.read().await;
            for (_, conn) in connections.iter() {
                if conn.is_alive() {
                    return Ok(conn.clone());
                }
            }
        }

        // Try to connect to a bootstrap server
        for server in &self.bootstrap_servers {
            match self.connect_to_bootstrap(server).await {
                Ok(conn) => {
                    let conn = Arc::new(conn);
                    // Store with a temporary node_id (-1)
                    let mut connections = self.connections.write().await;
                    connections.insert(-1, conn.clone());
                    return Ok(conn);
                }
                Err(e) => {
                    tracing::debug!(server, error = %e, "Failed to connect to bootstrap server");
                }
            }
        }

        Err(Error::Config(
            "Failed to connect to any bootstrap server".to_owned(),
        ))
    }

    /// Update known brokers from metadata.
    pub async fn update_brokers(&self, brokers: Vec<BrokerEndpoint>) {
        let mut broker_map = self.brokers.write().await;
        broker_map.clear();
        for broker in brokers {
            broker_map.insert(broker.node_id, broker);
        }
    }

    /// Add or update a single broker endpoint.
    ///
    /// This is useful when discovering coordinators that might not be
    /// in the initial metadata response.
    pub async fn add_broker(&self, broker: BrokerEndpoint) {
        let mut broker_map = self.brokers.write().await;
        broker_map.insert(broker.node_id, broker);
    }

    /// Connect to a bootstrap server.
    async fn connect_to_bootstrap(&self, server: &str) -> Result<BrokerConnection, Error> {
        let (host, port) = parse_host_port(server)?;
        let endpoint = BrokerEndpoint::new(-1, host.clone(), port);
        self.connect_to_endpoint(&endpoint).await
    }

    /// Connect to a broker endpoint.
    async fn connect_to_endpoint(&self, endpoint: &BrokerEndpoint) -> Result<BrokerConnection, Error> {
        let addrs: Vec<_> = endpoint.to_socket_addrs()?.collect();
        let addr = addrs
            .first()
            .ok_or_else(|| ConnectionError::DnsResolution {
                host: endpoint.host.clone(),
                message: "No addresses resolved".to_owned(),
            })?;

        if let Some(tls_config) = &self.config.tls {
            // TLS connection
            let tls_stream = connect_tls(*addr, &endpoint.host, tls_config).await?;
            BrokerConnection::from_stream(tls_stream, self.config.client_id.clone())
        } else {
            // Plain TCP connection
            BrokerConnection::connect(*addr, self.config.client_id.clone()).await
        }
    }
}

/// Parse host:port string.
fn parse_host_port(server: &str) -> Result<(String, u16), Error> {
    let parts: Vec<&str> = server.rsplitn(2, ':').collect();
    match parts.as_slice() {
        [port_str, host] => {
            let port = port_str
                .parse()
                .map_err(|_| ConnectionError::InvalidAddress(server.to_owned()))?;
            Ok((host.to_string(), port))
        }
        _ => Err(ConnectionError::InvalidAddress(server.to_owned()).into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_host_port() {
        let (host, port) = parse_host_port("localhost:9092").unwrap();
        assert_eq!(host, "localhost");
        assert_eq!(port, 9092);

        let (host, port) = parse_host_port("192.168.1.1:9093").unwrap();
        assert_eq!(host, "192.168.1.1");
        assert_eq!(port, 9093);

        let (host, port) = parse_host_port("[::1]:9092").unwrap();
        assert_eq!(host, "[::1]");
        assert_eq!(port, 9092);
    }

    #[test]
    fn test_broker_endpoint() {
        let endpoint = BrokerEndpoint::new(1, "localhost", 9092);
        assert_eq!(endpoint.node_id, 1);
        assert_eq!(endpoint.host, "localhost");
        assert_eq!(endpoint.port, 9092);
    }
}
