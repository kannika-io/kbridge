//! Error types for the Kafka client.

use std::io;

/// Main error type for kafka-native operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Connection-related errors
    #[error("Connection error: {0}")]
    Connection(#[from] ConnectionError),

    /// Protocol encoding/decoding errors
    #[error("Protocol error: {0}")]
    Protocol(#[from] ProtocolError),

    /// Kafka protocol error response
    #[error("Kafka error: {message}")]
    Kafka {
        /// Kafka error code
        code: i16,
        /// Human-readable error message
        message: String,
    },

    /// Request timed out
    #[error("Request timed out")]
    Timeout,

    /// Broker not available
    #[error("Broker not available: node_id={0}")]
    BrokerNotAvailable(i32),

    /// Topic not found
    #[error("Topic not found: {0}")]
    TopicNotFound(String),

    /// Partition not found
    #[error("Partition not found: {topic}[{partition}]")]
    PartitionNotFound {
        /// Topic name
        topic: String,
        /// Partition number
        partition: i32,
    },

    /// Invalid configuration
    #[error("Invalid configuration: {0}")]
    Config(String),

    /// Missing required configuration
    #[error("Missing required configuration: {0}")]
    MissingConfig(String),

    /// Consumer group coordinator not found
    #[error("Coordinator not found for group: {0}")]
    CoordinatorNotFound(String),
}

/// Connection-related errors.
#[derive(Debug, thiserror::Error)]
pub enum ConnectionError {
    /// IO error during connection
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    /// TLS handshake or configuration error
    #[error("TLS error: {0}")]
    Tls(String),

    /// Connection was closed unexpectedly
    #[error("Connection closed")]
    Closed,

    /// DNS resolution failed
    #[error("DNS resolution failed for {host}: {message}")]
    DnsResolution {
        /// Host that failed to resolve
        host: String,
        /// Error message
        message: String,
    },

    /// Failed to parse broker address
    #[error("Invalid broker address: {0}")]
    InvalidAddress(String),
}

/// Protocol encoding/decoding errors.
#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    /// Failed to encode request
    #[error("Failed to encode request: {0}")]
    Encode(String),

    /// Failed to decode response
    #[error("Failed to decode response: {0}")]
    Decode(String),

    /// Unsupported API version
    #[error("Unsupported API version: api_key={api_key}, version={version}")]
    UnsupportedVersion {
        /// API key
        api_key: i16,
        /// Requested version
        version: i16,
    },

    /// Correlation ID mismatch
    #[error("Correlation ID mismatch: expected {expected}, got {actual}")]
    CorrelationMismatch {
        /// Expected correlation ID
        expected: i32,
        /// Actual correlation ID
        actual: i32,
    },
}

impl Error {
    /// Create a Kafka protocol error from error code.
    pub fn kafka(code: i16, message: impl Into<String>) -> Self {
        Self::Kafka {
            code,
            message: message.into(),
        }
    }
}
