//! Connection management for Kafka brokers.

pub mod pool;
pub mod tcp;
pub mod tls;

pub use pool::ConnectionPool;
pub use tcp::BrokerConnection;
