//! Pure Rust Kafka client using rustls (no OpenSSL dependency).
//!
//! This crate provides a Kafka client with the following features:
//! - Consumer for metadata, offset operations, and message streaming
//! - Producer for sending messages
//! - AdminClient for cluster operations
//! - Batch OffsetFetch for multiple consumer groups
//!
//! # Example
//!
//! ```no_run
//! use kafka_native::{ConsumerProperties, Consumer};
//!
//! # async fn example() -> Result<(), kafka_native::Error> {
//! let mut props = ConsumerProperties::new();
//! props.set("bootstrap.servers", "localhost:9092");
//! props.set("group.id", "my-group");
//!
//! let consumer = props.into_consumer()?;
//! let metadata = consumer.fetch_metadata(std::time::Duration::from_secs(5)).await?;
//! # Ok(())
//! # }
//! ```

mod config;
mod connection;
mod error;

pub mod broker;
pub mod types;

mod admin;
mod consumer;
mod producer;

pub use config::{ConsumerProperties, ProducerProperties};
pub use error::Error;

pub use admin::{AdminClient, GroupDescription, GroupMemberInfo};
pub use consumer::{Consumer, GroupInfo, MessageStream};
pub use producer::Producer;

pub use types::message::{Headers, Message};
pub use types::topic_partition::{CommitMode, Offset, TopicPartitionElement, TopicPartitionList};
