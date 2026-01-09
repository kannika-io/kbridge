#![allow(dead_code)]
use std::collections::HashMap;
use std::future::Future;
use std::time::Duration;

pub mod client;
mod commands;
pub mod errors;
pub mod kafka;
pub mod partition;
pub mod prelude;
pub mod snapshot;
pub mod transform;
pub use prelude::*;

#[cfg(test)]
pub mod test {
    pub mod table;
    pub mod kafka {
        pub mod cluster;
    }
}

/// A trait for bridging Kafka consumer group offsets between different clusters or topics.
///
/// This trait provides the core functionality for migrating consumer group offsets from a source
/// Kafka cluster to a target cluster, handling the transformation of offsets based on message
/// headers that contain the original source offsets.
///
/// # Workflow
///
/// The typical workflow for using a `BridgeClient` is:
/// 1. Fetch current consumer group offsets from the source cluster
/// 2. Calculate corresponding target offsets by reading messages and their headers
/// 3. Apply the calculated offsets to consumer groups on the target cluster
///
/// # Example
///
/// ```rust,no_run
/// # use bridge_core::{BridgeClient, KafkaBridgeClient, BridgeConfig};
/// # use std::time::Duration;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let config = BridgeConfig::new("localhost:9092".to_string());
/// let client: KafkaBridgeClient = config.into();
///
/// // Fetch current offsets from source cluster
/// let source_offsets = client.fetch_source_offsets_from_cluster(&None, Duration::from_secs(5))?;
///
/// // Calculate target offsets based on message headers
/// let target_offsets = client.calculate_target_offsets("source-offset", &None, source_offsets).await?;
///
/// // Apply offsets to target cluster (with confirmation)
/// client.apply_target_offsets(&None, target_offsets, &|offsets| {
///     println!("About to apply {} offset records. Continue? (y/n)", offsets.size());
///     // In real code, read user input here
///     true
/// }).await?;
/// # Ok(())
/// # }
/// ```
pub trait BridgeClient {
    /// The error type returned by operations on this client.
    type Error;

    /// Fetches the current consumer group offsets from the Kafka cluster.
    ///
    /// This method retrieves the committed offsets for all consumer groups and their
    /// associated topic-partition combinations from the source Kafka cluster.
    ///
    /// # Returns
    ///
    /// Returns an `OffsetSnapshot` containing all the current consumer group offset records,
    /// or an error if the operation fails (e.g., due to network issues or authentication problems).
    ///
    /// # Errors
    ///
    /// This method will return an error if:
    /// - The Kafka cluster is unreachable
    /// - Authentication or authorization fails
    /// - There are issues reading consumer group metadata
    fn fetch_source_offsets_from_cluster(
        &self,
        topics: &Option<Vec<String>>,
        client_timeout: Duration,
    ) -> Result<OffsetSnapshot, Self::Error>;

    /// Calculates target offsets by reading messages and extracting source offsets from headers.
    ///
    /// This method consumes messages from the target Kafka topics and looks for a specific
    /// header containing the original source offset. It then builds a mapping from source
    /// offsets to target offsets, allowing consumer groups to resume from the correct
    /// position in the target cluster.
    ///
    /// # Parameters
    ///
    /// * `legacy_offset_header` - The name of the message header that contains the source offset
    /// * `offset_snapshot` - The current consumer group offsets from the source cluster
    ///
    /// # Returns
    ///
    /// Returns a new `OffsetSnapshot` with the calculated target offsets that correspond
    /// to the source offsets, or an error if the calculation fails.
    ///
    /// # Errors
    ///
    /// This method will return an error if:
    /// - Messages cannot be consumed from the target topics
    /// - The specified header is missing from messages
    /// - Header values cannot be parsed as valid offsets
    /// - Consumer initialization or subscription fails
    fn calculate_target_offsets(
        &self,
        legacy_offset_header: &str,
        topics: &Option<Vec<String>>,
        offset_snapshot: OffsetSnapshot,
    ) -> impl Future<Output = Result<OffsetSnapshot, Self::Error>>;

    /// Applies the calculated target offsets to consumer groups on the target cluster.
    ///
    /// This method commits the provided offsets to the appropriate consumer groups,
    /// effectively setting their position in the target cluster topics. Before applying
    /// the offsets, it calls the provided confirmation function to allow for user approval.
    ///
    /// # Parameters
    ///
    /// * `offset_snapshot` - The target offsets to apply to consumer groups
    /// * `confirmation_request` - A function that receives the offsets and returns whether to proceed
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if all offsets were successfully applied, or an error if the operation fails.
    ///
    /// # Errors
    ///
    /// This method will return an error if:
    /// - The confirmation function returns `false`
    /// - Consumer groups cannot be created or accessed
    /// - Offset commits fail due to network or authorization issues
    /// - The operation is cancelled
    fn apply_target_offsets(
        &self,
        topics: &Option<Vec<String>>,
        offset_snapshot: OffsetSnapshot,
        confirmation_request: &dyn Fn(&OffsetSnapshot) -> bool,
    ) -> impl Future<Output = Result<(), Self::Error>>;
}

impl From<BridgeConfig> for KafkaBridgeClient {
    fn from(config: BridgeConfig) -> Self {
        KafkaBridgeClient { config }
    }
}

pub struct KafkaBridgeClient {
    config: BridgeConfig,
}

pub struct BridgeConfig {
    bootstrap_server: String,
    optional_client_properties: Option<Properties>,
}

impl BridgeConfig {
    pub fn new(bootstrap_server: String) -> Self {
        BridgeConfig {
            bootstrap_server,
            optional_client_properties: None,
        }
    }

    pub fn set_optional_client_properties(
        mut self,
        optional_client_properties: Option<HashMap<String, String>>,
    ) -> Self {
        self.optional_client_properties = optional_client_properties;
        self
    }

    pub fn bootstrap_server(&self) -> &str {
        self.bootstrap_server.as_str()
    }

    pub fn optional_client_properties(&self) -> &Option<Properties> {
        &self.optional_client_properties
    }
}
