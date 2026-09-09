use async_trait::async_trait;

use crate::prelude::*;

use std::time::Duration;

pub mod kafka;

/// The source to read consumer group offsets from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OffsetSource {
    /// Query the committed consumer group offsets from the cluster itself.
    GroupCoordinator,
    /// Read offsets from a restored copy of the internal `__consumer_offsets`
    /// topic with the given name.
    Topic(String),
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
/// # use bridge_core::{BridgeClient, KafkaBridgeClient, KafkaBridgeConfig, OffsetSource};
/// # use std::time::Duration;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let config = KafkaBridgeConfig::new("localhost:9092".to_string());
/// let client: KafkaBridgeClient = config.into();
///
/// // Fetch current offsets from source cluster
/// let topics = vec!["orders".to_string(), "payments".to_string()];
/// let source_offsets = client.fetch_offsets(topics.clone(), Duration::from_secs(5), OffsetSource::GroupCoordinator).await?;
///
/// // Calculate target offsets based on message headers
/// let target_offsets = client.calculate_target_offsets("source-offset", topics.clone(), source_offsets).await?;
///
/// // Apply offsets to target cluster (dry_run = false to actually apply)
/// client.apply_target_offsets(topics, target_offsets, false).await?;
/// # Ok(())
/// # }
/// ```
#[async_trait]
pub trait BridgeClient {
    /// The error type returned by operations on this client.
    type Error;

    /// Fetches the current consumer group offsets from the given source.
    ///
    /// With [`OffsetSource::GroupCoordinator`], this retrieves the committed offsets
    /// for all consumer groups and their associated topic-partition combinations
    /// from the source Kafka cluster.
    ///
    /// With [`OffsetSource::Topic`], this instead reads every record of the given
    /// topic up to the log-end offsets captured at start, decodes the
    /// `OffsetCommitKey`/`OffsetCommitValue` wire format, and keeps the last
    /// commit per [group, topic, partition]. Use this when the internal
    /// `__consumer_offsets` topic of the source cluster has been backed up and
    /// restored to a regular topic.
    ///
    /// # Parameters
    ///
    /// * `topics` - If non-empty, only offsets for these topics are returned
    /// * `source` - Where to read the offsets from
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
    /// - The offsets topic does not exist, or reading it times out
    async fn fetch_offsets(
        &self,
        topics: impl IntoIterator<Item = String> + Send,
        client_timeout: Duration,
        source: OffsetSource,
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
    async fn calculate_target_offsets(
        &self,
        legacy_offset_header: impl Into<String> + Send,
        topics: impl IntoIterator<Item = String> + Send,
        offset_snapshot: OffsetSnapshot,
    ) -> Result<OffsetSnapshot, Self::Error>;

    /// Applies the calculated target offsets to consumer groups on the target cluster.
    ///
    /// This method commits the provided offsets to the appropriate consumer groups,
    /// effectively setting their position in the target cluster topics.
    ///
    /// # Parameters
    ///
    /// * `offset_snapshot` - The target offsets to apply to consumer groups
    /// * `dry_run` - If true, validates the operation without actually applying offsets
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if all offsets were successfully applied (or validated in dry-run mode),
    /// or an error if the operation fails.
    ///
    /// # Errors
    ///
    /// This method will return an error if:
    /// - Consumer groups cannot be created or accessed
    /// - Offset commits fail due to network or authorization issues
    async fn apply_target_offsets(
        &self,
        topics: impl IntoIterator<Item = String> + Send,
        offset_snapshot: OffsetSnapshot,
        dry_run: bool,
    ) -> Result<(), Self::Error>;
}
