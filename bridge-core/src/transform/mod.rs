mod batch;
mod search;

use std::{cmp::Ordering, num::NonZeroUsize, sync::Arc};

use batch::*;
use search::*;

pub use search::{SearchProgress, Window};

use crate::{
    kafka::{partition::KafkaPartition, source::RecordStreamConsumer},
    prelude::*,
    transform::search::binary::{BinarySearch, BinarySearchOpts},
};

pub struct ConsumerGroupMigrator {
    // TODO make this a partition provider to decouple from Kafka
    consumer: Arc<RecordStreamConsumer>,

    // Source of offsets to search for
    offset_source: OffsetSource,
}

/// Progress of [`ConsumerGroupMigrator::migrate`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MigrationProgress<'a> {
    /// `done` of `total` partitions are mapped. Reported once with `done = 0`
    /// before the first partition, then after each partition.
    Mapped { done: u64, total: u64 },
    /// Mapping of `partition` of `topic` starts.
    Partition {
        topic: &'a str,
        partition: PartitionNumber,
    },
    /// The binary search through the current partition advanced.
    Search(SearchProgress),
}

#[derive(Debug, thiserror::Error)]
pub enum MigrationError {
    #[error("Failed to read partition")]
    PartitionReadError,

    #[error("Search error: {0}")]
    SearchError(SearchError<KafkaError>),

    #[error(
        "Consumer group '{consumer_group}' for topic '{topic}' partition {partition} was not mapped during migration"
    )]
    ConsumerNotMapped {
        consumer_group: String,
        topic: String,
        partition: i32,
    },

    #[error("Failed to fetch watermarks: {0}")]
    WatermarkFetchError(crate::kafka::KafkaError),
}

impl ConsumerGroupMigrator {
    pub fn new<T>(consumer: T) -> Self
    where
        T: Into<Arc<RecordStreamConsumer>>,
    {
        ConsumerGroupMigrator {
            consumer: consumer.into(),
            offset_source: OffsetSource::default(),
        }
    }

    pub fn search_offset_in_header(mut self, key: impl Into<String>) -> Self {
        self.offset_source = OffsetSource::from_header(key);
        self
    }

    /// Maps every offset in `snapshot` to its position in the target partitions.
    ///
    /// `progress` reports each partition as it is mapped, and every seek of
    /// the binary search within it; see [`MigrationProgress`].
    pub async fn migrate(
        &self,
        snapshot: &OffsetSnapshot,
        mut progress: impl FnMut(MigrationProgress<'_>),
    ) -> Result<OffsetSnapshot, MigrationError> {
        let batches: Vec<PartitionBatch> = snapshot.into();
        let total = batches.len() as u64;
        progress(MigrationProgress::Mapped { done: 0, total });

        let mut transformed = OffsetSnapshot::with_capacity(snapshot.len());

        for (done, batch) in (1..).zip(batches) {
            tracing::debug!("Processing batch: {:?}", batch);
            progress(MigrationProgress::Partition {
                topic: &batch.topic,
                partition: batch.partition,
            });

            let watermarks = self
                .consumer
                .fetch_watermarks(&batch.topic, batch.partition)
                .await
                .map_err(MigrationError::WatermarkFetchError)?;
            let window = Window::from(watermarks);

            // An empty target partition has nothing to search. Position every
            // consumer group at the high watermark so it only consumes records
            // produced after the migration.
            if window.is_empty() {
                tracing::warn!(
                    topic = %batch.topic,
                    partition = batch.partition,
                    offset = window.high,
                    consumer_groups = batch.consumers.len(),
                    "Target partition is empty; mapping consumer groups to the high watermark"
                );
                for (consumer_group, _) in batch.consumers {
                    transformed.push(OffsetRecord {
                        consumer_group,
                        topic: batch.topic.clone(),
                        partition: batch.partition,
                        offset: window.high,
                    });
                }
                progress(MigrationProgress::Mapped { done, total });
                continue;
            }

            let mut partition =
                KafkaPartition::open(self.consumer.clone(), &batch.topic, batch.partition)
                    .await
                    .map_err(|e| {
                        tracing::error!("Error obtaining consumer for partition: {}", e);
                        MigrationError::PartitionReadError
                    })?;

            let scan = {
                let opts = BinarySearchOpts::new()
                    .with_search_window(window)
                    .with_seq_scan_size(NonZeroUsize::new(1).unwrap());

                partition
                    .binary_search(batch.offsets(), self.offset_source.clone(), opts, |p| {
                        progress(MigrationProgress::Search(p))
                    })
                    .await
                    .map_err(MigrationError::SearchError)?
            };

            for consumer in batch.consumers {
                let Some(SearchResult::Found(mapping)) = scan.get_result(consumer.1) else {
                    return Err(MigrationError::ConsumerNotMapped {
                        consumer_group: consumer.0.clone(),
                        topic: batch.topic.clone(),
                        partition: batch.partition,
                    });
                };
                let record = OffsetRecord {
                    consumer_group: consumer.0.clone(),
                    topic: batch.topic.clone(),
                    partition: batch.partition,
                    offset: mapping.new_offset,
                };

                transformed.push(record);
            }
            progress(MigrationProgress::Mapped { done, total });
        }

        Ok(transformed)
    }
}

/// Mapping from an old offset to a new offset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OffsetMapping {
    /// The old offset being searched for
    pub old_offset: i64,
    /// The new offset in the partition where this old offset maps to
    pub new_offset: i64,
}

impl PartialOrd for OffsetMapping {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OffsetMapping {
    fn cmp(&self, other: &Self) -> Ordering {
        self.old_offset.cmp(&other.old_offset)
    }
}

impl OffsetMapping {
    /// Creates a new OffsetMapping
    pub fn new(old_offset: i64, new_offset: i64) -> Self {
        OffsetMapping {
            old_offset,
            new_offset,
        }
    }
}

impl From<(Offset, Offset)> for OffsetMapping {
    fn from((old_offset, new_offset): (i64, i64)) -> Self {
        OffsetMapping {
            old_offset,
            new_offset,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        prelude::*,
        test::kafka::cluster::ContainerizedCluster,
        transform::{ConsumerGroupMigrator, MigrationProgress},
    };

    #[tokio::test]
    async fn test_migrate_kafka_partitions() -> Result<(), anyhow::Error> {
        use crate::test::kafka::cluster::MockClusterExt;
        let broker = ContainerizedCluster::start().await?;
        broker.create_topic("migration.events", 3).await?;
        broker.create_topic("help.events", 1).await?;

        let header_fn = |i| {
            let new_offset: i64 = i * 2;
            maplit::hashmap! {
                "offset".to_string() => new_offset.to_string(),
            }
        };

        let _ = futures::future::join_all(vec![
            broker.produce_with_headers("migration.events", 0, 400, header_fn),
            broker.produce_with_headers("migration.events", 1, 500, header_fn),
            broker.produce_with_headers("migration.events", 2, 600, header_fn),
            broker.produce_with_headers("help.events", 0, 2, header_fn),
        ])
        .await;

        let (consumer, consumer_task) = broker.record_consumer("migrate-partitions").await?;
        tokio::spawn(consumer_task);

        let snapshot: OffsetSnapshot = indoc::indoc! {
            r#"
            claude,migration.events,0,333
            claude,migration.events,1,444
            claude,migration.events,2,555
            tom,migration.events,0,123
            tom,migration.events,1,234
            tom,migration.events,2,456
            willem,help.events,0,1
            "#
        }
        .parse()
        .expect("failed to parse snapshot");

        let migrator = ConsumerGroupMigrator::new(consumer).search_offset_in_header("offset");

        let mut progress = Vec::new();
        let transformed = migrator
            .migrate(&snapshot, |p| {
                if let MigrationProgress::Mapped { done, total } = p {
                    progress.push((done, total));
                }
            })
            .await?;

        // One step per partition: three of migration.events, one of help.events
        assert_eq!(progress, vec![(0, 4), (1, 4), (2, 4), (3, 4), (4, 4)]);

        let expected: OffsetSnapshot = indoc::indoc! {
            r#"
            claude,migration.events,0,166
            claude,migration.events,1,222
            claude,migration.events,2,277
            tom,migration.events,0,61
            tom,migration.events,1,117
            tom,migration.events,2,228
            willem,help.events,0,0
            "#
        }
        .parse()
        .expect("failed to parse snapshot");

        crate::test::snapshot::assert_eq(transformed, expected);

        Ok(())
    }

    /// A consumer group committed on a partition that is empty in the target
    /// cluster (no records, or all records removed by retention) should be
    /// mapped to the partition's high watermark instead of failing.
    #[tokio::test]
    async fn test_migrate_empty_target_partition() -> Result<(), anyhow::Error> {
        use crate::test::kafka::cluster::MockClusterExt;
        let broker = ContainerizedCluster::start().await?;
        broker.create_topic("empty.partition", 2).await?;

        // Only partition 0 receives records; partition 1 stays empty.
        broker
            .produce_with_headers("empty.partition", 0, 100, |i| {
                maplit::hashmap! {
                    "offset".to_string() => (i * 2).to_string(),
                }
            })
            .await?;

        let (consumer, consumer_task) = broker.record_consumer("migrate-empty").await?;
        tokio::spawn(consumer_task);

        let snapshot: OffsetSnapshot = indoc::indoc! {
            r#"
            orders-app,empty.partition,0,50
            billing-app,empty.partition,1,42
            audit-app,empty.partition,1,0
            "#
        }
        .parse()
        .expect("failed to parse snapshot");

        let migrator = ConsumerGroupMigrator::new(consumer).search_offset_in_header("offset");

        let mut progress = Vec::new();
        let transformed = migrator
            .migrate(&snapshot, |p| {
                if let MigrationProgress::Mapped { done, total } = p {
                    progress.push((done, total));
                }
            })
            .await?;

        // Empty partitions are skipped without a search, but still count as mapped
        assert_eq!(progress, vec![(0, 2), (1, 2), (2, 2)]);

        let expected: OffsetSnapshot = indoc::indoc! {
            r#"
            orders-app,empty.partition,0,25
            billing-app,empty.partition,1,0
            audit-app,empty.partition,1,0
            "#
        }
        .parse()
        .expect("failed to parse snapshot");

        crate::test::snapshot::assert_eq(transformed, expected);

        Ok(())
    }
}
