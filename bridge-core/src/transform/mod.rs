mod batch;
mod search;

use std::{cmp::Ordering, num::NonZeroUsize, sync::Arc};

use batch::*;
use search::*;

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

#[derive(Debug, thiserror::Error)]
pub enum MigrationError {
    #[error("Failed to read partition")]
    PartitionReadError,

    #[error("Search error")]
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

    pub async fn migrate(
        &self,
        snapshot: &OffsetSnapshot,
    ) -> Result<OffsetSnapshot, MigrationError> {
        let batches: Vec<PartitionBatch> = snapshot.into();

        let mut transformed = OffsetSnapshot::with_capacity(snapshot.len());

        for batch in batches {
            tracing::debug!("Processing batch: {:?}", batch);
            let mut partition =
                KafkaPartition::open(self.consumer.clone(), &batch.topic, batch.partition)
                    .await
                    .map_err(|e| {
                        eprintln!("Error obtaining consumer for partition: {}", e);
                        MigrationError::PartitionReadError
                    })?;

            let scan = {
                let watermarks = self
                    .consumer
                    .fetch_watermarks(&batch.topic, batch.partition)
                    .await
                    .map_err(MigrationError::WatermarkFetchError)?;

                let window = Window::from(watermarks);

                let opts = BinarySearchOpts::default()
                    .with_search_window(window)
                    .with_seq_scan_size(NonZeroUsize::new(1).unwrap());

                partition
                    .binary_search(batch.offsets(), self.offset_source.clone(), opts)
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
    use std::time::Instant;

    use crate::{
        prelude::*,
        test::kafka::cluster::ContainerizedCluster,
        transform::{ConsumerGroupMigrator, search::OffsetSource},
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

        let transformed = migrator.migrate(&snapshot).await?;

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

        crate::test::snapshot::assertions::assert_eq(transformed, expected);

        Ok(())
    }
}
