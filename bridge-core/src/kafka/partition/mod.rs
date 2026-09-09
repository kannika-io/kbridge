use crate::kafka::source::PartitionRecordStream;
use crate::kafka::source::PartitionRecordStreamConsumer;
use crate::kafka::source::RecordStreamConsumer;
use crate::partition::*;
use crate::prelude::*;

use std::sync::Arc;

/// A Kafka partition that supports seeking by offset and timestamp.
/// When dropped, it will unassign itself from the partition if it was assigned.
pub struct KafkaPartition {
    // consumer: Arc<RecordStreamConsumer>,
    partition: PartitionRecordStreamConsumer,
}

impl KafkaPartition {
    /// Opens a seekable partition for the given topic and partition ID.
    /// Assignment is not done until a stream is created using `stream()`.
    pub async fn open(
        consumer: Arc<RecordStreamConsumer>,
        topic: impl Into<String>,
        partition: i32,
    ) -> Result<Self, KafkaError> {
        let partition = consumer
            .consumer_for_partition(topic.into(), partition)
            .await?;
        Ok(Self {
            // consumer: consumer.clone(),
            partition,
        })
    }
}

impl Partition for KafkaPartition {
    type Error = KafkaError;
    type Message = PartitionRecord;
    type Stream = PartitionRecordStream;

    /// Creates a stream for consuming messages from the partition.
    /// This will assign the partition to the consumer.
    /// If the partition is already assigned, this will return an error.
    async fn stream(&mut self) -> Result<Self::Stream, Self::Error> {
        self.partition.create_stream().await
    }
}

impl SeekablePartition for KafkaPartition {
    // Seek to the specified offset.
    async fn seek_from_offset(&mut self, offset: i64) -> Result<(), Self::Error> {
        let Ok(_tpl) = self.partition.seek_offset(offset).await else {
            return Err(KafkaError::InvalidSeek);
        };
        Ok(())
    }
}
