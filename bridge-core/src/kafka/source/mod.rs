use crate::kafka::KafkaError;
use crate::kafka::properties::KafkaConsumerProperties;
use crate::kafka::source::consumer::PartitionConsumerEvent;
use crate::prelude::*;

use consumer::PartitionConsumer;

mod context;
use context::CustomConsumerContext;
pub use context::OutOfBandError;
pub use context::TopicOutOfBandErrorStream;

mod consumer;
pub use consumer::RecordStreamConsumerTask;

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use rdkafka::consumer::Consumer;
use rdkafka::util::Timeout;
use rdkafka::{Offset, TopicPartitionList};

type StreamConsumer = rdkafka::consumer::StreamConsumer<CustomConsumerContext>;

/// A Kafka Stream Consumer.
///
/// It is not possible to consume records directly from that object.
/// One should call [`RecordStreamConsumer::consumer_for_partition`] to create a stream consumer for a particular topic partition.
pub struct RecordStreamConsumer {
    /// The underlying rdkafka consumer
    consumer: Arc<StreamConsumer>,
    /// A channel to send topic subscriptions to the consumer loop.
    topic_subscriber: consumer::PartitionSubscriber,
}

impl RecordStreamConsumer {
    /// Creates a new consumer.
    /// Returns a future that runs the consumer loop along with the [`RecordStreamConsumer`] object.
    pub fn new(
        properties: KafkaConsumerProperties,
    ) -> Result<(Self, RecordStreamConsumerTask), KafkaError> {
        log::info!("Instantiating consumer - properties {:?}", properties);
        let context = CustomConsumerContext::default();
        let consumer = properties.into_stream_consumer_with_context(context)?;
        let consumer = Arc::new(consumer);

        let (task, topic_subscriber) =
            consumer::RecordStreamConsumerTask::new(Arc::clone(&consumer));

        let this = Self {
            consumer,
            topic_subscriber,
        };

        Ok((this, task))
    }

    /// Lists all topics on the cluster.
    ///
    /// Caution: this function may block
    pub fn list_topics(&self) -> Result<Vec<TopicName>, KafkaError> {
        let topics = self
            .consumer
            .fetch_metadata(None, Duration::from_secs(30))?
            .topics()
            .iter()
            .map(|m| m.name().to_owned())
            .collect();
        Ok(topics)
    }

    pub async fn fetch_watermarks(
        &self,
        topic: &TopicName,
        partition: PartitionNumber,
    ) -> Result<(i64, i64), KafkaError> {
        self.consumer
            .fetch_watermarks(topic, partition, Duration::from_secs(30))
            .map_err(|e| KafkaError::from(e))
    }

    /// Creates a consumer for a specific topic.
    /// No new assignment are done until the [`PartitionRecordStreamConsumer::create_stream`] is called.
    pub async fn consumer_for_partition(
        &self,
        topic: TopicName,
        partition: PartitionNumber,
    ) -> Result<PartitionRecordStreamConsumer, KafkaError> {
        // TODO: Avoid calling this for every partition
        let metadata = self
            .consumer
            .fetch_metadata(Some(&topic), Timeout::After(Duration::from_secs(30)))
            .map_err(|err| KafkaError::Generic(err.to_string()))?;

        let Some(metadata) = metadata.topics().first() else {
            log::error!("Configured topic `{topic}` can not be accessed on the remote server.");
            return Err(KafkaError::TopicNotFound(topic));
        };

        if metadata.partitions().is_empty() {
            log::error!("Configured topic `{topic}` can not be accessed on the remote server.");
            return Err(KafkaError::PartitionNotFound(topic, partition));
        }

        let mut assignment = TopicPartitionList::new();
        assignment.add_partition_offset(metadata.name(), partition, Offset::Beginning)?;

        let topic_consumer = PartitionRecordStreamConsumer {
            topic: metadata.name().to_owned(),
            partition,
            partition_subscriber: self.topic_subscriber.clone(),
        };

        Ok(topic_consumer)
    }

    /// Creates a channel for out-of-band errors caught by our custom Consumer Context for a specific topic.
    pub fn subscribe_to_out_of_band_errors(&self, topic: TopicName) -> TopicOutOfBandErrorStream {
        self.consumer
            .context()
            .subscribe_to_out_of_band_errors(topic)
    }
}

/// A stream client to Kafka that returns records of a specific partition.
pub struct PartitionRecordStreamConsumer {
    topic: TopicName,
    partition: PartitionNumber,
    partition_subscriber: consumer::PartitionSubscriber,
}

impl PartitionRecordStreamConsumer {
    pub fn topic(&self) -> &TopicName {
        &self.topic
    }

    pub fn partition(&self) -> PartitionNumber {
        self.partition
    }

    pub async fn seek_offset(&mut self, offset: i64) -> Result<TopicPartitionList, KafkaError> {
        tracing::trace!(
            %self.topic,
            %self.partition,
            %offset,
            "Seeking partition to offset"
        );

        self.partition_subscriber
            .seek_offset(self.topic.clone(), self.partition(), offset)
            .await
            .map_err(|_| KafkaError::Generic("Unreachable consumer task".into()))
    }

    pub async fn create_stream(&mut self) -> Result<PartitionRecordStream, KafkaError> {
        tracing::trace!(%self.topic, %self.partition, "Assigning partition");

        let msgstream = self
            .partition_subscriber
            .subscribe(self.topic().to_owned(), self.partition)
            .await?
            .ok_or_else(|| KafkaError::Generic("Unreachable consumer task".into()))?;

        Ok(PartitionRecordStream { msgstream })
    }
}

/// A stream of Kafka records for a specific partition.
pub struct PartitionRecordStream {
    msgstream: PartitionConsumer,
}

impl futures::Stream for PartitionRecordStream {
    type Item = Result<PartitionConsumerEvent, KafkaError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use futures::StreamExt;
        self.msgstream
            .poll_next_unpin(cx)
            .map_err(|err| KafkaError::from(err))
    }
}
