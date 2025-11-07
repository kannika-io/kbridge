use crate::kafka::KafkaError;
use crate::kafka::properties::KafkaConsumerProperties;
use crate::partition::PartitionRecord;
use crate::prelude::*;

use consumer::PartitionConsumer;

mod context;
use context::CustomConsumerContext;
pub use context::OutOfBandError;
pub use context::TopicOutOfBandErrorStream;

mod consumer;
pub use consumer::RecordStreamConsumerTask;

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use rdkafka::consumer::Consumer;
use rdkafka::util::Timeout;
use rdkafka::{Offset, TopicPartitionList};

/// Maps a (TopicName, PartitionID) to a (current offset, max_offset) pair.
pub type StreamProgress = HashMap<(TopicName, i32), (i64, i64)>;

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

    /// Creates a consumer for a specific topic.
    /// No new assignment are done until the [`PartitionRecordStreamConsumer::create_stream`] is called.
    pub async fn consumer_for_partition(
        &self,
        topic: TopicName,
        partition: PartitionNumber,
    ) -> Result<PartitionRecordStreamConsumer, KafkaError> {
        let metadata = self
            .consumer
            .fetch_metadata(Some(&topic), Timeout::After(Duration::from_secs(30)))
            .map_err(|err| KafkaError::Generic(err.to_string()))?;

        let Some(metadata) = metadata.topics().first() else {
            log::error!("Configured topic `{topic}` can not be accessed on the remote server.");
            return Err(KafkaError::NotFound(topic));
        };

        if metadata.partitions().is_empty() {
            log::error!("Configured topic `{topic}` can not be accessed on the remote server.");
            return Err(KafkaError::NotFound(topic));
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

    /// Compute the overall progress of the consumer for all active assignments
    pub fn progress(&self) -> Result<StreamProgress, KafkaError> {
        // Current position
        let position = self.consumer.position()?;

        if position.count() == 0 {
            return Ok(StreamProgress::default());
        }

        // Create a partition list to fetch end offsets of assigned topics and partitions.
        let end_offsets = {
            let mut end_offsets = TopicPartitionList::new();
            for pos in position.elements() {
                end_offsets.add_partition_offset(pos.topic(), pos.partition(), Offset::End)?;
            }
            self.consumer
                .offsets_for_times(end_offsets, Timeout::After(Duration::from_secs(5)))?
                .to_topic_map()
        };

        let mut progress = StreamProgress::new();
        for pos in position.elements() {
            let entry = (pos.topic().to_owned(), pos.partition());
            let max_offset = end_offsets
                .get(&entry)
                .copied()
                .and_then(Offset::to_raw)
                .unwrap_or(-1);
            if max_offset < 0 {
                // Topic gone
                continue;
            }
            let cur_offset = match pos.offset() {
                Offset::Beginning => 0,
                Offset::End => max_offset,
                Offset::Stored => max_offset,
                Offset::Invalid => max_offset,
                Offset::Offset(n) => n.max(0),
                Offset::OffsetTail(n) => (max_offset - n).max(0),
            };
            let cur_offset = cur_offset.min(max_offset);
            progress.insert(entry, (cur_offset, max_offset));
        }

        Ok(progress)
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

        // let start_offsets = self
        //     .assignment
        //     .elements_for_topic(&self.topic)
        //     .into_iter()
        //     .filter_map(|e| {
        //         let partition: u32 = e.partition().try_into().ok()?;
        //         let offset: u64 = e.offset().to_raw()?.try_into().ok()?;
        //         Some((partition, offset))
        //     });

        // let msgstream = RewindBlocker::new(start_offsets, downstream_consumer);

        Ok(PartitionRecordStream { msgstream })
    }
}

/// A stream of Kafka records for a specific partition.
pub struct PartitionRecordStream {
    // msgstream: RewindBlocker<PartitionConsumer>,
    msgstream: PartitionConsumer,
}

impl futures::Stream for PartitionRecordStream {
    type Item = Result<PartitionRecord, KafkaError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use futures::StreamExt;
        self.msgstream
            .poll_next_unpin(cx)
            .map_err(|err| KafkaError::from(err))
    }
}
