use crate::partition::{PartitionEvent, PartitionRecord};
use crate::prelude::*;

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::channel::mpsc;
use futures::channel::oneshot;
use futures::{FutureExt, SinkExt, StreamExt, TryStreamExt};

use rdkafka::consumer::Consumer;
use rdkafka::error::KafkaError;
use rdkafka::{Message, TopicPartitionList};

use ahash::{HashMap, HashSet};

mod assignment;
use assignment::AssignmentGuard;

use crate::kafka::source::StreamConsumer;

// Commands sent to the consumer task
enum SubscriberCommand {
    Subscribe(SubscriptionRequest),
    Seek(SeekRequest),
}

// Events a partition consumer can receive from the consumer task
enum SubscriberEvent {
    // A batch of records, available for consumption.
    Batch { records: Vec<PartitionRecord> },

    // A partition has been seeked to a new offset.
    // We must clear any buffered records to avoid delivering out-of-order records.
    PartitionSeeked,
}

struct SubscriptionRequest {
    topic: TopicName,
    partition: PartitionNumber,
    consumer: oneshot::Sender<Result<PartitionConsumer, KafkaError>>,
}

struct SeekRequest {
    topic: TopicName,
    partition: PartitionNumber,
    offset: Offset,
    response: oneshot::Sender<Result<TopicPartitionList, KafkaError>>,
}

#[derive(Debug, Clone)]
pub struct PartitionSubscriber {
    sender: mpsc::Sender<SubscriberCommand>,
}

pub type PartitionConsumerEvent = PartitionEvent<PartitionRecord>;

pub struct PartitionConsumer {
    _assign_guard: AssignmentGuard,
    buffered: Option<std::vec::IntoIter<PartitionConsumerEvent>>,
    receiver: mpsc::Receiver<Result<SubscriberEvent, KafkaError>>,
}

impl PartitionConsumer {
    fn next_buffered(&mut self) -> Option<PartitionConsumerEvent> {
        self.buffered.as_mut().and_then(|iter| iter.next())
    }
}

impl futures::Stream for PartitionConsumer {
    type Item = Result<PartitionConsumerEvent, KafkaError>;

    #[inline(always)]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let mut this = self.as_mut();

            if let Some(event) = this.next_buffered() {
                break Poll::Ready(Some(Ok(event)));
            }

            match futures::ready!(this.receiver.poll_next_unpin(cx)) {
                None => {
                    break Poll::Ready(None);
                }
                Some(Err(err)) => {
                    break Poll::Ready(Some(Err(err)));
                }
                Some(Ok(evt)) => match evt {
                    SubscriberEvent::Batch { records } => {
                        // Turn batch into record events
                        this.buffered = Some(
                            records
                                .into_iter()
                                .map(PartitionConsumerEvent::Message)
                                .collect::<Vec<_>>()
                                .into_iter(),
                        );
                    }
                    SubscriberEvent::PartitionSeeked => {
                        // Clear buffered records and emit a Seeked event
                        this.buffered = Some(vec![PartitionConsumerEvent::Seeked].into_iter());
                    }
                },
            }
        }
    }
}

pub struct RecordStreamConsumerTask {
    task: futures::future::BoxFuture<'static, ()>,
}

impl RecordStreamConsumerTask {
    pub fn new(kafka_consumer: Arc<StreamConsumer>) -> (Self, PartitionSubscriber) {
        // A queue for subscription requests
        let (tx, rx) = mpsc::channel(8);

        let this = Self {
            task: Box::pin(consumer_loop(kafka_consumer, rx)),
        };

        let subscriber = PartitionSubscriber { sender: tx };

        (this, subscriber)
    }
}

impl futures::Future for RecordStreamConsumerTask {
    type Output = ();

    #[inline(always)]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.task.poll_unpin(cx)
    }
}

impl PartitionSubscriber {
    pub async fn subscribe(
        &mut self,
        topic: TopicName,
        partition: PartitionNumber,
    ) -> Result<Option<PartitionConsumer>, KafkaError> {
        let (consumer_tx, consumer_rx) = oneshot::channel();

        let send_outcome = self
            .sender
            .send(SubscriberCommand::Subscribe(SubscriptionRequest {
                topic,
                partition,
                consumer: consumer_tx,
            }))
            .await;

        if send_outcome.is_err() {
            // Couldn't reach the consumer task: the subscription channel is closed
            return Ok(None);
        }

        let Ok(consumer_outcome) = consumer_rx.await else {
            // Couldn't reach the consumer task: the oneshot channel was closed
            return Ok(None);
        };

        let consumer = consumer_outcome?;

        Ok(Some(consumer))
    }

    pub async fn seek_offset(
        &mut self,
        topic: TopicName,
        partition: PartitionNumber,
        offset: Offset,
    ) -> Result<TopicPartitionList, KafkaError> {
        let (response_tx, response_rx) = oneshot::channel();

        let send_outcome = self
            .sender
            .send(SubscriberCommand::Seek(SeekRequest {
                topic,
                partition,
                offset,
                response: response_tx,
            }))
            .await;

        if send_outcome.is_err() {
            // Couldn't reach the consumer task: the subscription channel is closed
            return Err(KafkaError::Subscription(
                "Consumer task is not running".to_string(),
            ));
        }

        let Ok(seek_outcome) = response_rx.await else {
            // Couldn't reach the consumer task: the oneshot channel was closed
            return Err(KafkaError::Subscription(
                "Consumer task is not running".to_string(),
            ));
        };

        seek_outcome
    }
}

struct Subscriber {
    _assign_guard: AssignmentGuard,
    events: mpsc::Sender<Result<SubscriberEvent, KafkaError>>,
}

type TopicPartition = (TopicName, PartitionNumber);
type Subscribers = HashMap<TopicPartition, Subscriber>;

async fn consumer_loop(
    kafka_consumer: Arc<StreamConsumer>,
    mut commands: mpsc::Receiver<SubscriberCommand>,
) {
    let mut subscriptions: Subscribers = Subscribers::default();
    let mut consumers_gone = HashSet::default();

    let mut kafka_stream = kafka_consumer.stream().try_ready_chunks(128);

    loop {
        if !consumers_gone.is_empty() {
            subscriptions.retain(|_, consumer| !consumer.events.is_closed());
            consumers_gone.clear();
        }

        futures::select_biased! {
            // Handle new commands
            cmd = commands.select_next_some() => {
                handle_command(&kafka_consumer, &mut subscriptions, cmd).await;
            }

            // Handle new Kafka messages
            maybe_record = kafka_stream.try_next().fuse() => {
                use futures::stream::TryReadyChunksError;

                let (mut records, err) = match maybe_record {
                    Err(TryReadyChunksError(records, error)) => (records, Some(error)),
                    Ok(Some(records)) => (records, None),
                    Ok(None) => {
                        tracing::warn!("Consumer EOF");
                        break;
                    }
                };

                // Sort records by topic and partition. The sort is stable, so offsets stay in order.
                records.sort_by(|r1, r2| (r1.topic(), r1.partition()).cmp(&(r2.topic(), r2.partition())));

                // Then group records by topic partition in a Vec<(TopicPartition, Vec<PartitionRecord>)>
                use itertools::Itertools;
                let grouped_records = records.iter().chunk_by(|r| (r.topic(), r.partition()));
                let grouped_records : Vec<(TopicPartition, Vec<PartitionRecord>)> = grouped_records.into_iter().map(|((topic,partition), records)| {
                    let topic = topic.to_owned();
                    let records = records.map(PartitionRecord::from).collect();
                    ((topic,partition), records)
                }).collect();

                // Partitions seeked while delivering this chunk. Their remaining records predate the seek.
                let mut seeked = HashSet::default();

                // Finally, send records as batches downstream
                for (key, records) in grouped_records {
                    if seeked.contains(&key) {
                        tracing::trace!(topic = %key.0, partition = %key.1, "Dropping records fetched before seek");
                        continue;
                    }

                    let num_records = records.len();
                    let (topic, partition) = key.clone();

                    match deliver_batch(&kafka_consumer, &mut subscriptions, &mut commands, &mut seeked, key, records).await {
                        Delivery::Sent => {
                            tracing::trace!("Fed {num_records} records to topic queue `{topic}`");
                        }
                        Delivery::Seeked => {
                            tracing::trace!(%topic, %partition, "Dropping records fetched before seek");
                        }
                        Delivery::NoSubscriber => {
                            tracing::debug!("No subscriber for topic partition `{topic}:{partition}`");
                            consumers_gone.insert(topic);
                        }
                        Delivery::Gone => {
                            tracing::debug!(%topic, %partition, "Subcriber gone.");
                            consumers_gone.insert(topic);
                        }
                    }
                }

                // When an error is encountered, the error is propagated to all subscribed consumers
                if let Some(err) = err {
                    tracing::error!(%err, "Kafka consumer error");
                    // Forward the error to all clients and cleanup
                    for subscriber in subscriptions.values_mut() {
                        subscriber.events.feed(Err(err.clone())).await.ok();
                        subscriber.events.close().await.ok();
                    }
                    subscriptions.clear();
                }
            }
        }
    }
}

/// Handles a subscriber command.
/// Returns the topic partition that was seeked, if any.
async fn handle_command(
    kafka_consumer: &Arc<StreamConsumer>,
    subscriptions: &mut Subscribers,
    cmd: SubscriberCommand,
) -> Option<TopicPartition> {
    match cmd {
        SubscriberCommand::Subscribe(SubscriptionRequest {
            topic,
            partition,
            consumer,
        }) => {
            let assignment = {
                let mut tpl = TopicPartitionList::new();
                tpl.add_partition(&topic, partition);
                tpl
            };

            // Attempt to assign new partition. This will fail if an assignment already exists for that partition.
            tracing::debug!(%topic, %partition, ?assignment, "New assignment: {assignment:?}");
            if let Err(err) = kafka_consumer.incremental_assign(&assignment) {
                tracing::debug!(%err, %topic, %partition, ?assignment, "Failed to add assignment: {err}");
                consumer.send(Err(err)).ok();
                return None;
            }

            // This clears the assignment as soon as one of its clone is dropped.
            // We give one copy to the sending part of the subscriber's queue (our copy), and another
            // to the receiving part of the queue (the migration job's copy).
            let assignment_guard = AssignmentGuard::new(Arc::clone(kafka_consumer), assignment);

            let (tx, rx) = mpsc::channel(4);

            let record_consumer = PartitionConsumer {
                _assign_guard: assignment_guard.clone(),
                buffered: None,
                receiver: rx,
            };

            if consumer.send(Ok(record_consumer)).is_ok() {
                subscriptions.insert(
                    (topic, partition),
                    Subscriber {
                        _assign_guard: assignment_guard,
                        events: tx,
                    },
                );
            }

            None
        }
        SubscriberCommand::Seek(SeekRequest {
            topic,
            partition,
            offset,
            response,
        }) => {
            // Find the subscriber
            let key = (topic, partition);
            let Some(subscriber) = subscriptions.get_mut(&key) else {
                let err = format!("No subscriber for topic partition `{}:{}`", key.0, key.1);
                response.send(Err(KafkaError::Subscription(err))).ok();
                return None;
            };

            let seek_result = {
                let mut tpl = TopicPartitionList::new();
                let _ =
                    tpl.add_partition_offset(&key.0, partition, rdkafka::Offset::Offset(offset));

                kafka_consumer.seek_partitions(tpl, std::time::Duration::from_secs(5))
            };

            // Respond before emitting the Seeked event: the subscriber only reads its stream
            // again once the seek has completed, so the event may wait for channel capacity.
            let _ = response.send(seek_result);

            let _ = subscriber
                .events
                .feed(Ok(SubscriberEvent::PartitionSeeked))
                .await;

            let _ = subscriber.events.flush().await;

            Some(key)
        }
    }
}

enum Delivery {
    Sent,
    // The partition was seeked before the batch could be delivered; the batch was dropped.
    Seeked,
    NoSubscriber,
    Gone,
}

/// Delivers a batch of records to a subscriber, handling commands while its channel is full.
///
/// Waiting for channel capacity without handling commands deadlocks:
/// a subscriber stops reading its stream while it waits for a seek to complete.
/// Partitions seeked meanwhile are added to `seeked`; a batch for a seeked partition is dropped.
async fn deliver_batch(
    kafka_consumer: &Arc<StreamConsumer>,
    subscriptions: &mut Subscribers,
    commands: &mut mpsc::Receiver<SubscriberCommand>,
    seeked: &mut HashSet<TopicPartition>,
    key: TopicPartition,
    records: Vec<PartitionRecord>,
) -> Delivery {
    loop {
        let outcome = {
            let Some(subscriber) = subscriptions.get_mut(&key) else {
                return Delivery::NoSubscriber;
            };

            futures::select_biased! {
                cmd = commands.select_next_some() => Err(cmd),
                ready = futures::future::poll_fn(|cx| subscriber.events.poll_ready(cx)).fuse() => Ok(ready),
            }
        };

        match outcome {
            Err(cmd) => {
                if let Some(seeked_key) = handle_command(kafka_consumer, subscriptions, cmd).await {
                    seeked.insert(seeked_key);
                }
                if seeked.contains(&key) {
                    return Delivery::Seeked;
                }
            }
            Ok(Err(_)) => return Delivery::Gone,
            Ok(Ok(())) => {
                let Some(subscriber) = subscriptions.get_mut(&key) else {
                    return Delivery::NoSubscriber;
                };
                return match subscriber
                    .events
                    .start_send(Ok(SubscriberEvent::Batch { records }))
                {
                    Ok(()) => Delivery::Sent,
                    Err(_) => Delivery::Gone,
                };
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::StreamExt;

    use crate::kafka::partition::KafkaPartition;
    use crate::partition::{Partition, PartitionEvent, SeekablePartition};
    use crate::prelude::*;
    use crate::test::kafka::cluster::{ContainerizedCluster, MockClusterExt};

    /// A seek must complete even when the caller has stopped reading its stream
    /// and the consumer task has filled the partition's channel with prefetched records.
    #[tokio::test]
    async fn test_seek_while_partition_channel_is_full() -> anyhow::Result<()> {
        let broker = ContainerizedCluster::start().await?;
        let topic = "seek.full.channel";
        broker.create_topic(topic, 1).await?;
        broker.produce(topic, 0, 5_000).await?;

        let (consumer, consumer_task) = broker.record_consumer("seek-full-channel").await?;
        tokio::spawn(consumer_task);

        let mut partition = KafkaPartition::open(consumer, topic, 0).await?;
        let mut stream = partition.stream().await?;

        // Read a single record, then stop reading the stream.
        let first = tokio::time::timeout(Duration::from_secs(30), stream.next())
            .await?
            .expect("stream ended")?;
        assert!(first.as_message().is_some());

        // Give the consumer task time to fill the channel with prefetched records.
        tokio::time::sleep(Duration::from_secs(2)).await;

        tokio::time::timeout(Duration::from_secs(10), partition.seek_from_offset(0))
            .await
            .expect("seek deadlocked while the partition channel was full")?;

        // Records from before the seek are followed by a Seeked event,
        // after which the stream restarts at the seek offset.
        let next_offset = tokio::time::timeout(Duration::from_secs(30), async {
            let mut seeked = false;
            while let Some(event) = stream.next().await {
                match event? {
                    PartitionEvent::Seeked => seeked = true,
                    PartitionEvent::Message(msg) if seeked => return Ok(msg.offset()),
                    PartitionEvent::Message(_) => {}
                }
            }
            anyhow::bail!("stream ended before a record after the seek")
        })
        .await??;
        assert_eq!(next_offset, 0);

        Ok(())
    }
}
