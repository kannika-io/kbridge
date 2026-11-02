use crate::partition::{PartitionEvent, PartitionRecord};
use crate::prelude::*;

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::channel::mpsc;
use futures::channel::oneshot;
use futures::{FutureExt, SinkExt, StreamExt, TryStreamExt};

use rdkafka::consumer::Consumer;
use rdkafka::error::KafkaError;
use rdkafka::{Message, TopicPartitionList};

use ahash::HashMap;

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

type Event = Result<SubscriberEvent, KafkaError>;
type TopicPartition = (TopicName, PartitionNumber);

struct Subscriber {
    _assign_guard: AssignmentGuard,
    events: mpsc::Sender<Event>,
    // Events waiting for room in `events`, in delivery order.
    // The partition is paused while events are waiting, so this stays small.
    outbox: VecDeque<Event>,
    paused: bool,
    // Set after a Kafka error: the channel is closed once the outbox is empty.
    closing: bool,
}

impl Subscriber {
    /// Moves events from the outbox into the channel until it is full.
    /// Returns false when the receiving side is gone.
    fn flush(&mut self) -> bool {
        while let Some(event) = self.outbox.pop_front() {
            if let Err(err) = self.events.try_send(event) {
                if err.is_disconnected() {
                    return false;
                }
                self.outbox.push_front(err.into_inner());
                break;
            }
        }
        true
    }
}

/// Dispatches commands and Kafka records to partition subscribers.
///
/// Never waits for room in a subscriber's channel: a subscriber waiting for a seek
/// does not read its stream, so waiting would deadlock the consumer task.
/// Events that do not fit wait in the subscriber's outbox instead.
struct Dispatcher {
    kafka_consumer: Arc<StreamConsumer>,
    subscriptions: HashMap<TopicPartition, Subscriber>,
}

impl Dispatcher {
    fn handle_command(&mut self, cmd: SubscriberCommand) {
        match cmd {
            SubscriberCommand::Subscribe(request) => self.subscribe(request),
            SubscriberCommand::Seek(request) => self.seek(request),
        }
    }

    fn subscribe(&mut self, request: SubscriptionRequest) {
        let SubscriptionRequest {
            topic,
            partition,
            consumer,
        } = request;

        let assignment = {
            let mut tpl = TopicPartitionList::new();
            tpl.add_partition(&topic, partition);
            tpl
        };

        // Attempt to assign new partition. This will fail if an assignment already exists for that partition.
        tracing::debug!(%topic, %partition, ?assignment, "New assignment: {assignment:?}");
        if let Err(err) = self.kafka_consumer.incremental_assign(&assignment) {
            tracing::debug!(%err, %topic, %partition, ?assignment, "Failed to add assignment: {err}");
            consumer.send(Err(err)).ok();
            return;
        }

        // This clears the assignment as soon as one of its clone is dropped.
        // We give one copy to the sending part of the subscriber's queue (our copy), and another
        // to the receiving part of the queue (the migration job's copy).
        let assignment_guard = AssignmentGuard::new(Arc::clone(&self.kafka_consumer), assignment);

        let (tx, rx) = mpsc::channel(4);

        let record_consumer = PartitionConsumer {
            _assign_guard: assignment_guard.clone(),
            buffered: None,
            receiver: rx,
        };

        if consumer.send(Ok(record_consumer)).is_ok() {
            let key = (topic, partition);
            // A previous subscription for this partition may have left it paused, and the pause
            // survives unassign/assign. Removing it resumes the partition.
            self.remove(&key);
            self.subscriptions.insert(
                key,
                Subscriber {
                    _assign_guard: assignment_guard,
                    events: tx,
                    outbox: VecDeque::new(),
                    paused: false,
                    closing: false,
                },
            );
        }
    }

    fn seek(&mut self, request: SeekRequest) {
        let SeekRequest {
            topic,
            partition,
            offset,
            response,
        } = request;

        let key = (topic, partition);
        if !self.is_open(&key) {
            let err = format!("No subscriber for topic partition `{}:{}`", key.0, key.1);
            response.send(Err(KafkaError::Subscription(err))).ok();
            return;
        }

        let seek_result = {
            let mut tpl = TopicPartitionList::new();
            let _ = tpl.add_partition_offset(&key.0, partition, rdkafka::Offset::Offset(offset));

            self.kafka_consumer
                .seek_partitions(tpl, std::time::Duration::from_secs(5))
        };

        response.send(seek_result).ok();

        // Records fetched before the seek are still delivered, ahead of this event.
        self.deliver(&key, Ok(SubscriberEvent::PartitionSeeked));
    }

    fn deliver_records(&mut self, records: Vec<(TopicPartition, Vec<PartitionRecord>)>) {
        for (key, records) in records {
            if !self.is_open(&key) {
                tracing::debug!("No subscriber for topic partition `{}:{}`", key.0, key.1);
                continue;
            }

            let num_records = records.len();
            self.deliver(&key, Ok(SubscriberEvent::Batch { records }));
            tracing::trace!("Queued {num_records} records for topic `{}`", key.0);
        }
    }

    fn deliver(&mut self, key: &TopicPartition, event: Event) {
        if let Some(subscriber) = self.subscriptions.get_mut(key) {
            subscriber.outbox.push_back(event);
            self.flush(key);
        }
    }

    /// Flushes a subscriber's outbox.
    /// The partition is paused while events are waiting, and resumed once the outbox is empty.
    fn flush(&mut self, key: &TopicPartition) {
        let Some(subscriber) = self.subscriptions.get_mut(key) else {
            return;
        };

        if !subscriber.flush() {
            tracing::debug!(topic = %key.0, partition = %key.1, "Subscriber gone.");
            self.remove(key);
            return;
        }

        if subscriber.closing && subscriber.outbox.is_empty() {
            subscriber.events.close_channel();
            self.remove(key);
            return;
        }

        let waiting = !subscriber.outbox.is_empty();
        if waiting != subscriber.paused {
            match self.set_paused(key, waiting) {
                Ok(()) => {
                    if let Some(subscriber) = self.subscriptions.get_mut(key) {
                        subscriber.paused = waiting;
                    }
                }
                Err(err) => {
                    tracing::warn!(%err, topic = %key.0, partition = %key.1, "Failed to pause or resume partition");
                }
            }
        }
    }

    fn set_paused(&self, key: &TopicPartition, paused: bool) -> Result<(), KafkaError> {
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition(&key.0, key.1);
        if paused {
            self.kafka_consumer.pause(&tpl)
        } else {
            self.kafka_consumer.resume(&tpl)
        }
    }

    /// Ready with the subscribers whose outbox can make progress: their channel has room or is closed.
    fn poll_outboxes(&mut self, cx: &mut Context<'_>) -> Poll<Vec<TopicPartition>> {
        let ready: Vec<TopicPartition> = self
            .subscriptions
            .iter_mut()
            .filter_map(|(key, subscriber)| {
                let ready =
                    !subscriber.outbox.is_empty() && subscriber.events.poll_ready(cx).is_ready();
                ready.then(|| key.clone())
            })
            .collect();

        if ready.is_empty() {
            Poll::Pending
        } else {
            Poll::Ready(ready)
        }
    }

    /// Forwards a Kafka error to all subscribers, after their waiting events, then closes them.
    fn fail_all(&mut self, err: KafkaError) {
        tracing::error!(%err, "Kafka consumer error");
        let keys: Vec<TopicPartition> = self.subscriptions.keys().cloned().collect();
        for key in keys {
            if let Some(subscriber) = self.subscriptions.get_mut(&key)
                && !subscriber.closing
            {
                subscriber.outbox.push_back(Err(err.clone()));
                subscriber.closing = true;
            }
            self.flush(&key);
        }
    }

    /// Removes subscribers whose stream was dropped.
    fn remove_closed(&mut self) {
        let closed: Vec<TopicPartition> = self
            .subscriptions
            .iter()
            .filter(|(_, subscriber)| subscriber.events.is_closed())
            .map(|(key, _)| key.clone())
            .collect();
        for key in closed {
            tracing::debug!(topic = %key.0, partition = %key.1, "Subscriber gone.");
            self.remove(&key);
        }
    }

    /// Removes a subscriber, resuming its partition if it was paused.
    fn remove(&mut self, key: &TopicPartition) {
        if let Some(subscriber) = self.subscriptions.remove(key)
            && subscriber.paused
        {
            self.set_paused(key, false).ok();
        }
    }

    fn is_open(&self, key: &TopicPartition) -> bool {
        self.subscriptions
            .get(key)
            .is_some_and(|subscriber| !subscriber.closing)
    }
}

async fn consumer_loop(
    kafka_consumer: Arc<StreamConsumer>,
    mut commands: mpsc::Receiver<SubscriberCommand>,
) {
    let mut kafka_stream = kafka_consumer.stream().try_ready_chunks(128);

    let mut dispatcher = Dispatcher {
        kafka_consumer: Arc::clone(&kafka_consumer),
        subscriptions: HashMap::default(),
    };

    loop {
        dispatcher.remove_closed();

        futures::select_biased! {
            // Handle new commands
            cmd = commands.select_next_some() => {
                dispatcher.handle_command(cmd);
            }

            // Deliver events waiting for room in a subscriber's channel
            ready = futures::future::poll_fn(|cx| dispatcher.poll_outboxes(cx)).fuse() => {
                for key in ready {
                    dispatcher.flush(&key);
                }
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

                dispatcher.deliver_records(grouped_records);

                // When an error is encountered, the error is propagated to all subscribed consumers
                if let Some(err) = err {
                    dispatcher.fail_all(err);
                }
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
