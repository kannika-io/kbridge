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
use assignment::{AssignmentGuard, AssignmentHandle, Reassigned};

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
    start: Offset,
    consumer: oneshot::Sender<Result<PartitionConsumer, KafkaError>>,
}

struct SeekRequest {
    topic: TopicName,
    partition: PartitionNumber,
    offset: Offset,
    response: oneshot::Sender<Result<(), KafkaError>>,
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
    /// Assigns a partition, starting at the absolute offset `start`.
    pub async fn subscribe(
        &mut self,
        topic: TopicName,
        partition: PartitionNumber,
        start: Offset,
    ) -> Result<Option<PartitionConsumer>, KafkaError> {
        let (consumer_tx, consumer_rx) = oneshot::channel();

        let send_outcome = self
            .sender
            .send(SubscriberCommand::Subscribe(SubscriptionRequest {
                topic,
                partition,
                start,
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

    /// Seeks a subscribed partition.
    /// Returns `Ok(None)` when the consumer task can't be reached.
    pub async fn seek_offset(
        &mut self,
        topic: TopicName,
        partition: PartitionNumber,
        offset: Offset,
    ) -> Result<Option<()>, KafkaError> {
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
            return Ok(None);
        }

        let Ok(seek_outcome) = response_rx.await else {
            // Couldn't reach the consumer task: the oneshot channel was closed
            return Ok(None);
        };

        seek_outcome.map(Some)
    }
}

type Event = Result<SubscriberEvent, KafkaError>;
type TopicPartition = (TopicName, PartitionNumber);

struct Subscriber {
    assign_guard: AssignmentGuard,
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
    async fn handle_command(&mut self, cmd: SubscriberCommand) {
        match cmd {
            SubscriberCommand::Subscribe(request) => self.subscribe(request).await,
            SubscriberCommand::Seek(request) => self.seek(request).await,
        }
    }

    /// Runs a librdkafka call that waits until librdkafka has applied it (assign, seek, pause, resume)
    /// on the blocking thread pool, so it does not block a Tokio worker thread.
    async fn blocking<T: Send + 'static>(
        &self,
        call: impl FnOnce(&StreamConsumer) -> T + Send + 'static,
    ) -> T {
        let consumer = Arc::clone(&self.kafka_consumer);
        tokio::task::spawn_blocking(move || call(&consumer))
            .await
            .expect("librdkafka call panicked")
    }

    async fn subscribe(&mut self, request: SubscriptionRequest) {
        let SubscriptionRequest {
            topic,
            partition,
            start,
            consumer,
        } = request;

        // Assign at an absolute offset, so librdkafka starts fetching right away.
        // Without an offset, it first asks the group coordinator for the committed offset,
        // and rejects seeks until that answer arrives. A logical offset (beginning, end) is looked up
        // at the broker instead, and a seek made before that answer is undone 500ms later,
        // when librdkafka retries the lookup it considers outdated.
        let mut assignment = TopicPartitionList::new();
        if let Err(err) =
            assignment.add_partition_offset(&topic, partition, rdkafka::Offset::Offset(start))
        {
            consumer.send(Err(err)).ok();
            return;
        }

        // Attempt to assign new partition. This will fail if an assignment already exists for that partition.
        tracing::debug!(%topic, %partition, ?assignment, "New assignment: {assignment:?}");
        let (assignment, result) = self
            .blocking(move |consumer| {
                let result = consumer.incremental_assign(&assignment);
                (assignment, result)
            })
            .await;
        if let Err(err) = result {
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
            self.remove(&key).await;
            self.subscriptions.insert(
                key,
                Subscriber {
                    assign_guard: assignment_guard,
                    events: tx,
                    outbox: VecDeque::new(),
                    paused: false,
                    closing: false,
                },
            );
        }
    }

    async fn seek(&mut self, request: SeekRequest) {
        let SeekRequest {
            topic,
            partition,
            offset,
            response,
        } = request;

        let key = (topic, partition);
        let assignment = match self.subscriptions.get(&key) {
            Some(subscriber) if !subscriber.closing => subscriber.assign_guard.handle(),
            _ => {
                let err = format!("no subscriber for `{}` partition {}", key.0, key.1);
                response.send(Err(KafkaError::Seek(err))).ok();
                return;
            }
        };

        let (topic, partition) = key.clone();
        let seek_result = self
            .blocking(move |consumer| {
                seek_partition(consumer, &assignment, &topic, partition, offset)
            })
            .await;
        let seeked = seek_result.is_ok();
        response.send(seek_result).ok();

        // Records fetched before the seek are still delivered, ahead of this event.
        // A failed seek leaves the partition where it was, so there is nothing to mark.
        if seeked {
            self.deliver(&key, Ok(SubscriberEvent::PartitionSeeked))
                .await;
        }
    }

    async fn deliver_records(&mut self, records: Vec<(TopicPartition, Vec<PartitionRecord>)>) {
        for (key, records) in records {
            if !self.is_open(&key) {
                tracing::debug!("No subscriber for topic partition `{}:{}`", key.0, key.1);
                continue;
            }

            let num_records = records.len();
            self.deliver(&key, Ok(SubscriberEvent::Batch { records }))
                .await;
            tracing::trace!("Queued {num_records} records for topic `{}`", key.0);
        }
    }

    async fn deliver(&mut self, key: &TopicPartition, event: Event) {
        if let Some(subscriber) = self.subscriptions.get_mut(key) {
            subscriber.outbox.push_back(event);
            self.flush(key).await;
        }
    }

    /// Flushes a subscriber's outbox.
    /// The partition is paused while events are waiting, and resumed once the outbox is empty.
    async fn flush(&mut self, key: &TopicPartition) {
        let Some(subscriber) = self.subscriptions.get_mut(key) else {
            return;
        };

        if !subscriber.flush() {
            tracing::debug!(topic = %key.0, partition = %key.1, "Subscriber gone.");
            self.remove(key).await;
            return;
        }

        if subscriber.closing && subscriber.outbox.is_empty() {
            subscriber.events.close_channel();
            self.remove(key).await;
            return;
        }

        let waiting = !subscriber.outbox.is_empty();
        if waiting != subscriber.paused {
            match self.set_paused(key, waiting).await {
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

    async fn set_paused(&self, key: &TopicPartition, paused: bool) -> Result<(), KafkaError> {
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition(&key.0, key.1);
        self.blocking(move |consumer| {
            if paused {
                consumer.pause(&tpl)
            } else {
                consumer.resume(&tpl)
            }
        })
        .await
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
    async fn fail_all(&mut self, err: KafkaError) {
        tracing::error!(%err, "Kafka consumer error");
        let keys: Vec<TopicPartition> = self.subscriptions.keys().cloned().collect();
        for key in keys {
            if let Some(subscriber) = self.subscriptions.get_mut(&key)
                && !subscriber.closing
            {
                subscriber.outbox.push_back(Err(err.clone()));
                subscriber.closing = true;
            }
            self.flush(&key).await;
        }
    }

    /// Removes subscribers whose stream was dropped.
    async fn remove_closed(&mut self) {
        let closed: Vec<TopicPartition> = self
            .subscriptions
            .iter()
            .filter(|(_, subscriber)| subscriber.events.is_closed())
            .map(|(key, _)| key.clone())
            .collect();
        for key in closed {
            tracing::debug!(topic = %key.0, partition = %key.1, "Subscriber gone.");
            self.remove(&key).await;
        }
    }

    /// Removes a subscriber, resuming its partition if it was paused.
    async fn remove(&mut self, key: &TopicPartition) {
        if let Some(subscriber) = self.subscriptions.remove(key)
            && subscriber.paused
        {
            self.set_paused(key, false).await.ok();
        }
    }

    fn is_open(&self, key: &TopicPartition) -> bool {
        self.subscriptions
            .get(key)
            .is_some_and(|subscriber| !subscriber.closing)
    }
}

/// Seeks a partition and checks the per-partition result.
/// Blocks until librdkafka has applied the seek, so run it on the blocking pool.
///
/// Every failure is a `KafkaError::Seek` that names the partition and offset.
fn seek_partition(
    consumer: &StreamConsumer,
    assignment: &AssignmentHandle,
    topic: &str,
    partition: PartitionNumber,
    offset: Offset,
) -> Result<(), KafkaError> {
    use rdkafka::types::RDKafkaErrorCode;

    let seek_error = |reason: &dyn std::fmt::Display| {
        KafkaError::Seek(format!(
            "`{topic}` partition {partition} to offset {offset}: {reason}"
        ))
    };

    let mut target = TopicPartitionList::new();
    target
        .add_partition_offset(topic, partition, rdkafka::Offset::Offset(offset))
        .map_err(|_| seek_error(&"invalid offset"))?;

    let tpl = consumer
        .seek_partitions(target.clone(), std::time::Duration::from_secs(5))
        .map_err(|err| match err {
            KafkaError::Seek(reason) => seek_error(&reason),
            err => seek_error(&err),
        })?;

    // `seek_partitions` only fails as a whole; a seek rejected for the partition is reported in its entry.
    // The entry's error comes back as `KafkaError::OffsetFetch`, so keep only its code.
    let code = tpl
        .elements()
        .iter()
        .find_map(|elem| elem.error().err())
        .map(|err| err.rdkafka_error_code().unwrap_or(RDKafkaErrorCode::Fail));
    let Some(code) = code else {
        return Ok(());
    };

    // `__STATE`: librdkafka is not fetching the partition. Our own assignment can be waiting
    // for earlier unassignments to stop. Assigning it again at the offset makes it start there.
    if code == RDKafkaErrorCode::State {
        match assignment.reassign(&target) {
            Reassigned::Yes => return Ok(()),
            Reassigned::NotAssigned(err) => {
                return Err(seek_error(&format!(
                    "assigning at the offset failed: {err}"
                )));
            }
            // The partition was released meanwhile, or is not assigned at all: report the seek's own error.
            Reassigned::Released => {
                tracing::debug!(%topic, %partition, "Partition released before it could be assigned at the seek offset");
            }
            Reassigned::NotUnassigned(err) => {
                tracing::debug!(%err, %topic, %partition, "Failed to unassign partition to assign it at the seek offset");
            }
        }
    }

    Err(seek_error(&code))
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
        dispatcher.remove_closed().await;

        futures::select_biased! {
            // Handle new commands
            cmd = commands.select_next_some() => {
                dispatcher.handle_command(cmd).await;
            }

            // Deliver events waiting for room in a subscriber's channel
            ready = futures::future::poll_fn(|cx| dispatcher.poll_outboxes(cx)).fuse() => {
                for key in ready {
                    dispatcher.flush(&key).await;
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

                dispatcher.deliver_records(grouped_records).await;

                // When an error is encountered, the error is propagated to all subscribed consumers
                if let Some(err) = err {
                    dispatcher.fail_all(err).await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use futures::StreamExt;
    use rdkafka::TopicPartitionList;
    use rdkafka::consumer::Consumer;

    use crate::kafka::KafkaError;
    use crate::kafka::partition::KafkaPartition;
    use crate::partition::{Partition, PartitionEvent, PartitionRecord, SeekablePartition};
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

    /// Reads the stream until the first record after a Seeked event, and returns its offset.
    async fn offset_after_seeked<S, E>(stream: &mut S) -> anyhow::Result<i64>
    where
        S: futures::Stream<Item = Result<PartitionEvent<PartitionRecord>, E>> + Unpin,
        E: std::error::Error + Send + Sync + 'static,
    {
        tokio::time::timeout(Duration::from_secs(30), async {
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
        .await?
    }

    /// Fails when the stream delivers a Seeked event within a short while.
    async fn assert_no_seeked<S, E>(stream: &mut S) -> anyhow::Result<()>
    where
        S: futures::Stream<Item = Result<PartitionEvent<PartitionRecord>, E>> + Unpin,
        E: std::error::Error + Send + Sync + 'static,
    {
        let drained = tokio::time::timeout(Duration::from_secs(2), async {
            while let Some(event) = stream.next().await {
                if matches!(event?, PartitionEvent::Seeked) {
                    anyhow::bail!("Seeked event emitted after a failed seek");
                }
            }
            Ok(())
        })
        .await;
        drained.unwrap_or(Ok(()))
    }

    /// A seek straight after assigning the partition must take effect.
    /// The partition is assigned at an explicit offset, so librdkafka starts fetching it
    /// without first asking the group coordinator for a committed offset.
    #[tokio::test]
    async fn test_seek_right_after_assign_reaches_the_offset() -> anyhow::Result<()> {
        let broker = ContainerizedCluster::start().await?;
        let topic = "seek.after.assign";
        broker.create_topic(topic, 1).await?;
        broker.produce(topic, 0, 10).await?;

        let (consumer, consumer_task) = broker.record_consumer("seek-after-assign").await?;
        tokio::spawn(consumer_task);

        let mut partition = KafkaPartition::open(consumer, topic, 0).await?;
        let mut stream = partition.stream().await?;

        tokio::time::timeout(Duration::from_secs(30), partition.seek_from_offset(5)).await??;

        assert_eq!(offset_after_seeked(&mut stream).await?, 5);

        Ok(())
    }

    /// After a seek straight after assigning, the stream must continue from the seek offset,
    /// and must not jump back later. Had the partition started at a logical offset (e.g. beginning),
    /// librdkafka would look that offset up; a lookup answered after the seek is retried 500ms later
    /// and undoes the seek.
    #[tokio::test]
    async fn test_seek_right_after_assign_is_not_undone() -> anyhow::Result<()> {
        let broker = ContainerizedCluster::start().await?;
        let topic = "seek.after.assign.kept";
        broker.create_topic(topic, 1).await?;
        broker.produce(topic, 0, 20).await?;

        let (consumer, consumer_task) = broker.record_consumer("seek-after-assign-kept").await?;
        tokio::spawn(consumer_task);

        let mut partition = KafkaPartition::open(consumer, topic, 0).await?;

        // Hold back every broker answer until the seek is done: assigning and seeking need no broker.
        broker.pause().await?;
        let mut stream = partition.stream().await?;
        let seek =
            tokio::time::timeout(Duration::from_secs(10), partition.seek_from_offset(5)).await;
        broker.unpause().await?;
        seek??;

        // Read past the 500ms retry, until the stream has been idle for a while.
        let mut seeked = false;
        let mut offsets = Vec::new();
        while let Ok(Some(event)) =
            tokio::time::timeout(Duration::from_millis(1500), stream.next()).await
        {
            match event? {
                PartitionEvent::Seeked => seeked = true,
                PartitionEvent::Message(msg) if seeked => offsets.push(msg.offset()),
                PartitionEvent::Message(_) => {}
            }
        }
        assert_eq!(offsets, (5..20).collect::<Vec<i64>>());

        Ok(())
    }

    /// librdkafka can delay starting an assigned partition, e.g. while earlier unassignments are still stopping.
    /// A seek in that window must still take effect.
    #[tokio::test]
    async fn test_seek_before_fetching_starts_reaches_the_offset() -> anyhow::Result<()> {
        let broker = ContainerizedCluster::start().await?;
        let topic = "seek.before.fetching";
        broker.create_topic(topic, 1).await?;
        broker.produce(topic, 0, 10).await?;

        let (consumer, consumer_task) = broker.record_consumer("seek-before-fetching").await?;
        tokio::spawn(consumer_task);

        let mut partition = KafkaPartition::open(Arc::clone(&consumer), topic, 0).await?;
        let mut stream = partition.stream().await?;

        // Assign the partition again without a start offset: librdkafka waits for the group coordinator
        // to return the committed offset before it starts fetching.
        let mut assignment = TopicPartitionList::new();
        assignment.add_partition(topic, 0);
        consumer.consumer.incremental_unassign(&assignment)?;
        consumer.consumer.incremental_assign(&assignment)?;

        tokio::time::timeout(Duration::from_secs(30), partition.seek_from_offset(5)).await??;

        assert_eq!(offset_after_seeked(&mut stream).await?, 5);

        Ok(())
    }

    /// A seek that librdkafka rejects for the partition must return librdkafka's error,
    /// and must not emit a Seeked event: records after it would be read as if they came after the seek.
    #[tokio::test]
    async fn test_failed_seek_is_an_error_without_seeked_event() -> anyhow::Result<()> {
        let broker = ContainerizedCluster::start().await?;
        let topic = "seek.failed";
        broker.create_topic(topic, 1).await?;
        broker.produce(topic, 0, 10).await?;

        let (consumer, consumer_task) = broker.record_consumer("seek-failed").await?;
        tokio::spawn(consumer_task);

        let mut partition = KafkaPartition::open(Arc::clone(&consumer), topic, 0).await?;
        let mut stream = partition.stream().await?;

        // Unassign the partition behind the consumer task's back.
        // The unassignment has stopped the fetcher by the time the seek reaches it,
        // so librdkafka rejects the seek with `__STATE` on every run.
        let mut assignment = TopicPartitionList::new();
        assignment.add_partition(topic, 0);
        consumer.consumer.incremental_unassign(&assignment)?;

        let seek =
            tokio::time::timeout(Duration::from_secs(30), partition.seek_from_offset(0)).await?;
        let err = seek.expect_err("failed seek was reported as Ok");
        assert!(
            matches!(&err, KafkaError::Seek(_)),
            "unexpected error: {err:?}"
        );
        assert_eq!(
            err.to_string(),
            "seek failed: `seek.failed` partition 0 to offset 0: State (Local: Erroneous state)"
        );

        assert_no_seeked(&mut stream).await?;

        // Restore the assignment, so dropping the partition unassigns it cleanly.
        consumer.consumer.incremental_assign(&assignment)?;

        Ok(())
    }

    /// An offset librdkafka cannot represent must fail the seek instead of seeking somewhere else.
    #[tokio::test]
    async fn test_seek_to_negative_offset_is_an_error_without_seeked_event() -> anyhow::Result<()> {
        let broker = ContainerizedCluster::start().await?;
        let topic = "seek.negative";
        broker.create_topic(topic, 1).await?;
        broker.produce(topic, 0, 10).await?;

        let (consumer, consumer_task) = broker.record_consumer("seek-negative").await?;
        tokio::spawn(consumer_task);

        let mut partition = KafkaPartition::open(consumer, topic, 0).await?;
        let mut stream = partition.stream().await?;

        let seek =
            tokio::time::timeout(Duration::from_secs(30), partition.seek_from_offset(-5)).await?;
        let err = seek.expect_err("seek to a negative offset was reported as Ok");
        assert!(
            matches!(&err, KafkaError::Seek(_)),
            "unexpected error: {err:?}"
        );

        assert_no_seeked(&mut stream).await?;

        Ok(())
    }

    /// A seek's re-assign fallback must not re-add a partition whose guard was dropped meanwhile:
    /// it would stay assigned without a guard, and the next subscription for it would fail.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_seek_fallback_never_outlives_the_assignment_guard() -> anyhow::Result<()> {
        use super::AssignmentGuard;

        let broker = ContainerizedCluster::start().await?;
        let topic = "seek.guard.race";
        broker.create_topic(topic, 1).await?;
        broker.produce(topic, 0, 10).await?;

        let (consumer, _consumer_task) = broker.record_consumer("seek-guard-race").await?;
        let raw = Arc::clone(&consumer.consumer);

        let leaked = tokio::task::spawn_blocking(move || -> anyhow::Result<usize> {
            let mut leaked = 0;
            for i in 0..300u64 {
                // Without a start offset, the partition waits for the group coordinator,
                // so the seek gets `__STATE` and takes the re-assign fallback.
                let mut assignment = TopicPartitionList::new();
                assignment.add_partition(topic, 0);
                raw.incremental_assign(&assignment)?;
                let guard = AssignmentGuard::new(Arc::clone(&raw), assignment.clone());

                let barrier = Arc::new(std::sync::Barrier::new(2));
                let seek = {
                    let raw = Arc::clone(&raw);
                    let barrier = Arc::clone(&barrier);
                    let handle = guard.handle();
                    std::thread::spawn(move || {
                        barrier.wait();
                        super::seek_partition(&raw, &handle, topic, 0, 5)
                    })
                };
                barrier.wait();
                std::thread::sleep(Duration::from_micros(i * 5));
                drop(guard);
                let _ = seek.join().expect("seek panicked");

                if raw.assignment()?.count() > 0 {
                    leaked += 1;
                    raw.incremental_unassign(&assignment)?;
                }
            }
            Ok(leaked)
        })
        .await??;

        assert_eq!(
            leaked, 0,
            "partition stayed assigned after its guard was dropped"
        );
        Ok(())
    }
}
