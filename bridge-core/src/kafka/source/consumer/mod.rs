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

async fn consumer_loop(
    kafka_consumer: Arc<StreamConsumer>,
    mut commands: mpsc::Receiver<SubscriberCommand>,
) {
    struct Subscriber {
        _assign_guard: AssignmentGuard,
        events: mpsc::Sender<Result<SubscriberEvent, KafkaError>>,
    }

    type TopicPartition = (TopicName, PartitionNumber);
    type Subscribers = HashMap<TopicPartition, Subscriber>;

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
                match cmd {
                    SubscriberCommand::Subscribe(SubscriptionRequest { topic, partition, consumer }) => {

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
                            continue;
                        }

                        // This clears the assignment as soon as one of its clone is dropped.
                        // We give one copy to the sending part of the subscriber's queue (our copy), and another
                        // to the receiving part of the queue (the migration job's copy).
                        let assignment_guard = AssignmentGuard::new(Arc::clone(&kafka_consumer), assignment);

                        let (tx, rx) = mpsc::channel(4);

                        let record_consumer = PartitionConsumer {
                            _assign_guard: assignment_guard.clone(),
                            buffered: None,
                            receiver: rx,
                        };

                        if consumer.send(Ok(record_consumer)).is_ok() {
                            subscriptions.insert((topic,partition), Subscriber {
                                _assign_guard: assignment_guard,
                                events: tx
                            });
                        }
                    },
                    SubscriberCommand::Seek(SeekRequest { topic, partition, offset, response }) => {

                        // Find the subscriber
                        let Some(subscriber) = subscriptions.get_mut(&(topic.clone(), partition)) else {
                            let err = format!("No subscriber for topic partition `{topic}:{partition}`");
                            response.send(Err(KafkaError::Subscription(err))).ok();
                            continue;
                        };

                        let seek_result = {
                            let mut tpl = TopicPartitionList::new();
                            let _ = tpl.add_partition_offset(
                                &topic,
                                partition,
                                rdkafka::Offset::Offset(offset),
                            );

                            kafka_consumer.seek_partitions(tpl, std::time::Duration::from_secs(5))
                        };

                        match seek_result {
                            Ok(tpl) => {
                                let _ = response.send(Ok(tpl));
                            },
                            Err(err) => {
                                let _ = response.send(Err(err));
                            }
                        }

                        let _ = subscriber.events
                            .feed(Ok(SubscriberEvent::PartitionSeeked))
                            .await;

                        let _ = subscriber.events.flush().await;
                    }
                };
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

                // Sort records by topic name
                records.sort_by(|r1, r2| r1.topic().cmp(r2.topic()));

                // Then group records by topic name in a Vec<(TopicName, Vec<ProtoRecord>)>
                use itertools::Itertools;
                let grouped_records = records.iter().chunk_by(|r| (r.topic(), r.partition()));
                let grouped_records : Vec<((TopicName,PartitionNumber), Vec<PartitionRecord>)> = grouped_records.into_iter().map(|((topic,partition), records)| {
                    let topic = topic.to_owned();
                    let records = records.map(PartitionRecord::from).collect();
                    ((topic,partition), records)
                }).collect();

                // Finally, send records as batches downstream
                for ((topic,partition), records) in grouped_records {
                    let Some(subscriber) = subscriptions.get_mut(&(topic.clone(), partition)) else {
                        tracing::debug!("No subscriber for topic partition `{topic}:{partition}`");
                        consumers_gone.insert(topic.to_owned());
                        continue;
                    };

                    let num_records = records.len();

                    if subscriber.events.feed(Ok(SubscriberEvent::Batch{records})).await.is_err() {
                        tracing::debug!(%topic, %partition, "Subcriber gone.");
                        consumers_gone.insert(topic.to_owned());
                        continue;
                    }
                    else {
                        tracing::trace!("Fed {num_records} records to topic queue `{topic}`")
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
