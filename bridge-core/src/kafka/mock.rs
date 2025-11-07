use crate::partition::*;
use crate::prelude::*;

use std::collections::HashMap;
use std::time::Duration;

use rdkafka::ClientContext;
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::StreamConsumer;
use rdkafka::message::OwnedHeaders;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};

use crate::partition::PartitionRecord;
use rdkafka::mocking::MockCluster;

pub trait MockClusterExt {
    type Consumer;
    type Producer;
    type Error;

    fn consumer(&self, group: &str) -> Result<Self::Consumer, Self::Error>;

    fn producer(&self) -> Result<Self::Producer, KafkaError>;

    fn produce(
        &self,
        topic: impl Into<String>,
        partition: i32,
        amount: i64,
    ) -> impl Future<Output = Result<(), Self::Error>>;

    fn produce_with_headers<F>(
        &self,
        topic: impl Into<String>,
        partition: i32,
        amount: i64,
        factory: F,
    ) -> impl Future<Output = Result<(), Self::Error>>
    where
        F: Fn(i64) -> HashMap<String, String>;
}

impl<'a, C> MockClusterExt for MockCluster<'a, C>
where
    C: ClientContext + 'static,
{
    type Consumer = StreamConsumer;
    type Producer = FutureProducer<DefaultClientContext>;
    type Error = KafkaError;

    fn consumer(&self, group: &str) -> Result<StreamConsumer, KafkaError> {
        use crate::kafka::client_config::ConfigBuilder;

        let mut config = rdkafka::ClientConfig::new()
            .set_bootstrap_server(&self.bootstrap_servers())
            .disable_auto_commit()
            .set_reset_from_beginning();
        config.set("group.id", group);

        let consumer: StreamConsumer = config.create()?;
        Ok(consumer)
    }

    fn producer(&self) -> Result<FutureProducer<DefaultClientContext>, KafkaError> {
        use crate::kafka::client_config::ConfigBuilder;

        let config = rdkafka::ClientConfig::new().set_bootstrap_server(&self.bootstrap_servers());

        let producer = config.create::<FutureProducer<DefaultClientContext>>()?;
        Ok(producer)
    }

    // TODO refactor because it is very slow
    async fn produce(
        &self,
        topic: impl Into<String>,
        partition: i32,
        amount: i64,
    ) -> Result<(), KafkaError> {
        let topic = topic.into();
        let producer: FutureProducer = self.producer()?;

        for i in 0..amount {
            let key = format!("key-{}", i);
            let payload = format!("value-{}", i);

            // Send and wait for delivery
            if let Err((e, _)) = producer
                .send(
                    FutureRecord::to(topic.as_str())
                        .partition(partition)
                        .key(&key)
                        .payload(&payload),
                    Duration::from_secs(1),
                )
                .await
            {
                eprintln!("Failed to send message {}: {}", i, e);
            }
        }

        // Wait for everything to be delivered
        let _ = producer.flush(Duration::from_secs(5));
        Ok(())
    }

    // Produce messages with headers
    async fn produce_with_headers<F>(
        &self,
        topic: impl Into<String>,
        partition: i32,
        amount: i64,
        customize: F,
    ) -> Result<(), KafkaError>
    where
        F: Fn(i64) -> HashMap<String, String>,
    {
        let topic = topic.into();
        let producer: FutureProducer = self.producer()?;

        for i in 0..amount {
            let i_as_bytes = i.to_string().into_bytes();

            let record = FutureRecord::to(&topic)
                .partition(partition)
                .key(&i_as_bytes)
                .payload(&i_as_bytes);

            let headers = customize(i);

            let kafka_headers = OwnedHeaders::new();
            let kafka_headers = headers.into_iter().fold(kafka_headers, |hdrs, (k, v)| {
                hdrs.insert(rdkafka::message::Header {
                    key: &k,
                    value: Some(&v),
                })
            });

            let record = record.headers(kafka_headers);

            if let Err((e, _)) = producer.send(record, Duration::from_millis(100)).await {
                eprintln!("Failed to send message {}: {}", i, e);
            }
        }

        // Wait for everything to be delivered
        let _ = producer.flush(Duration::from_secs(5));
        Ok(())
    }
}

pub trait MessageCollectionExt {
    fn assert_all_from_partition(&self, partition: i32);
}

impl MessageCollectionExt for Vec<Result<PartitionRecord, KafkaError>> {
    fn assert_all_from_partition(&self, partition: i32) {
        assert!(
            self.iter().all(|msg| {
                msg.as_ref()
                    .map(|m| m.partition() == partition)
                    .unwrap_or(false)
            }),
            "Not all messages are from partition {partition}"
        );
    }
}
