mod librdkafka;
mod testcontainer;
pub use testcontainer::ContainerizedCluster;

use crate::kafka::properties::KafkaConsumerProperties;
use crate::kafka::properties::KafkaProducerProperties;
use crate::kafka::source::RecordStreamConsumer;
use crate::kafka::source::RecordStreamConsumerTask;
use crate::kafka::*;

use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;
use std::time::Duration;

use rdkafka::ClientContext;
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::Consumer;
use rdkafka::consumer::DefaultConsumerContext;
use rdkafka::message::OwnedHeaders;
use rdkafka::producer::Producer;
use rdkafka::producer::{FutureProducer, FutureRecord};

type KafkaConsumer = rdkafka::consumer::StreamConsumer<DefaultConsumerContext>;
type KafkaProducer = rdkafka::producer::FutureProducer<DefaultClientContext>;

#[allow(async_fn_in_trait)]
pub trait MockClusterExt {
    async fn consumer_properties(&self) -> KafkaConsumerProperties;

    async fn producer_properties(&self) -> KafkaProducerProperties;

    async fn consumer_with_group(&self, group: &str) -> Result<KafkaConsumer, MockClusterError> {
        let mut properties = self.consumer_properties().await;
        properties.insert("group.id", group);

        properties
            .into_stream_consumer()
            .map_err(MockClusterError::KafkaError)
    }

    async fn record_consumer(
        &self,
        group: &str,
    ) -> Result<(Arc<RecordStreamConsumer>, RecordStreamConsumerTask), KafkaError> {
        let mut properties = self.consumer_properties().await;
        properties.insert("group.id", group);

        let (consumer, task) = RecordStreamConsumer::new(properties)?;

        Ok((Arc::new(consumer), task))
    }

    async fn producer(&self) -> Result<KafkaProducer, MockClusterError> {
        self.producer_properties()
            .await
            .into_future_producer()
            .map_err(MockClusterError::KafkaError)
    }

    async fn produce(
        &self,
        topic: &str,
        partition: i32,
        amount: i64,
    ) -> Result<(), MockClusterError> {
        let producer = self.producer().await?;

        // Send all messages by spawning tasks for parallel transmission
        // Producer clone is cheap (Arc internally) and necessary for moving into tasks
        let mut handles = Vec::with_capacity(amount as usize);

        for i in 0..amount {
            let key = format!("key-{}", i);
            let payload = format!("value-{}", i);
            let producer = producer.clone();
            let topic = topic.to_string();

            let handle = tokio::spawn(async move {
                producer
                    .send(
                        FutureRecord::to(&topic)
                            .partition(partition)
                            .key(&key)
                            .payload(&payload),
                        Duration::from_secs(10),
                    )
                    .await
            });

            handles.push(handle);
        }

        // Wait for all sends to complete
        for handle in handles {
            if let Ok(Err((e, _))) = handle.await {
                eprintln!("Failed to send message: {}", e);
            }
        }

        // Flush to ensure all messages are delivered
        producer
            .flush(Duration::from_secs(30))
            .map_err(MockClusterError::KafkaError)?;

        Ok(())
    }

    /// Produces messages with headers sequentially, ensuring message order is preserved.
    async fn produce_with_headers<F>(
        &self,
        topic: &str,
        partition: i32,
        amount: i64,
        header_fn: F,
    ) -> Result<(), MockClusterError>
    where
        F: Fn(i64) -> HashMap<String, String> + Send,
    {
        let producer: FutureProducer = self.producer().await?;

        for i in 0..amount {
            let i_as_string = i.to_string();
            let headers = header_fn(i);

            let mut kafka_headers = OwnedHeaders::new();
            for (k, v) in headers {
                kafka_headers = kafka_headers.insert(rdkafka::message::Header {
                    key: &k,
                    value: Some(&v),
                });
            }

            let result = producer
                .send(
                    FutureRecord::to(topic)
                        .partition(partition)
                        .key(&i_as_string)
                        .payload(&i_as_string)
                        .headers(kafka_headers),
                    Duration::from_secs(10),
                )
                .await;

            if let Err((e, _)) = result {
                eprintln!("Failed to send message {}: {}", i, e);
                return Err(MockClusterError::KafkaError(e));
            }
        }

        producer
            .flush(Duration::from_secs(30))
            .map_err(MockClusterError::KafkaError)?;

        Ok(())
    }

    async fn get_watermarks(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<(i64, i64), MockClusterError> {
        let consumer: KafkaConsumer = self.consumer_with_group("watermarks").await?;
        let (low, high) = consumer.fetch_watermarks(topic, partition, Duration::from_secs(1))?;
        Ok((low, high))
    }

    async fn create_topic(&self, topic: &str, partitions: i32) -> Result<(), MockClusterError>;
}

#[derive(thiserror::Error, Debug)]
pub enum MockClusterError {
    KafkaError(#[from] rdkafka::error::KafkaError),
    TestcontainerError(String),
}

impl Display for MockClusterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MockClusterError::KafkaError(err) => write!(f, "Kafka error: {}", err),
            MockClusterError::TestcontainerError(err) => write!(f, "Testcontainer error: {}", err),
        }
    }
}
