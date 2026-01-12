use crate::kafka::properties::{KafkaConsumerProperties, KafkaProducerProperties};
use crate::test::kafka::cluster::{MockClusterError, MockClusterExt};

use std::time::Duration;
use std::vec;

use rdkafka::ClientConfig;
use rdkafka::admin::{AdminOptions, NewTopic, TopicReplication};
use rdkafka::consumer::{BaseConsumer, Consumer};
use testcontainers::core::IntoContainerPort;
use testcontainers::core::logs::consumer::LogConsumer;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt, TestcontainersError};

pub struct ContainerizedCluster {
    container: ContainerAsync<GenericImage>,
    kafka_port: u16,
}

struct StdoutLogConsumer;

impl LogConsumer for StdoutLogConsumer {
    fn accept<'a>(
        &'a self,
        record: &'a testcontainers::core::logs::LogFrame,
    ) -> futures::future::BoxFuture<'a, ()> {
        use futures::future::FutureExt;
        let str = String::from_utf8(record.bytes().to_vec()).unwrap();
        println!("{str}");
        futures::future::ready(()).boxed()
    }
}

impl ContainerizedCluster {
    pub async fn start() -> Result<Self, TestcontainersError> {
        // Find an available port
        let kafka_port = Self::find_available_port()?;

        let redpanda_image =
            GenericImage::new("docker.redpanda.com/redpandadata/redpanda", "v24.3.1")
                .with_mapped_port(kafka_port, 9092.tcp())
                // .with_log_consumer(StdoutLogConsumer)
                .with_cmd(vec![
                    "redpanda",
                    "start",
                    "--mode=dev-container",
                    "--smp=1",
                    "--overprovisioned",
                    "--kafka-addr",
                    "PLAINTEXT://0.0.0.0:9092",
                    "--advertise-kafka-addr",
                    &format!("PLAINTEXT://127.0.0.1:{}", kafka_port),
                ]);

        let container = redpanda_image.start().await?;

        let bootstrap = format!("127.0.0.1:{}", kafka_port);
        Self::wait_for_broker_ready(&bootstrap).await?;

        Ok(Self {
            container,
            kafka_port,
        })
    }

    fn find_available_port() -> Result<u16, TestcontainersError> {
        // Bind to port 0 to let the OS assign an available port
        let listener = std::net::TcpListener::bind("127.0.0.1:0").map_err(|e| {
            TestcontainersError::Other(format!("Failed to find available port: {}", e).into())
        })?;
        let port = listener
            .local_addr()
            .map_err(|e| {
                TestcontainersError::Other(format!("Failed to get local address: {}", e).into())
            })?
            .port();
        // Listener is dropped here, freeing the port
        Ok(port)
    }

    async fn wait_for_broker_ready(bootstrap: &str) -> Result<(), TestcontainersError> {
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", bootstrap);
        config.set("metadata.max.age.ms", "1000");

        let timeout = Duration::from_secs(10);
        let start = std::time::Instant::now();

        loop {
            if start.elapsed() > timeout {
                return Err(TestcontainersError::Other(
                    "Timeout waiting for Redpanda broker to be ready".into(),
                ));
            }

            match config.create::<BaseConsumer>() {
                Ok(consumer) => match consumer.fetch_metadata(None, Duration::from_secs(1)) {
                    Ok(metadata) => {
                        if !metadata.brokers().is_empty() {
                            return Ok(());
                        }
                    }
                    Err(_e) => {}
                },
                Err(_e) => {}
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    pub async fn kafka_port(&self) -> Result<u16, TestcontainersError> {
        Ok(self.kafka_port)
    }

    pub async fn bootstrap_servers(&self) -> Result<String, TestcontainersError> {
        Ok(format!("127.0.0.1:{}", self.kafka_port))
    }
}

impl MockClusterExt for ContainerizedCluster {
    async fn consumer_properties(&self) -> KafkaConsumerProperties {
        let props = vec![(
            "bootstrap.servers".to_string(),
            self.bootstrap_servers().await.unwrap(),
        )];
        KafkaConsumerProperties::from_iter(props)
    }

    async fn producer_properties(&self) -> KafkaProducerProperties {
        let props = vec![(
            "bootstrap.servers".to_string(),
            self.bootstrap_servers().await.unwrap(),
        )];
        KafkaProducerProperties::from_iter(props)
    }

    async fn create_topic(&self, topic: &str, partitions: i32) -> Result<(), MockClusterError> {
        let props: KafkaConsumerProperties = self.consumer_properties().await;
        let client = props.into_admin_client()?;
        let new_topic = NewTopic::new(topic, partitions, TopicReplication::Fixed(1));
        let opts = AdminOptions::new().request_timeout(Some(Duration::from_secs(10)));
        let results = client.create_topics(vec![&new_topic], &opts).await?;

        for result in results {
            if let Err((_topic_name, error_code)) = result {
                return Err(MockClusterError::KafkaError(
                    rdkafka::error::KafkaError::AdminOp(error_code),
                ));
            }
        }

        tracing::debug!("Created topic {}", topic);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_testcontainers_cluster() -> anyhow::Result<()> {
        let cluster = ContainerizedCluster::start().await?;

        cluster.create_topic("test-topic", 3).await?;
        cluster.produce("test-topic", 0, 10).await?;

        let (low, high) = cluster.get_watermarks("test-topic", 0).await?;
        assert_eq!(low, 0);
        assert_eq!(high, 10);

        Ok(())
    }
}
