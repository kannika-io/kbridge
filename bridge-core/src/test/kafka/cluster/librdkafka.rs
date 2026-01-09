use crate::kafka::properties::KafkaProducerProperties;

use super::*;
use rdkafka::mocking::MockCluster;

impl<'a, C> MockClusterExt for MockCluster<'a, C>
where
    C: ClientContext + 'static,
{
    async fn consumer_properties(&self) -> KafkaConsumerProperties {
        let props = vec![("bootstrap.servers".to_string(), self.bootstrap_servers())];
        KafkaConsumerProperties::from_iter(props)
    }

    async fn producer_properties(&self) -> KafkaProducerProperties {
        let props = vec![("bootstrap.servers".to_string(), self.bootstrap_servers())];
        KafkaProducerProperties::from_iter(props)
    }

    async fn create_topic(&self, topic: &str, partitions: i32) -> Result<(), MockClusterError> {
        self.create_topic(topic, partitions, 1)?;
        Ok(())
    }
}
