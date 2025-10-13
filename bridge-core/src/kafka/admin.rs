use rdkafka::{
    ClientConfig, admin::AdminClient, client::DefaultClientContext, config::FromClientConfig,
    error::KafkaError,
};

pub fn initialize_admin(
    config: &mut ClientConfig,
) -> Result<AdminClient<DefaultClientContext>, KafkaError> {
    AdminClient::<DefaultClientContext>::from_config(config)
}
