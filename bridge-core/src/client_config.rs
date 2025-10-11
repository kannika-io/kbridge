use rdkafka::ClientConfig;

pub trait ConfigBuilder {
    fn set_bootstrap_server(&mut self, bootstrap_server: &str) -> &mut ClientConfig;

    fn set_consumer_group_id(&mut self, consumer_group_id: &str) -> &mut ClientConfig;

    fn disable_auto_commit(&mut self) -> &mut ClientConfig;

    fn set_optional_properties(&mut self, properties: Option<Vec<String>>) -> &mut ClientConfig;
}

impl ConfigBuilder for ClientConfig {
    fn set_bootstrap_server(&mut self, bootstrap_server: &str) -> &mut ClientConfig {
        self.set("bootstrap.servers", bootstrap_server);
        self
    }

    fn set_consumer_group_id(&mut self, consumer_group_id: &str) -> &mut ClientConfig {
        self.set("group.id", consumer_group_id);
        self
    }

    fn disable_auto_commit(&mut self) -> &mut ClientConfig {
        self.set("enable.auto.commit", "false");
        self
    }

    fn set_optional_properties(
        &mut self,
        optional_properties: Option<Vec<String>>,
    ) -> &mut ClientConfig {
        if let Some(properties) = optional_properties {
            for property in properties {
                let property_elements: Vec<&str> = property.split('=').collect();
                if property_elements.len() == 2 {
                    self.set(property_elements[0], property_elements[1]);
                }
            }
        }
        self
    }
}
