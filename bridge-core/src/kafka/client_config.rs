use std::collections::HashMap;

use rdkafka::ClientConfig;
use tracing::warn;

pub const DEFAULT_GROUP_ID: &str = "bridge-consumer-group";
pub const GROUP_ID_KEY: &str = "group.id";

pub trait ConfigBuilder {
    fn set_bootstrap_server(self, bootstrap_server: &str) -> Self;

    fn set_reset_from_beginning(self) -> Self;

    fn disable_auto_commit(self) -> Self;

    fn set_properties(self, properties: &HashMap<String, String>) -> Self;
}

impl ConfigBuilder for ClientConfig {
    fn set_bootstrap_server(mut self, bootstrap_server: &str) -> Self {
        self.set("bootstrap.servers", bootstrap_server);
        self
    }

    fn set_reset_from_beginning(mut self) -> Self {
        self.set("auto.offset.reset", "earliest");
        self
    }

    fn disable_auto_commit(mut self) -> Self {
        self.set("enable.auto.commit", "false");
        self
    }

    fn set_properties(mut self, properties: &HashMap<String, String>) -> Self {
        let mut group_id_present = false;
        for (key, value) in properties {
            if key == "group.id" {
                group_id_present = true;
            }
            self.set(key, value);
        }
        if !group_id_present {
            warn!("property {GROUP_ID_KEY} is not set. Setting it to \"{DEFAULT_GROUP_ID}\"");
            self.set(GROUP_ID_KEY, DEFAULT_GROUP_ID);
        }
        self
    }
}
