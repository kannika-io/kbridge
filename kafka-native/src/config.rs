//! Configuration types for Kafka clients.

use std::collections::BTreeMap;

use crate::error::Error;
use crate::{AdminClient, Consumer, Producer};

/// Sensitive properties that should be redacted in debug output.
const SENSITIVE_PROPERTIES: &[&str] = &[
    "ssl.key.password",
    "ssl.key.pem",
    "ssl.ca.pem",
    "ssl.keystore.password",
    "sasl.password",
    "sasl.username",
    "sasl.oauthbearer.client.secret",
];

/// Consumer-specific properties.
///
/// # Example
///
/// ```
/// use kafka_native::ConsumerProperties;
///
/// let mut props = ConsumerProperties::new();
/// props.set("bootstrap.servers", "localhost:9092");
/// props.set("group.id", "my-consumer-group");
/// ```
#[derive(Clone, PartialEq, Eq)]
pub struct ConsumerProperties {
    inner: BTreeMap<String, String>,
}

impl ConsumerProperties {
    /// Create a new empty ConsumerProperties.
    pub fn new() -> Self {
        Self {
            inner: BTreeMap::new(),
        }
    }

    /// Set a configuration property.
    pub fn set(&mut self, key: impl Into<String>, value: impl Into<String>) -> &mut Self {
        self.inner.insert(key.into(), value.into());
        self
    }

    /// Get a configuration property value.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.inner.get(key).map(|s| s.as_str())
    }

    /// Get bootstrap servers.
    pub fn bootstrap_servers(&self) -> Option<&str> {
        self.get("bootstrap.servers")
    }

    /// Get group ID.
    pub fn group_id(&self) -> Option<&str> {
        self.get("group.id")
    }

    /// Get client ID.
    pub fn client_id(&self) -> Option<&str> {
        self.get("client.id")
    }

    /// Create a Consumer from these properties.
    pub fn into_consumer(self) -> Result<Consumer, Error> {
        Consumer::new(self)
    }

    /// Create an AdminClient from these properties.
    pub fn into_admin_client(self) -> Result<AdminClient, Error> {
        AdminClient::new(self.into())
    }

    /// Apply default values for missing properties.
    fn apply_defaults(&mut self) {
        // Generate default group.id if not set
        if !self.inner.contains_key("group.id") {
            self.inner
                .insert("group.id".to_owned(), "kafka-native".to_owned());
        }

        // Generate default client.id if not set
        if !self.inner.contains_key("client.id") {
            let id = ulid::Ulid::new();
            self.inner
                .insert("client.id".to_owned(), format!("kafka-native-{id}"));
        }

        // Disable auto-commit by default
        if !self.inner.contains_key("enable.auto.commit") {
            self.inner
                .insert("enable.auto.commit".to_owned(), "false".to_owned());
        }

        // Default offset reset behavior
        if !self.inner.contains_key("auto.offset.reset") {
            self.inner
                .insert("auto.offset.reset".to_owned(), "smallest".to_owned());
        }

        // Default request timeout
        if !self.inner.contains_key("request.timeout.ms") {
            self.inner
                .insert("request.timeout.ms".to_owned(), "30000".to_owned());
        }
    }

    /// Get all properties as a reference.
    pub(crate) fn properties(&self) -> &BTreeMap<String, String> {
        &self.inner
    }
}

impl Default for ConsumerProperties {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for ConsumerProperties {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut fmt = f.debug_struct("ConsumerProperties");

        for (key, val) in &self.inner {
            if SENSITIVE_PROPERTIES.contains(&key.as_str()) {
                fmt.field(key, &"REDACTED");
            } else {
                fmt.field(key, val);
            }
        }

        fmt.finish()
    }
}

impl FromIterator<(String, String)> for ConsumerProperties {
    fn from_iter<T: IntoIterator<Item = (String, String)>>(iter: T) -> Self {
        let mut props = Self {
            inner: iter.into_iter().collect(),
        };
        props.apply_defaults();
        props
    }
}

/// Producer-specific properties.
///
/// # Example
///
/// ```
/// use kafka_native::ProducerProperties;
///
/// let mut props = ProducerProperties::new();
/// props.set("bootstrap.servers", "localhost:9092");
/// ```
#[derive(Clone, PartialEq, Eq)]
pub struct ProducerProperties {
    inner: BTreeMap<String, String>,
}

impl ProducerProperties {
    /// Create a new empty ProducerProperties.
    pub fn new() -> Self {
        Self {
            inner: BTreeMap::new(),
        }
    }

    /// Set a configuration property.
    ///
    /// Note: Setting `group.id` will be ignored as it's not applicable to producers.
    pub fn set(&mut self, key: impl Into<String>, value: impl Into<String>) -> &mut Self {
        let key = key.into();
        if key == "group.id" {
            tracing::warn!("Refusing to set `group.id` property on producer");
            return self;
        }
        self.inner.insert(key, value.into());
        self
    }

    /// Get a configuration property value.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.inner.get(key).map(|s| s.as_str())
    }

    /// Get bootstrap servers.
    pub fn bootstrap_servers(&self) -> Option<&str> {
        self.get("bootstrap.servers")
    }

    /// Get client ID.
    pub fn client_id(&self) -> Option<&str> {
        self.get("client.id")
    }

    /// Create a Producer from these properties.
    pub fn into_producer(self) -> Result<Producer, Error> {
        Producer::new(self)
    }

    /// Create an AdminClient from these properties.
    pub fn into_admin_client(self) -> Result<AdminClient, Error> {
        AdminClient::new(self.into())
    }

    /// Apply default values for missing properties.
    fn apply_defaults(&mut self) {
        // Remove group.id if present (not applicable to producers)
        self.inner.remove("group.id");

        // Generate default client.id if not set
        if !self.inner.contains_key("client.id") {
            let id = ulid::Ulid::new();
            self.inner
                .insert("client.id".to_owned(), format!("kafka-native-{id}"));
        }

        // Default acks
        if !self.inner.contains_key("acks") {
            self.inner.insert("acks".to_owned(), "all".to_owned());
        }

        // Default request timeout
        if !self.inner.contains_key("request.timeout.ms") {
            self.inner
                .insert("request.timeout.ms".to_owned(), "30000".to_owned());
        }
    }

    /// Get all properties as a reference.
    pub(crate) fn properties(&self) -> &BTreeMap<String, String> {
        &self.inner
    }
}

impl Default for ProducerProperties {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for ProducerProperties {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut fmt = f.debug_struct("ProducerProperties");

        for (key, val) in &self.inner {
            if SENSITIVE_PROPERTIES.contains(&key.as_str()) {
                fmt.field(key, &"REDACTED");
            } else {
                fmt.field(key, val);
            }
        }

        fmt.finish()
    }
}

impl FromIterator<(String, String)> for ProducerProperties {
    fn from_iter<T: IntoIterator<Item = (String, String)>>(iter: T) -> Self {
        let mut props = Self {
            inner: iter.into_iter().collect(),
        };
        props.apply_defaults();
        props
    }
}

/// Common properties shared between consumer and producer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CommonProperties {
    pub(crate) inner: BTreeMap<String, String>,
}

impl From<ConsumerProperties> for CommonProperties {
    fn from(props: ConsumerProperties) -> Self {
        Self { inner: props.inner }
    }
}

impl From<ProducerProperties> for CommonProperties {
    fn from(props: ProducerProperties) -> Self {
        Self { inner: props.inner }
    }
}

impl CommonProperties {
    pub(crate) fn bootstrap_servers(&self) -> Option<&str> {
        self.inner.get("bootstrap.servers").map(|s| s.as_str())
    }

    pub(crate) fn client_id(&self) -> Option<&str> {
        self.inner.get("client.id").map(|s| s.as_str())
    }

    pub(crate) fn request_timeout_ms(&self) -> u64 {
        self.inner
            .get("request.timeout.ms")
            .and_then(|s| s.parse().ok())
            .unwrap_or(30000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_consumer_properties_defaults() {
        let props: ConsumerProperties = [("bootstrap.servers".to_owned(), "localhost:9092".to_owned())]
            .into_iter()
            .collect();

        assert!(props.get("group.id").is_some());
        assert!(props.get("client.id").is_some());
        assert_eq!(props.get("enable.auto.commit"), Some("false"));
        assert_eq!(props.get("auto.offset.reset"), Some("smallest"));
    }

    #[test]
    fn test_producer_properties_no_group_id() {
        let props: ProducerProperties = [
            ("bootstrap.servers".to_owned(), "localhost:9092".to_owned()),
            ("group.id".to_owned(), "should-be-removed".to_owned()),
        ]
        .into_iter()
        .collect();

        assert!(props.get("group.id").is_none());
    }

    #[test]
    fn test_sensitive_properties_redacted() {
        let mut props = ConsumerProperties::new();
        props.set("bootstrap.servers", "localhost:9092");
        props.set("sasl.password", "secret123");

        let debug_str = format!("{:?}", props);
        assert!(debug_str.contains("localhost:9092"));
        assert!(debug_str.contains("REDACTED"));
        assert!(!debug_str.contains("secret123"));
    }
}
