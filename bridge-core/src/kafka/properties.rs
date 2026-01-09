use std::collections::BTreeMap;

use rdkafka::{
    admin::AdminClient,
    client::DefaultClientContext,
    config::{ClientConfig as KafkaClientConfig, RDKafkaLogLevel},
    consumer::{BaseConsumer, ConsumerContext, DefaultConsumerContext, StreamConsumer},
    error::KafkaResult,
    producer::{
        BaseProducer, DefaultProducerContext, FutureProducer, ProducerContext, ThreadedProducer,
    },
};

/// Default value for the "queue.buffering.max.kbytes" rdkafka producer setting.
const DEFAULT_QUEUE_BUFFERING_MAX_KBYTES: usize = 131072; // 128 MB

/// Default value for the 'fetch.queue.backoff.ms' rdkafka consumer setting.
const DEFAULT_FETCH_QUEUE_BACKOFF_MS: u32 = 100;

/// Default value for the 'queued.min.message' rdkafka consumer setting.
const DEFAULT_QUEUED_MIN_MESSAGES: u32 = 1_000_000;

/// Default value for the 'queued.max.messages.kbytes' rdkafka consumer setting.
const DEFAULT_QUEUED_MAX_MESSAGES_KBYTES: u32 = 262_144; // 256 MB

/// Default value for the 'fetch.message.max.bytes' rdkafka consumer setting.
const DEFAULT_FETCH_MESSAGE_MAX_BYTES: u32 = 4_194_304; // 4 MB

/// Kafka properties
#[derive(Clone, Hash, Eq, PartialEq)]
struct KafkaProperties {
    props: BTreeMap<String, String>,
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct KafkaConsumerProperties {
    inner: KafkaProperties,
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct KafkaProducerProperties {
    inner: KafkaProperties,
}

impl std::fmt::Debug for KafkaProperties {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let masked_props = maplit::btreeset! {
            "ssl.key.password",
            "ssl.key.pem",
            "ssl_key",
            "ssl.ca.pem",
            "ssl_ca",
            "ssl.keystore.password",
            "sasl.password",
            "sasl.username",
            "sasl.oauthbearer.client.secret",
        };

        let mut fmt = f.debug_struct("KafkaProperties");

        for (key, val) in &self.props {
            if masked_props.contains(key.as_str()) {
                fmt.field(key, &"REDACTED");
            } else {
                fmt.field(key, val);
            }
        }

        fmt.finish()
    }
}

impl std::fmt::Debug for KafkaConsumerProperties {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.inner)
    }
}

impl std::fmt::Debug for KafkaProducerProperties {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.inner)
    }
}

impl FromIterator<(String, String)> for KafkaConsumerProperties {
    fn from_iter<T: IntoIterator<Item = (String, String)>>(iter: T) -> Self {
        let mut props: BTreeMap<_, _> = iter.into_iter().collect();

        if !props.contains_key("group.id") {
            props.insert("group.id".to_owned(), "kbridge".to_owned());
        }

        if !props.contains_key("client.id") {
            let id = ulid::Ulid::new();
            props.insert("client.id".to_owned(), format!("kbridge-{id}"));
        }

        if !props.contains_key("fetch.queue.backoff.ms") {
            props.insert(
                "fetch.queue.backoff.ms".to_string(),
                DEFAULT_FETCH_QUEUE_BACKOFF_MS.to_string(),
            );
        }

        if !props.contains_key("queued.min.messages") {
            props.insert(
                "queued.min.messages".to_string(),
                DEFAULT_QUEUED_MIN_MESSAGES.to_string(),
            );
        }

        if !props.contains_key("queued.max.messages.kbytes") {
            props.insert(
                "queued.max.messages.kbytes".to_string(),
                DEFAULT_QUEUED_MAX_MESSAGES_KBYTES.to_string(),
            );
        }

        if !props.contains_key("fetch.message.max.bytes") {
            props.insert(
                "fetch.message.max.bytes".to_string(),
                DEFAULT_FETCH_MESSAGE_MAX_BYTES.to_string(),
            );
        }

        if !props.contains_key("enable.auto.commit") {
            props.insert("enable.auto.commit".to_string(), "false".to_string());
        }

        // When we request offsets that don't exist, seek to the first available offset.
        props.insert("auto.offset.reset".to_string(), "smallest".to_string());

        Self {
            inner: KafkaProperties { props },
        }
    }
}

impl FromIterator<(String, String)> for KafkaProducerProperties {
    fn from_iter<T: IntoIterator<Item = (String, String)>>(iter: T) -> Self {
        let mut props: BTreeMap<_, _> = iter.into_iter().collect();

        props.remove("group.id");

        if !props.contains_key("acks") {
            props.insert("acks".to_string(), "all".to_string());
        }

        if !props.contains_key("allow.auto.create.topics") {
            props.insert("allow.auto.create.topics".to_string(), "false".to_string());
        }

        if !props.contains_key("queue.buffering.max.kbytes") {
            props.insert(
                "queue.buffering.max.kbytes".to_string(),
                DEFAULT_QUEUE_BUFFERING_MAX_KBYTES.to_string(),
            );
        }

        Self {
            inner: KafkaProperties { props },
        }
    }
}

impl KafkaProducerProperties {
    pub fn insert(&mut self, key: impl Into<String>, val: impl Into<String>) -> bool {
        let key = key.into();

        if key == "group.id" {
            tracing::warn!("Refusing to set the `group.id` property.");
            false
        } else {
            if self.inner.props.contains_key(&key) {
                tracing::warn!("Overriding previous value for {key}");
            }
            self.inner.props.insert(key, val.into());
            true
        }
    }

    pub fn insert_if_not_set(&mut self, key: impl Into<String>, val: impl Into<String>) -> bool {
        let key = key.into();

        #[allow(clippy::map_entry)]
        if self.inner.props.contains_key(&key) {
            false
        } else if key == "group.id" {
            tracing::warn!("Refusing to set the `group.id` property.");
            false
        } else {
            self.inner.props.insert(key, val.into());
            true
        }
    }

    pub fn into_admin_client(self) -> KafkaResult<AdminClient<DefaultClientContext>> {
        let mut config = KafkaClientConfig::from_iter(self.inner.props);
        config.set_log_level(RDKafkaLogLevel::Info);
        config.create()
    }

    pub fn into_base_producer(self) -> KafkaResult<BaseProducer<DefaultProducerContext>> {
        let mut config = KafkaClientConfig::from_iter(self.inner.props);
        config.set_log_level(RDKafkaLogLevel::Info);
        config.create()
    }

    pub fn into_base_producer_with_context<C: ProducerContext>(
        self,
        context: C,
    ) -> KafkaResult<BaseProducer<C>> {
        let mut config = KafkaClientConfig::from_iter(self.inner.props);
        config.set_log_level(RDKafkaLogLevel::Info);
        config.create_with_context(context)
    }

    pub fn into_threaded_producer(self) -> KafkaResult<ThreadedProducer<DefaultProducerContext>> {
        let mut config = KafkaClientConfig::from_iter(self.inner.props);
        config.set_log_level(RDKafkaLogLevel::Info);
        config.create()
    }

    pub fn into_threaded_producer_with_context<C: ProducerContext>(
        self,
        context: C,
    ) -> KafkaResult<ThreadedProducer<C>> {
        let mut config = KafkaClientConfig::from_iter(self.inner.props);
        config.set_log_level(RDKafkaLogLevel::Info);
        config.create_with_context(context)
    }

    pub fn into_future_producer(self) -> KafkaResult<FutureProducer<DefaultClientContext>> {
        let mut config = KafkaClientConfig::from_iter(self.inner.props);
        config.set_log_level(RDKafkaLogLevel::Info);
        config.create()
    }

    pub fn into_future_producer_with_context<C: ProducerContext>(
        self,
        context: C,
    ) -> KafkaResult<FutureProducer<C>> {
        let mut config = KafkaClientConfig::from_iter(self.inner.props);
        config.set_log_level(RDKafkaLogLevel::Info);
        config.create_with_context(context)
    }
}

impl KafkaConsumerProperties {
    pub fn insert(&mut self, key: impl Into<String>, val: impl Into<String>) -> bool {
        let key = key.into();

        if self.inner.props.contains_key(&key) {
            tracing::warn!("Overriding previous value for {key}");
        }
        self.inner.props.insert(key, val.into());
        true
    }

    pub fn into_admin_client(self) -> KafkaResult<AdminClient<DefaultClientContext>> {
        let mut config = KafkaClientConfig::from_iter(self.inner.props);
        config.set_log_level(RDKafkaLogLevel::Info);
        config.create()
    }

    pub fn into_base_consumer(self) -> KafkaResult<BaseConsumer<DefaultConsumerContext>> {
        let mut config = KafkaClientConfig::from_iter(self.inner.props);
        config.set_log_level(RDKafkaLogLevel::Info);
        config.create()
    }

    pub fn into_stream_consumer(self) -> KafkaResult<StreamConsumer<DefaultConsumerContext>> {
        let mut config = KafkaClientConfig::from_iter(self.inner.props);
        config.set_log_level(RDKafkaLogLevel::Info);
        config.create()
    }

    pub fn into_stream_consumer_with_context<C: ConsumerContext + 'static>(
        self,
        context: C,
    ) -> KafkaResult<StreamConsumer<C>> {
        let mut config = KafkaClientConfig::from_iter(self.inner.props);
        config.set_log_level(RDKafkaLogLevel::Info);
        config.create_with_context(context)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sensitive_property_is_redacted() {
        use std::fmt::Write;

        let props = maplit::btreemap! {
            "bootstrap.servers".to_owned() => "somewhere".to_owned(),
            "sasl.username".to_owned() => "The Dude".to_owned(),
            "sasl.password".to_owned() => "The rug really tied the room together.".to_owned(),
        };

        let props = KafkaConsumerProperties::from_iter(props.clone());

        let mut debug_str = String::new();
        write!(&mut debug_str, "{props:?}").unwrap();

        dbg!(&debug_str);
        assert!(debug_str.contains(r#"bootstrap.servers: "somewhere""#));
        assert!(debug_str.contains(r#"sasl.username: "REDACTED""#));
        assert!(debug_str.contains(r#"sasl.password: "REDACTED""#));
    }
}
