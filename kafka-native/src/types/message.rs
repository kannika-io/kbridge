//! Message types for consumed records.

use bytes::Bytes;

/// Headers attached to a Kafka message.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Headers {
    headers: Vec<(String, Bytes)>,
}

impl Headers {
    /// Create empty headers.
    pub fn new() -> Self {
        Self {
            headers: Vec::new(),
        }
    }

    /// Create headers with the given capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            headers: Vec::with_capacity(capacity),
        }
    }

    /// Add a header.
    pub fn insert(&mut self, key: impl Into<String>, value: impl Into<Bytes>) {
        self.headers.push((key.into(), value.into()));
    }

    /// Get a header value by key.
    ///
    /// If there are multiple headers with the same key, returns the first one.
    pub fn get(&self, key: &str) -> Option<&[u8]> {
        self.headers
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_ref())
    }

    /// Get all values for a header key.
    pub fn get_all(&self, key: &str) -> Vec<&[u8]> {
        self.headers
            .iter()
            .filter(|(k, _)| k == key)
            .map(|(_, v)| v.as_ref())
            .collect()
    }

    /// Iterate over all headers.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &[u8])> {
        self.headers.iter().map(|(k, v)| (k.as_str(), v.as_ref()))
    }

    /// Get the number of headers.
    pub fn len(&self) -> usize {
        self.headers.len()
    }

    /// Check if there are no headers.
    pub fn is_empty(&self) -> bool {
        self.headers.is_empty()
    }
}

impl FromIterator<(String, Bytes)> for Headers {
    fn from_iter<T: IntoIterator<Item = (String, Bytes)>>(iter: T) -> Self {
        Self {
            headers: iter.into_iter().collect(),
        }
    }
}

/// A message consumed from Kafka.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    /// Topic the message was consumed from.
    pub topic: String,
    /// Partition the message was consumed from.
    pub partition: i32,
    /// Offset of the message in the partition.
    pub offset: i64,
    /// Timestamp of the message (milliseconds since epoch).
    pub timestamp: i64,
    /// Message key (optional).
    pub key: Option<Bytes>,
    /// Message payload (optional).
    pub payload: Option<Bytes>,
    /// Message headers.
    pub headers: Headers,
}

impl Message {
    /// Create a new message.
    pub fn new(
        topic: impl Into<String>,
        partition: i32,
        offset: i64,
        timestamp: i64,
        key: Option<Bytes>,
        payload: Option<Bytes>,
        headers: Headers,
    ) -> Self {
        Self {
            topic: topic.into(),
            partition,
            offset,
            timestamp,
            key,
            payload,
            headers,
        }
    }

    /// Get the message headers.
    pub fn headers(&self) -> &Headers {
        &self.headers
    }

    /// Get the message key as bytes.
    pub fn key(&self) -> Option<&[u8]> {
        self.key.as_ref().map(|b| b.as_ref())
    }

    /// Get the message payload as bytes.
    pub fn payload(&self) -> Option<&[u8]> {
        self.payload.as_ref().map(|b| b.as_ref())
    }

    /// Get the message key as a string (lossy UTF-8 conversion).
    pub fn key_str(&self) -> Option<std::borrow::Cow<'_, str>> {
        self.key
            .as_ref()
            .map(|b| String::from_utf8_lossy(b.as_ref()))
    }

    /// Get the message payload as a string (lossy UTF-8 conversion).
    pub fn payload_str(&self) -> Option<std::borrow::Cow<'_, str>> {
        self.payload
            .as_ref()
            .map(|b| String::from_utf8_lossy(b.as_ref()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_headers() {
        let mut headers = Headers::new();
        headers.insert("key1", Bytes::from("value1"));
        headers.insert("key2", Bytes::from("value2"));
        headers.insert("key1", Bytes::from("value1b"));

        assert_eq!(headers.get("key1"), Some(b"value1".as_slice()));
        assert_eq!(headers.get("key2"), Some(b"value2".as_slice()));
        assert_eq!(headers.get("nonexistent"), None);

        let all_key1 = headers.get_all("key1");
        assert_eq!(all_key1.len(), 2);

        assert_eq!(headers.len(), 3);
    }

    #[test]
    fn test_message() {
        let msg = Message::new(
            "test-topic",
            0,
            100,
            1234567890,
            Some(Bytes::from("key")),
            Some(Bytes::from("payload")),
            Headers::new(),
        );

        assert_eq!(msg.topic, "test-topic");
        assert_eq!(msg.partition, 0);
        assert_eq!(msg.offset, 100);
        assert_eq!(msg.key_str(), Some(std::borrow::Cow::Borrowed("key")));
        assert_eq!(msg.payload_str(), Some(std::borrow::Cow::Borrowed("payload")));
    }
}
