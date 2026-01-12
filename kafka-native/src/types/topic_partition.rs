//! Topic-partition types.

use crate::error::Error;

/// Offset position for a partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Offset {
    /// Beginning of the partition (earliest available offset).
    Beginning,
    /// End of the partition (latest offset).
    End,
    /// Specific offset value.
    Offset(i64),
    /// Stored offset (use committed offset).
    Stored,
    /// Invalid/unset offset.
    Invalid,
}

impl Offset {
    /// Convert to raw i64 value for Kafka protocol.
    pub fn to_raw(&self) -> i64 {
        match self {
            Offset::Beginning => -2,
            Offset::End => -1,
            Offset::Offset(o) => *o,
            Offset::Stored => -1000,
            Offset::Invalid => -1001,
        }
    }

    /// Create from raw i64 value from Kafka protocol.
    pub fn from_raw(value: i64) -> Self {
        match value {
            -2 => Offset::Beginning,
            -1 => Offset::End,
            -1000 => Offset::Stored,
            -1001 => Offset::Invalid,
            o if o >= 0 => Offset::Offset(o),
            _ => Offset::Invalid,
        }
    }
}

impl Default for Offset {
    fn default() -> Self {
        Offset::Invalid
    }
}

/// Commit mode for offset commits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitMode {
    /// Wait for the commit to complete.
    Sync,
    /// Don't wait for the commit to complete.
    Async,
}

/// An element in a TopicPartitionList.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TopicPartitionElement {
    /// Topic name.
    pub topic: String,
    /// Partition number.
    pub partition: i32,
    /// Offset for this partition.
    pub offset: Offset,
}

impl TopicPartitionElement {
    /// Create a new element with invalid offset.
    pub fn new(topic: impl Into<String>, partition: i32) -> Self {
        Self {
            topic: topic.into(),
            partition,
            offset: Offset::Invalid,
        }
    }

    /// Create a new element with a specific offset.
    pub fn with_offset(topic: impl Into<String>, partition: i32, offset: Offset) -> Self {
        Self {
            topic: topic.into(),
            partition,
            offset,
        }
    }
}

/// A list of topic-partitions with optional offsets.
///
/// This is similar to rdkafka's TopicPartitionList.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TopicPartitionList {
    elements: Vec<TopicPartitionElement>,
}

impl TopicPartitionList {
    /// Create a new empty list.
    pub fn new() -> Self {
        Self {
            elements: Vec::new(),
        }
    }

    /// Create a new list with the given capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            elements: Vec::with_capacity(capacity),
        }
    }

    /// Add a partition without an offset.
    pub fn add_partition(&mut self, topic: &str, partition: i32) {
        self.elements
            .push(TopicPartitionElement::new(topic, partition));
    }

    /// Add a partition with a specific offset.
    pub fn add_partition_offset(
        &mut self,
        topic: &str,
        partition: i32,
        offset: Offset,
    ) -> Result<(), Error> {
        self.elements
            .push(TopicPartitionElement::with_offset(topic, partition, offset));
        Ok(())
    }

    /// Get all elements.
    pub fn elements(&self) -> &[TopicPartitionElement] {
        &self.elements
    }

    /// Get mutable access to all elements.
    pub fn elements_mut(&mut self) -> &mut [TopicPartitionElement] {
        &mut self.elements
    }

    /// Get the number of elements.
    pub fn len(&self) -> usize {
        self.elements.len()
    }

    /// Check if the list is empty.
    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }

    /// Find an element by topic and partition.
    pub fn find(&self, topic: &str, partition: i32) -> Option<&TopicPartitionElement> {
        self.elements
            .iter()
            .find(|e| e.topic == topic && e.partition == partition)
    }

    /// Find a mutable element by topic and partition.
    pub fn find_mut(&mut self, topic: &str, partition: i32) -> Option<&mut TopicPartitionElement> {
        self.elements
            .iter_mut()
            .find(|e| e.topic == topic && e.partition == partition)
    }

    /// Set the offset for a specific topic-partition.
    /// Returns Ok(()) if the element was found and updated, Err if not found.
    pub fn set_offset(&mut self, topic: &str, partition: i32, offset: Offset) -> Result<(), Error> {
        if let Some(elem) = self.find_mut(topic, partition) {
            elem.offset = offset;
            Ok(())
        } else {
            Err(Error::PartitionNotFound {
                topic: topic.to_owned(),
                partition,
            })
        }
    }

    /// Iterate over elements.
    pub fn iter(&self) -> impl Iterator<Item = &TopicPartitionElement> {
        self.elements.iter()
    }
}

impl IntoIterator for TopicPartitionList {
    type Item = TopicPartitionElement;
    type IntoIter = std::vec::IntoIter<TopicPartitionElement>;

    fn into_iter(self) -> Self::IntoIter {
        self.elements.into_iter()
    }
}

impl<'a> IntoIterator for &'a TopicPartitionList {
    type Item = &'a TopicPartitionElement;
    type IntoIter = std::slice::Iter<'a, TopicPartitionElement>;

    fn into_iter(self) -> Self::IntoIter {
        self.elements.iter()
    }
}

impl FromIterator<TopicPartitionElement> for TopicPartitionList {
    fn from_iter<T: IntoIterator<Item = TopicPartitionElement>>(iter: T) -> Self {
        Self {
            elements: iter.into_iter().collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_offset_conversion() {
        assert_eq!(Offset::Beginning.to_raw(), -2);
        assert_eq!(Offset::End.to_raw(), -1);
        assert_eq!(Offset::Offset(100).to_raw(), 100);

        assert_eq!(Offset::from_raw(-2), Offset::Beginning);
        assert_eq!(Offset::from_raw(-1), Offset::End);
        assert_eq!(Offset::from_raw(100), Offset::Offset(100));
    }

    #[test]
    fn test_topic_partition_list() {
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition("topic1", 0);
        tpl.add_partition("topic1", 1);
        tpl.add_partition_offset("topic2", 0, Offset::Offset(100)).unwrap();

        assert_eq!(tpl.len(), 3);
        assert!(!tpl.is_empty());

        let elem = tpl.find("topic1", 0).unwrap();
        assert_eq!(elem.topic, "topic1");
        assert_eq!(elem.partition, 0);

        let elem = tpl.find("topic2", 0).unwrap();
        assert_eq!(elem.offset, Offset::Offset(100));
    }
}
