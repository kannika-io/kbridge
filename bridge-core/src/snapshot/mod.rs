/// Module for managing snapshots of consumer group offsets.
/// An `OffsetSnapshot` contains multiple `OffsetRecord`s,
/// each representing the offset of a specific consumer group for a given topic and partition.
pub mod csv;

use std::{
    collections::HashSet,
    fmt::{self, Debug, Display, Formatter},
};

#[derive(Clone, Default, Debug)]
pub struct OffsetSnapshot {
    records: Vec<OffsetRecord>,
}

impl OffsetSnapshot {
    pub fn new() -> Self {
        OffsetSnapshot::default()
    }

    /// Returns an iterator over references to the records
    pub fn iter(&self) -> std::slice::Iter<'_, OffsetRecord> {
        self.records.iter()
    }

    /// Returns an iterator over mutable references to the records
    pub fn iter_mut(&mut self) -> std::slice::IterMut<'_, OffsetRecord> {
        self.records.iter_mut()
    }

    /// Returns the number of records
    pub fn size(&self) -> usize {
        self.records.len()
    }

    /// Returns true if there are no records
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    // Adds a new OffsetRecord to the snapshot
    // TODO: Prevent duplicates
    pub fn push(&mut self, record: OffsetRecord) {
        self.records.push(record);
    }

    pub fn contains(&self, record: &OffsetRecord) -> bool {
        self.records.contains(record)
    }

    #[cfg(test)]
    pub fn get_opt(
        &self,
        consumer_group: impl Into<String>,
        topic: impl Into<String>,
        partition: i32,
    ) -> Option<&OffsetRecord> {
        let consumer_group = consumer_group.into();
        let topic = topic.into();
        // TODO: Optimize lookup with a HashMap if performance becomes an issue
        self.records.iter().find(|record| {
            record.consumer_group == consumer_group
                && record.topic == topic
                && record.partition == partition
        })
    }

    /// Returns a set of unique topic names in the snapshot
    pub fn topics(&self) -> HashSet<&str> {
        self.records.iter().map(|r| r.topic.as_str()).collect()
    }

    /// Filters the snapshot based on a predicate function
    pub fn filter<F>(&self, predicate: F) -> OffsetSnapshot
    where
        F: Fn(&OffsetRecord) -> bool,
    {
        OffsetSnapshot {
            records: self
                .records
                .iter()
                .filter(|r| predicate(r))
                .cloned()
                .collect(),
        }
    }

    /// Filters the snapshot to include only records with topics in the provided list.
    /// If the list is empty, returns the original snapshot.
    pub fn filter_by_topic<S: AsRef<str>>(&self, topics: &[S]) -> OffsetSnapshot {
        if topics.is_empty() {
            return self.clone();
        }
        let topic_set: HashSet<&str> = topics.iter().map(|s| s.as_ref()).collect();
        self.filter(|record| topic_set.contains(record.topic.as_str()))
    }
}

impl IntoIterator for OffsetSnapshot {
    type Item = OffsetRecord;
    type IntoIter = std::vec::IntoIter<OffsetRecord>;

    fn into_iter(self) -> Self::IntoIter {
        self.records.into_iter()
    }
}

impl<'a> IntoIterator for &'a OffsetSnapshot {
    type Item = &'a OffsetRecord;
    type IntoIter = std::slice::Iter<'a, OffsetRecord>;

    fn into_iter(self) -> Self::IntoIter {
        self.records.iter()
    }
}

impl<'a> IntoIterator for &'a mut OffsetSnapshot {
    type Item = &'a mut OffsetRecord;
    type IntoIter = std::slice::IterMut<'a, OffsetRecord>;

    fn into_iter(self) -> Self::IntoIter {
        self.records.iter_mut()
    }
}

impl Display for OffsetSnapshot {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        for record in &self.records {
            writeln!(
                f,
                "{},{},{},{}",
                record.consumer_group, record.topic, record.partition, record.offset
            )?;
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, Clone, serde::Deserialize)]
pub struct OffsetRecord {
    pub consumer_group: String,
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

impl Display for OffsetRecord {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{},{},{},{}",
            self.consumer_group, self.topic, self.partition, self.offset
        )
    }
}

impl FromIterator<OffsetRecord> for OffsetSnapshot {
    fn from_iter<I: IntoIterator<Item = OffsetRecord>>(iter: I) -> Self {
        OffsetSnapshot {
            records: iter.into_iter().collect(),
        }
    }
}

impl From<Vec<OffsetRecord>> for OffsetSnapshot {
    fn from(records: Vec<OffsetRecord>) -> Self {
        OffsetSnapshot { records }
    }
}

#[cfg(test)]
impl std::str::FromStr for OffsetSnapshot {
    type Err = crate::snapshot::csv::CsvSnapshotError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use csv::FromCsv;
        OffsetSnapshot::from_csv(s.as_bytes())
    }
}
