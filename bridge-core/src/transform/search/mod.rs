use std::{cmp::Ordering, collections::BTreeMap};

/// Module for transforming consumer group offsets using search strategies.
/// The available search strategies include:
/// - Binary Search: Efficiently locate offsets by performing a binary search on a partition by using seek operations.
use crate::prelude::*;

mod metrics;
mod window;
pub use metrics::*;
pub use window::*;

pub mod binary;

#[derive(Debug, Clone)]
pub enum OffsetSource {
    Header(String),
}

impl OffsetSource {
    pub fn from_header(key: impl Into<String>) -> Self {
        OffsetSource::Header(key.into())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ExtractOffsetError {
    #[error("Header not found: {0}")]
    HeaderNotFound(String),
    #[error("Invalid offset header value: {0}")]
    InvalidHeaderValue(String),
}

impl OffsetSource {
    fn extract<M: Message>(&self, message: &M) -> Result<i64, ExtractOffsetError> {
        match self {
            OffsetSource::Header(key) => {
                let vec = message.header(key).ok_or_else(|| {
                    ExtractOffsetError::HeaderNotFound(format!("Header '{}' not found", key))
                })?;

                let string = std::str::from_utf8(&vec).map_err(|e| {
                    ExtractOffsetError::InvalidHeaderValue(format!(
                        "Header '{}' is not valid UTF-8: {}",
                        key, e
                    ))
                })?;

                let offset = string.parse::<i64>().map_err(|e| {
                    ExtractOffsetError::InvalidHeaderValue(format!(
                        "Header '{}' value '{}' is not a valid i64: {}",
                        key, string, e
                    ))
                })?;

                Ok(offset)
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SearchError<E> {
    #[error("Seek error: {0}")]
    SeekError(E),
    #[error("Stream error: {0}")]
    StreamError(E),
    #[error("Extract offset error: {0}")]
    ExtractError(#[from] ExtractOffsetError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SearchResult {
    Found(OffsetMapping),
    NotFound(Offset),
}

/// Mapping from an old offset to a new offset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OffsetMapping {
    /// The old offset being searched for
    pub old_offset: i64,
    /// The new offset in the partition where this old offset maps to
    pub new_offset: i64,
}

impl PartialOrd for OffsetMapping {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OffsetMapping {
    fn cmp(&self, other: &Self) -> Ordering {
        self.old_offset.cmp(&other.old_offset)
    }
}

impl OffsetMapping {
    /// Creates a new OffsetMapping
    pub fn new(old_offset: i64, new_offset: i64) -> Self {
        OffsetMapping {
            old_offset,
            new_offset,
        }
    }
}

impl From<(Offset, Offset)> for OffsetMapping {
    fn from((old_offset, new_offset): (i64, i64)) -> Self {
        OffsetMapping {
            old_offset,
            new_offset,
        }
    }
}

// The results of a partition scan
#[derive(Debug, Default)]
pub struct PartitionScan {
    // search results, ordered by old_offset
    results: BTreeMap<Offset, SearchResult>,
    metrics: PartitionScanMetrics,
}

impl PartitionScan {
    pub fn new(
        results: impl IntoIterator<Item = SearchResult>,
        metrics: PartitionScanMetrics,
    ) -> Self {
        let mut results_map = BTreeMap::new();
        for result in results {
            match &result {
                SearchResult::Found(mapping) => {
                    results_map.insert(mapping.old_offset, result);
                }
                SearchResult::NotFound(old_offset) => {
                    results_map.insert(*old_offset, result);
                }
            }
        }
        Self {
            results: results_map,
            metrics,
        }
    }

    pub fn results_iter(&self) -> impl Iterator<Item = &SearchResult> {
        self.results.values()
    }

    pub fn get_result(&self, old_offset: Offset) -> Option<&SearchResult> {
        self.results.get(&old_offset)
    }

    pub fn metrics(&self) -> &PartitionScanMetrics {
        &self.metrics
    }
}
