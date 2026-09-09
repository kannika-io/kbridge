use std::{collections::BTreeMap, num::NonZeroUsize};

use futures::StreamExt;

use crate::{partition::*, prelude::*, transform::search::*};
// Shadows the client-level `OffsetSource` re-exported by the prelude.
use crate::transform::search::OffsetSource;

/// Trait for performing binary searches.
pub trait BinarySearch {
    type Output;
    type Error;

    /// Performs binary search to find offset mappings for the given old offsets
    ///
    /// Returns a Vec of OffsetMappings in the same order as the input offsets.
    /// If an offset is not found, it maps to the next available offset.
    async fn binary_search(
        &mut self,
        offsets: impl IntoIterator<Item = i64>,
        offset_source: OffsetSource,
        opts: BinarySearchOpts,
    ) -> Result<Self::Output, Self::Error>;
}

/// Configuration options for binary search
#[derive(Debug, Clone)]
pub struct BinarySearchOpts {
    // Optional window to limit the initial search range.
    // If None, the full offset range is used (0 to i64::MAX).
    // Preferably set this to narrow down the search space.
    pub(super) search_window: Option<Window>,

    /// Optimization: check from the end first if most offsets are recent.
    /// Number of messages to check from the tail.
    // pub(super) tail_sample_size: Option<usize>,

    /// Number of messages to scan sequentially after each binary search step.
    /// Default is 1, meaning no sequential scan but only single-message checks.
    pub(super) seq_scan_size: NonZeroUsize,
}

impl Default for BinarySearchOpts {
    fn default() -> Self {
        Self {
            search_window: None,
            seq_scan_size: NonZeroUsize::new(1000).expect("1 is 0 in another universe"),
            // tail_sample_size: None,
        }
    }
}

impl BinarySearchOpts {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_search_window(mut self, window: impl Into<Window>) -> Self {
        let window = window.into();
        self.search_window = Some(window);
        self
    }

    pub fn with_seq_scan_size(mut self, size: NonZeroUsize) -> Self {
        self.seq_scan_size = size;
        self
    }

    // pub fn with_tail_sample_size(mut self, size: usize) -> Self {
    //     self.tail_sample_size = Some(size);
    //     self
    // }
}

// Internal struct to manage the binary search process on a partition.
struct PartitionBinarySearchContext<'a, P, E>
where
    P: SeekablePartition,
{
    partition: &'a mut P,
    stream: P::Stream,
    work: SearchTaskQueue,
    results: Vec<OffsetMapping>,
    offset_source: OffsetSource,
    window: Window,
    _marker: std::marker::PhantomData<E>,
    cursor: Offset,
    metrics: PartitionScanMetrics,
}

struct Scan {
    window: Window,
    seen: Vec<OffsetMapping>,
    low_high: Option<(OffsetMapping, OffsetMapping)>,
}

impl Scan {
    fn new(window: Window) -> Self {
        Self {
            window,
            seen: vec![],
            low_high: None,
        }
    }

    fn register(&mut self, mapping: OffsetMapping) {
        self.seen.push(mapping);

        // Optimization: avoid calculating lowest/highest in each task update
        let low_high = self.low_high.get_or_insert((mapping, mapping));
        if mapping.old_offset < low_high.0.old_offset {
            low_high.0 = mapping;
        }
        if mapping.old_offset > low_high.1.old_offset {
            low_high.1 = mapping;
        }
    }

    // returns the lowest and the highest
    fn low_high(&self) -> Option<&(OffsetMapping, OffsetMapping)> {
        self.low_high.as_ref()
    }
}

impl FromIterator<OffsetMapping> for Scan {
    fn from_iter<T: IntoIterator<Item = OffsetMapping>>(iter: T) -> Self {
        let mut mappings = Vec::new();
        let mut min_offset = i64::MAX;
        let mut max_offset = i64::MIN;

        for mapping in iter {
            min_offset = min_offset.min(mapping.new_offset);
            max_offset = max_offset.max(mapping.new_offset);
            mappings.push(mapping);
        }

        let window = if mappings.is_empty() {
            Window::empty()
        } else {
            Window::from_range(min_offset..=max_offset)
        };

        let mut scan = Scan::new(window);
        for mapping in mappings {
            scan.register(mapping);
        }
        scan
    }
}

impl<P> BinarySearch for P
where
    P: SeekablePartition,
{
    type Output = PartitionScan;
    type Error = SearchError<P::Error>;

    async fn binary_search(
        &mut self,
        offsets: impl IntoIterator<Item = i64>,
        offset_source: OffsetSource,
        opts: BinarySearchOpts,
    ) -> Result<Self::Output, Self::Error> {
        let offsets = offsets.into_iter().collect::<Vec<i64>>();

        if offsets.is_empty() {
            return Ok(PartitionScan::default());
        }

        let stream = self.stream().await.map_err(SearchError::Stream)?;

        let window = opts.search_window.unwrap_or_default();
        let work = SearchTaskQueue::new(offsets, window);
        let results = Vec::with_capacity(work.len());
        let cursor = window.low;
        let partition = self;
        let metrics = PartitionScanMetrics::default();

        // Initialize the search context
        let mut ctx = PartitionBinarySearchContext {
            partition,
            stream,
            offset_source,
            work,
            results,
            window,
            cursor,
            metrics,
            _marker: std::marker::PhantomData::<P::Error>,
        };

        while !ctx.work.is_empty() && !ctx.window.is_empty() {
            let scan_window = ctx.window.slice(ctx.cursor, opts.seq_scan_size);
            let scan = ctx.seq_scan(scan_window).await?;
            ctx.check(scan);
            ctx.advance_cursor();
        }

        Ok(ctx.result())
    }
}

impl<'a, P, E> PartitionBinarySearchContext<'a, P, E>
where
    P: SeekablePartition,
{
    fn handle<M: Message>(&mut self, msg: &M) -> Result<OffsetMapping, SearchError<P::Error>> {
        self.metrics.messages += 1;

        let old_offset = self.offset_source.extract(msg)?;
        let new_offset = msg.offset();
        let mapping = OffsetMapping {
            old_offset,
            new_offset,
        };

        self.register(&mapping);

        Ok(mapping)
    }

    fn register(&mut self, mapping: &OffsetMapping) {
        if self.work.contains(&mapping.old_offset) {
            self.results.push(*mapping);
            self.work.remove(&mapping.old_offset);
        }
    }

    // Sequentially scans messages in the given window.
    // If work items are found during the scan, they are recorded in the results.
    // Returns the results of the scan with the recorded mappings.
    async fn seq_scan(&mut self, window: Window) -> Result<Scan, SearchError<P::Error>> {
        let mut scan = Scan::new(window);

        // Seek to the start of the scan window
        self.partition
            .seek_from_offset(scan.window.low)
            .await
            .map_err(SearchError::Seek)?;

        // Drain old messages from the previous seek
        self.drain_check().await?;

        // TODO add timeout with feature flag to avoid blocking forever
        // TODO track avg msg size?
        // TODO Should we allow messages where no offset can be extracted?
        while let Some(evt) = self.stream.next().await {
            let evt = evt.map_err(SearchError::Stream)?;

            let msg = evt
                .as_message()
                .expect("Only messages should be returned from the stream at this point.");

            let mapping = self.handle(msg)?;

            scan.register(mapping);

            if msg.offset().saturating_add(1) >= scan.window.high {
                break;
            }
        }

        Ok(scan)
    }

    // Drain old messages until we get a `Seeked` event.
    // Then check the drained messages with the current work items.
    async fn drain_check(&mut self) -> Result<(), SearchError<P::Error>> {
        use crate::partition::DrainUntilSeeked;
        let drained = self
            .stream
            .drain_until_seeked()
            .await
            .map_err(SearchError::Stream)?;

        let mappings = {
            let mut mappings = Vec::with_capacity(drained.len());
            for msg in drained.iter() {
                let mapping = self.handle(msg)?;
                mappings.push(mapping);
            }
            mappings
        };

        debug_assert!(
            mappings
                .windows(2)
                .all(|w| w[0].new_offset < w[1].new_offset && w[0].old_offset <= w[1].old_offset),
            "Drained messages should be in order"
        );

        self.check(Scan::from_iter(mappings));

        Ok(())
    }

    // Checks the current work items against the given scan results.
    // Updates the work queue and results accordingly.
    // Advances the cursor to the next target window midpoint.
    fn check(&mut self, scan: Scan) {
        let mappings = self.work.check(&scan);
        mappings.iter().for_each(|m| {
            self.register(m);
        });

        if let Some((seen_low, seen_high)) = scan.low_high() {
            debug_assert!(
                self.work.iter().all(|task| {
                    !(task.old_offset >= seen_low.old_offset
                        && task.old_offset <= seen_high.old_offset)
                }),
                "There should be no more work items after the scan"
            );
        }
    }

    fn advance_cursor(&mut self) {
        if let Some(target) = self.work.target() {
            self.cursor = target.window.midpoint();
        }
    }

    // Returns the final partition scan result.
    // Consumes the context.
    fn result(self) -> PartitionScan {
        let total = self.results.len() + self.work.len();
        let mut results = Vec::with_capacity(total);

        results.extend(self.results.into_iter().map(SearchResult::Found));

        results.extend(
            self.work
                .iter()
                .map(|task| SearchResult::NotFound(task.old_offset)),
        );

        PartitionScan::new(results, self.metrics)
    }
}

#[derive(Debug)]
struct SearchTaskQueue {
    // Work, sorted by old_offset
    tasks: BTreeMap<Offset, SearchTask>,
}

#[derive(Debug)]
struct SearchTask {
    old_offset: Offset,
    window: Window,
}

impl SearchTaskQueue {
    fn new(offsets: impl IntoIterator<Item = i64>, window: Window) -> Self {
        let tasks = BTreeMap::from_iter(
            offsets
                .into_iter()
                .map(|old_offset| (old_offset, SearchTask { old_offset, window })),
        );
        Self { tasks }
    }

    fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    // Returns the target task with the smallest old_offset
    fn target(&self) -> Option<&SearchTask> {
        self.tasks.first_key_value().map(|(_, v)| v)
    }

    fn iter(&self) -> impl Iterator<Item = &SearchTask> {
        self.tasks.values()
    }

    fn contains(&self, offset: &Offset) -> bool {
        self.tasks.contains_key(offset)
    }

    fn remove(&mut self, offset: &Offset) -> Option<SearchTask> {
        self.tasks.remove(offset)
    }

    fn len(&self) -> usize {
        self.tasks.len()
    }

    fn check(&mut self, scan: &Scan) -> Vec<OffsetMapping> {
        let mut mappings = vec![];
        for task in self.tasks.values_mut() {
            if let Some(mapping) = task.check(scan) {
                mappings.push(mapping);
            }
        }
        mappings
    }
}

impl SearchTask {
    fn check(&mut self, scan: &Scan) -> Option<OffsetMapping> {
        self.update_window(scan);
        self.check_mappings(scan)
    }

    fn check_mappings(&mut self, scan: &Scan) -> Option<OffsetMapping> {
        let (low, high) = scan.low_high()?;

        if self.old_offset >= low.old_offset && self.old_offset <= high.old_offset {
            // Task is in a gap
            // scan.seen
            //     .iter()
            //     .find(|m| m.old_offset >= self.old_offset)
            //     .map(|m| OffsetMapping {
            //         old_offset: self.old_offset,
            //         new_offset: (m.new_offset - 1).max(0),
            //     })
            // Binary search within a binary search!
            let idx = scan
                .seen
                .partition_point(|m| m.old_offset < self.old_offset);
            scan.seen.get(idx).map(|m| OffsetMapping {
                old_offset: self.old_offset,
                new_offset: (m.new_offset - 1).max(0),
            })
        } else if self.window.len() <= 1 {
            // Window has been narrowed down to a single offset
            Some(OffsetMapping {
                old_offset: self.old_offset,
                new_offset: self.window.low,
            })
        } else {
            None
        }
    }

    fn update_window(&mut self, scan: &Scan) {
        if let Some((low, high)) = scan.low_high() {
            // Our target is lower than the lowest offset seen.
            // Our target is below the scanned window
            if self.old_offset < low.old_offset {
                self.window.narrow_high(low.new_offset);

            // Our target is greater than the highest offset seen,
            // then our target must be above the scanned window
            } else if self.old_offset > high.old_offset {
                self.window.narrow_low(high.new_offset);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{kafka::partition::KafkaPartition, test::kafka::cluster::ContainerizedCluster};

    #[tokio::test]
    async fn test_kafka_partition_binary_search() -> anyhow::Result<()> {
        use crate::test::kafka::cluster::MockClusterExt;
        let broker = ContainerizedCluster::start().await?;

        let topic = "meeseeks";
        broker.create_topic(topic, 1).await?;
        broker
            .produce_with_headers(topic, 0, 1000, |i| {
                maplit::hashmap! {
                    "offset".to_owned() => (i * 2).to_string(),
                }
            })
            .await?;

        let (consumer, consumer_task) = broker.record_consumer("group").await?;
        tokio::task::spawn(consumer_task);

        let mut partition = KafkaPartition::open(consumer.clone(), topic, 0).await?;

        // Only even offsets exists because of the header factory above
        // Uneven offsets should map to the previous offset
        type OffsetArray = [i64; 8];
        let search_offsets: OffsetArray = [0, 100, 500, 999, 1000, 1500, 1999, 2500];
        let expected_offsets: OffsetArray = [0, 50, 250, 499, 500, 750, 999, 999];
        let expted_results: Vec<_> = expected_offsets.into_iter().zip(search_offsets).collect();

        let watermarks = broker.get_watermarks(topic, 0).await?;
        let opts = BinarySearchOpts::new()
            .with_search_window(watermarks)
            .with_seq_scan_size(NonZeroUsize::new(1000).unwrap());

        let scan = partition
            .binary_search(search_offsets, OffsetSource::from_header("offset"), opts)
            .await?;

        for result in scan.results_iter() {
            match &result {
                SearchResult::Found(mapping) => {
                    println!("{} -> {}", mapping.old_offset, mapping.new_offset);
                }
                SearchResult::NotFound(offset) => {
                    println!("Offset not found: {}", offset);
                }
            }
        }

        for (result, (_old_offset, _new_offset)) in scan.results_iter().zip(expted_results) {
            assert!(matches!(
                result,
                SearchResult::Found(OffsetMapping {
                    old_offset: _old_offset,
                    new_offset: _new_offset,
                })
            ));
        }

        Ok(())
    }
}
