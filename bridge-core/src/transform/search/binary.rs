use std::{collections::BTreeMap, num::NonZeroUsize};

use futures::StreamExt;

use crate::{partition::*, prelude::*, transform::search::*};

/// Trait for performing binary searches.
pub trait BinarySearch {
    type Output;
    type Error;

    /// Performs binary search to find offset mappings for the given old offsets
    ///
    /// Returns a Vec of OffsetMappings in the same order as the input offsets.
    /// If an offset is not found, it maps to the next available offset.
    ///
    /// `progress` is called once before the first seek and after every seek.
    async fn binary_search(
        &mut self,
        offsets: impl IntoIterator<Item = i64>,
        offset_source: OffsetSource,
        opts: BinarySearchOpts,
        progress: impl FnMut(SearchProgress),
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
    // Number of distinct offsets searched for
    offsets: usize,
    // Halvings needed to narrow every offset's window down to one position
    initial_halvings: f64,
    observations: Observations,
}

/// Halvings needed to narrow `window` down to a single position. Computed
/// from the bounds rather than `Window::len`, which wraps if `low > high`.
fn halvings(window: &Window) -> f64 {
    (window.high.saturating_sub(window.low).max(1) as f64).log2()
}

struct Scan {
    window: Window,
    // Messages in the order the consumer delivered them
    seen: Vec<OffsetMapping>,
}

impl Scan {
    fn new(window: Window) -> Self {
        Self {
            window,
            seen: vec![],
        }
    }

    // Registers the next message of the scan. A scan reads consecutive
    // messages, so the partition offset must increase and the offset in the
    // header must not decrease.
    fn register<E>(&mut self, mapping: OffsetMapping) -> Result<(), SearchError<E>> {
        if let Some(&last) = self.seen.last() {
            OutOfOrderPartitionOffsets::check(last, mapping)?;
            NonIncreasingOffsets::check(last, mapping)?;
        }
        self.seen.push(mapping);
        Ok(())
    }

    // Returns the messages with the lowest and the highest header offset,
    // which `register` guarantees are the first and the last
    fn low_high(&self) -> Option<(OffsetMapping, OffsetMapping)> {
        Some((*self.seen.first()?, *self.seen.last()?))
    }

    // Builds a scan from consecutive messages
    fn try_from_mappings<E>(
        mappings: impl IntoIterator<Item = OffsetMapping>,
    ) -> Result<Self, SearchError<E>> {
        let mut scan = Scan::new(Window::empty());
        for mapping in mappings {
            scan.register(mapping)?;
        }
        if let Some((first, last)) = scan.low_high() {
            scan.window = Window::from_range(first.new_offset..=last.new_offset);
        }
        Ok(scan)
    }
}

/// Header offsets at both ends of every scan, keyed by partition offset.
/// Each scan is checked internally, so checking its ends against each other
/// covers every message seen. Holds two entries per scan, however large the
/// partition.
#[derive(Debug, Default)]
struct Observations(BTreeMap<Offset, Offset>);

impl Observations {
    fn observe(&mut self, scan: &Scan) -> Result<(), NonIncreasingOffsets> {
        if let Some((first, last)) = scan.low_high() {
            self.insert(first)?;
            self.insert(last)?;
        }
        Ok(())
    }

    fn insert(&mut self, mapping: OffsetMapping) -> Result<(), NonIncreasingOffsets> {
        use std::ops::Bound::{Excluded, Unbounded};

        let at = mapping.new_offset;
        if let Some((&new_offset, &old_offset)) = self.0.range(..at).next_back() {
            NonIncreasingOffsets::check((old_offset, new_offset).into(), mapping)?;
        }
        if let Some((&new_offset, &old_offset)) = self.0.range((Excluded(at), Unbounded)).next() {
            NonIncreasingOffsets::check(mapping, (old_offset, new_offset).into())?;
        }
        self.0.insert(at, mapping.old_offset);
        Ok(())
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
        mut progress: impl FnMut(SearchProgress),
    ) -> Result<Self::Output, Self::Error> {
        let offsets = offsets.into_iter().collect::<Vec<i64>>();

        if offsets.is_empty() {
            return Ok(PartitionScan::default());
        }

        let stream = self.stream().await.map_err(SearchError::Stream)?;

        let window = opts.search_window.unwrap_or_default();
        let work = SearchTaskQueue::new(offsets, window);
        let results = Vec::with_capacity(work.len());
        let offsets = work.len();
        let initial_halvings = work.remaining_halvings();
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
            offsets,
            initial_halvings,
            observations: Observations::default(),
            _marker: std::marker::PhantomData::<P::Error>,
        };

        progress(ctx.progress());
        while !ctx.work.is_empty() && !ctx.window.is_empty() {
            let scan_window = ctx.window.slice(ctx.cursor, opts.seq_scan_size);
            let scan = ctx.seq_scan(scan_window).await?;
            ctx.check(scan)?;
            ctx.advance_cursor();
            progress(ctx.progress());
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
        self.metrics.seeks += 1;

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

            scan.register(mapping)?;

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

        self.check(Scan::try_from_mappings(mappings)?)
    }

    // Checks the current work items against the given scan results.
    // Updates the work queue and results accordingly.
    // Fails if the scan contradicts the messages seen before, as the search
    // relies on the offset in the header increasing with the partition offset.
    fn check(&mut self, scan: Scan) -> Result<(), SearchError<P::Error>> {
        self.observations.observe(&scan)?;

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

        Ok(())
    }

    fn advance_cursor(&mut self) {
        if let Some(target) = self.work.target() {
            self.cursor = target.window.midpoint();
        }
    }

    fn progress(&self) -> SearchProgress {
        let narrowed = if self.initial_halvings > 0.0 {
            1.0 - self.work.remaining_halvings() / self.initial_halvings
        } else {
            1.0
        };
        SearchProgress {
            mapped: self.offsets - self.work.len(),
            offsets: self.offsets,
            seeks: self.metrics.seeks,
            window: self
                .work
                .target()
                .map_or(Window::empty(), |task| task.window),
            narrowed: narrowed.clamp(0.0, 1.0),
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
    // Sum of `halvings` over the tasks' windows, kept up to date as windows
    // narrow and tasks are removed, so progress doesn't walk every task
    remaining_halvings: f64,
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
        let remaining_halvings = tasks.len() as f64 * halvings(&window);
        Self {
            tasks,
            remaining_halvings,
        }
    }

    // Halvings left before every task's window is narrowed to one position
    fn remaining_halvings(&self) -> f64 {
        if self.tasks.is_empty() {
            // Avoid reporting rounding residue once all work is done
            0.0
        } else {
            self.remaining_halvings.max(0.0)
        }
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
        let task = self.tasks.remove(offset)?;
        self.remaining_halvings -= halvings(&task.window);
        Some(task)
    }

    fn len(&self) -> usize {
        self.tasks.len()
    }

    fn check(&mut self, scan: &Scan) -> Vec<OffsetMapping> {
        let mut mappings = vec![];
        for task in self.tasks.values_mut() {
            let window = task.window;
            let mapping = task.check(scan);
            if task.window != window {
                self.remaining_halvings -= halvings(&window) - halvings(&task.window);
            }
            if let Some(mapping) = mapping {
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

    use std::{
        collections::HashMap,
        convert::Infallible,
        sync::{Arc, Mutex},
        task::Poll,
    };

    use futures::stream::BoxStream;

    use crate::{kafka::partition::KafkaPartition, test::kafka::cluster::ContainerizedCluster};

    #[test]
    fn halvings_counts_bits_and_never_wraps() {
        assert_eq!(halvings(&Window::new(0, 1)), 0.0);
        assert_eq!(halvings(&Window::new(0, 1024)), 10.0);
        assert_eq!(halvings(&Window::empty()), 0.0);
        // An inverted window, which `Window::len` would wrap to a huge length
        assert_eq!(halvings(&Window { low: 10, high: 5 }), 0.0);
    }

    /// In-memory partition whose messages carry the given legacy offsets in
    /// the `offset` header, at partition offsets `0..legacy.len()`.
    struct FakePartition {
        legacy: Arc<Vec<i64>>,
        cursor: Arc<Mutex<FakeCursor>>,
    }

    #[derive(Default)]
    struct FakeCursor {
        position: usize,
        seeked: bool,
    }

    struct FakeMessage {
        offset: i64,
        legacy: i64,
    }

    impl FakePartition {
        fn new(legacy: impl IntoIterator<Item = i64>) -> Self {
            Self {
                legacy: Arc::new(legacy.into_iter().collect()),
                cursor: Arc::default(),
            }
        }

        fn watermarks(&self) -> Window {
            Window::new(0, self.legacy.len() as i64)
        }
    }

    impl Message for FakeMessage {
        fn partition(&self) -> i32 {
            0
        }

        fn offset(&self) -> i64 {
            self.offset
        }

        fn timestamp(&self) -> i64 {
            0
        }

        fn headers(&self) -> HashMap<String, Vec<u8>> {
            HashMap::from_iter(self.header("offset").map(|v| ("offset".to_owned(), v)))
        }

        fn header(&self, key: impl Into<String>) -> Option<Vec<u8>> {
            (key.into() == "offset").then(|| self.legacy.to_string().into_bytes())
        }
    }

    impl Partition for FakePartition {
        type Error = Infallible;
        type Message = FakeMessage;
        type Stream = BoxStream<'static, Result<PartitionEvent<FakeMessage>, Infallible>>;

        async fn stream(&mut self) -> Result<Self::Stream, Self::Error> {
            let legacy = self.legacy.clone();
            let cursor = self.cursor.clone();
            let stream = futures::stream::poll_fn(move |_| {
                let mut cursor = cursor.lock().unwrap();
                if std::mem::take(&mut cursor.seeked) {
                    return Poll::Ready(Some(Ok(PartitionEvent::Seeked)));
                }
                let Some(&legacy) = legacy.get(cursor.position) else {
                    return Poll::Ready(None);
                };
                let offset = cursor.position as i64;
                cursor.position += 1;
                Poll::Ready(Some(Ok(PartitionEvent::Message(FakeMessage {
                    offset,
                    legacy,
                }))))
            });
            Ok(stream.boxed())
        }
    }

    impl SeekablePartition for FakePartition {
        async fn seek_from_offset(&mut self, offset: Offset) -> Result<(), Self::Error> {
            let mut cursor = self.cursor.lock().unwrap();
            cursor.position = offset as usize;
            cursor.seeked = true;
            Ok(())
        }
    }

    async fn search(
        legacy: impl IntoIterator<Item = i64>,
        offsets: impl IntoIterator<Item = i64>,
        seq_scan_size: usize,
    ) -> Result<PartitionScan, SearchError<Infallible>> {
        let mut partition = FakePartition::new(legacy);
        let opts = BinarySearchOpts::new()
            .with_search_window(partition.watermarks())
            .with_seq_scan_size(NonZeroUsize::new(seq_scan_size).unwrap());
        partition
            .binary_search(offsets, OffsetSource::from_header("offset"), opts, |_| {})
            .await
    }

    // A restore that ran three times repeats the offsets in the header
    fn restored_three_times(len: i64) -> impl Iterator<Item = i64> {
        (0..3).flat_map(move |_| 0..len)
    }

    fn assert_non_increasing(result: Result<PartitionScan, SearchError<Infallible>>) {
        let err = result.expect_err("search must fail on non-increasing header offsets");
        let SearchError::NonIncreasingOffsets(err) = err else {
            panic!("unexpected error: {err}");
        };
        assert!(err.earlier.new_offset < err.later.new_offset, "{err}");
        assert!(err.earlier.old_offset > err.later.old_offset, "{err}");
    }

    #[tokio::test]
    async fn binary_search_fails_instead_of_inverting_a_window() {
        // Used to invert the window of offset 1 and panic (#67)
        assert_non_increasing(search(restored_three_times(10), [1, 6], 1).await);
    }

    #[tokio::test]
    async fn binary_search_fails_on_contradicting_seeks() {
        // Used to silently map offset 500 into the second restore (#67)
        assert_non_increasing(search(restored_three_times(1000), [100, 500, 999], 1).await);
    }

    #[tokio::test]
    async fn binary_search_fails_on_decrease_within_a_scan() {
        assert_non_increasing(search(restored_three_times(10), [1, 6], 1000).await);
    }

    #[test]
    fn scan_blames_the_consumer_for_out_of_order_partition_offsets() {
        // Header offsets decrease too, but only because the messages arrived
        // out of order, which the data can't be blamed for
        let delivered = [(10, 5), (9, 4)].map(OffsetMapping::from);

        let err = Scan::try_from_mappings::<Infallible>(delivered)
            .map(|_| ())
            .expect_err("out of order partition offsets must fail the scan");

        let SearchError::OutOfOrderPartitionOffsets(err) = err else {
            panic!("unexpected error: {err}");
        };
        assert_eq!((err.previous, err.next), (delivered[0], delivered[1]));
    }

    #[tokio::test]
    async fn binary_search_accepts_gaps_and_repeats_in_increasing_offsets() -> anyhow::Result<()> {
        // Even offsets only, and offset 1000 at partition offsets 500 and 501
        let legacy = (0..1000).map(|i: i64| 2 * if i > 500 { i - 1 } else { i });
        let scan = search(legacy, [0, 3, 1000, 1997], 1).await?;

        let found: Vec<_> = scan
            .results_iter()
            .map(|r| match r {
                SearchResult::Found(m) => (m.old_offset, m.new_offset),
                SearchResult::NotFound(o) => panic!("offset {o} not found"),
            })
            .collect();
        assert_eq!(found, [(0, 0), (3, 1), (1000, 500), (1997, 999)]);
        Ok(())
    }

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

        let mut progress = Vec::new();
        let scan = partition
            .binary_search(
                search_offsets,
                OffsetSource::from_header("offset"),
                opts,
                |p| progress.push(p),
            )
            .await?;

        let first = progress.first().expect("progress before the first seek");
        assert_eq!((first.mapped, first.seeks, first.narrowed), (0, 0, 0.0));
        let last = progress.last().expect("progress after the last seek");
        assert_eq!((last.mapped, last.offsets), (8, 8));
        assert_eq!(last.narrowed, 1.0);
        assert!(last.window.is_empty());
        assert_eq!(last.seeks as usize, progress.len() - 1);
        assert!(
            progress.windows(2).all(|w| w[0].narrowed <= w[1].narrowed),
            "narrowing never goes backwards: {progress:?}"
        );

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
