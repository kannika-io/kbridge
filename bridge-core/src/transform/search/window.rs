#![allow(dead_code)]

/// A window representing a range of offsets.
/// Used to limit the search space during offset transformations.
use std::{
    fmt,
    num::{NonZero, NonZeroUsize},
};

use crate::prelude::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Window {
    // The lower bound of the window (inclusive).
    pub low: Offset,

    // The upper bound of the window (exclusive).
    pub high: Offset,
}

impl From<(Offset, Offset)> for Window {
    fn from(bounds: (Offset, Offset)) -> Self {
        Self::new(bounds.0, bounds.1)
    }
}

impl Window {
    pub fn new(low: Offset, high: Offset) -> Self {
        debug_assert!(low <= high, "Window low must be <= high");
        Self { low, high }
    }

    pub fn low(&self) -> Offset {
        self.low
    }

    pub fn high(&self) -> Offset {
        self.high
    }

    pub fn mid(&self) -> Offset {
        self.low + (self.high - self.low) / 2
    }

    // Returns a window centered around the mid-point with the given range.
    // Stays within the current window bounds.
    pub fn around_mid(&self, range: NonZeroUsize) -> Self {
        let range = range.get() as i64;
        let mid = self.mid();
        let half_range = range / 2;
        let new_low = mid.saturating_sub(half_range).max(self.low);
        let new_high = mid.saturating_add(half_range).min(self.high);
        Self::new(new_low, new_high)
    }

    // Narrows the current window to the intersection with the new bounds.
    pub fn narrow(&mut self, new_low: Offset, new_high: Offset) {
        let low = new_low.max(self.low);
        let high = new_high.min(self.high);
        debug_assert!(low <= high, "Window low must be <= high after narrowing");
        self.low = low;
        self.high = high;
    }

    pub fn narrow_low(&mut self, new_low: Offset) {
        let low = new_low.max(self.low);
        debug_assert!(
            low <= self.high,
            "Window low must be <= high after narrowing"
        );
        self.low = low;
    }

    pub fn narrow_high(&mut self, new_high: Offset) {
        let high = new_high.min(self.high);
        debug_assert!(
            self.low <= high,
            "Window high must be >= low after narrowing"
        );
        self.high = high;
    }

    pub fn is_empty(&self) -> bool {
        self.low >= self.high
    }

    pub fn len(&self) -> usize {
        (self.high - self.low) as usize
    }

    // Returns a new window starting at `start` with size `size`.
    // If the requested window exceeds the current window bounds, it is clamped.
    pub fn slice(&self, start: i64, size: NonZero<usize>) -> Self {
        let size = size.get() as i64;
        let low = start.max(self.low);
        let high = low.saturating_add(size).min(self.high);
        Self::new(low, high)
    }

    /// Returns a new window clamped to the bounds of another window.
    /// If there is no intersection, returns an empty window [0, 0).
    /// Note: If either window is already empty, the result will be empty.
    pub fn clamp(&self, other: &Window) -> Self {
        let low = self.low.max(other.low);
        let high = self.high.min(other.high);
        if low >= high {
            Self::new(0, 0) // empty window
        } else {
            Self::new(low, high)
        }
    }

    // Returns the midpoint of the window.
    pub(crate) fn midpoint(&self) -> i64 {
        self.low + (self.high - self.low) / 2
    }

    pub fn contains(&self, offset: Offset) -> bool {
        offset >= self.low && offset < self.high
    }

    pub fn range(&self, range: impl std::ops::RangeBounds<Offset>) -> Self {
        use std::ops::Bound::*;

        let low = match range.start_bound() {
            Included(&start) => start.max(self.low),
            Excluded(&start) => (start + 1).max(self.low),
            Unbounded => self.low,
        };

        let high = match range.end_bound() {
            Included(&end) => (end + 1).min(self.high),
            Excluded(&end) => end.min(self.high),
            Unbounded => self.high,
        };

        Self::new(low, high)
    }

    pub fn from_range(range: impl std::ops::RangeBounds<Offset>) -> Self {
        use std::ops::Bound::*;

        let low = match range.start_bound() {
            Included(&start) => start,
            Excluded(&start) => start + 1,
            Unbounded => 0,
        };

        let high = match range.end_bound() {
            Included(&end) => end + 1,
            Excluded(&end) => end,
            Unbounded => i64::MAX,
        };

        Self::new(low, high)
    }

    pub(crate) fn empty() -> Window {
        Window::new(0, 0)
    }
}

impl Default for Window {
    fn default() -> Self {
        Self {
            low: 0,
            high: i64::MAX,
        }
    }
}

impl fmt::Display for Window {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}, {})", self.low, self.high)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_range_and_midpoint() {
        let cursor = 500;
        let window = Window::new(0, 1000);
        let new_cursor = window.range(..cursor).midpoint();
        assert_eq!(new_cursor, 250);
    }

    mod around_mid {

        use super::*;

        #[test]
        fn test_around_mid_within_bounds() {
            let window = Window::new(0, 100);
            let new_window = window.around_mid(NonZeroUsize::new(20).unwrap());
            assert_eq!(new_window, (40, 60).into());
        }

        #[test]
        fn test_around_mid_at_bounds() {
            let window = Window::new(0, 50);
            let new_window = window.around_mid(NonZeroUsize::new(100).unwrap());
            assert_eq!(new_window, (0, 50).into());
        }

        #[test]
        fn test_around_mid_out_of_bounds() {
            let window = Window::new(30, 70);
            let new_window = window.around_mid(NonZeroUsize::new(100).unwrap());
            assert_eq!(new_window, (30, 70).into());
        }

        #[test]
        fn test_around_mid_one() {
            let window = Window::new(30, 70);
            let new_window = window.around_mid(NonZeroUsize::new(1).unwrap());
            assert_eq!(new_window, (50, 50).into());
        }

        #[test]
        fn test_around_mid_bounds() {
            let window = Window::new(0, 1);
            let new_window = window.around_mid(NonZeroUsize::new(usize::MAX).unwrap());
            assert_eq!(new_window, (0, 0).into());
        }
    }

    mod slice {

        use super::*;

        #[test]
        fn test_slice_within_bounds() {
            let window = Window::new(0, 100);
            let sliced = window.slice(20, NonZero::new(30).unwrap());
            assert_eq!(sliced, Window::new(20, 50));
        }

        #[test]
        fn test_slice_exceeds_high() {
            let window = Window::new(0, 100);
            let sliced = window.slice(80, NonZero::new(30).unwrap());
            assert_eq!(sliced, Window::new(80, 100));
        }

        #[test]
        fn test_slice_below_low() {
            let window = Window::new(50, 150);
            let sliced = window.slice(20, NonZero::new(40).unwrap());
            assert_eq!(sliced, Window::new(50, 90));
        }

        #[test]
        fn test_slice_outside_bounds() {
            let window = Window::new(50, 100);
            let sliced = window.slice(0, NonZero::new(30).unwrap());
            assert_eq!(sliced, Window::new(50, 80));
        }
    }

    mod clamp {

        use super::*;

        #[test]
        fn test_clamp_no_overlap() {
            let window = Window::new(0, 50);
            let other = Window::new(100, 150);
            assert_eq!(window.clamp(&other), Window::new(0, 0));
        }

        #[test]
        fn test_clamp_partial_overlap() {
            let window = Window::new(0, 100);
            let other = Window::new(50, 150);
            assert_eq!(window.clamp(&other), Window::new(50, 100));
        }
    }

    mod midpoint {
        use super::*;

        #[test]
        fn test_midpoint_even() {
            let window = Window::new(0, 100);
            assert_eq!(window.midpoint(), 50);
        }

        #[test]
        fn test_midpoint_odd() {
            let window = Window::new(0, 101);
            assert_eq!(window.midpoint(), 50);
        }

        #[test]
        fn test_midpoint_single_value() {
            let window = Window::new(42, 42);
            assert_eq!(window.midpoint(), 42);
        }

        #[test]
        fn test_midpoint_large_values() {
            let window = Window::new(i64::MAX - 100, i64::MAX);
            assert_eq!(window.midpoint(), i64::MAX - 50);
        }

        #[test]
        fn test_midpoint_zero() {
            let window = Window::new(0, 0);
            assert_eq!(window.midpoint(), 0);
        }
    }

    mod range {

        use super::*;

        #[test]
        fn test_range_inclusive() {
            let window = Window::new(10, 50);
            let ranged = window.range(20..=40);
            assert_eq!(ranged, Window::new(20, 41));
        }

        #[test]
        fn test_range_exclusive() {
            let window = Window::new(10, 50);
            let ranged = window.range(20..40);
            assert_eq!(ranged, Window::new(20, 40));
        }

        #[test]
        fn test_range_unbounded_start() {
            let window = Window::new(10, 50);
            let ranged = window.range(..30);
            assert_eq!(ranged, Window::new(10, 30));
        }

        #[test]
        fn test_range_unbounded_end() {
            let window = Window::new(10, 50);
            let ranged = window.range(30..);
            assert_eq!(ranged, Window::new(30, 50));
        }

        #[test]
        fn test_range_unbounded_both() {
            let window = Window::new(10, 50);
            let ranged = window.range(..);
            assert_eq!(ranged, Window::new(10, 50));
        }
    }
}
