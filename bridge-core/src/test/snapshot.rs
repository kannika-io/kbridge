//! Test utilities for snapshot assertions.

use crate::prelude::*;
use crate::test::table::TableBuilder;
use std::collections::HashSet;

pub mod assertions {
    use super::*;

    pub fn assert_eq<L, R>(left: L, right: R)
    where
        L: AsRef<OffsetSnapshot>,
        R: AsRef<OffsetSnapshot>,
    {
        let left = left.as_ref();
        let right = right.as_ref();
        let left_set: HashSet<_> = left.iter().collect();
        let right_set: HashSet<_> = right.iter().collect();

        assert_eq!(
            left_set.len(),
            left.len(),
            "Left snapshot has duplicate records"
        );
        assert_eq!(
            right_set.len(),
            right.len(),
            "Right snapshot has duplicate records"
        );

        enum Diff {
            Missing {
                topic: String,
                partition: i32,
                group: String,
            },
            Extra {
                topic: String,
                partition: i32,
                group: String,
                offset: i64,
            },
            DifferentOffset {
                topic: String,
                partition: i32,
                group: String,
                left_offset: i64,
                right_offset: i64,
            },
        }

        // Get diffs and panic if diffs are found
        let mut diffs = Vec::new();

        // Check for missing and different offsets
        for left_record in left.iter() {
            match right.iter().find(|r| {
                r.consumer_group == left_record.consumer_group
                    && r.topic == left_record.topic
                    && r.partition == left_record.partition
            }) {
                None => diffs.push(Diff::Missing {
                    topic: left_record.topic.clone(),
                    partition: left_record.partition,
                    group: left_record.consumer_group.clone(),
                }),
                Some(right_record) if right_record.offset != left_record.offset => {
                    diffs.push(Diff::DifferentOffset {
                        topic: left_record.topic.clone(),
                        partition: left_record.partition,
                        group: left_record.consumer_group.clone(),
                        left_offset: left_record.offset,
                        right_offset: right_record.offset,
                    });
                }
                _ => {} // Records match
            }
        }

        // Check for extra records in right
        for right_record in right.iter() {
            if !left.iter().any(|l| {
                l.consumer_group == right_record.consumer_group
                    && l.topic == right_record.topic
                    && l.partition == right_record.partition
            }) {
                diffs.push(Diff::Extra {
                    topic: right_record.topic.clone(),
                    partition: right_record.partition,
                    group: right_record.consumer_group.clone(),
                    offset: right_record.offset,
                });
            }
        }

        if !diffs.is_empty() {
            let mut table = TableBuilder::new(vec![
                "Type".to_string(),
                "Group".to_string(),
                "Topic".to_string(),
                "Partition".to_string(),
                "Left".to_string(),
                "Right".to_string(),
            ]);

            for diff in diffs {
                match diff {
                    Diff::Missing {
                        topic,
                        partition,
                        group,
                    } => {
                        table.add_row(vec![
                            "Missing".to_string(),
                            group,
                            topic,
                            partition.to_string(),
                            "".to_string(),
                            "".to_string(),
                        ]);
                    }
                    Diff::Extra {
                        topic,
                        partition,
                        group,
                        offset,
                    } => {
                        table.add_row(vec![
                            "Extra".to_string(),
                            group,
                            topic,
                            partition.to_string(),
                            "".to_string(),
                            offset.to_string(),
                        ]);
                    }
                    Diff::DifferentOffset {
                        topic,
                        partition,
                        group,
                        left_offset,
                        right_offset,
                    } => {
                        table.add_row(vec![
                            "Different".to_string(),
                            group,
                            topic,
                            partition.to_string(),
                            left_offset.to_string(),
                            right_offset.to_string(),
                        ]);
                    }
                }
            }

            panic!("Snapshots are not equal:\n\n{}", table.build());
        }
    }
}
