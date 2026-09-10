//! Module for reading and writing CSV consumer group offset snapshots.
use crate::prelude::*;
use std::collections::BTreeMap;
use std::io;

// A trait for types that can be created from CSV data.
pub trait FromCsv<R>: Sized {
    type Err;

    fn from_csv(reader: R) -> Result<Self, Self::Err>;
}

//
#[derive(thiserror::Error, Debug)]
pub enum CsvSnapshotError {
    #[error("Malformed record at line {0}: {1}")]
    MalformedRecord(u64, String),
}

const CSV_HEADERS: [&str; 4] = ["consumer_group", "topic", "partition", "offset"];

impl<R> FromCsv<R> for OffsetSnapshot
where
    R: io::Read,
{
    type Err = CsvSnapshotError;

    fn from_csv(reader: R) -> Result<Self, Self::Err> {
        let mut csv_reader = ::csv::ReaderBuilder::new().from_reader(reader);
        csv_reader.set_headers(::csv::StringRecord::from(CSV_HEADERS.to_vec()));

        // Deduplicate on (consumer_group, topic, partition), latest offset wins
        let mut offsets: BTreeMap<(String, String, i32), i64> = BTreeMap::new();
        for record_result in csv_reader.deserialize::<OffsetRecord>() {
            let record = record_result.map_err(|error| {
                let line = error.position().map(|p| p.line()).unwrap_or(0);
                CsvSnapshotError::MalformedRecord(line, error.to_string())
            })?;
            offsets.insert(
                (record.consumer_group, record.topic, record.partition),
                record.offset,
            );
        }

        Ok(offsets
            .into_iter()
            .map(
                |((consumer_group, topic, partition), offset)| OffsetRecord {
                    consumer_group,
                    topic,
                    partition,
                    offset,
                },
            )
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_from_csv_success() {
        let csv_data = indoc::indoc! {r#"
            group1,topic1,0,100
            group2,topic2,1,200
        "#};

        let snapshot = OffsetSnapshot::from_csv(csv_data.as_bytes()).unwrap();

        assert_eq!(snapshot.len(), 2);
        assert!(
            snapshot
                .get_opt("group1", "topic1", 0)
                .is_some_and(|r| r.offset == 100)
        );
        assert!(
            snapshot
                .get_opt("group2", "topic2", 1)
                .is_some_and(|r| r.offset == 200)
        );
    }

    #[test]
    fn test_snapshot_from_csv_deduplicates_latest_wins() {
        let csv_data = indoc::indoc! {r#"
            group1,topic1,0,100
            group2,topic2,1,200
            group1,topic1,0,300
        "#};

        let snapshot = OffsetSnapshot::from_csv(csv_data.as_bytes()).unwrap();

        assert_eq!(snapshot.len(), 2);
        assert!(
            snapshot
                .get_opt("group1", "topic1", 0)
                .is_some_and(|r| r.offset == 300)
        );
    }

    #[test]
    fn test_snapshot_from_csv_sorts_records() {
        let csv_data = indoc::indoc! {r#"
            group2,topic2,1,200
            group1,topic1,1,150
            group1,topic1,0,100
        "#};

        let snapshot = OffsetSnapshot::from_csv(csv_data.as_bytes()).unwrap();

        let records: Vec<String> = snapshot.iter().map(ToString::to_string).collect();
        assert_eq!(
            records,
            vec![
                "group1,topic1,0,100",
                "group1,topic1,1,150",
                "group2,topic2,1,200",
            ]
        );
    }

    #[test]
    fn test_snapshot_from_csv_with_errors() {
        let csv_data = indoc::indoc! {r#"
            group1,topic1,0,100
            group2,topic2,0
        "#};

        let result = OffsetSnapshot::from_csv(csv_data.as_bytes());

        assert!(matches!(
            result,
            Err(CsvSnapshotError::MalformedRecord(2, _)),
        ));
    }
}
