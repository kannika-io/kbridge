use crate::prelude::*;
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

        let snapshot: OffsetSnapshot = csv_reader
            .deserialize::<OffsetRecord>()
            .map(|record_result| {
                record_result.map_err(|error| {
                    let line = error.position().map(|p| p.line()).unwrap_or(0);
                    CsvSnapshotError::MalformedRecord(line, error.to_string())
                })
            })
            // Short-circuit collecting on first error
            .collect::<Result<_, _>>()?;

        Ok(snapshot)
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

        assert_eq!(snapshot.size(), 2);
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
