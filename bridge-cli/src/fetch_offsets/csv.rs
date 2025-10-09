use std::{error::Error, path::PathBuf};

use bridge_core::{
    OffsetRecord, OffsetSnapshot,
    import::{ImportError, OffsetSnapshotImporter},
};
use serde::Deserialize;

pub struct CsvOffsetSnapshotImporter {
    pub file_path: PathBuf,
}

const CSV_HEADERS: [&str; 4] = ["consumer_group", "topic", "partition", "offset"];

pub fn convert(value: String) -> Result<OffsetRecord, ImportError> {
    let mut reader = csv::ReaderBuilder::new().from_reader(value.as_bytes());
    reader.set_headers(csv::StringRecord::from(CSV_HEADERS.to_vec()));
    let result = import_records(reader.deserialize::<Record>())?;
    Ok(result[0].clone())
}

impl OffsetSnapshotImporter for CsvOffsetSnapshotImporter {
    fn import(&self) -> Result<OffsetSnapshot, ImportError> {
        if let Ok(mut reader) = csv::Reader::from_path(&self.file_path) {
            reader.set_headers(csv::StringRecord::from(CSV_HEADERS.to_vec()));
            import_records(reader.deserialize::<Record>())
        } else {
            Err(ImportError::ResourceNotFound(format!(
                "{:?} could not be opened.",
                self.file_path
            )))
        }
    }
}

fn import_records(
    records: impl Iterator<Item = Result<Record, impl Error>>,
) -> Result<Vec<OffsetRecord>, ImportError> {
    let mut parse_errors: Vec<String> = vec![];
    let mut snapshot: OffsetSnapshot = vec![];
    for record in records {
        match record {
            Ok(record) => snapshot.push(OffsetRecord {
                consumer_group: record.consumer_group,
                topic: record.topic,
                partition: record.partition,
                offset: record.offset,
            }),
            Err(error) => parse_errors.push(error.to_string()),
        };
    }

    if parse_errors.is_empty() {
        Ok(snapshot)
    } else {
        Err(ImportError::ParseErrors(parse_errors))
    }
}

#[derive(Deserialize)]
struct Record {
    consumer_group: String,
    topic: String,
    partition: i32,
    offset: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_import_from_csv_should_import_correctly() {
        let data = "consumer,streamiz.weather.combined,0,54
                        consumer,streamiz.weather.combined,1,81";
        let mut reader = csv::ReaderBuilder::new().from_reader(data.as_bytes());
        reader.set_headers(csv::StringRecord::from(CSV_HEADERS.to_vec()));
        let result = import_records(reader.deserialize::<Record>());

        let assertion = OffsetRecord {
            topic: "streamiz.weather.combined".to_string(),
            partition: 0,
            offset: 54,
            consumer_group: "consumer".to_string(),
        };

        assert_eq!(assertion, result.unwrap()[0]);
    }

    #[test]
    fn test_import_from_csv_invalid_csv_should_fail() {
        let data = "consumer,streamiz.weather.combined,0
                        consumer,streamiz.weather.combined,1,81";
        let mut reader = csv::ReaderBuilder::new().from_reader(data.as_bytes());
        reader.set_headers(csv::StringRecord::from(CSV_HEADERS.to_vec()));
        let result = import_records(reader.deserialize::<Record>());

        assert!(result.is_err());
    }

    #[test]
    fn test_import_from_non_existing_file_should_fail() {
        let offset_snapshot = CsvOffsetSnapshotImporter {
            file_path: "nonexistingfiles.csv".into(),
        };
        let result = offset_snapshot.import();

        assert!(result.is_err());
    }
}
