use std::error::Error;

use bridge_core::{
    OffsetRecord, OffsetSnapshot,
    import::{ImportError, OffsetSnapshotImporter},
};
use serde::Deserialize;

pub struct CsvOffsetSnapshotImporter {
    pub file_path: &'static str,
    pub consumer_group: &'static str,
}

impl OffsetSnapshotImporter for CsvOffsetSnapshotImporter {
    fn import(&self) -> Result<OffsetSnapshot, ImportError> {
        if let Ok(mut reader) = csv::Reader::from_path(self.file_path) {
            reader.set_headers(csv::StringRecord::from(vec!["topic", "partition", "offset"]));
            import_records(reader.deserialize::<Record>(), self.consumer_group)
        } else {
            Err(ImportError::ResourceNotFound(format!("{} could not be opened.", self.file_path)))
        }
    }
}

fn import_records(records: impl Iterator<Item = Result<Record, impl Error>>, consumer_group: &str) -> Result<Vec<OffsetRecord>, ImportError> {
    let mut parse_errors: Vec<String> = vec![];
    let mut snapshot: OffsetSnapshot = vec![];
    for record in records {
        match record {
            Ok(record) => snapshot.push(OffsetRecord {
                topic: record.topic,
                partition: record.partition,
                offset: record.offset,
                consumer_group: consumer_group.to_string(),
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
    topic: String,
    partition: i32,
    offset: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_import_from_csv_should_import_correctly() {
        let data = "\"streamiz.weather.combined\",0,54
                        \"streamiz.weather.combined\",1,81";
        let mut reader = csv::ReaderBuilder::new().from_reader(data.as_bytes());
        reader.set_headers(csv::StringRecord::from(vec!["topic", "partition", "offset"]));
        let result = import_records(reader.deserialize::<Record>(), "testing");

        let assertion = OffsetRecord {
            topic: "streamiz.weather.combined".to_string(),
            partition: 0,
            offset: 54,
            consumer_group: "testing".to_string(),
        };

        assert_eq!(assertion, result.unwrap()[0]);
    }

    #[test]
    fn test_import_from_csv_invalid_csv_should_fail() {
        let data = "\"streamiz.weather.combined\",0
                        \"streamiz.weather.combined\",1,81";
        let mut reader = csv::ReaderBuilder::new().from_reader(data.as_bytes());
        reader.set_headers(csv::StringRecord::from(vec!["topic", "partition", "offset"]));
        let result = import_records(reader.deserialize::<Record>(), "testing");

        assert!(result.is_err());
    }

    #[test]
    fn test_import_from_non_existing_file_should_fail() {
        let offset_snapshot = CsvOffsetSnapshotImporter {
            file_path: "nonexistingfiles.csv",
            consumer_group: "testconsumergroup",
        };
        let result = offset_snapshot.import();

        assert!(result.is_err());
    }
}
