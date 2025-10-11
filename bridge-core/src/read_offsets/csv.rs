use std::{
    error::Error,
    path::PathBuf,
};

use crate::commands::fetch_source_offsets::OffsetSnapshotImporter;
use crate::{OffsetRecord, OffsetSnapshot};
use crate::commands::fetch_source_offsets::errors::ImportError;

pub struct CsvOffsetSnapshotImporter {
    pub file_path: PathBuf,
}

const CSV_HEADERS: [&str; 4] = ["consumer_group", "topic", "partition", "offset"];

pub fn convert(value: String) -> Result<OffsetRecord, ImportError> {
    let mut reader = csv::ReaderBuilder::new().from_reader(value.as_bytes());
    reader.set_headers(csv::StringRecord::from(CSV_HEADERS.to_vec()));
    let result = import_records(reader.deserialize::<OffsetRecord>())?;
    Ok(result[0].clone())
}

impl OffsetSnapshotImporter for CsvOffsetSnapshotImporter {
    fn import(&self) -> Result<OffsetSnapshot, ImportError> {
        if let Ok(mut reader) = csv::Reader::from_path(&self.file_path) {
            reader.set_headers(csv::StringRecord::from(CSV_HEADERS.to_vec()));
            import_records(reader.deserialize::<OffsetRecord>())
        } else {
            Err(ImportError::ResourceNotFound(format!(
                "{:?} could not be opened.",
                self.file_path
            )))
        }
    }
}

fn import_records(
    records: impl Iterator<Item = Result<OffsetRecord, impl Error>>,
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

#[cfg(test)]
mod tests {
    use tempfile::NamedTempFile;

    use super::*;
    use std::fs;
    use crate::commands::fetch_source_offsets::errors::ImportError;

    #[test]
    fn test_convert_success() {
        let csv_line = "test-group,test-topic,0,100".to_string();

        let result = convert(csv_line).unwrap();

        assert_eq!(result.consumer_group, "test-group");
        assert_eq!(result.topic, "test-topic");
        assert_eq!(result.partition, 0);
        assert_eq!(result.offset, 100);
    }

    #[test]
    fn test_convert_invalid_format() {
        let csv_line = "invalid,format".to_string();

        let result = convert(csv_line);

        assert!(result.is_err());
        match result.unwrap_err() {
            ImportError::ParseErrors(errors) => {
                assert!(!errors.is_empty());
            }
            _ => panic!("Expected ParseErrors"),
        }
    }

    #[test]
    fn test_convert_invalid_partition() {
        let csv_line = "test-group,test-topic,invalid,100".to_string();

        let result = convert(csv_line);

        assert!(result.is_err());
        match result.unwrap_err() {
            ImportError::ParseErrors(errors) => {
                assert!(!errors.is_empty());
            }
            _ => panic!("Expected ParseErrors"),
        }
    }

    #[test]
    fn test_import_records_success() {
        let records: Vec<Result<OffsetRecord, ImportError>> = vec![
            Ok(OffsetRecord {
                consumer_group: "group1".to_string(),
                topic: "topic1".to_string(),
                partition: 0,
                offset: 100,
            }),
            Ok(OffsetRecord {
                consumer_group: "group2".to_string(),
                topic: "topic2".to_string(),
                partition: 1,
                offset: 200,
            }),
        ];

        let result = import_records(records.into_iter()).unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].consumer_group, "group1");
        assert_eq!(result[0].topic, "topic1");
        assert_eq!(result[0].partition, 0);
        assert_eq!(result[0].offset, 100);
        assert_eq!(result[1].consumer_group, "group2");
        assert_eq!(result[1].topic, "topic2");
        assert_eq!(result[1].partition, 1);
        assert_eq!(result[1].offset, 200);
    }

    #[test]
    fn test_import_records_with_errors() {
        let records = vec![
            Ok(OffsetRecord {
                consumer_group: "group1".to_string(),
                topic: "topic1".to_string(),
                partition: 0,
                offset: 100,
            }),
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "parse error",
            )),
        ];

        let result = import_records(records.into_iter());

        assert!(result.is_err());
        match result.unwrap_err() {
            ImportError::ParseErrors(errors) => {
                assert_eq!(errors.len(), 1);
                assert!(errors[0].contains("parse error"));
            }
            _ => panic!("Expected ParseErrors"),
        }
    }

    #[test]
    fn test_csv_offset_snapshot_importer_success() -> Result<(), Box<dyn std::error::Error>> {
        let temp_file = NamedTempFile::new()?;
        let csv_content = "test-group,test-topic,0,100\ntest-group2,test-topic2,1,200";
        fs::write(&temp_file, csv_content)?;

        let importer = CsvOffsetSnapshotImporter {
            file_path: temp_file.path().to_path_buf(),
        };

        let result = importer.import().unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].consumer_group, "test-group");
        assert_eq!(result[0].topic, "test-topic");
        assert_eq!(result[0].partition, 0);
        assert_eq!(result[0].offset, 100);
        assert_eq!(result[1].consumer_group, "test-group2");
        assert_eq!(result[1].topic, "test-topic2");
        assert_eq!(result[1].partition, 1);
        assert_eq!(result[1].offset, 200);

        Ok(())
    }

    #[test]
    fn test_csv_offset_snapshot_importer_file_not_found() {
        let importer = CsvOffsetSnapshotImporter {
            file_path: PathBuf::from("/nonexistent/path/file.csv"),
        };

        let result = importer.import();

        assert!(result.is_err());
        match result.unwrap_err() {
            ImportError::ResourceNotFound(msg) => {
                assert!(msg.contains("/nonexistent/path/file.csv"));
            }
            _ => panic!("Expected ResourceNotFound"),
        }
    }

    #[test]
    fn test_csv_offset_snapshot_importer_invalid_csv() -> Result<(), Box<dyn std::error::Error>> {
        let temp_file = NamedTempFile::new()?;
        let csv_content =
            "consumer_group,topic,partition,offset\ntest-group,test-topic,invalid,100";
        fs::write(&temp_file, csv_content)?;

        let importer = CsvOffsetSnapshotImporter {
            file_path: temp_file.path().to_path_buf(),
        };

        let result = importer.import();

        assert!(result.is_err());
        match result.unwrap_err() {
            ImportError::ParseErrors(errors) => {
                assert!(!errors.is_empty());
            }
            _ => panic!("Expected ParseErrors"),
        }

        Ok(())
    }
}
