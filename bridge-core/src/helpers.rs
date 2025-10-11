use std::io::{stdin, BufRead};
use std::path::PathBuf;
use crate::commands::fetch_source_offsets::OffsetSnapshotImporter;
use crate::{OffsetRecord, OffsetSnapshot};
use crate::commands::fetch_source_offsets::errors::ImportError;
use crate::read_offsets::csv::{convert, CsvOffsetSnapshotImporter};

pub fn get_from_stdin() -> Vec<OffsetRecord> {
    stdin()
        .lock()
        .lines()
        .map_while(Result::ok)
        .filter_map(|line| convert(line).ok())
        .collect()
}
/// Fetches offset records, either from stdin or from a CSV file on the specified path
pub fn fetch_offset_records(
    from_stdin: bool,
    source_offsets_csv_file_location: Option<PathBuf>,
) -> Result<OffsetSnapshot, ImportError> {
    match from_stdin {
        true => Ok(get_from_stdin()),
        false => {
            if let Some(path) = source_offsets_csv_file_location {
                let offset_snapshot_importer = CsvOffsetSnapshotImporter { file_path: path };
                offset_snapshot_importer.import()
            } else {
                Err(ImportError::ResourceNotFound(
                    "Import path is required if --from-stdin is not specified.".to_string(),
                ))
            }
        }
    }
}
