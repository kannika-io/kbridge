use std::path::PathBuf;

use bridge_core::{
    OffsetSnapshot,
    import::{ImportError, OffsetSnapshotImporter},
};

use crate::fetch_offsets::csv::{CsvOffsetSnapshotImporter, get_from_stdin};

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
