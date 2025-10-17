use log::trace;

use crate::commands::fetch_source_offsets::OffsetSnapshotImporter;
use crate::commands::fetch_source_offsets::errors::ImportError;
use crate::read_offsets::csv::{CsvOffsetSnapshotImporter, convert};
use crate::{CsvInput, OffsetRecord, OffsetSnapshot};
use std::collections::HashSet;
use std::fmt::Display;
use std::io::{BufRead, stdin, IsTerminal};

pub fn get_from_stdin() -> Vec<OffsetRecord> {
    trace!("Fetching offset records from stdin");
    
    // Check if stdin is a terminal (interactive) - if so, return empty vec to avoid blocking
    if stdin().is_terminal() {
        trace!("Stdin is a terminal, returning empty offset records");
        return Vec::new();
    }
    
    stdin()
        .lock()
        .lines()
        .map_while(Result::ok)
        .filter_map(|line| convert(line).ok())
        .collect()
}
/// Get offset records, either from stdin or from a CSV file on the specified path
pub fn get_offset_records(input: &Option<CsvInput>) -> Result<OffsetSnapshot, ImportError> {
    match input {
        Some(CsvInput::Stdin) | None => Ok(get_from_stdin()),
        Some(CsvInput::File(path)) => {
            let offset_snapshot_importer = CsvOffsetSnapshotImporter { file_path: path };
            offset_snapshot_importer.import()
        }
    }
}

pub fn get_unique_topics_from_offset_snapshot(offset_snapshot: &OffsetSnapshot) -> Vec<&str> {
    offset_snapshot
        .iter()
        .map(|o| o.topic.as_str())
        // Filter out duplicates. source_offsets can contain duplicate topic names in case multiple
        // consumer groups are present
        .collect::<HashSet<_>>()
        .into_iter()
        .collect()
}

impl Display for OffsetRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{},{},{},{}",
            self.consumer_group, self.topic, self.partition, self.offset
        )
    }
}

impl Display for CsvInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(format!("{self:?}").as_str())
    }
}
