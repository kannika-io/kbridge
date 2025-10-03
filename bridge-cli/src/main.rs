use bridge_core::{import::OffsetSnapshotImporter, transform};
use anyhow::Result;

use crate::fetch_offsets::csv::CsvOffsetSnapshotImporter;

mod fetch_offsets;


#[tokio::main]
async fn main() -> Result<()> {
    simple_logger::SimpleLogger::new().env().init().unwrap();
    let offset_snapshot_importer = CsvOffsetSnapshotImporter {
        file_path: "offsets.csv",
        consumer_group: "testconsumergroup",
    };
    let result = offset_snapshot_importer.import()?;

    let transformed_result = transform::transform(
        "localhost:9092",
        &["orders"],
        "test",
        result.iter().map(|r| r.offset).collect(),
    )
    .await?;

    Ok::<>(())
}
