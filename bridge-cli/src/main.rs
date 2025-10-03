use bridge_core::{
    import::{ImportError, OffsetSnapshotImporter},
    transform::{self, transformation_errors::TransformationError},
};
use thiserror::Error;

use crate::fetch_offsets::csv::CsvOffsetSnapshotImporter;

mod fetch_offsets;

#[tokio::main]
async fn main() -> Result<(), GeneralError> {
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

    todo!()
}

#[derive(Error, Debug)]
enum GeneralError {
    #[error("Failed during importing of source offsets. Reason: {0}")]
    ImportError(ImportError),
    #[error("Failed during offset transformation. Reason: {0}")]
    TransformationError(TransformationError),
}

impl From<TransformationError> for GeneralError {
    fn from(value: TransformationError) -> Self {
        GeneralError::TransformationError(value)
    }
}
