use bridge_core::{
    export::apply_target_offsets,
    import::{ImportError, OffsetSnapshotImporter},
    transform::{get_target_offsets, transformation_errors::TransformationError},
};
use thiserror::Error;

use crate::fetch_offsets::csv::CsvOffsetSnapshotImporter;

mod fetch_offsets;

#[tokio::main]
async fn main() -> Result<(), GeneralError> {
    simple_logger::SimpleLogger::new()
        .env()
        .init().unwrap();

    let offset_snapshot_importer = CsvOffsetSnapshotImporter {
        file_path: "offsets.csv",
        consumer_group: "console-consumer",
    };
    let result = offset_snapshot_importer.import()?;

    let transformed_result = get_target_offsets(
        "localhost:9093",
        &["orders-1", "orders-2", "orders-3"],
        "Offset",
        &result,
    )
    .await?;

    println!("{transformed_result:?}");
    apply_target_offsets("localhost:9093", &transformed_result)
        .await
        .unwrap();


    Ok(())
}

#[derive(Error, Debug)]
enum GeneralError {
    #[error("Failed during importing of source offsets. Reason: {0}")]
    ImportError(#[from] ImportError),
    #[error("Failed during offset transformation. Reason: {0}")]
    TransformationError(#[from] TransformationError),
}
