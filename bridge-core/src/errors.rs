use thiserror::Error;

use crate::{
    commands::{
        apply_target_offsets::errors::ApplyOffsetsError,
        calculate_target_offsets::errors::TransformationError, errors::FetchSourceOffsetsError,
        fetch_source_offsets::errors::ImportOffsetsError,
    },
    snapshot::csv::CsvSnapshotError,
};

#[derive(Error, Debug)]
pub enum BridgeError {
    #[error("Failed while parsing CSV snapshot. Reason: {0}")]
    CsvSnapshot(#[from] CsvSnapshotError),
    #[error("Failed during offset transformation. Reason: {0}")]
    Transformation(#[from] TransformationError),
    #[error("Failed during offset transformation. Reason: {0}")]
    ApplyOffsets(#[from] ApplyOffsetsError),
    #[error("Failed during offset import. Reason: {0}")]
    ImportOffsets(#[from] ImportOffsetsError),
    #[error("Failed during offset import. Reason: {0}")]
    FetchSourceOffsetsError(#[from] FetchSourceOffsetsError),
}
