use thiserror::Error;
use bridge_core::commands::apply_target_offsets::errors::ApplyOffsetsError;
use bridge_core::commands::calculate_target_offsets::errors::TransformationError;
use bridge_core::commands::errors::FetchSourceOffsetsError;
use bridge_core::commands::fetch_source_offsets::errors::{ImportError, ImportOffsetsError};

#[derive(Error, Debug)]
pub enum GeneralError {
    #[error("Failed during importing of source offsets. Reason: {0}")]
    Import(#[from] ImportError),
    #[error("Failed during offset transformation. Reason: {0}")]
    Transformation(#[from] TransformationError),
    #[error("Failed during offset transformation. Reason: {0}")]
    ApplyOffsets(#[from] ApplyOffsetsError),
    #[error("Failed during offset import. Reason: {0}")]
    ImportOffsets(#[from] ImportOffsetsError),
    #[error("Failed during offset import. Reason: {0}")]
    FetchSourceOffsetsError(#[from] FetchSourceOffsetsError),
}
