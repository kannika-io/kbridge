use bridge_core::{
    export::ApplyOffsetsError, import::ImportError, transform::errors::TransformationError,
};
use thiserror::Error;

use crate::{
    fetch_offsets::client::ImportOffsetsError, fetch_source_offsets::FetchSourceOffsetsError,
};

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
