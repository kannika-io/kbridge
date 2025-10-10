use thiserror::Error;

use crate::OffsetSnapshot;

pub trait OffsetSnapshotImporter {
    fn import(&self) -> Result<OffsetSnapshot, ImportError>;
}

#[derive(Error, Debug)]
pub enum ImportError {
    #[error("Resource to import not found. Reason: {0}")]
    ResourceNotFound(String),
    #[error("Errors during parsing of input. Reason(s): {0:?}")]
    ParseErrors(ValidationErrors),
}

type ValidationErrors = Vec<String>;
