use thiserror::Error;

use crate::OffsetSnapshot;

// Implement this to import snapshot from e.g. yml, csv, rest API, ...
pub trait OffsetSnapshotImporter {
    fn import(&self) -> Result<OffsetSnapshot, ImportError>;
}

// This can be in the core since it is
pub trait OffsetSnapshotValidator {
    fn validate(&self) -> Result<(), ValidateError>;
}

#[derive(Error, Debug)]
pub enum ImportError {
    #[error("Resource to import not found. Reason: {0}")]
    ResourceNotFound(String),
    #[error("Errors during parsing of input. Reason(s): {0:?}")]
    ParseErrors(ValidationErrors),
}

type ValidationErrors = Vec<String>;

#[derive(Debug)]
// TODO
pub enum ValidateError {
    DuplicateConsumerGroup(String),
}
