use crate::OffsetSnapshot;

// Implement this to import snapshot from e.g. yml, csv, rest API, ...
pub trait OffsetSnapshotImporter {
    fn import(&self) -> Result<OffsetSnapshot, ImportError>;
}

// This can be in the core since it is 
pub trait OffsetSnapshotValidator {
    fn validate(&self) -> Result<(), ValidateError>;
}

#[derive(Debug)]
pub enum ImportError {
    ResourceNotFound(String),
    ParseErrors(Vec<String>),
}

#[derive(Debug)]
pub enum ValidateError {
    DuplicateConsumerGroup(String)
}
