#[derive(Debug, PartialEq, Eq)]
pub struct OffsetRecord {
    pub topic: String,
    pub partition: usize,
    pub offset: usize,
    pub consumer_group: String,
}

pub type OffsetSnapshot = Vec<OffsetRecord>;

pub trait OffsetSnapshotImporter {
    fn import(&self) -> Result<OffsetSnapshot, ImportError>;
}

// To implement
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
