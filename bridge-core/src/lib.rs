pub mod import;
pub mod transform;

#[derive(Debug, PartialEq, Eq)]
pub struct OffsetRecord {
    pub topic: String,
    pub partition: usize,
    pub offset: usize,
    pub consumer_group: String,
}

pub type OffsetSnapshot = Vec<OffsetRecord>;
