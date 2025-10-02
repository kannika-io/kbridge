#[derive(Debug)]
pub struct OffsetRecord {
    topic: String,
    partition: usize,
    offset: usize,
    consumer_group:  String
}

pub type OffsetSnapshot = Vec<OffsetRecord>;

