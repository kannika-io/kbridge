impl OffsetRecord {
    pub fn new(topic: &str, consumer_group: &str, partition: usize, offset: usize) -> OffsetRecord {
        OffsetRecord {
            topic: topic.to_string(),
            partition,
            offset,
            consumer_group: consumer_group.to_string(),
        }
    }
}

#[derive(Debug)]
pub struct OffsetRecord {
    pub topic: String,
    pub partition: usize,
    pub offset: usize,
    pub consumer_group: String,
}

pub type OffsetSnapshot = Vec<OffsetRecord>;
