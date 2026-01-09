use std::fmt;

#[derive(Debug, Default)]
pub struct PartitionScanMetrics {
    pub seeks: u64,
    pub messages: u64,
}

impl fmt::Display for PartitionScanMetrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "seeks: {}, messages: {}", self.seeks, self.messages)
    }
}
