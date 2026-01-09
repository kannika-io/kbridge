use crate::prelude::*;

// A batch of offsets for a specific partition.
#[derive(Debug)]
pub(super) struct PartitionBatch {
    pub(super) topic: Topic,
    pub(super) partition: PartitionNumber,
    pub(super) consumers: Vec<(ConsumerGroup, Offset)>,
}

impl PartitionBatch {
    #[inline]
    pub fn offsets(&self) -> impl Iterator<Item = i64> + '_ {
        self.consumers.iter().map(|(_, offset)| *offset)
    }
}

impl From<&OffsetSnapshot> for Vec<PartitionBatch> {
    fn from(snapshot: &OffsetSnapshot) -> Self {
        use ahash::HashMapExt;
        // Pre-allocate capacity if snapshot size is known
        let capacity = snapshot.len().min(1024); // reasonable upper bound
        let mut partition_map: ahash::HashMap<
            (Topic, PartitionNumber),
            Vec<(ConsumerGroup, Offset)>,
        > = ahash::HashMap::with_capacity(capacity);

        for record in snapshot {
            partition_map
                .entry((record.topic.clone(), record.partition))
                .or_insert_with(Vec::new)
                .push((record.consumer_group.clone(), record.offset));
        }

        // Pre-allocate the result vector
        let mut result = Vec::with_capacity(partition_map.len());
        result.extend(
            partition_map
                .into_iter()
                .map(|((topic, partition), consumers)| PartitionBatch {
                    topic,
                    partition,
                    consumers,
                }),
        );
        result
    }
}

// #[derive(Clone, Default, Debug)]
// pub struct OffsetSnapshot {
//     records: Vec<OffsetRecord>,
// }
// #[derive(Debug, PartialEq, Eq, Clone, serde::Deserialize)]
// pub struct OffsetRecord {
//     pub consumer_group: String,
//     pub topic: String,
//     pub partition: i32,
//     pub offset: i64,
// }
