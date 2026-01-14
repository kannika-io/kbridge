use rdkafka::{
    ClientConfig, Offset, TopicPartitionList,
    consumer::{BaseConsumer, CommitMode, Consumer},
};
use tracing::trace;

use crate::OffsetSnapshot;
use crate::commands::apply_target_offsets::errors::ApplyOffsetsError;

pub async fn apply_target_offsets(
    consumer_config: &mut ClientConfig,
    offset_snapshot: &OffsetSnapshot,
) -> Result<(), ApplyOffsetsError> {
    for (group, snapshot) in offset_snapshot.group_by_consumer() {
        consumer_config.set("group.id", &group);
        let consumer: BaseConsumer = consumer_config.create()?;

        let mut topic_partition_list = TopicPartitionList::new();

        for record in snapshot.iter() {
            topic_partition_list.add_partition_offset(
                &record.topic,
                record.partition,
                Offset::Offset(record.offset),
            )?;
        }

        trace!("Commiting partition list {:#?}", topic_partition_list);
        consumer.commit(&topic_partition_list, CommitMode::Sync)?;
    }
    Ok(())
}
