use crate::{
    ConsumerGroup, ConsumerGroupRecord, Offset, OffsetRecord, OffsetSnapshot, TransformationRecord,
};
use rdkafka::Message;
use std::collections::HashMap;
use crate::commands::calculate_target_offsets::errors::TransformationError;
use crate::commands::calculate_target_offsets::transform::consumer_group_offset_mapping::insert_offset_transformations;

pub fn try_find_missing_offsets(
    message: &rdkafka::message::BorrowedMessage,
    source_offset: i64,
    source_offsets: &OffsetSnapshot,
    transformations: &mut HashMap<ConsumerGroup, Vec<TransformationRecord>>,
    missing_offsets: &mut Vec<ConsumerGroupRecord>,
    nearest_offsets: &mut HashMap<ConsumerGroupRecord, Offset>,
) -> Result<(), TransformationError> {
    let source_offsets_for_partition: Vec<&OffsetRecord> = source_offsets
        .iter()
        .filter(|o| o.partition == message.partition() && o.topic == message.topic())
        .collect();

    for offset in source_offsets_for_partition.iter() {
        if offset.offset == source_offset {
            insert_offset_transformations(
                transformations,
                &offset.offset,
                &message.offset(),
                message.topic().to_string(),
                message.partition(),
                offset.consumer_group.clone(),
            )?;

            missing_offsets.retain(|o| {
                !(o.0 == offset.consumer_group
                    && o.1 == offset.topic
                    && o.2 == offset.partition
                    && o.3 == offset.offset)
            });
        } else if !nearest_offsets.contains_key(&(
            offset.consumer_group.clone(),
            offset.topic.clone(),
            offset.partition,
            offset.offset,
        )) && source_offset > offset.offset
        {
            nearest_offsets.insert(
                (
                    offset.consumer_group.clone(),
                    offset.topic.clone(),
                    offset.partition,
                    offset.offset,
                ),
                source_offset,
            );
        }
    }

    Ok(())
}
