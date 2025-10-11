use log::info;
use std::collections::HashMap;
use crate::{ConsumerGroup, Offset, Partition, Topic, TransformationRecord};
use crate::commands::calculate_target_offsets::errors::{OffsetMappingTransformationError, TransformationError};

pub fn insert_offset_transformations(
    transformations: &mut HashMap<ConsumerGroup, Vec<TransformationRecord>>,
    source_offset_from_message: &Offset,
    current_offset_from_message: &Offset,
    topic: Topic,
    partition: Partition,
    consumer_group: ConsumerGroup,
) -> Result<(), OffsetMappingTransformationError> {
    match transformations.get_mut(&consumer_group) {
        Some(transformations_for_consumer_group) => {
            transformations_for_consumer_group.push((
                topic,
                partition,
                *source_offset_from_message,
                *current_offset_from_message,
            ));
        }
        None => {
            let mut offsets: HashMap<i64, i64> = HashMap::new();

            offsets.insert(*source_offset_from_message, *current_offset_from_message);
            transformations.insert(
                consumer_group,
                vec![(
                    topic,
                    partition,
                    *source_offset_from_message,
                    *current_offset_from_message,
                )],
            );
        }
    }
    Ok(())
}

pub fn handle_missing_offsets(
    mut transformations: HashMap<ConsumerGroup, Vec<(Topic, Partition, Offset, Offset)>>,
    missing_offsets: Vec<(ConsumerGroup, Topic, Partition, Offset)>,
    nearest_offsets: HashMap<(ConsumerGroup, Topic, Partition, Offset), Offset>,
) -> Result<HashMap<ConsumerGroup, Vec<TransformationRecord>>, TransformationError> {
    if missing_offsets.is_empty() {
        return Ok(transformations);
    }

    info!("{missing_offsets:?}");

    let mut still_missing_offsets = vec![];
    for missing_offset in missing_offsets {
        let nearest_offset_option: Option<&i64> = nearest_offsets.get(&(
            missing_offset.0.clone(),
            missing_offset.1.clone(),
            missing_offset.2,
            missing_offset.3,
        ));

        if let Some(nearest_offset) = nearest_offset_option {
            let consumer_group_transformation = transformations.get_mut(&missing_offset.0.clone());
            if let Some(transformation) = consumer_group_transformation {
                transformation.push((
                    missing_offset.1.clone(),
                    missing_offset.2,
                    missing_offset.3,
                    *nearest_offset,
                ));
            } else {
                transformations.insert(
                    missing_offset.0.clone(),
                    vec![(
                        missing_offset.1.clone(),
                        missing_offset.2,
                        missing_offset.3,
                        *nearest_offset,
                    )],
                );
            }
        } else {
            still_missing_offsets.push(missing_offset);
        }
    }

    if still_missing_offsets.is_empty() {
        Ok(transformations)
    } else {
        Err(TransformationError::MissingOffsets(still_missing_offsets))
    }
}