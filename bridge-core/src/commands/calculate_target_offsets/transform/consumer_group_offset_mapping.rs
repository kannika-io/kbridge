use crate::commands::calculate_target_offsets::errors::{
    OffsetMappingTransformationError, TransformationError,
};
use crate::{ConsumerGroup, Offset, Partition, Topic, TransformationRecord};
use log::info;
use std::collections::HashMap;

/// Inserts a new offset transformation record into the transformations map.
///
/// This function tracks the mapping between source offsets (from the original Kafka cluster)
/// and target offsets (from the destination Kafka cluster) for a specific consumer group,
/// topic, and partition combination.
///
/// # Arguments
///
/// * `transformations` - A mutable reference to the transformations map that stores
///   transformation records grouped by consumer group
/// * `source_offset_from_message` - The original offset from the source Kafka cluster
/// * `current_offset_from_message` - The corresponding offset in the target Kafka cluster
/// * `topic` - The Kafka topic name
/// * `partition` - The partition number within the topic
/// * `consumer_group` - The consumer group identifier
///
/// # Returns
///
/// * `Ok(())` - If the transformation was successfully inserted
/// * `Err(OffsetMappingTransformationError)` - If there was an error during insertion
///
/// # Behavior
///
/// If the consumer group already exists in the transformations map, the new transformation
/// record is appended to the existing vector. If the consumer group doesn't exist, a new
/// entry is created with the transformation record as the first element.
///
/// # Example
///
/// ```rust
/// use std::collections::HashMap;
/// use bridge_core::{ConsumerGroup, TransformationRecord};
///
/// let mut transformations: HashMap<ConsumerGroup, Vec<TransformationRecord>> = HashMap::new();
/// let result = insert_offset_transformations(
///     &mut transformations,
///     &100,  // source offset
///     &200,  // target offset
///     "my-topic".to_string(),
///     0,     // partition
///     "my-consumer-group".to_string(),
/// );
/// assert!(result.is_ok());
/// ```
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

/// Handles missing offset transformations by attempting to find nearest available offsets.
///
/// When offset transformations are missing (i.e., a source offset doesn't have a corresponding
/// target offset), this function attempts to resolve them using the nearest available offsets.
/// This is useful in scenarios where not all offsets from the source cluster have exact matches
/// in the target cluster, often due to data differences or partial replication.
///
/// # Arguments
///
/// * `transformations` - A map of existing offset transformations grouped by consumer group.
///   Each transformation record contains (Topic, Partition, SourceOffset, TargetOffset)
/// * `missing_offsets` - A vector of offsets that couldn't be found during the initial
///   transformation process. Each entry contains (ConsumerGroup, Topic, Partition, SourceOffset)
/// * `nearest_offsets` - A map that provides the nearest available target offset for each
///   missing source offset. The key is (ConsumerGroup, Topic, Partition, SourceOffset) and
///   the value is the nearest target offset
///
/// # Returns
///
/// * `Ok(HashMap<ConsumerGroup, Vec<TransformationRecord>>)` - The updated transformations map
///   with missing offsets resolved using nearest available offsets
/// * `Err(TransformationError::MissingOffsets)` - If some offsets still cannot be resolved
///   even after attempting to use nearest offsets
///
/// # Behavior
///
/// 1. If there are no missing offsets, returns the original transformations unchanged
/// 2. For each missing offset, attempts to find a nearest offset in the provided map
/// 3. If a nearest offset is found, adds a new transformation record to the appropriate consumer group
/// 4. If no nearest offset is available, adds the offset to the "still missing" list
/// 5. Returns an error if any offsets remain unresolved after processing
///
/// # Logging
///
/// The function logs the missing offsets at info level for debugging purposes.
///
/// # Example
///
/// ```rust
/// use std::collections::HashMap;
/// use bridge_core::{ConsumerGroup, TransformationRecord};
///
/// let transformations = HashMap::new();
/// let missing_offsets = vec![
///     ("group1".to_string(), "topic1".to_string(), 0, 100),
/// ];
/// let mut nearest_offsets = HashMap::new();
/// nearest_offsets.insert(
///     ("group1".to_string(), "topic1".to_string(), 0, 100),
///     200, // nearest target offset
/// );
///
/// let result = handle_missing_offsets(transformations, missing_offsets, nearest_offsets);
/// assert!(result.is_ok());
/// ```
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
