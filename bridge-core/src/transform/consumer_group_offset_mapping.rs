use super::errors::{OffsetMappingTransformationError, TransformationError};
use crate::transform::{ConsumerGroup, Offset, Partition, Topic, TransformationRecord};
use log::info;
use std::collections::HashMap;

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

#[cfg(test)]
mod tests {
    use super::*;

    //    #[test]
    //    fn test_insert_offset_transformations_new_topic_and_mapping() {
    //        let mut transformations = HashMap::new();
    //        let source_offset_from_message = &100i64;
    //        let current_offset_from_message = &500i64;
    //        let topic = "test-topic".to_string();
    //        let partition = 1;
    //        let key = (topic.clone(), partition);
    //
    //        let result = insert_offset_transformations(
    //            &mut transformations,
    //            source_offset_from_message,
    //            current_offset_from_message,
    //            topic,
    //            partition,
    //        );
    //
    //        assert!(result.is_ok());
    //        assert_eq!(transformations.len(), 1);
    //        assert!(transformations.contains_key(&key));
    //        assert_eq!(transformations[&key][&100i64], 500i64);
    //    }
    //
    //    #[test]
    //    fn test_insert_offset_transformations_existing_topic_new_mapping() {
    //        let mut transformations = HashMap::new();
    //        let mut existing_offsets = HashMap::new();
    //        existing_offsets.insert(50i64, 250i64);
    //        let topic = "test-topic".to_string();
    //        let partition = 1;
    //        let key = (topic.clone(), partition);
    //        transformations.insert(key.clone(), existing_offsets);
    //
    //        let source_offset_from_message = &100i64;
    //        let current_offset_from_message = &500i64;
    //
    //        let result = insert_offset_transformations(
    //            &mut transformations,
    //            source_offset_from_message,
    //            current_offset_from_message,
    //            topic,
    //            partition,
    //        );
    //
    //        assert!(result.is_ok());
    //        assert_eq!(transformations.len(), 1);
    //
    //        assert_eq!(transformations[&key].len(), 2);
    //        assert_eq!(transformations[&key][&50i64], 250i64);
    //        assert_eq!(transformations[&key][&100i64], 500i64);
    //    }
    //
    //    #[test]
    //    fn test_insert_offset_transformations_duplicate_mapping_same_target() {
    //        let mut transformations = HashMap::new();
    //        let mut existing_offsets = HashMap::new();
    //        existing_offsets.insert(100i64, 500i64);
    //        let partition = 1;
    //        let topic = "test-topic".to_string();
    //        let key = (topic.clone(), partition);
    //        transformations.insert(key.clone(), existing_offsets);
    //
    //        let source_offset_from_message = &100i64;
    //        let current_offset_from_message = &500i64;
    //
    //        let result = insert_offset_transformations(
    //            &mut transformations,
    //            source_offset_from_message,
    //            current_offset_from_message,
    //            topic,
    //            partition,
    //        );
    //
    //        assert!(result.is_ok());
    //        assert_eq!(transformations[&key][&100i64], 500i64);
    //    }
    //
    //    #[test]
    //    fn test_insert_offset_transformations_duplicate_mapping_different_target() {
    //        let mut transformations = HashMap::new();
    //        let mut existing_offsets = HashMap::new();
    //        existing_offsets.insert(100i64, 500i64);
    //        let topic = "test-topic".to_string();
    //        let partition = 1;
    //        let key = (topic, partition);
    //        transformations.insert(key.clone(), existing_offsets);
    //
    //        let source_offset_from_message = &100i64;
    //        let current_offset_from_message = &600i64;
    //        let topic = "test-topic".to_string();
    //
    //        let result = insert_offset_transformations(
    //            &mut transformations,
    //            source_offset_from_message,
    //            current_offset_from_message,
    //            topic,
    //            partition,
    //        );
    //
    //        assert!(result.is_err());
    //        match result.unwrap_err() {
    //            OffsetMappingTransformationError::SourceOffsetAlreadyPresent {
    //                source_offset,
    //                target_offset,
    //                previous_target_offset,
    //            } => {
    //                assert_eq!(source_offset, 100i64);
    //                assert_eq!(target_offset, 600i64);
    //                assert_eq!(previous_target_offset, 500i64);
    //            }
    //        }
    //    }
    //
    //    #[test]
    //    fn test_insert_offset_transformations_multiple_topics() {
    //        let mut transformations = HashMap::new();
    //        let mut existing_offsets_1 = HashMap::new();
    //        existing_offsets_1.insert(50i64, 250i64);
    //        let topic_1 = "topic1".to_string();
    //        let partition_1 = 1;
    //        let key_1 = (topic_1, partition_1);
    //
    //        transformations.insert(key_1.clone(), existing_offsets_1);
    //
    //        let topic_2 = "topic2".to_string();
    //        let partition_2 = 0;
    //        let key_2 = (topic_2.clone(), partition_2);
    //
    //        let source_offset_from_message = &100i64;
    //        let current_offset_from_message = &500i64;
    //
    //        let result = insert_offset_transformations(
    //            &mut transformations,
    //            source_offset_from_message,
    //            current_offset_from_message,
    //            topic_2,
    //            partition_2,
    //        );
    //
    //        assert!(result.is_ok());
    //        assert_eq!(transformations.len(), 2);
    //        assert!(transformations.contains_key(&key_1));
    //        assert!(transformations.contains_key(&key_2));
    //        assert_eq!(transformations[&key_1][&50i64], 250i64);
    //        assert_eq!(transformations[&key_2][&100i64], 500i64);
    //    }
}
