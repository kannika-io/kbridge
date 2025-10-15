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
    use std::collections::HashMap;

    #[test]
    fn test_insert_offset_transformations_new_consumer_group() {
        let mut transformations: HashMap<ConsumerGroup, Vec<TransformationRecord>> = HashMap::new();
        let consumer_group = "test-group".to_string();
        let topic = "test-topic".to_string();
        let partition = 0;
        let source_offset = 100;
        let target_offset = 200;

        let result = insert_offset_transformations(
            &mut transformations,
            &source_offset,
            &target_offset,
            topic.clone(),
            partition,
            consumer_group.clone(),
        );

        assert!(result.is_ok());
        assert_eq!(transformations.len(), 1);
        assert!(transformations.contains_key(&consumer_group));
        
        let records = transformations.get(&consumer_group).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0], (topic, partition, source_offset, target_offset));
    }

    #[test]
    fn test_insert_offset_transformations_existing_consumer_group() {
        let mut transformations: HashMap<ConsumerGroup, Vec<TransformationRecord>> = HashMap::new();
        let consumer_group = "test-group".to_string();
        
        // Insert initial transformation
        transformations.insert(
            consumer_group.clone(),
            vec![("topic1".to_string(), 0, 50, 100)],
        );

        let topic = "topic2".to_string();
        let partition = 1;
        let source_offset = 150;
        let target_offset = 250;

        let result = insert_offset_transformations(
            &mut transformations,
            &source_offset,
            &target_offset,
            topic.clone(),
            partition,
            consumer_group.clone(),
        );

        assert!(result.is_ok());
        assert_eq!(transformations.len(), 1);
        
        let records = transformations.get(&consumer_group).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0], ("topic1".to_string(), 0, 50, 100));
        assert_eq!(records[1], (topic, partition, source_offset, target_offset));
    }

    #[test]
    fn test_insert_offset_transformations_multiple_consumer_groups() {
        let mut transformations: HashMap<ConsumerGroup, Vec<TransformationRecord>> = HashMap::new();
        
        // Insert for first consumer group
        let result1 = insert_offset_transformations(
            &mut transformations,
            &100,
            &200,
            "topic1".to_string(),
            0,
            "group1".to_string(),
        );

        // Insert for second consumer group
        let result2 = insert_offset_transformations(
            &mut transformations,
            &300,
            &400,
            "topic2".to_string(),
            1,
            "group2".to_string(),
        );

        assert!(result1.is_ok());
        assert!(result2.is_ok());
        assert_eq!(transformations.len(), 2);
        assert!(transformations.contains_key("group1"));
        assert!(transformations.contains_key("group2"));
        
        let group1_records = transformations.get("group1").unwrap();
        let group2_records = transformations.get("group2").unwrap();
        assert_eq!(group1_records.len(), 1);
        assert_eq!(group2_records.len(), 1);
    }

    #[test]
    fn test_handle_missing_offsets_empty_missing_offsets() {
        let transformations: HashMap<ConsumerGroup, Vec<TransformationRecord>> = HashMap::new();
        let missing_offsets = vec![];
        let nearest_offsets = HashMap::new();

        let result = handle_missing_offsets(transformations, missing_offsets, nearest_offsets);

        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0);
    }

    #[test]
    fn test_handle_missing_offsets_with_nearest_offsets_available() {
        let mut transformations: HashMap<ConsumerGroup, Vec<TransformationRecord>> = HashMap::new();
        transformations.insert(
            "existing-group".to_string(),
            vec![("topic1".to_string(), 0, 100, 200)],
        );

        let missing_offsets = vec![
            ("group1".to_string(), "topic1".to_string(), 0, 150),
            ("group2".to_string(), "topic2".to_string(), 1, 250),
        ];

        let mut nearest_offsets = HashMap::new();
        nearest_offsets.insert(
            ("group1".to_string(), "topic1".to_string(), 0, 150),
            175,
        );
        nearest_offsets.insert(
            ("group2".to_string(), "topic2".to_string(), 1, 250),
            275,
        );

        let result = handle_missing_offsets(transformations, missing_offsets, nearest_offsets);

        assert!(result.is_ok());
        let final_transformations = result.unwrap();
        assert_eq!(final_transformations.len(), 3);
        
        // Check existing group is preserved
        assert!(final_transformations.contains_key("existing-group"));
        
        // Check new groups were added
        assert!(final_transformations.contains_key("group1"));
        assert!(final_transformations.contains_key("group2"));
        
        let group1_records = final_transformations.get("group1").unwrap();
        let group2_records = final_transformations.get("group2").unwrap();
        
        assert_eq!(group1_records.len(), 1);
        assert_eq!(group1_records[0], ("topic1".to_string(), 0, 150, 175));
        
        assert_eq!(group2_records.len(), 1);
        assert_eq!(group2_records[0], ("topic2".to_string(), 1, 250, 275));
    }

    #[test]
    fn test_handle_missing_offsets_add_to_existing_consumer_group() {
        let mut transformations: HashMap<ConsumerGroup, Vec<TransformationRecord>> = HashMap::new();
        transformations.insert(
            "test-group".to_string(),
            vec![("topic1".to_string(), 0, 100, 200)],
        );

        let missing_offsets = vec![
            ("test-group".to_string(), "topic2".to_string(), 1, 300),
        ];

        let mut nearest_offsets = HashMap::new();
        nearest_offsets.insert(
            ("test-group".to_string(), "topic2".to_string(), 1, 300),
            350,
        );

        let result = handle_missing_offsets(transformations, missing_offsets, nearest_offsets);

        assert!(result.is_ok());
        let final_transformations = result.unwrap();
        assert_eq!(final_transformations.len(), 1);
        
        let test_group_records = final_transformations.get("test-group").unwrap();
        assert_eq!(test_group_records.len(), 2);
        assert_eq!(test_group_records[0], ("topic1".to_string(), 0, 100, 200));
        assert_eq!(test_group_records[1], ("topic2".to_string(), 1, 300, 350));
    }

    #[test]
    fn test_handle_missing_offsets_no_nearest_offsets_available() {
        let transformations: HashMap<ConsumerGroup, Vec<TransformationRecord>> = HashMap::new();
        let missing_offsets = vec![
            ("group1".to_string(), "topic1".to_string(), 0, 150),
            ("group2".to_string(), "topic2".to_string(), 1, 250),
        ];
        let nearest_offsets = HashMap::new(); // No nearest offsets available

        let result = handle_missing_offsets(transformations, missing_offsets.clone(), nearest_offsets);

        assert!(result.is_err());
        match result.unwrap_err() {
            TransformationError::MissingOffsets(still_missing) => {
                assert_eq!(still_missing.len(), 2);
                assert!(still_missing.contains(&("group1".to_string(), "topic1".to_string(), 0, 150)));
                assert!(still_missing.contains(&("group2".to_string(), "topic2".to_string(), 1, 250)));
            }
            _ => panic!("Expected MissingOffsets error"),
        }
    }

    #[test]
    fn test_handle_missing_offsets_partial_nearest_offsets() {
        let transformations: HashMap<ConsumerGroup, Vec<TransformationRecord>> = HashMap::new();
        let missing_offsets = vec![
            ("group1".to_string(), "topic1".to_string(), 0, 150),
            ("group2".to_string(), "topic2".to_string(), 1, 250),
        ];

        let mut nearest_offsets = HashMap::new();
        // Only provide nearest offset for group1, not group2
        nearest_offsets.insert(
            ("group1".to_string(), "topic1".to_string(), 0, 150),
            175,
        );

        let result = handle_missing_offsets(transformations, missing_offsets, nearest_offsets);

        assert!(result.is_err());
        match result.unwrap_err() {
            TransformationError::MissingOffsets(still_missing) => {
                assert_eq!(still_missing.len(), 1);
                assert_eq!(still_missing[0], ("group2".to_string(), "topic2".to_string(), 1, 250));
            }
            _ => panic!("Expected MissingOffsets error"),
        }
    }
}
