use std::collections::HashMap;
use log::info;
use crate::transform::transformation_errors::OffsetMappingTransformationError;


pub fn insert_offset_transformations(
    transformations: &mut HashMap<String, HashMap<i64, i64>>,
    source_offset_from_message: &i64,
    current_offset_from_message: &i64,
    topic: &str,
) -> Result<(), OffsetMappingTransformationError> {
        match transformations.get_mut(topic)
        {
            Some(transformations) => {
                match transformations.get(source_offset_from_message) {
                    Some(existing_target_offset) => {
                        // Source offset already mapped - this could indicate duplicate processing
                        if existing_target_offset != current_offset_from_message {
                            return Err(
                                OffsetMappingTransformationError::SourceOffsetAlreadyPresent {
                                    source_offset: *source_offset_from_message,
                                    target_offset: *current_offset_from_message,
                                    previous_target_offset: *existing_target_offset,
                                },
                            );
                        }
                    }
                    None => {
                        // New mapping
                        transformations.insert(*source_offset_from_message, *current_offset_from_message);
                        info!(
                            "Mapped source offset {source_offset_from_message} to target offset {current_offset_from_message}"
                        );
                    }
                };
            },
            None => {
                let mut offsets : HashMap<i64, i64> = HashMap::new();
                offsets.insert(*source_offset_from_message, *current_offset_from_message);
                transformations.insert(topic.to_string(), offsets);
                info!(
                    "Mapped source offset {source_offset_from_message} to target offset {current_offset_from_message}"
                );
            }
        }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_offset_transformations_new_topic_and_mapping() {
        let mut transformations = HashMap::new();
        let source_offset_from_message = &100i64;
        let current_offset_from_message = &500i64;
        let topic = "test-topic";

        let result = insert_offset_transformations(
            &mut transformations,
            source_offset_from_message,
            current_offset_from_message,
            topic,
        );

        assert!(result.is_ok());
        assert_eq!(transformations.len(), 1);
        assert!(transformations.contains_key(topic));
        assert_eq!(transformations[topic][&100i64], 500i64);
    }

    #[test]
    fn test_insert_offset_transformations_existing_topic_new_mapping() {
        let mut transformations = HashMap::new();
        let mut existing_offsets = HashMap::new();
        existing_offsets.insert(50i64, 250i64);
        transformations.insert("test-topic".to_string(), existing_offsets);

        let source_offset_from_message = &100i64;
        let current_offset_from_message = &500i64;
        let topic = "test-topic";

        let result = insert_offset_transformations(
            &mut transformations,
            source_offset_from_message,
            current_offset_from_message,
            topic,
        );

        assert!(result.is_ok());
        assert_eq!(transformations.len(), 1);
        assert_eq!(transformations[topic].len(), 2);
        assert_eq!(transformations[topic][&50i64], 250i64);
        assert_eq!(transformations[topic][&100i64], 500i64);
    }

    #[test]
    fn test_insert_offset_transformations_duplicate_mapping_same_target() {
        let mut transformations = HashMap::new();
        let mut existing_offsets = HashMap::new();
        existing_offsets.insert(100i64, 500i64);
        transformations.insert("test-topic".to_string(), existing_offsets);

        let source_offset_from_message = &100i64;
        let current_offset_from_message = &500i64;
        let topic = "test-topic";

        let result = insert_offset_transformations(
            &mut transformations,
            source_offset_from_message,
            current_offset_from_message,
            topic,
        );

        assert!(result.is_ok());
        assert_eq!(transformations[topic][&100i64], 500i64);
    }

    #[test]
    fn test_insert_offset_transformations_duplicate_mapping_different_target() {
        let mut transformations = HashMap::new();
        let mut existing_offsets = HashMap::new();
        existing_offsets.insert(100i64, 500i64);
        transformations.insert("test-topic".to_string(), existing_offsets);

        let source_offset_from_message = &100i64;
        let current_offset_from_message = &600i64;
        let topic = "test-topic";

        let result = insert_offset_transformations(
            &mut transformations,
            source_offset_from_message,
            current_offset_from_message,
            topic,
        );

        assert!(result.is_err());
        match result.unwrap_err() {
            OffsetMappingTransformationError::SourceOffsetAlreadyPresent {
                source_offset,
                target_offset,
                previous_target_offset,
            } => {
                assert_eq!(source_offset, 100i64);
                assert_eq!(target_offset, 600i64);
                assert_eq!(previous_target_offset, 500i64);
            }
        }
    }

    #[test]
    fn test_insert_offset_transformations_multiple_topics() {
        let mut transformations = HashMap::new();
        let mut existing_offsets = HashMap::new();
        existing_offsets.insert(50i64, 250i64);
        transformations.insert("topic1".to_string(), existing_offsets);

        let source_offset_from_message = &100i64;
        let current_offset_from_message = &500i64;
        let topic = "topic2";

        let result = insert_offset_transformations(
            &mut transformations,
            source_offset_from_message,
            current_offset_from_message,
            topic,
        );

        assert!(result.is_ok());
        assert_eq!(transformations.len(), 2);
        assert!(transformations.contains_key("topic1"));
        assert!(transformations.contains_key("topic2"));
        assert_eq!(transformations["topic1"][&50i64], 250i64);
        assert_eq!(transformations["topic2"][&100i64], 500i64);
    }
}
