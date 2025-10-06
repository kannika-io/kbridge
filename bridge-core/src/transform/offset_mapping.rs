use std::collections::HashMap;
use log::info;
use crate::transform::transformation_errors::OffsetMappingTransformationError;


pub fn insert_offset_transformations(
    transformations: &mut HashMap<String, HashMap<i64, i64>>,
    source_offsets: &[&i64],
    source_offset_from_message: &i64,
    current_offset_from_message: &i64,
    topic: &str,
) -> Result<(), OffsetMappingTransformationError> {
    if source_offsets.contains(&source_offset_from_message) {
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
    }
    Ok(())
}
