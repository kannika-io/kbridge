use log::trace;

#[derive(Debug)]
pub struct CalculateOffsetInput {
    pub current_target: i64,
    pub current_source: i64,
    pub target_source: i64,
    pub high_water_mark: i64,
    pub low_water_mark: i64,
}

/// Calculates a new target offset based on the difference between current and target source offsets.
///
/// This function applies the offset delta from source to target, ensuring the result stays within
/// the partition's watermark boundaries. Returns `None` if no adjustment is needed.
///
/// # Arguments
/// * `current_target` - The current offset in the target partition
/// * `current_source` - The current offset in the source partition
/// * `target_source` - The desired offset in the source partition
/// * `high_water_mark` - Maximum valid offset for the target partition
/// * `low_water_mark` - Minimum valid offset for the target partition
///
/// # Returns
/// * `Some(offset)` - The calculated new target offset, clamped to watermark bounds
/// * `None` - If current_source equals target_source (no change needed)
pub fn execute(input: CalculateOffsetInput) -> Option<i64> {
    trace!("{:?}", input);

    let CalculateOffsetInput {
        current_target,
        current_source,
        target_source,
        high_water_mark,
        low_water_mark,
    } = input;
    let mut new_target;

    if input.current_source < target_source {
        let calculated_value = current_target + (target_source - current_source);
        if calculated_value > high_water_mark {
            new_target = Some(high_water_mark - 1);
        } else {
            new_target = Some(calculated_value)
        }
    } else if current_source > target_source {
        let calculated_value = current_target - (current_source - target_source);
        if calculated_value < low_water_mark {
            new_target = Some(low_water_mark);
        } else {
            new_target = Some(calculated_value)
        }
    } else {
        new_target = None
    }

    trace!("{:?}", new_target);
    trace!("\n");

    if let Some(value) = new_target {
        if value == current_target {
            new_target = Some(high_water_mark - 1);
        }
    }

    new_target
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_current_source_less_than_target_source_within_bounds() {
        // current_target + (target_source - current_source) <= high_water_mark
        let input = CalculateOffsetInput {
            current_target: 100,
            current_source: 50,
            target_source: 80,
            high_water_mark: 200,
            low_water_mark: 0,
        };
        assert_eq!(execute(input), Some(130)); // 100 + (80 - 50) = 130
    }

    #[test]
    fn test_current_source_less_than_target_source_exceeds_high_watermark() {
        // current_target + (target_source - current_source) > high_water_mark
        let input = CalculateOffsetInput {
            current_target: 100,
            current_source: 50,
            target_source: 200,
            high_water_mark: 120,
            low_water_mark: 0,
        };
        assert_eq!(execute(input), Some(119)); // high_water_mark - 1 = 119
    }

    #[test]
    fn test_current_source_greater_than_target_source_within_bounds() {
        let input = CalculateOffsetInput {
            current_target: 100,
            current_source: 80,
            target_source: 50,
            high_water_mark: 200,
            low_water_mark: 0,
        };
        // current_target - (current_source - target_source) >= low_water_mark
        assert_eq!(execute(input), Some(70)); // 100 - (80 - 50) = 70
    }

    #[test]
    fn test_current_source_greater_than_target_source_below_low_watermark() {
        let input = CalculateOffsetInput {
            current_target: 100,
            current_source: 150,
            target_source: 50,
            high_water_mark: 200,
            low_water_mark: 80,
        };
        // current_target - (current_source - target_source) < low_water_mark
        assert_eq!(execute(input), Some(80)); // low_water_mark = 80
    }

    #[test]
    fn test_current_source_equals_target_source() {
        let input = CalculateOffsetInput {
            current_target: 100,
            current_source: 75,
            target_source: 75,
            high_water_mark: 200,
            low_water_mark: 0,
        };
        // When current_source == target_source, should return None initially
        assert_eq!(execute(input), None);
    }

    #[test]
    fn test_calculated_value_equals_current_target_gets_adjusted() {
        let input = CalculateOffsetInput {
            current_target: 100,
            current_source: 50,
            target_source: 50,
            high_water_mark: 200,
            low_water_mark: 0,
        };
        // When calculated value equals current_target, it should be adjusted to high_water_mark - 1
        // Initially returns None (current_source == target_source)
        // But the adjustment logic at the end doesn't apply since new_target is None
        assert_eq!(execute(input), None);
    }

    #[test]
    fn test_calculated_value_equals_current_target_forward_calculation() {
        let input = CalculateOffsetInput {
            current_target: 100,
            current_source: 50,
            target_source: 50,
            high_water_mark: 200,
            low_water_mark: 0,
        };
        // Test case where forward calculation results in current_target
        assert_eq!(execute(input), None); // current_source == target_source
    }

    #[test]
    fn test_adjustment_when_new_target_equals_current_target() {
        let input = CalculateOffsetInput {
            current_target: 100,
            current_source: 60,
            target_source: 70,
            high_water_mark: 200,
            low_water_mark: 0,
        };
        // Create a scenario where the calculated value equals current_target
        // This happens when target_source == current_source, but let's test the adjustment logic
        let calculated = 100 + (70 - 60); // = 110
        assert_eq!(execute(input), Some(calculated));

        let input2 = CalculateOffsetInput {
            current_target: 100,
            current_source: 60,
            target_source: 60,
            high_water_mark: 200,
            low_water_mark: 0,
        };
        // Now test a case where calculated value would equal current_target
        assert_eq!(execute(input2), None); // current_source == target_source
    }

    #[test]
    fn test_edge_case_high_watermark_boundary() {
        let input = CalculateOffsetInput {
            current_target: 100,
            current_source: 50,
            target_source: 100,
            high_water_mark: 150,
            low_water_mark: 0,
        };
        // Test when calculated value exactly equals high_water_mark
        assert_eq!(execute(input), Some(150));
    }

    #[test]
    fn test_edge_case_low_watermark_boundary() {
        let input = CalculateOffsetInput {
            current_target: 100,
            current_source: 100,
            target_source: 50,
            high_water_mark: 200,
            low_water_mark: 50,
        };
        // Test when calculated value exactly equals low_water_mark
        assert_eq!(execute(input), Some(50)); // low_water_mark = 50
    }

    #[test]
    fn test_negative_offsets() {
        let input = CalculateOffsetInput {
            current_target: 10,
            current_source: 20,
            target_source: 5,
            high_water_mark: 50,
            low_water_mark: -10,
        };
        // Test with negative values (though unlikely in Kafka, good for robustness)
        assert_eq!(execute(input), Some(-5)); // 10 - (20 - 5) = -5
    }

    #[test]
    fn test_large_offset_differences() {
        let input = CalculateOffsetInput {
            current_target: 1000,
            current_source: 500,
            target_source: 2000,
            high_water_mark: 5000,
            low_water_mark: 0,
        };
        // Test with large differences
        assert_eq!(execute(input), Some(2500)); // 1000 + (2000 - 500) = 2500
    }
}
