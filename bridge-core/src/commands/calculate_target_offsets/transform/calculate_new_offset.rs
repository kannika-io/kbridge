use log::trace;

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
pub fn execute(
    current_target: i64,
    current_source: i64,
    target_source: i64,
    high_water_mark: i64,
    low_water_mark: i64,
) -> Option<i64> {
    let mut new_target = None;
    trace!(
        "{}, {}, {}, {}, {}",
        current_target, current_source, target_source, high_water_mark, low_water_mark
    );
    if current_source < target_source {
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
        let result = execute(100, 50, 80, 200, 0);
        assert_eq!(result, Some(130)); // 100 + (80 - 50) = 130
    }

    #[test]
    fn test_current_source_less_than_target_source_exceeds_high_watermark() {
        // current_target + (target_source - current_source) > high_water_mark
        let result = execute(100, 50, 200, 120, 0);
        assert_eq!(result, Some(119)); // high_water_mark - 1 = 119
    }

    #[test]
    fn test_current_source_greater_than_target_source_within_bounds() {
        // current_target - (current_source - target_source) >= low_water_mark
        let result = execute(100, 80, 50, 200, 0);
        assert_eq!(result, Some(70)); // 100 - (80 - 50) = 70
    }

    #[test]
    fn test_current_source_greater_than_target_source_below_low_watermark() {
        // current_target - (current_source - target_source) < low_water_mark
        let result = execute(100, 150, 50, 200, 80);
        assert_eq!(result, Some(80)); // low_water_mark = 80
    }

    #[test]
    fn test_current_source_equals_target_source() {
        // When current_source == target_source, should return None initially
        let result = execute(100, 75, 75, 200, 0);
        assert_eq!(result, None);
    }

    #[test]
    fn test_calculated_value_equals_current_target_gets_adjusted() {
        // When calculated value equals current_target, it should be adjusted to high_water_mark - 1
        let result = execute(100, 50, 50, 200, 0);
        // Initially returns None (current_source == target_source)
        // But the adjustment logic at the end doesn't apply since new_target is None
        assert_eq!(result, None);
    }

    #[test]
    fn test_calculated_value_equals_current_target_forward_calculation() {
        // Test case where forward calculation results in current_target
        let result = execute(100, 50, 50, 200, 0);
        assert_eq!(result, None); // current_source == target_source
    }

    #[test]
    fn test_adjustment_when_new_target_equals_current_target() {
        // Create a scenario where the calculated value equals current_target
        // This happens when target_source == current_source, but let's test the adjustment logic
        let result = execute(100, 60, 70, 200, 0);
        let calculated = 100 + (70 - 60); // = 110
        assert_eq!(result, Some(110));

        // Now test a case where calculated value would equal current_target
        let result2 = execute(100, 60, 60, 200, 0);
        assert_eq!(result2, None); // current_source == target_source
    }

    #[test]
    fn test_edge_case_high_watermark_boundary() {
        // Test when calculated value exactly equals high_water_mark
        let result = execute(100, 50, 100, 150, 0);
        assert_eq!(result, Some(149)); // high_water_mark - 1 = 149
    }

    #[test]
    fn test_edge_case_low_watermark_boundary() {
        // Test when calculated value exactly equals low_water_mark
        let result = execute(100, 100, 50, 200, 50);
        assert_eq!(result, Some(50)); // low_water_mark = 50
    }

    #[test]
    fn test_negative_offsets() {
        // Test with negative values (though unlikely in Kafka, good for robustness)
        let result = execute(10, 20, 5, 50, -10);
        assert_eq!(result, Some(-5)); // 10 - (20 - 5) = -5
    }

    #[test]
    fn test_large_offset_differences() {
        // Test with large differences
        let result = execute(1000, 500, 2000, 5000, 0);
        assert_eq!(result, Some(2500)); // 1000 + (2000 - 500) = 2500
    }
}
