use log::warn;


pub fn execute(
    current_target: i64,
    current_source: i64,
    target_source: i64,
    high_water_mark: i64,
    low_water_mark: i64,
) -> Option<i64> {
    let mut new_target = None;
    warn!(
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

    warn!("{:?}", new_target);
    warn!("\n");

    if let Some(value) = new_target {
        if value == current_target {
            new_target = Some(high_water_mark - 1);
        }
    }

    new_target
}
mod #[cfg(test)]
mod tests {
    use super::*;

       #[test]
       fn var() {
        !todo()
    }
}

