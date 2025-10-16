use bridge_core::OffsetRecord;

pub const ORDERS_1_TOPIC: &str = "orders-1";
pub const ORDERS_2_TOPIC: &str = "orders-2";
pub const ORDERS_3_TOPIC: &str = "orders-3";

pub const CONSUMER_GROUP_1: &str = "console-consumer";
pub const CONSUMER_GROUP_2: &str = "console-consumer-2";

pub const OFFSET_HEADER: &str = "Offset";

pub const SOURCE_BOOTSTRAP_SERVER: &str = "localhost:9092";
pub const TARGET_BOOTSTRAP_SERVER: &str = "localhost:9093";

pub fn get_expected_stub_offsets_filtered_by_topics(
    source_offsets: Vec<OffsetRecord>,
    topic_names: Vec<String>,
) -> Vec<OffsetRecord> {
    source_offsets
        .into_iter()
        .filter(|o| topic_names.contains(&o.topic))
        .collect()
}

pub fn get_expected_offsets_filtered_by_topic(
    source_offsets: Vec<OffsetRecord>,
    topic_name: &str,
) -> Vec<OffsetRecord> {
    source_offsets
        .into_iter()
        .filter(|o| o.topic == topic_name)
        .collect()
}

pub fn get_expected_source_offsets() -> [OffsetRecord; 6] {
    [
        OffsetRecord {
            topic: ORDERS_1_TOPIC.to_string(),
            partition: 0,
            offset: 563,
            consumer_group: CONSUMER_GROUP_2.to_string(),
        },
        OffsetRecord {
            topic: ORDERS_2_TOPIC.to_string(),
            partition: 0,
            offset: 772,
            consumer_group: CONSUMER_GROUP_2.to_string(),
        },
        OffsetRecord {
            topic: ORDERS_3_TOPIC.to_string(),
            partition: 0,
            offset: 802,
            consumer_group: CONSUMER_GROUP_2.to_string(),
        },
        OffsetRecord {
            topic: ORDERS_1_TOPIC.to_string(),
            partition: 0,
            offset: 1000,
            consumer_group: CONSUMER_GROUP_1.to_string(),
        },
        OffsetRecord {
            topic: ORDERS_2_TOPIC.to_string(),
            partition: 0,
            offset: 1300,
            consumer_group: CONSUMER_GROUP_1.to_string(),
        },
        OffsetRecord {
            topic: ORDERS_3_TOPIC.to_string(),
            partition: 0,
            offset: 500,
            consumer_group: CONSUMER_GROUP_1.to_string(),
        },
    ]
}

pub fn get_expected_stub_target_offsets() -> [OffsetRecord; 6] {
    [
        OffsetRecord {
            topic: ORDERS_1_TOPIC.to_string(),
            partition: 0,
            offset: 563,
            consumer_group: CONSUMER_GROUP_2.to_string(),
        },
        OffsetRecord {
            topic: ORDERS_2_TOPIC.to_string(),
            partition: 0,
            offset: 772,
            consumer_group: CONSUMER_GROUP_2.to_string(),
        },
        OffsetRecord {
            topic: ORDERS_3_TOPIC.to_string(),
            partition: 0,
            offset: 879,
            consumer_group: CONSUMER_GROUP_2.to_string(),
        },
        OffsetRecord {
            topic: ORDERS_1_TOPIC.to_string(),
            partition: 0,
            offset: 1000,
            consumer_group: CONSUMER_GROUP_1.to_string(),
        },
        OffsetRecord {
            topic: ORDERS_2_TOPIC.to_string(),
            partition: 0,
            offset: 1300,
            consumer_group: CONSUMER_GROUP_1.to_string(),
        },
        OffsetRecord {
            topic: ORDERS_3_TOPIC.to_string(),
            partition: 0,
            offset: 879,
            consumer_group: CONSUMER_GROUP_1.to_string(),
        },
    ]
}
