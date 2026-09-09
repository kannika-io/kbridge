//! Test utilities for encoding records of the `__consumer_offsets` topic.

/// Encodes an `OffsetCommitKey` (key version 0 or 1).
pub fn encode_offset_commit_key(version: i16, group: &str, topic: &str, partition: i32) -> Vec<u8> {
    let mut buf = version.to_be_bytes().to_vec();
    encode_string(&mut buf, group);
    encode_string(&mut buf, topic);
    buf.extend_from_slice(&partition.to_be_bytes());
    buf
}

/// Encodes a `GroupMetadataKey` (key version 2).
pub fn encode_group_metadata_key(group: &str) -> Vec<u8> {
    let mut buf = 2i16.to_be_bytes().to_vec();
    encode_string(&mut buf, group);
    buf
}

/// Encodes an `OffsetCommitValue` (value version 3):
/// offset, leader epoch, metadata and commit timestamp.
pub fn encode_offset_commit_value(offset: i64) -> Vec<u8> {
    let mut buf = 3i16.to_be_bytes().to_vec();
    buf.extend_from_slice(&offset.to_be_bytes());
    buf.extend_from_slice(&0i32.to_be_bytes());
    encode_string(&mut buf, "");
    buf.extend_from_slice(&0i64.to_be_bytes());
    buf
}

fn encode_string(buf: &mut Vec<u8>, value: &str) {
    buf.extend_from_slice(&(value.len() as i16).to_be_bytes());
    buf.extend_from_slice(value.as_bytes());
}
