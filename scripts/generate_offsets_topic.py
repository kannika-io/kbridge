#!/usr/bin/env python3
"""Generate a __consumer_offsets-style topic for testing `fetch --offsets-topic`.

Usage: generate_offsets_topic.py [broker] [topic] [partitions] [messages_per_partition]
Defaults: localhost:9092 consumer-offsets-big 12 1000000

Requires: pip install confluent-kafka
"""
import struct
import sys

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

BROKER = sys.argv[1] if len(sys.argv) > 1 else "localhost:9092"
TOPIC = sys.argv[2] if len(sys.argv) > 2 else "consumer-offsets-big"
PARTITIONS = int(sys.argv[3]) if len(sys.argv) > 3 else 12
PER_PARTITION = int(sys.argv[4]) if len(sys.argv) > 4 else 1_000_000

GROUPS = 100          # distinct consumer groups
SOURCE_PARTITIONS = 50  # distinct partitions of the (fake) source topic


def encode_string(s: str) -> bytes:
    b = s.encode()
    return struct.pack(">h", len(b)) + b


def offset_commit_key(group: str, topic: str, partition: int) -> bytes:
    # key version 1: group, topic, partition
    return struct.pack(">h", 1) + encode_string(group) + encode_string(topic) \
        + struct.pack(">i", partition)


def offset_commit_value(offset: int) -> bytes:
    # value version 3: offset, leader epoch, metadata, commit timestamp
    return struct.pack(">h", 3) + struct.pack(">q", offset) \
        + struct.pack(">i", 0) + encode_string("") + struct.pack(">q", 0)


def main() -> None:
    admin = AdminClient({"bootstrap.servers": BROKER})
    if TOPIC not in admin.list_topics(timeout=10).topics:
        admin.create_topics([NewTopic(TOPIC, num_partitions=PARTITIONS)])[TOPIC].result()
        print(f"Created topic '{TOPIC}' with {PARTITIONS} partitions")

    producer = Producer({
        "bootstrap.servers": BROKER,
        "queue.buffering.max.messages": 1_000_000,
        "linger.ms": 50,
        "compression.type": "lz4",
    })

    total = PARTITIONS * PER_PARTITION
    sent = 0
    for p in range(PARTITIONS):
        for i in range(PER_PARTITION):
            key = offset_commit_key(f"group-{i % GROUPS}", "orders", i % SOURCE_PARTITIONS)
            value = offset_commit_value(i)
            while True:
                try:
                    producer.produce(TOPIC, key=key, value=value, partition=p)
                    break
                except BufferError:
                    producer.poll(0.5)
            sent += 1
            if sent % 100_000 == 0:
                producer.poll(0)
                print(f"\r{sent}/{total}", end="", flush=True)
    producer.flush()
    print(f"\nDone: {total} records across {PARTITIONS} partitions in '{TOPIC}'")
    print(f"Try: target/debug/kbridge fetch -b {BROKER} --offsets-topic {TOPIC} > /dev/null")


if __name__ == "__main__":
    main()
