#!/usr/bin/env bash
# Generates a __consumer_offsets-style topic: 12 partitions, 1M records each.
# Usage: generate_offsets_topic.sh [broker] [topic] [partitions] [per_partition]
set -euo pipefail
cd "$(dirname "$0")"
exec uv run --with confluent-kafka generate_offsets_topic.py \
  "${1:-localhost:9092}" "${2:-consumer-offsets-big}" "${3:-12}" "${4:-1000000}"
