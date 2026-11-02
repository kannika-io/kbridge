#!/bin/bash

# Sets up a topic with 2 partitions whose partition 1 is empty in the target cluster.
# In the source both partitions have records and committed offsets, but only partition 0 is restored.

set -e

# Configuration
TOPIC=$1
NUM_MESSAGES=$2
CONSUMER_GROUP=$3
SOURCE_BOOTSTRAP_SERVERS=$4
TARGET_BOOTSTRAP_SERVERS=$5

for bootstrap_servers in $SOURCE_BOOTSTRAP_SERVERS $TARGET_BOOTSTRAP_SERVERS; do
    kafka-topics --bootstrap-server $bootstrap_servers --create --if-not-exists --topic $TOPIC --partitions 2
done

# Keyed records spread over both partitions
kafka-verifiable-producer \
    --bootstrap-server $SOURCE_BOOTSTRAP_SERVERS \
    --topic $TOPIC \
    --max-messages $NUM_MESSAGES \
    --repeating-keys $NUM_MESSAGES

# Commit offsets on both partitions
kafka-console-consumer \
    --bootstrap-server $SOURCE_BOOTSTRAP_SERVERS \
    --topic $TOPIC \
    --group $CONSUMER_GROUP \
    --max-messages $NUM_MESSAGES \
    --from-beginning > /dev/null

# Restore partition 0 only. Keys hash to the same partition in the target.
kafka-console-consumer \
    --bootstrap-server $SOURCE_BOOTSTRAP_SERVERS \
    --property print.offset=true \
    --property print.key=true \
    --topic $TOPIC \
    --partition 0 \
    --offset earliest \
    --timeout-ms 10000 \
| awk -F'[:\t]' -v OFS='|' '{print "Offset:"$2*2, $3, $4}' \
| kafka-console-producer \
    --bootstrap-server $TARGET_BOOTSTRAP_SERVERS \
    --property "parse.headers=true" \
    --property "headers.delimiter=|" \
    --property "parse.key=true" \
    --property "key.separator=|" \
    --topic $TOPIC

echo "Finished partial restoration of topic '$TOPIC': partition 1 is empty in the target"
