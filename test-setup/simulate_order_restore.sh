#!/bin/bash

# Script to produce 1000 messages to the 'orders' topic using kafka-verifiable-producer.sh

set -e

# Configuration
TOPIC=$1
NUM_MESSAGES=$2
SOURCE_BOOTSTRAP_SERVERS=$3
TARGET_BOOTSTRAP_SERVERS=$4

echo "Starting to consume $NUM_MESSAGES messages from topic '$TOPIC' for restoration..."

PIPE_FILE=$(mktemp -u) 
mkfifo "$PIPE_FILE"

kafka-console-producer.sh \
     --bootstrap-server $TARGET_BOOTSTRAP_SERVERS \
     --property "parse.headers=true"\
     --property "headers.delimiter=|"\
     --property "parse.key=true"\
     --property "key.separator=|"\
     --batch-size 1 \
     --topic $TOPIC < "$PIPE_FILE" &

PRODUCER_PID=$!

kafka-console-consumer.sh \
    --bootstrap-server $SOURCE_BOOTSTRAP_SERVERS \
    --property print.offset=true \
    --topic $TOPIC \
    --max-messages $NUM_MESSAGES \
    --from-beginning \
| sed 's/\s/|/g' \
    > "$PIPE_FILE"                                                                           

wait $PRODUCER_ID

echo "Finished restoration of $NUM_MESSAGES messages from topic '$TOPIC'"

rm -f $PIPE_FILE
