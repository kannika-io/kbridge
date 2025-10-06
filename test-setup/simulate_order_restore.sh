#!/bin/bash

# Script to produce 1000 messages to the 'orders' topic using kafka-verifiable-producer.sh

set -e

# Configuration
TOPIC=$1
BOOTSTRAP_SERVERS="localhost:9092"
TARGET_TOPIC=$2
NUM_MESSAGES=$3

echo "Starting to consume $NUM_MESSAGES messages from topic '$TOPIC' for restoration..."

PIPE_FILE=$(mktemp -u) 
mkfifo "$PIPE_FILE"

kafka-console-producer.sh \
     --bootstrap-server $BOOTSTRAP_SERVERS\
     --property "parse.headers=true"\
     --property "headers.delimiter=|"\
     --batch-size 1 \
     --topic $TARGET_TOPIC < "$PIPE_FILE" &

PRODUCER_PID=$!

kafka-console-consumer.sh \
    --bootstrap-server $BOOTSTRAP_SERVERS \
    --property print.offset=true \
    --topic $TOPIC \
    --max-messages $NUM_MESSAGES \
    --from-beginning \
| sed 's/\s/|/g' \
    > "$PIPE_FILE"                                                                           

wait $PRODUCER_ID

echo "Finished restoration of $NUM_MESSAGES messages from topic '$TOPIC'"

rm -f $PIPE_FILE
