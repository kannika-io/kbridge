#!/bin/bash

set -e

# Configuration
TOPIC=$1
NUM_MESSAGES=$2
CONSUMER_GROUP=$3
BOOTSTRAP_SERVERS="localhost:9092"

kafka-console-consumer.sh \
    --bootstrap-server $BOOTSTRAP_SERVERS \
    --topic $TOPIC \
    --group  $CONSUMER_GROUP \
    --max-messages $NUM_MESSAGES \
    --from-beginning \

echo "Finished consuming $NUM_MESSAGES messages from topic '$TOPIC'"
