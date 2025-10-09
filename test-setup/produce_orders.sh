#!/bin/bash

# Script to produce 1000 messages to the 'orders' topic using kafka-verifiable-producer.sh

set -e

# Configuration
TOPIC=$1
NUM_MESSAGES=$2
BOOTSTRAP_SERVERS=$3

echo "Starting to produce $NUM_MESSAGES messages to topic '$TOPIC'..."

kafka-verifiable-producer.sh \
    --bootstrap-server $BOOTSTRAP_SERVERS \
    --topic $TOPIC \
    --max-messages $NUM_MESSAGES \
    --repeating-keys $NUM_MESSAGES

echo "Finished producing $NUM_MESSAGES messages to topic '$TOPIC'"
