#!/bin/bash

# Script to produce 1000 messages to the 'orders' topic using kafka-verifiable-producer.sh

set -e

# Configuration
TOPIC="orders"
NUM_MESSAGES=1000
BOOTSTRAP_SERVERS="localhost:9092"

echo "Starting to produce $NUM_MESSAGES messages to topic '$TOPIC'..."

kafka-verifiable-producer.sh \
    --bootstrap-server $BOOTSTRAP_SERVERS \
    --topic $TOPIC \
    --max-messages $NUM_MESSAGES \
    --throughput -1

echo "Finished producing $NUM_MESSAGES messages to topic '$TOPIC'"
