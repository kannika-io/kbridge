#!/bin/bash

# Configuration
SOURCE_TOPIC="${SOURCE_TOPIC:-source-topic}"
TARGET_TOPIC="${TARGET_TOPIC:-target-topic}"
BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-localhost:9092}"
CONSUMER_GROUP="${CONSUMER_GROUP:-message-bridge-group}"
OFFSET_HEADER_KEY="${OFFSET_HEADER_KEY:-previous-offset}"

# Temporary files for communication between processes
CONSUMER_OUTPUT=$(mktemp)
PRODUCER_INPUT=$(mktemp)

# Cleanup function
cleanup() {
    echo "Cleaning up..."
    rm -f "$CONSUMER_OUTPUT" "$PRODUCER_INPUT"
    # Kill background processes
    jobs -p | xargs -r kill
    exit 0
}

# Set up signal handlers
trap cleanup SIGINT SIGTERM

echo "Starting message bridge from $SOURCE_TOPIC to $TARGET_TOPIC"
echo "Bootstrap servers: $BOOTSTRAP_SERVERS"
echo "Consumer group: $CONSUMER_GROUP"

# Start kafka-verifiable-consumer in the background
kafka-verifiable-consumer \
    --bootstrap-server "$BOOTSTRAP_SERVERS" \
    --topic "$SOURCE_TOPIC" \
    --group-id "$CONSUMER_GROUP" \
    --verbose > "$CONSUMER_OUTPUT" &

CONSUMER_PID=$!

# Start kafka-producer in the background, reading from named pipe
mkfifo "$PRODUCER_INPUT"
kafka-console-producer \
    --bootstrap-server "$BOOTSTRAP_SERVERS" \
    --topic "$TARGET_TOPIC" \
    --property "parse.headers=true" \
    --property "headers.delimiter=|" < "$PRODUCER_INPUT" &

PRODUCER_PID=$!

# Process messages
previous_offset=""

echo "Processing messages... (Press Ctrl+C to stop)"

tail -f "$CONSUMER_OUTPUT" | while IFS= read -r line; do
    # Parse JSON output from kafka-verifiable-consumer
    if echo "$line" | jq -e '.name == "records_consumed"' > /dev/null 2>&1; then
        # Extract message details
        topic=$(echo "$line" | jq -r '.topic')
        partition=$(echo "$line" | jq -r '.partition')
        offset=$(echo "$line" | jq -r '.offset')
        value=$(echo "$line" | jq -r '.value // empty')
        
        # Skip if no value
        if [ -z "$value" ] || [ "$value" = "null" ]; then
            continue
        fi
        
        echo "Consumed: topic=$topic, partition=$partition, offset=$offset, value=$value"
        
        # Prepare message for producer
        if [ -n "$previous_offset" ]; then
            # Add previous offset as header
            message="$OFFSET_HEADER_KEY:$previous_offset|$value"
            echo "Producing with header: $OFFSET_HEADER_KEY:$previous_offset, value: $value"
        else
            # First message, no previous offset
            message="$value"
            echo "Producing first message (no previous offset): $value"
        fi
        
        # Send to producer
        echo "$message" > "$PRODUCER_INPUT"
        
        # Update previous offset for next iteration
        previous_offset="$offset"
    fi
done

# Wait for background processes
wait $CONSUMER_PID $PRODUCER_PID
