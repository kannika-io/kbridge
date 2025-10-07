#/bin/bash
kafka-topics --create --bootstrap-server broker-target:29092 --topic orders-1
kafka-topics --create --bootstrap-server broker-target:29092 --topic orders-2
kafka-topics --create --bootstrap-server broker-target:29092 --topic orders-3 --config cleanup.policy=delete --config retention.bytes=10000 --config segment.bytes=5000

exit 0;
