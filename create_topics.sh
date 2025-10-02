#/bin/bash
kafka-topics --create --bootstrap-server broker:29092 --topic orders --partitions 5
kafka-topics --create --bootstrap-server broker:29092 --topic orders-restore --partitions 5

exit 0;
