teardown:
	docker compose down -v

setup:
	docker compose up -d && \
	./test-setup/produce_orders.sh orders-1 2000 localhost:9092 && \
	./test-setup/produce_orders.sh orders-2 3000 localhost:9092 && \
	./test-setup/produce_orders.sh orders-3 1000 localhost:9092 && \
	./test-setup/simulate_order_restore.sh orders-1 2000 localhost:9092 localhost:9093 && \
	./test-setup/simulate_order_restore.sh orders-2 3000 localhost:9092 localhost:9093 && \
	./test-setup/simulate_order_restore.sh orders-3 1000 localhost:9092 localhost:9093 && \
	./test-setup/consume_orders.sh orders-1 1000 console-consumer localhost:9092 && \
	./test-setup/consume_orders.sh orders-2 1300 console-consumer localhost:9092 && \
	./test-setup/consume_orders.sh orders-3 500 console-consumer localhost:9092

export-offsets:
	/usr/local/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --export --group console-consumer --topic orders-1 --to-current --dry-run --reset-offsets > offsets.csv && \
	/usr/local/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --export --group console-consumer --topic orders-2 --to-current --dry-run --reset-offsets >> offsets.csv && \
	/usr/local/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --export --group console-consumer --topic orders-3 --to-current --dry-run --reset-offsets >> offsets.csv

run-example:
	just setup && \
	just export-offsets && \
	sleep 10 && \
	RUST_LOG=WARN cargo run --  --bootstrap-server localhost:9093 --legacy-offset-header Offset --offsets-csv-file-location ./offsets.csv
