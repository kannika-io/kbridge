teardown:
	docker compose down -v

setup:
	docker compose up -d && \
	./test-setup/produce_orders.sh orders-1 2000 && \
	./test-setup/produce_orders.sh orders-2 3000 && \
	./test-setup/produce_orders.sh orders-3 1000 && \
	./test-setup/simulate_order_restore.sh orders-1 orders-1-restore 2000 && \
	./test-setup/simulate_order_restore.sh orders-2 orders-2-restore 3000 && \
	./test-setup/simulate_order_restore.sh orders-3 orders-3-restore 1000 && \
	./test-setup/consume_orders.sh orders-1 1000 console-consumer && \
	./test-setup/consume_orders.sh orders-2 1300 console-consumer && \
	./test-setup/consume_orders.sh orders-3 500 console-consumer

export-offsets:
	/usr/local/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --export --group console-consumer --topic orders-1 --to-current --dry-run --reset-offsets >> offsets.csv && \
	/usr/local/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --export --group console-consumer --topic orders-2 --to-current --dry-run --reset-offsets >> offsets.csv && \
	/usr/local/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --export --group console-consumer --topic orders-3 --to-current --dry-run --reset-offsets >> offsets.csv
