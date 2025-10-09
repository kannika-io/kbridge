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
	./test-setup/consume_orders.sh orders-3 500 console-consumer localhost:9092 && \
	./test-setup/consume_orders.sh orders-1 563 console-consumer-2 localhost:9092 && \
	./test-setup/consume_orders.sh orders-2 772 console-consumer-2 localhost:9092 && \
	./test-setup/consume_orders.sh orders-3 802 console-consumer-2 localhost:9092

apply-offsets:
	cargo run -- fetch-source -b localhost:9092 | cargo run -- calculate-intermediary --bootstrap-server localhost:9093 --from-stdin --legacy-offset-header Offset | cargo run -- apply-intermediary -b localhost:9093 --from-stdin

run-example:
	just setup && \
	just apply-offsets

