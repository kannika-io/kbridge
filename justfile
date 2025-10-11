teardown:
	docker compose down -v

setup:
	docker compose up -d
	for topic_count in "orders-1 2000" "orders-2 3000" "orders-3 1000"; do \
		BROKER_INIT_COMMAND="/test-setup/produce_orders.sh $topic_count broker-source:29092" docker compose up broker-init; \
	done
	for topic_count in "orders-1 2000" "orders-2 3000" "orders-3 1000"; do \
		BROKER_INIT_COMMAND="/test-setup/simulate_order_restore.sh $topic_count broker-source:29092 broker-target:29092" docker compose up broker-init; \
	done
	for topic_count in "orders-1 1000" "orders-2 1300" "orders-3 500"; do \
		BROKER_INIT_COMMAND="/test-setup/consume_orders.sh $topic_count console-consumer broker-source:29092" docker compose up broker-init; \
	done
	for topic_count in "orders-1 563" "orders-2 772" "orders-3 802"; do \
		BROKER_INIT_COMMAND="/test-setup/consume_orders.sh $topic_count console-consumer-2 broker-source:29092" docker compose up broker-init; \
	done

apply-offsets:
	cargo run -- fetch-source -b localhost:9092 \
	| cargo run -- calculate-intermediary --bootstrap-server localhost:9093 --from-stdin --legacy-offset-header Offset \
	| cargo run -- apply-intermediary -b localhost:9093 --from-stdin

run-example:
	just setup && \
	just apply-offsets

