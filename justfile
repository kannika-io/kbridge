teardown:
	docker compose down -v

setup-ci:
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

setup-local-dev:
	just setup-ci && docker compose -f docker-compose.yml -f docker-compose-local-dev.yml up -d

calculate-offsets:
	cargo run -- fetch -b localhost:9092 \
	| cargo run -- calculate --bootstrap-server localhost:9093 -H Offset \

apply-offsets:
	cargo run -- fetch -b localhost:9092 \
	| cargo run -- calculate --bootstrap-server localhost:9093 --from-stdin --legacy-offset-header Offset \
	| cargo run -- apply -b localhost:9093 --from-stdin

run-example:
	just setup-ci && \
	just apply-offsets

run-docs:
	cargo doc --no-deps && \
	docker compose -f docker-compose-docs.yml up --build
	
