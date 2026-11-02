VENV := .venv
PIP := $(VENV)/bin/pip
PYTEST := $(VENV)/bin/pytest
QUERY_TEST_FILE = tests/test_querys.py
MIDDLEWARE_TEST_FILE = tests/test_middleware.py
PROTOCOL_TEST_FILE = tests/test_protocol.py
SIZE ?= small
N ?= 1
export SIZE

.PHONY: setup up down rabbit-up rabbit-down test test-unit test-integration logs

setup:
	python3 -m venv $(VENV)
	$(PIP) install -r requirements.txt

up:
	mkdir -p output
	COMPOSE_HTTP_TIMEOUT=300 docker compose -f docker-compose.yaml up --build --remove-orphans --detach
	docker compose -f docker-compose.yaml logs --follow
down:
	docker compose -f docker-compose.yaml stop -t 5
	docker compose -f docker-compose.yaml down

test: switch
	mkdir -p output
	rm -rf ./output/*
	COMPOSE_HTTP_TIMEOUT=300 docker compose -f docker-compose.yaml up --build --remove-orphans --detach
	docker logs --follow client_0
	docker wait $$(docker ps -a --filter "name=client_" --format '{{.Names}}')
	EXPECTED_DIR=data/expected/$(SIZE) OUTPUT_ROOT=output \
		$(PYTEST) $(QUERY_TEST_FILE) -v; EXIT_CODE=$$?; \
	docker compose -f docker-compose.yaml stop -t 5; \
	docker compose -f docker-compose.yaml down; \
	exit $$EXIT_CODE

logs:
	docker compose -f docker-compose.yaml logs

test-query1:
	$(PYTEST) $(QUERY_TEST_FILE)::test_query1 -v

test-query2:
	$(PYTEST) $(QUERY_TEST_FILE)::test_query2 -v

test-query3:
	$(PYTEST) $(QUERY_TEST_FILE)::test_query3 -v

test-query4:
	$(PYTEST) $(QUERY_TEST_FILE)::test_query4 -v

test-query5:
	$(PYTEST) $(QUERY_TEST_FILE)::test_query5 -v

test-protocol:
	$(PYTEST) $(PROTOCOL_TEST_FILE) -v

rabbit-up:
	docker start rabbit 2>/dev/null || docker run -d --name rabbit -p 5672:5672 -p 15672:15672 rabbitmq:3-management
	@echo "Esperando a RabbitMQ..."
	@for i in $$(seq 1 40); do \
		$(VENV)/bin/python -c "import pika; pika.BlockingConnection(pika.ConnectionParameters('localhost')).close()" >/dev/null 2>&1 \
			&& echo "RabbitMQ listo" && exit 0; \
		sleep 2; \
	done; echo "RabbitMQ no respondió a tiempo: mirá docker logs rabbit"; exit 1

rabbit-down:
	docker rm -f rabbit

rabbit-test: rabbit-up
	$(PYTEST) $(MIDDLEWARE_TEST_FILE) -v

CLIENT_GATEWAY_TESTS = tests/test_protocol.py tests/test_client_dataset.py \
	tests/test_client_results_writer.py tests/test_client.py tests/test_gateway.py \
	tests/test_client_gateway_e2e.py

.PHONY: test-client-gateway test-gateway-rabbit run-gateway run-client

test-client-gateway:
	$(PYTEST) $(CLIENT_GATEWAY_TESTS) -v

test-gateway-rabbit: rabbit-up
	$(PYTEST) tests/test_gateway_rabbit.py -v

run-gateway:
	PYTHONPATH=src GATEWAY_ID=1 RABBIT_HOST=localhost $(VENV)/bin/python -m gateway

run-client:
	PYTHONPATH=src $(VENV)/bin/python -m client --data-dir data/sample/medium_dataset --output-dir output

run-fake-system:
	PYTHONPATH=src $(VENV)/bin/python tools/fake_system.py --gw-id 1 --rabbit-host localhost

.PHONY: scenarios switch test-scenario test-scenarios test-random

scenarios:
	python3 tools/scenarios.py generate --size $(SIZE) --random $(or $(RANDOM_N),0) --seed $(or $(SEED),0)

switch: scenarios
	@python3 tools/scenarios.py list; \
	n="$(N)"; if [ -z "$$n" ]; then read -p "Selecciona uno: " n; fi; \
	python3 tools/scenarios.py switch $$n

test-scenario:             ## con Docker: levanta docker-compose.yaml, espera a los clientes y compara
	mkdir -p output && rm -rf ./output/*
	COMPOSE_HTTP_TIMEOUT=300 docker compose -f docker-compose.yaml up --build --remove-orphans --detach
	python3 tools/verify_output.py --wait --expected data/expected/$(or $(SIZE),small); EXIT_CODE=$$?; \
	docker compose -f docker-compose.yaml stop -t 5; docker compose -f docker-compose.yaml down; \
	exit $$EXIT_CODE

test-scenarios:            ## SIN Docker: corre todos los escenarios fijos en un proceso (RabbitMQ falso)
	PYTHONPATH=src:tests:tools $(VENV)/bin/python tests/real_flow.py

test-random:               ## SIN Docker: N escenarios aleatorios (make test-random RANDOM_N=10 SEED=3)
	PYTHONPATH=src:tests:tools $(VENV)/bin/python tests/real_flow.py --random $(or $(RANDOM_N),10) --seed $(or $(SEED),0)
