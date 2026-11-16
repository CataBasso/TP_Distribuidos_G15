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

COMPOSE := docker compose -f docker-compose.yaml
COMPOSE_ALL := $(COMPOSE) --profile clients
KEEP ?= 1
CLEAN_OUTPUT = rm -rf ./output/* 2>/dev/null || docker run --rm -v "$(PWD)/output:/o" alpine sh -c 'rm -rf /o/*'


# 1) make build        construye (o reutiliza la cache de) las imágenes: SOLO hace falta cuando cambia el código
# 2) make system-up    levanta RabbitMQ + Gateways + etapas y las deja corriendo (no toca los clientes)
# 3) make run-clients  corre los clientes del escenario sobre el sistema ya levantado (repetible)
# 4) make verify       compara output/ con data/expected/$(SIZE)
# 5) make system-down  recién acá se baja todo
.PHONY: build system-up run-clients verify system-down new-client

build:                     ## imagen base + una imagen por TIPO de etapa (capas cacheadas)
	docker build -t tpdist/base:latest -f src/Dockerfile.base src/
	$(COMPOSE_ALL) build

system-up:                 ## levanta el sistema (sin clientes) y espera a que esté listo
	mkdir -p output
	COMPOSE_HTTP_TIMEOUT=300 $(COMPOSE) up -d --no-build --remove-orphans --wait

run-clients:               ## corre todos los client_* sobre el sistema ya levantado
	mkdir -p output
	$(CLEAN_OUTPUT)
	$(COMPOSE_ALL) up -d --no-build --no-deps --force-recreate $$(python3 tools/verify_output.py --list-clients)

new-client:                ## un cliente más contra el sistema en marcha: make new-client C=client_1
	$(COMPOSE_ALL) up -d --no-build --no-deps --force-recreate $(C)

verify:                    ## espera a los clientes y compara con lo esperado (no baja nada)
	python3 tools/verify_output.py --wait --expected data/expected/$(SIZE)

system-down:
	$(COMPOSE_ALL) stop -t 5
	$(COMPOSE_ALL) down -v --remove-orphans

up: build system-up run-clients
	$(COMPOSE_ALL) logs --follow
down: system-down

test: switch build
	mkdir -p output
	$(CLEAN_OUTPUT)
	$(MAKE) --no-print-directory system-up run-clients
	docker logs --follow client_0
	docker wait $$(docker ps -a --filter "name=client_" --format '{{.Names}}')
	EXPECTED_DIR=data/expected/$(SIZE) OUTPUT_ROOT=output \
		$(PYTEST) $(QUERY_TEST_FILE) -v; EXIT_CODE=$$?; \
	if [ "$(KEEP)" = "0" ]; then $(MAKE) --no-print-directory system-down; fi; \
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

.PHONY: scenarios switch test-scenario test-all-scenarios test-scenarios test-random

scenarios:
	python3 tools/scenarios.py generate --size $(SIZE) --random $(or $(RANDOM_N),0) --seed $(or $(SEED),0) --rabbit-cpus $(or $(RABBIT_CPUS),2.0)

switch: scenarios
	@python3 tools/scenarios.py list; \
	n="$(N)"; if [ -z "$$n" ]; then read -p "Selecciona uno: " n; fi; \
	python3 tools/scenarios.py switch $$n

test-scenario: build      ##con Docker: levanta el sistema, corre los clientes, compara y guarda logs. KEEP=0 baja todo al final (por defecto queda corriendo)
	mkdir -p output logs
	$(CLEAN_OUTPUT)
	$(COMPOSE_ALL) down -v --remove-orphans
	$(MAKE) --no-print-directory system-up run-clients
	python3 tools/verify_output.py --wait --expected data/expected/$(or $(SIZE),small); EXIT_CODE=$$?; \
	$(COMPOSE_ALL) logs --no-color > logs/$(or $(LOG),last).log 2>&1; \
	if [ "$(KEEP)" = "0" ]; then $(MAKE) --no-print-directory system-down; fi; \
	exit $$EXIT_CODE

test-all-scenarios:        ##con Docker: todos los escenarios fijos (python3 tools/scenarios.py ids), uno tras otro (SIZE=small|medium)
	@fail=0; for n in $$(python3 tools/scenarios.py ids); do \
	  echo "=== escenario $$n ($(or $(SIZE),small)) ==="; \
	  python3 tools/scenarios.py generate --size $(or $(SIZE),small) --rabbit-cpus $(or $(RABBIT_CPUS),2.0) > /dev/null; \
	  python3 tools/scenarios.py switch $$n; \
	  $(MAKE) --no-print-directory test-scenario KEEP=0 SIZE=$(or $(SIZE),small) LOG=$(or $(SIZE),small)_$$n || fail=1; \
	done; echo; [ $$fail -eq 0 ] && echo "TODOS OK" || echo "HUBO FALLAS (logs en logs/)"; exit $$fail

test-scenarios:            ##SIN Docker: corre todos los escenarios fijos en un proceso (RabbitMQ falso)
	PYTHONPATH=src:tests:tools $(VENV)/bin/python tests/real_flow.py

test-random:               ##SIN Docker: N escenarios aleatorios (make test-random RANDOM_N=10 SEED=3)
	PYTHONPATH=src:tests:tools $(VENV)/bin/python tests/real_flow.py --random $(or $(RANDOM_N),10) --seed $(or $(SEED),0)