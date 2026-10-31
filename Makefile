VENV := .venv
PIP := $(VENV)/bin/pip
PYTEST := $(VENV)/bin/pytest
QUERY_TEST_FILE = tests/test_querys.py
MIDDLEWARE_TEST_FILE = tests/test_middleware.py

.PHONY: setup rabbit-up rabbit-down test-unit test-integration

setup:
	python3 -m venv $(VENV)
	$(PIP) install -r requirements.txt

up:
	@echo "TODO: docker compose up -d (cuando exista el compose)"

down:
	@echo "TODO: docker compose down"

test:
	$(PYTEST) $(UNIT_TEST_FILE)

test-query1:
	$(PYTEST) $(UNIT_TEST_FILE)::test_query1 -v

test-query2:
	$(PYTEST) $(UNIT_TEST_FILE)::test_query2 -v

test-query3:
	$(PYTEST) $(UNIT_TEST_FILE)::test_query3 -v

test-query4:
	$(PYTEST) $(UNIT_TEST_FILE)::test_query4 -v

test-query5:
	$(PYTEST) $(UNIT_TEST_FILE)::test_query5 -v

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
