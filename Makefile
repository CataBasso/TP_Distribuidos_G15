VENV := .venv
PIP := $(VENV)/bin/pip
PYTEST := $(VENV)/bin/pytest
UNIT_TEST_FILE = tests/test_querys.py

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

rabbit-down:
	docker rm -f rabbit
