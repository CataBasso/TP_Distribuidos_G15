VENV := .venv
PIP := $(VENV)/bin/pip
PYTEST := $(VENV)/bin/pytest

.PHONY: setup rabbit-up rabbit-down test-unit test-integration

setup:
	python3 -m venv $(VENV)
	$(PIP) install -r requirements.txt

up:
	@echo "TODO: docker compose up -d (cuando exista el compose)"

down:
	@echo "TODO: docker compose down"

test:
	$(PYTEST) tests/unit

rabbit-up:
	docker start rabbit 2>/dev/null || docker run -d --name rabbit -p 5672:5672 -p 15672:15672 rabbitmq:3-management

rabbit-down:
	docker rm -f rabbit

rabbit-test:
	$(PYTEST) tests/integration