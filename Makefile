# An evaluator should not have to remember docker commands.
#
# Python comes from the conda environment in ./venv when it exists, so every
# target works whether or not you have run 'conda activate ./venv' first.
VENV_PY := ./venv/bin/python
PY ?= $(shell [ -x $(VENV_PY) ] && echo $(VENV_PY) || echo python3)
PYTHON_VERSION ?= 3.10
export PYTHONPATH := src

.DEFAULT_GOAL := help
SCENARIO ?= COLD_CHAIN_EXCURSION
SEED ?= 42
DAYS ?= 35
RATES ?= 1000,2000,4000,8000

help:            ## show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	 awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[1m%-16s\033[0m %s\n", $$1, $$2}'

venv:            ## create the conda environment in ./venv (python 3.10)
	conda create -y -p ./venv python=$(PYTHON_VERSION)
	@echo ""
	@echo "created ./venv  —  activate it with:  conda activate ./venv"

install:         ## install python dependencies into ./venv
	$(PY) -m pip install -r requirements.txt

install-kafka:   ## optional: client libraries for the Docker stack
	$(PY) -m pip install -r requirements-kafka.txt

setup: venv install  ## create the environment and install everything in one go
	@echo ""
	@echo "ready. activate the environment with:  conda activate ./venv"

which-python:    ## show which interpreter the targets will use
	@$(PY) -c "import sys; print(sys.executable); print(sys.version)"

demo:            ## full pipeline, one scenario, no broker needed
	$(PY) -m lil demo --scenario $(SCENARIO) --seed $(SEED)

scenarios:       ## run all seven fault scenarios and compare
	$(PY) -m lil scenarios --seed $(SEED)

simulate: demo   ## alias for demo

kpis:            ## print business KPIs from the last run
	$(PY) -m lil kpis

history:         ## five weeks of operations through the same pipeline (for the models)
	$(PY) -m lil history --days $(DAYS) --seed $(SEED)

ml:              ## train, evaluate and register the models on the history run
	$(PY) -m lil ml

all-evidence: test scenarios history ml  ## everything the final report quotes, offline

api:             ## serve the API and dashboard on http://127.0.0.1:8000
	$(PY) -m uvicorn lil.api.app:app --host 127.0.0.1 --port 8000

test:            ## run the test suite
	$(PY) -m pytest tests -q

up:              ## start Kafka and PostgreSQL
	docker compose up -d
	@echo "waiting for kafka and postgres to become healthy..."
	@docker compose ps

up-full:         ## start the stack including the Kafka UI on :8080
	docker compose --profile full up -d

down:            ## stop the stack, keep the volumes
	docker compose down

reset:           ## stop the stack and wipe the volumes and the cluster id together
	docker compose down -v

logs:            ## tail the stack logs
	docker compose logs -f --tail=100

topics:          ## create the topics with their keys and retention
	./scripts/create_topics.sh

db-migrate:      ## apply the SQL migrations to a running postgres
	./scripts/migrate.sh

db-counts:       ## row counts and current shipment status in postgres
	$(PY) -m lil db counts

demo-kafka:      ## run the demo against the real broker (needs make up)
	$(PY) -m lil demo --scenario $(SCENARIO) --seed $(SEED) --backend kafka

demo-postgres:   ## offline demo that also writes the operational store (needs make up)
	$(PY) -m lil demo --scenario $(SCENARIO) --seed $(SEED) --postgres

stream:          ## the pipeline as a long-running consumer on the broker (Ctrl-C to stop)
	$(PY) -m lil stream --scenario $(SCENARIO) --seed $(SEED) --lake --postgres

produce:         ## publish a scenario to the broker at RATE events per second
	$(PY) -m lil produce --scenario $(SCENARIO) --seed $(SEED) $(if $(RATE),--rate $(RATE),)

bench:           ## latency, capacity and crash recovery on the broker (needs make up)
	$(PY) -m lil bench --rates $(RATES)

test-kafka:      ## the test suite including the broker integration test
	LIL_KAFKA_TESTS=1 $(PY) -m pytest tests -q

clean:           ## remove generated runs, history, models and lake files
	rm -rf data/runs data/lake data/bus data/history data/models data/bench data/checkpoints data/stream_lake

.PHONY: help venv setup install install-kafka which-python demo scenarios simulate kpis history ml all-evidence api test test-kafka up up-full down reset logs topics db-migrate db-counts demo-kafka demo-postgres stream produce bench clean
