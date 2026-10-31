# Common tasks. `make help` lists them.
PY ?= python
TF  = terraform -chdir=infra
BUCKET = $(shell $(TF) output -raw bucket 2>/dev/null)

.PHONY: help install test chaos lint local query build-lambda build-wheel deploy push-producer start stop backfill replay destroy destroy-all

help:
	@grep -E '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  %-15s %s\n", $$1, $$2}'

install: ## install the package with dev extras
	$(PY) -m pip install -e ".[dev]"

test: ## unit + integration + chaos tests (no AWS needed)
	$(PY) -m pytest --cov=stockstream --cov-report=term-missing

chaos: ## only the break-it tests
	$(PY) -m pytest -m chaos -v

lint: ## ruff + terraform fmt
	ruff check src tests glue_jobs
	terraform fmt -check -recursive infra

local: ## run the whole pipeline locally on mocked AWS (DAYS=5 SCALE=0.5)
	$(PY) -m stockstream.local.run_local --start 2026-09-28 --days $(or $(DAYS),5) --scale $(or $(SCALE),0.5) --out local_output

query: ## run sql/analytics with DuckDB over the local lake
	$(PY) -m stockstream.local.query_local --lake local_output

build-lambda: ## zip the stream processor (pandas/pyarrow come from the managed layer)
	rm -rf build/lambda && mkdir -p build/lambda
	$(PY) -m pip install --target build/lambda --no-deps jsonschema jsonschema-specifications referencing rpds-py attrs pyyaml \
		--platform manylinux2014_aarch64 --only-binary=:all: --python-version 3.12
	cp -r src/stockstream build/lambda/
	cd build/lambda && zip -qr ../stream_processor.zip . -x '*/__pycache__/*'

build-wheel: ## wheel for the Glue jobs
	$(PY) -m pip wheel --no-deps -w build .

deploy: build-lambda build-wheel ## terraform apply (needs infra/terraform.tfvars)
	$(TF) init -input=false && $(TF) apply

push-producer: ## build and push the producer image to ECR
	$(eval REPO := $(shell $(TF) output -raw ecr_repository))
	aws ecr get-login-password | docker login --username AWS --password-stdin $(firstword $(subst /, ,$(REPO)))
	docker build --platform linux/arm64 -f docker/producer.Dockerfile -t $(REPO):latest .
	docker push $(REPO):latest

start: ## turn the producer on (1 Fargate task)
	$(TF) apply -var producer_desired_count=1

stop: ## turn the producer off (no Fargate cost)
	$(TF) apply -var producer_desired_count=0

backfill: ## Alpaca history into bronze, then into silver (START=2026-09-01 END=2026-09-30)
	DATA_BUCKET=$(BUCKET) $(PY) -m stockstream.backfill.alpaca_backfill --start $(START) --end $(END)
	DATA_BUCKET=$(BUCKET) $(PY) -m stockstream.stream_processor.replay --prefix bronze/backfill/

replay: ## rebuild silver for one day from bronze (DATE=2026-10-29)
	DATA_BUCKET=$(BUCKET) $(PY) -m stockstream.stream_processor.replay --prefix bronze/dt=$(DATE)/

destroy: ## remove everything except the data bucket
	$(TF) destroy

destroy-all: ## remove everything including all data
	aws s3 rm s3://$(BUCKET) --recursive
	$(TF) destroy
