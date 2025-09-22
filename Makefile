.PHONY: install clean lint autoformat test ftest ftrace install run default-config docker-build docker-run docker-push

# Default to python3, but if its version is >3.11, try using python3.11 or python3.10.
PYTHON ?= python3
PYTHON_VERSION := $(shell $(PYTHON) -c "import sys; print(f'{sys.version_info[0]}.{sys.version_info[1]}')" 2>/dev/null)
CHECK_UNSUPPORTED_PYTHON_VERSION := $(shell $(PYTHON) -c "import sys; print(1 if sys.version_info[:2] > (3,11) else 0)" 2>/dev/null)
ifeq ($(CHECK_UNSUPPORTED_PYTHON_VERSION),1)
  ifneq ($(shell command -v python3.11 2>/dev/null),)
    PYTHON = python3.11
  else ifneq ($(shell command -v python3.10 2>/dev/null),)
    PYTHON = python3.10
  else
    $(error "Unsupported python version: $(PYTHON) (version $(PYTHON_VERSION)) is greater than 3.11 and neither python3.11 nor python3.10 are available.")
  endif
endif

$(info Using $(PYTHON) (version $(shell $(PYTHON) -c "import sys; print(f'{sys.version_info[0]}.{sys.version_info[1]}')")))


ARCH=$(shell uname -m)
PERF8?=no
SLOW_TEST_THRESHOLD=1 # seconds
VERSION=$(shell cat connectors/VERSION)
PACKAGE_NAME_VERSION="elasticsearch_connectors-$(VERSION)"

DOCKER_IMAGE_NAME?=docker.elastic.co/integrations/elastic-connectors
DOCKERFILE_PATH?=Dockerfile
DOCKERFILE_FTEST_PATH?=tests/Dockerfile.ftest

config.yml:
	- cp -n config.yml.example config.yml

.venv/bin/python: | config.yml
	$(PYTHON) -m venv .venv
	.venv/bin/pip install --upgrade pip
	.venv/bin/python -m pip install build

.venv/bin/pip-licenses: .venv/bin/python
	.venv/bin/pip install pip-licenses

notice: .venv/bin/python .venv/bin/pip-licenses .venv/bin/elastic-ingest
	.venv/bin/pip-licenses --format=plain-vertical --with-license-file --no-license-path > NOTICE.txt

install: .venv/bin/python .venv/bin/elastic-ingest notice

install-agent: .venv/bin/elastic-ingest


.venv/bin/elastic-ingest: .venv/bin/python
	.venv/bin/pip install -e .

.venv/bin/ruff: .venv/bin/python
	.venv/bin/pip install -r requirements/$(ARCH).txt
	.venv/bin/pip install -r requirements/tests.txt

.venv/bin/pytest: .venv/bin/python
	.venv/bin/pip install -e ".[tests,ftest]"

clean:
	rm -rf bin lib .venv include elasticsearch_connector.egg-info .coverage site-packages pyvenv.cfg include.site.python*.greenlet dist

lint: .venv/bin/python .venv/bin/ruff .venv/bin/elastic-ingest
	.venv/bin/ruff check connectors
	.venv/bin/ruff format connectors --check
	.venv/bin/ruff check tests
	.venv/bin/ruff format tests --check
	.venv/bin/ruff check scripts
	.venv/bin/ruff format scripts --check
	.venv/bin/pyright connectors
	.venv/bin/pyright tests

autoformat: .venv/bin/python .venv/bin/ruff .venv/bin/elastic-ingest
	.venv/bin/ruff check connectors --fix
	.venv/bin/ruff format connectors
	.venv/bin/ruff check tests --fix
	.venv/bin/ruff format tests
	.venv/bin/ruff check scripts --fix
	.venv/bin/ruff format scripts

test: .venv/bin/pytest
	.venv/bin/pytest --cov-report term-missing --cov-fail-under 92 --cov-report html --cov=connectors --fail-slow=$(SLOW_TEST_THRESHOLD) -sv tests

build-connectors-base-image:
	docker build . -f ${DOCKERFILE_PATH} -t connectors-base

ftest: .venv/bin/pytest $(DOCKERFILE_FTEST_PATH) build-connectors-base-image
	tests/ftest.sh $(NAME) $(PERF8)

ftrace: .venv/bin/pytest $(DOCKERFILE_FTEST_PATH)
	PERF8_TRACE=true tests/ftest.sh $(NAME) $(PERF8)

run: install
	.venv/bin/elastic-ingest

default-config: install
	.venv/bin/elastic-ingest --action config --service-type $(SERVICE_TYPE)

docker-build: $(DOCKERFILE_PATH)
	docker build --no-cache -f $(DOCKERFILE_PATH) -t $(DOCKER_IMAGE_NAME):$(VERSION)-SNAPSHOT .

docker-run:
	docker run -v $(PWD):/config $(DOCKER_IMAGE_NAME):$(VERSION)-SNAPSHOT /app/.venv/bin/elastic-ingest -c /config/config.yml --log-level=DEBUG

docker-push:
	docker push $(DOCKER_IMAGE_NAME):$(VERSION)-SNAPSHOT

## Agent Docker Zone
# Only use it for local testing, that's it
AGENT_ES_HOSTS?=[http://127.0.0.1:9200]
AGENT_ES_USERNAME?=elastic
AGENT_ES_PASSWORD?=changeme
AGENT_DOCKERFILE_NAME?=Dockerfile.agent
AGENT_DOCKER_IMAGE_NAME?=connectors-agent-component-local

agent-docker-build:
	docker build -t $(AGENT_DOCKER_IMAGE_NAME) -f $(AGENT_DOCKERFILE_NAME) .

agent-docker-run:
	docker run \
		--env ELASTICSEARCH_HOSTS=$(AGENT_ES_HOSTS) \
		--env ELASTICSEARCH_USERNAME=$(AGENT_ES_USERNAME) \
		--env ELASTICSEARCH_PASSWORD=$(AGENT_ES_PASSWORD) \
		--network host \
		$(AGENT_DOCKER_IMAGE_NAME)

agent-docker-all: agent-docker-build agent-docker-run
## End Agent Docker Zone

sdist: .venv/bin/python
	.venv/bin/python -m build --sdist

zip: sdist
	cd dist && \
	tar -xzf $(PACKAGE_NAME_VERSION).tar.gz && \
	zip -r $(PACKAGE_NAME_VERSION).zip $(PACKAGE_NAME_VERSION)/ && \
	rm -rf $(PACKAGE_NAME_VERSION)/

deps-csv: .venv/bin/pip-licenses
	mkdir -p dist
	.venv/bin/pip-licenses --format=csv --with-urls > dist/dependencies.csv
	.venv/bin/python scripts/deps-csv.py dist/dependencies.csv
