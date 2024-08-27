.PHONY: install clean lint autoformat test release ftest ftrace install run default-config release docker-build docker-run docker-push

PYTHON?=python3
ARCH=$(shell uname -m)
PERF8?=no
SLOW_TEST_THRESHOLD=1 # seconds
VERSION=$(shell cat connectors/VERSION)

DOCKER_IMAGE_NAME?=docker.elastic.co/enterprise-search/elastic-connectors
DOCKERFILE_PATH?=Dockerfile
DOCKERFILE_FTEST_PATH?=Dockerfile.ftest

config.yml:
	- cp -n config.yml.example config.yml

.venv/bin/python: config.yml
	$(PYTHON) -m venv .venv
	.venv/bin/pip install --upgrade pip
	.venv/bin/pip install --upgrade setuptools

.venv/bin/pip-licenses: .venv/bin/python
	.venv/bin/pip install pip-licenses

install: .venv/bin/python .venv/bin/pip-licenses .venv/bin/elastic-ingest
	.venv/bin/pip-licenses --format=plain-vertical --with-license-file --no-license-path > NOTICE.txt

.venv/bin/hatch: .venv/bin/python
	.venv/bin/pip install -r requirements/$(ARCH).txt

.venv/bin/elastic-ingest: .venv/bin/python
	.venv/bin/pip install -r requirements/$(ARCH).txt
	.venv/bin/pip install --editable .

.venv/bin/ruff: .venv/bin/python
	.venv/bin/pip install -r requirements/$(ARCH).txt
	.venv/bin/pip install -r requirements/tests.txt
	

.venv/bin/pytest: .venv/bin/python
	.venv/bin/pip install -r requirements/$(ARCH).txt
	.venv/bin/pip install -r requirements/tests.txt
	.venv/bin/pip install -r requirements/ftest.txt

clean:
	rm -rf bin lib .venv include elasticsearch_connector.egg-info .coverage site-packages pyvenv.cfg include.site.python*.greenlet dist

lint: .venv/bin/python .venv/bin/ruff .venv/bin/elastic-ingest
	.venv/bin/ruff check connectors
	.venv/bin/ruff format connectors --check
	.venv/bin/ruff check tests
	.venv/bin/ruff format tests --check
	.venv/bin/ruff check scripts
	.venv/bin/ruff format scripts --check
	.venv/bin/ruff check hatch_build.py
	.venv/bin/ruff format hatch_build.py --check
	.venv/bin/pyright connectors
	.venv/bin/pyright tests

autoformat: .venv/bin/python .venv/bin/ruff .venv/bin/elastic-ingest
	.venv/bin/ruff check connectors --fix
	.venv/bin/ruff format connectors
	.venv/bin/ruff check tests --fix
	.venv/bin/ruff format tests
	.venv/bin/ruff check scripts --fix
	.venv/bin/ruff format scripts
	.venv/bin/ruff check hatch_build.py --fix
	.venv/bin/ruff format hatch_build.py

test: .venv/bin/pytest .venv/bin/elastic-ingest
	.venv/bin/pytest --cov-report term-missing --cov-fail-under 92 --cov-report html --cov=connectors --fail-slow=$(SLOW_TEST_THRESHOLD) -sv tests

ftest: .venv/bin/pytest .venv/bin/elastic-ingest $(DOCKERFILE_FTEST_PATH)
	tests/ftest.sh $(NAME) $(PERF8)

ftrace: .venv/bin/pytest .venv/bin/elastic-ingest $(DOCKERFILE_FTEST_PATH)
	PERF8_TRACE=true tests/ftest.sh $(NAME) $(PERF8)

run: install
	.venv/bin/elastic-ingest

default-config: install
	.venv/bin/elastic-ingest --action config --service-type $(SERVICE_TYPE)

docker-build: $(DOCKERFILE_PATH)
	docker build -f $(DOCKERFILE_PATH) -t $(DOCKER_IMAGE_NAME):$(VERSION)-SNAPSHOT .

docker-run:
	docker run -v $(PWD):/config $(DOCKER_IMAGE_NAME):$(VERSION)-SNAPSHOT /app/.venv/bin/elastic-ingest -c /config/config.yml --log-level=DEBUG

docker-push:
	docker push $(DOCKER_IMAGE_NAME):$(VERSION)-SNAPSHOT

sdist: .venv/bin/hatch
	.venv/bin/hatch build

deps-csv: .venv/bin/pip-licenses
	mkdir -p dist
	.venv/bin/pip-licenses --format=csv --with-urls > dist/dependencies.csv
