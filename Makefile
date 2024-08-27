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

.venv/bin/black: .venv/bin/python
	.venv/bin/pip install -r requirements/$(ARCH).txt
	.venv/bin/pip install -r requirements/tests.txt
	

.venv/bin/pytest: .venv/bin/python
	.venv/bin/pip install -r requirements/$(ARCH).txt
	.venv/bin/pip install -r requirements/tests.txt
	.venv/bin/pip install -r requirements/ftest.txt

clean:
	rm -rf bin lib .venv include elasticsearch_connector.egg-info .coverage site-packages pyvenv.cfg include.site.python*.greenlet dist

lint: .venv/bin/python .venv/bin/black .venv/bin/elastic-ingest
	.venv/bin/black --check connectors
	.venv/bin/black --check tests
	.venv/bin/black --check scripts
	.venv/bin/black --check hatch_build.py
	.venv/bin/ruff connectors
	.venv/bin/ruff tests
	.venv/bin/ruff scripts
	.venv/bin/ruff hatch_build.py
	.venv/bin/pyright connectors
	.venv/bin/pyright tests

autoformat: .venv/bin/python .venv/bin/black .venv/bin/elastic-ingest
	.venv/bin/black connectors
	.venv/bin/black tests
	.venv/bin/black scripts
	.venv/bin/black hatch_build.py
	.venv/bin/ruff connectors --fix
	.venv/bin/ruff tests --fix
	.venv/bin/ruff scripts --fix
	.venv/bin/ruff hatch_build.py --fix

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
