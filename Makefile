.PHONY: test lint autoformat run ftest install dev release docker-build docker-run docker-push

PYTHON=python3.10
ARCH=$(shell uname -m)
PERF8?=no
SLOW_TEST_THRESHOLD=1 # seconds
VERSION=$(shell cat connectors/VERSION)


bin/python:
	$(PYTHON) -m venv .
	bin/pip install --upgrade pip

install: bin/python bin/elastic-ingest

dev: install
	bin/pip install -r requirements/tests.txt

bin/elastic-ingest: bin/python
	bin/pip install -r requirements/$(ARCH).txt
	bin/python setup.py develop

bin/black: bin/python
	bin/pip install -r requirements/$(ARCH).txt
	bin/pip install -r requirements/tests.txt
	

bin/pytest: bin/python
	bin/pip install -r requirements/$(ARCH).txt
	bin/pip install -r requirements/tests.txt

clean:
	rm -rf bin lib include

lint: bin/python bin/black bin/elastic-ingest
	bin/isort --check . --sp .isort.cfg
	bin/black --check connectors
	bin/black --check setup.py
	bin/flake8 connectors
	bin/flake8 setup.py
	bin/black --check scripts
	bin/flake8 scripts

autoformat: bin/python bin/black bin/elastic-ingest
	bin/isort . --sp .isort.cfg
	bin/black connectors
	bin/black setup.py
	bin/black scripts

test:	bin/pytest bin/elastic-ingest
	bin/pytest --cov-report term-missing --cov-fail-under 92 --cov-report html --cov=connectors --fail-slow=$(SLOW_TEST_THRESHOLD) -sv connectors/tests connectors/sources/tests

release: install
	bin/python setup.py sdist

ftest: bin/pytest bin/elastic-ingest
	connectors/tests/ftest.sh $(NAME) $(PERF8)

run: install
	bin/elastic-ingest

docker-build:
	docker build -t docker.elastic.co/enterprise-search/elastic-connectors:$(VERSION)-SNAPSHOT .

docker-run:
	docker run -v $(PWD):/config docker.elastic.co/enterprise-search/elastic-connectors:$(VERSION)-SNAPSHOT /app/bin/elastic-ingest -c /config/config.yml --log-level=DEBUG

docker-push:
	docker push docker.elastic.co/enterprise-search/elastic-connectors:$(VERSION)-SNAPSHOT
