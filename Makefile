.PHONY: test lint autoformat run ftest install release docker-build docker-run docker-push

PYTHON=python3.10
ARCH=$(shell uname -m)
PERF8?=no
SLOW_TEST_THRESHOLD=1 # seconds
VERSION=$(shell cat connectors/VERSION)
FAIL_ON_LICENSES=""

bin/python:
	$(PYTHON) -m venv .
	bin/pip install --upgrade pip

install: bin/python bin/elastic-ingest

bin/elastic-ingest: bin/python
	bin/pip install -r requirements/$(ARCH).txt
	bin/python setup.py develop

bin/black: bin/python
	bin/pip install -r requirements/$(ARCH).txt
	bin/pip install -r requirements/tests.txt

bin/pip-licenses: bin/python
	bin/pip install pip-licenses

bin/pytest: bin/python
	bin/pip install -r requirements/$(ARCH).txt
	bin/pip install -r requirements/tests.txt
	bin/pip install -r requirements/ftest.txt

clean:
	rm -rf bin lib include

check-licenses: bin/pip-licenses
	bin/pip-licenses --format=markdown --fail-on=$(FAIL_ON_LICENSES) > /tmp/deps-licenses.md
	diff deps-licenses.md /tmp/deps-licenses.md

update-licenses: bin/pip-licenses
	bin/pip-licenses --format=markdown --fail-on=$(FAIL_ON_LICENSES) > deps-licenses.md

lint: bin/python bin/black bin/elastic-ingest
	bin/black --check connectors
	bin/black --check tests
	bin/black --check setup.py
	bin/black --check scripts
	bin/ruff connectors
	bin/ruff tests
	bin/ruff setup.py
	bin/ruff scripts
	bin/pyright connectors
	bin/pyright tests

autoformat: bin/python bin/black bin/elastic-ingest
	bin/black connectors
	bin/black tests
	bin/black setup.py
	bin/black scripts
	bin/ruff connectors --fix
	bin/ruff tests --fix
	bin/ruff setup.py --fix
	bin/ruff scripts --fix

test:	bin/pytest bin/elastic-ingest
	bin/pytest --cov-report term-missing --cov-fail-under 92 --cov-report html --cov=connectors --fail-slow=$(SLOW_TEST_THRESHOLD) -sv tests

release: install
	bin/python setup.py sdist

ftest: bin/pytest bin/elastic-ingest
	tests/ftest.sh $(NAME) $(PERF8)

ftrace: bin/pytest bin/elastic-ingest
	PERF8_TRACE=true tests/ftest.sh $(NAME) $(PERF8)

run: install
	bin/elastic-ingest

default-config: install
	bin/elastic-ingest --action config --service-type $(SERVICE_TYPE)

docker-build:
	docker build -t docker.elastic.co/enterprise-search/elastic-connectors:$(VERSION)-SNAPSHOT .

docker-run:
	docker run -v $(PWD):/config docker.elastic.co/enterprise-search/elastic-connectors:$(VERSION)-SNAPSHOT /app/bin/elastic-ingest -c /config/config.yml --log-level=DEBUG

docker-push:
	docker push docker.elastic.co/enterprise-search/elastic-connectors:$(VERSION)-SNAPSHOT
