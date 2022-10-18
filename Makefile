.PHONY: black test lint run ftest install dev

PYTHON=python3

bin/python:
	$(PYTHON) -m venv .

install: bin/python
	bin/pip install -r requirements.txt

dev: install
	bin/python setup.py develop

lint:
	bin/black connectors
	bin/black setup.py
	bin/flake8 connectors --exclude fixtures
	bin/flake8 setup.py
	bin/black scripts
	bin/flake8 scripts

bin/pytest: dev
	bin/pip install -r test-requirements.txt

test:	bin/pytest
	bin/pytest --cov-report term-missing --cov-report html --cov=connectors -sv connectors/tests connectors/sources/tests

release: install
	bin/python setup.py sdist

ftest:
	connectors/tests/ftest.sh $(NAME)

run: install
	bin/elastic-ingest --debug
