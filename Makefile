.PHONY: test lint run ftest install dev

PYTHON=python3.10

bin/python:
	$(PYTHON) -m venv .
	bin/pip install --upgrade pip

install: bin/python
	bin/pip install -r requirements.txt

dev: install
	bin/pip install -r test-requirements.txt

bin/elastic-ingest: bin/python
	bin/pip install -r requirements.txt
	bin/python setup.py develop

bin/black: bin/python
	bin/pip install -r requirements.txt
	bin/pip install -r test-requirements.txt
	

bin/pytest: bin/python
	bin/pip install -r requirements.txt
	bin/pip install -r test-requirements.txt

lint: bin/python bin/black bin/elastic-ingest
	bin/black connectors
	bin/black setup.py
	bin/flake8 connectors --exclude fixtures
	bin/flake8 setup.py
	bin/black scripts
	bin/flake8 scripts


test:	bin/pytest bin/elastic-ingest
	bin/pytest --cov-report term-missing --cov-report html --cov=connectors -sv connectors/tests connectors/sources/tests

release:
	install
	bin/python setup.py sdist

ftest: bin/pytest bin/elastic-ingest
	connectors/tests/ftest.sh $(NAME)

run: 
	install
	bin/elastic-ingest --debug
