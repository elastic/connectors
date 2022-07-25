.PHONY: black test

bin/python:
	python3 -m venv .

install: bin/python
	bin/pip install -r requirements.txt
	bin/python setup.py develop

lint:
	bin/black connectors
	bin/black setup.py
	bin/flake8 connectors

test:
	bin/pytest --cov-report term-missing --cov-report html --cov=connectors -sv connectors/tests connectors/sources/tests
