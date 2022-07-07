.PHONY: black test

bin/python:
	python3 -m venv .

install: bin/python
	bin/pip install -r requirements.txt

lint:
	bin/black connectors
	bin/black setup.py
	bin/flake8 connectors

test:
	bin/pytest --cov=connectors -sv connectors/tests
