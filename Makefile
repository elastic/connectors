.PHONY: black test

bin/python:
	python3 -m venv .

install: bin/python
	bin/pip install -r requirements.txt

black:
	bin/black connectors
	bin/black setup.py

test:
	bin/pytest --cov=connectors -sv connectors/tests
