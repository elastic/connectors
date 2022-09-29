.PHONY: black test

PYTHON=python3

bin/python:
	$(PYTHON) -m venv .

install: bin/python
	bin/pip install -r requirements.txt
	bin/python setup.py develop

lint:
	bin/black connectors
	bin/black setup.py
	bin/flake8 connectors
	bin/flake8 setup.py
	bin/black scripts
	bin/flake8 scripts

test:
	bin/pytest --cov-report term-missing --cov-report html --cov=connectors -sv connectors/tests connectors/sources/tests


ftest:
	connectors/tests/ftest.sh $(NAME)
