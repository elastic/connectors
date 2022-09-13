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
	bin/fake-kibana --index-name search-$(NAME) --service-type $(NAME) --debug
	bin/elastic-ingest --one-sync --sync-now
	bin/elastic-ingest --one-sync --sync-now
	bin/python scripts/verify.py --index-name search-$(NAME) --service-type $(NAME) --size 3000
