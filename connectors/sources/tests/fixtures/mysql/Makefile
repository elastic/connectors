.PHONY: run-stack stop-stack load-data

PYTHON ?= python3.10
PIP ?= bin/pip

export VERSION=8.6.0-SNAPSHOT

run-stack:
	docker compose up -d

stop-stack:
	docker compose down --volumes

bin/python:
	$(PYTHON) -m venv .

install:
	$(PIP) install mysql-connector-python

load-data: install
	$(PYTHON) ./loadsample.py

remove-data: install
	$(PYTHON) ./remove.py

uninstall:
	rm -rf bin
	rm -rf include
	rm -rf lib
	rm -rf lib64
	rm -rf pyvenv.cfg
