PYTHON ?= python3
VENV_DIR ?= .venv/bin
SLOW_TEST_THRESHOLD = 1

.venv/bin/python:
	$(PYTHON) -m venv .venv
	$(VENV_DIR)/pip install --upgrade pip
	$(VENV_DIR)/python -m pip install build
	$(VENV_DIR)/pip install -e .

install: .venv/bin/python

clean:
	rm -rf bin lib .venv include elasticsearch_connectors_sources.egg-info .coverage site-packages pyvenv.cfg include.site.python*.greenlet dist


.venv/bin/pytest: .venv/bin/python
	.venv/bin/pip install ".[tests]"

.venv/bin/ruff: .venv/bin/python
	.venv/bin/pip install ".[tests]"

.venv/bin/pip-licenses: .venv/bin/python
	.venv/bin/pip install pip-licenses

lint: install .venv/bin/ruff
	.venv/bin/ruff check connectors_sources
	.venv/bin/ruff format connectors_sources --check
	.venv/bin/ruff check tests
	.venv/bin/ruff format tests --check
	.venv/bin/pyright connectors_sources
	.venv/bin/pyright tests

test: .venv/bin/pytest
	$(VENV_DIR)/pytest --cov-report term-missing --cov-fail-under 80 --cov-report html --cov=connectors_sources --fail-slow=$(SLOW_TEST_THRESHOLD) -sv tests

autoformat: install .venv/bin/ruff
	.venv/bin/ruff check connectors_sources --fix
	.venv/bin/ruff format connectors_sources
	.venv/bin/ruff check tests --fix
	.venv/bin/ruff format tests

notice: .venv/bin/python .venv/bin/pip-licenses
	.venv/bin/pip-licenses --format=plain-vertical --with-license-file --no-license-path > NOTICE.txt
