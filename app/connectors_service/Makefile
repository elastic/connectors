.PHONY: install clean lint autoformat test ftest ftrace install run default-config docker-build docker-run docker-push

# Default to python3, but if its version is >3.11, try using python3.11 or python3.10.
PYTHON ?= python3
PYTHON_VERSION := $(shell $(PYTHON) -c "import sys; print(f'{sys.version_info[0]}.{sys.version_info[1]}')" 2>/dev/null)
CHECK_UNSUPPORTED_PYTHON_VERSION := $(shell $(PYTHON) -c "import sys; print(1 if sys.version_info[:2] > (3,11) else 0)" 2>/dev/null)
ifeq ($(CHECK_UNSUPPORTED_PYTHON_VERSION),1)
  ifneq ($(shell command -v python3.11 2>/dev/null),)
    PYTHON = python3.11
  else ifneq ($(shell command -v python3.10 2>/dev/null),)
    PYTHON = python3.10
  else
    $(error "Unsupported python version: $(PYTHON) (version $(PYTHON_VERSION)) is greater than 3.11 and neither python3.11 nor python3.10 are available.")
  endif
endif

$(info Using $(PYTHON) (version $(shell $(PYTHON) -c "import sys; print(f'{sys.version_info[0]}.{sys.version_info[1]}')")))

ARCH=$(shell uname -m)
PERF8?=no
SLOW_TEST_THRESHOLD=1 # seconds
VERSION=$(shell cat connectors/VERSION)
PACKAGE_NAME_VERSION="elasticsearch_connectors-$(VERSION)"

config.yml:
	- cp -n config.yml.example config.yml

.venv/bin/python: | config.yml
	$(PYTHON) -m venv .venv
	.venv/bin/pip install --upgrade pip
	.venv/bin/python -m pip install build

.venv/bin/pip-licenses: .venv/bin/python
	.venv/bin/pip install pip-licenses

notice: .venv/bin/python .venv/bin/pip-licenses .venv/bin/elastic-ingest
	.venv/bin/pip-licenses --format=plain-vertical --with-license-file --no-license-path > NOTICE.txt

install: .venv/bin/python .venv/bin/elastic-ingest notice

install-agent: .venv/bin/elastic-ingest

.venv/bin/elastic-ingest: .venv/bin/python
	.venv/bin/pip install -e .

.venv/bin/ruff: .venv/bin/python
	.venv/bin/pip install ".[tests]"

.venv/bin/pytest: .venv/bin/python
	.venv/bin/pip install -e ".[tests,ftest]"

clean:
	rm -rf bin lib .venv include elasticsearch_connectors.egg-info .coverage site-packages pyvenv.cfg include.site.python*.greenlet dist

lint: .venv/bin/python .venv/bin/ruff .venv/bin/elastic-ingest
	.venv/bin/ruff check connectors
	.venv/bin/ruff format connectors --check
	.venv/bin/ruff check tests
	.venv/bin/ruff format tests --check
	.venv/bin/ruff check scripts
	.venv/bin/ruff format scripts --check
	.venv/bin/pyright connectors
	.venv/bin/pyright tests

autoformat: .venv/bin/python .venv/bin/ruff .venv/bin/elastic-ingest
	.venv/bin/ruff check connectors --fix
	.venv/bin/ruff format connectors
	.venv/bin/ruff check tests --fix
	.venv/bin/ruff format tests
	.venv/bin/ruff check scripts --fix
	.venv/bin/ruff format scripts

default-config: install
	.venv/bin/elastic-ingest --action config --service-type $(SERVICE_TYPE)

run: install
	.venv/bin/elastic-ingest
