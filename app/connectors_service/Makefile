.PHONY: install clean typecheck lint autoformat test ftest ftrace install run default-config docker-build docker-run docker-push fips-ftest

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

install-package: .venv/bin/python
	.venv/bin/python -m build ../../libs/connectors_sdk
	.venv/bin/pip install ../../libs/connectors_sdk/dist/*.whl
	.venv/bin/python -m build
	.venv/bin/pip install dist/*.whl

.venv/bin/elastic-ingest: .venv/bin/python
	.venv/bin/pip install -e ../../libs/connectors_sdk
	.venv/bin/pip install -e .

.venv/bin/ruff: .venv/bin/python
	.venv/bin/pip install ".[tests]"

.venv/bin/pytest: .venv/bin/python
	.venv/bin/pip install -e ".[tests,ftest]"

clean:
	rm -rf bin lib .venv include elasticsearch_connectors.egg-info .coverage site-packages pyvenv.cfg include.site.python*.greenlet dist build htmlcov requirements.txt

typecheck: .venv/bin/python .venv/bin/elastic-ingest
	.venv/bin/pyright connectors
	.venv/bin/pyright tests

lint: .venv/bin/python .venv/bin/ruff .venv/bin/elastic-ingest typecheck
	.venv/bin/ruff check connectors
	.venv/bin/ruff format connectors --check
	.venv/bin/ruff check tests
	.venv/bin/ruff format tests --check
	.venv/bin/ruff check scripts
	.venv/bin/ruff format scripts --check

autoformat: .venv/bin/python .venv/bin/ruff .venv/bin/elastic-ingest
	.venv/bin/ruff check connectors --fix
	.venv/bin/ruff format connectors
	.venv/bin/ruff check tests --fix
	.venv/bin/ruff format tests
	.venv/bin/ruff check scripts --fix
	.venv/bin/ruff format scripts

test: .venv/bin/python .venv/bin/elastic-ingest .venv/bin/pytest
	.venv/bin/pytest --cov-report term-missing --cov-fail-under 90 --cov-report html --cov=connectors --fail-slow=$(SLOW_TEST_THRESHOLD) -sv tests

freeze: .venv/bin/python
	# It looks a little insane, but hear me out.
	# Snyk CLI does not use local PIP, it runs its own copy of it and does not really respect some things that we have in pyproject.toml
	# For example relative file paths are not allowed, or evaluation of env variables are not possible while allowed.
	# So to make things work we need to convert our requirements to requirements.txt file so that snyk could run its scanning against it
	# Pipdeptree is able to actually produce requirements.txt file that contains both direct and transitive dependencies of the service
	# So when included to a docker artefact, for example, it should give accurate description of what's actually installed into the .venv
	# We pin <4.0.0 because 4.* requires rust toolkit and our docker images doesn't have it
	.venv/bin/pip install --quiet "pipdeptree<4.0.0"
	.venv/bin/pipdeptree --freeze --exclude elasticsearch-connectors,elasticsearch-connectors-sdk > requirements.txt
	# Delete it so that it's not left in the artefacts
	.venv/bin/pip uninstall --quiet pipdeptree -y

ftest: .venv/bin/pytest
	tests/ftest.sh $(NAME) $(PERF8)

ftrace: .venv/bin/pytest $(DOCKERFILE_FTEST_PATH)
	PERF8_TRACE=true tests/ftest.sh $(NAME) $(PERF8)

CONFIG_FILE ?= config.yml

run: install
	.venv/bin/elastic-ingest -c $(CONFIG_FILE)

default-config: install
	.venv/bin/elastic-ingest --action config --service-type $(SERVICE_TYPE)

sdist: .venv/bin/python
	.venv/bin/python -m build --sdist

deps-csv: .venv/bin/pip-licenses
	mkdir -p dist
	.venv/bin/pip-licenses --format=csv --with-urls > dist/dependencies.csv
	.venv/bin/python scripts/deps-csv.py dist/dependencies.csv

NON_FIPS_CONNECTORS := network_drive sharepoint_server

fips-ftest: .venv/bin/pytest
	@if echo "$(NON_FIPS_CONNECTORS)" | grep -qw "$(NAME)"; then \
		echo "SKIPPING: $(NAME) is not FIPS-compliant (uses NTLM authentication)"; \
	else \
		FIPS_MODE=true tests/ftest.sh $(NAME) $(PERF8); \
	fi
