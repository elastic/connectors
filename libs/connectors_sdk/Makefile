PYTHON ?= python3
VENV_DIR ?= .venv/bin
SLOW_TEST_THRESHOLD = 1

.venv/bin/python:
	$(PYTHON) -m venv .venv
	$(VENV_DIR)/pip install --upgrade pip
	$(VENV_DIR)/python -m pip install build
	$(VENV_DIR)/pip install -e .

install: .venv/bin/python

install-package:
	$(PYTHON) -m venv .venv
	$(VENV_DIR)/pip install --upgrade pip
	$(VENV_DIR)/python -m pip install build
	$(VENV_DIR)/python -m build
	$(VENV_DIR)/pip install dist/*.whl

clean:
	rm -rf bin lib .venv include elasticsearch_connectors_sdk.egg-info .coverage site-packages pyvenv.cfg include.site.python*.greenlet dist build htmlcov requirements.txt


.venv/bin/pytest: .venv/bin/python
	.venv/bin/pip install ".[tests]"

.venv/bin/ruff: .venv/bin/python
	.venv/bin/pip install ".[tests]"

.venv/bin/pip-licenses: .venv/bin/python
	.venv/bin/pip install pip-licenses

typecheck: install
	.venv/bin/pyright connectors_sdk
	.venv/bin/pyright tests

lint: install .venv/bin/ruff typecheck
	.venv/bin/ruff check connectors_sdk
	.venv/bin/ruff format connectors_sdk --check
	.venv/bin/ruff check tests
	.venv/bin/ruff format tests --check

test: .venv/bin/pytest
	$(VENV_DIR)/pytest --cov-report term-missing --cov-fail-under 80 --cov-report html --cov=connectors_sdk --fail-slow=$(SLOW_TEST_THRESHOLD) -sv tests

freeze: .venv/bin/python
	# It looks a little insane, but hear me out.
	# Snyk CLI does not use local PIP, it runs its own copy of it and does not really respect some things that we have in pyproject.toml
	# For example relative file paths are not allowed, or evaluation of env variables are not possible while allowed.
	# So to make things work we need to convert our requirements to requirements.txt file so that snyk could run its scanning against it
	# Pipdeptree is able to actually produce requirements.txt file that contains both direct and transitive dependencies of the service
	# So when included to a docker artefact, for example, it should give accurate description of what's actually installed into the .venv
	# We pin <4.0.0 because 4.* requires rust toolkit and our docker images doesn't have it
	.venv/bin/pip install --quiet "pipdeptree<4.0.0"
	.venv/bin/pipdeptree \
		--freeze \
		--exclude elasticsearch-connectors-sdk,pipdeptree \
		> requirements.txt
	# Delete it so that it's not left in the artefacts
	.venv/bin/pip uninstall --quiet pipdeptree -y

autoformat: install .venv/bin/ruff
	.venv/bin/ruff check connectors_sdk --fix
	.venv/bin/ruff format connectors_sdk
	.venv/bin/ruff check tests --fix
	.venv/bin/ruff format tests

notice: .venv/bin/python .venv/bin/pip-licenses
	.venv/bin/pip-licenses --format=plain-vertical --with-license-file --no-license-path > NOTICE.txt
