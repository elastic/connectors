app_dir := app/connectors_service
connectors_sdk_dir := libs/connectors_sdk

VERSION=$(shell cat app/connectors_service/connectors/VERSION)

DOCKER_IMAGE_NAME?=docker.elastic.co/integrations/elastic-connectors
DOCKERFILE_PATH?=Dockerfile
DOCKERFILE_FTEST_PATH?=app/connectors_service/tests/Dockerfile.ftest

FIPS_DOCKERFILE_PATH?=Dockerfile.fips
FIPS_DOCKERFILE_FTEST_PATH?=app/connectors_service/tests/Dockerfile.fips-ftest

PACKAGE_NAME_VERSION="elasticsearch_connectors-$(VERSION)"

install:
	cd $(connectors_sdk_dir); make install
	cd $(app_dir); make install

install-agent: install

install-package:
	cd $(connectors_sdk_dir); make install-package
	cd $(app_dir); make install-package

test: install
	cd $(connectors_sdk_dir); make test
	cd $(app_dir); make test

ftest: install build-connectors-base-image
	cd $(app_dir); make ftest NAME=$(NAME)

ftrace:
	cd $(app_dir); make ftrace

notice: install
	cd $(connectors_sdk_dir); make notice
	cd $(app_dir); make notice

typecheck: install
	cd $(connectors_sdk_dir); make typecheck
	cd $(app_dir); make typecheck

check-versions:
	@if ! diff -q $(app_dir)/connectors/VERSION $(connectors_sdk_dir)/connectors_sdk/VERSION > /dev/null 2>&1; then \
		echo "ERROR: VERSION files do not match!"; \
		echo "  $(app_dir)/connectors/VERSION: $$(cat $(app_dir)/connectors/VERSION)"; \
		echo "  $(connectors_sdk_dir)/connectors_sdk/VERSION: $$(cat $(connectors_sdk_dir)/connectors_sdk/VERSION)"; \
		exit 1; \
	fi
	@echo "VERSION files match: $$(cat $(app_dir)/connectors/VERSION)"

lint: install check-versions
	cd $(connectors_sdk_dir); make lint
	cd $(app_dir); make lint

autoformat: install
	cd $(connectors_sdk_dir); make autoformat
	cd $(app_dir); make autoformat

clean:
	cd $(connectors_sdk_dir); make clean
	cd $(app_dir); make clean
	rm -rf .coverage

config.yml:
	- cp -n $(app_dir)/config.yml.example config.yml

run: config.yml
	cd $(app_dir); make run CONFIG_FILE=$(CURDIR)/config.yml

build-connectors-base-image:
	docker build . -f ${DOCKERFILE_PATH} -t connectors-base

docker-build: $(DOCKERFILE_PATH) build-connectors-base-image
	docker build --no-cache -f $(DOCKERFILE_PATH) -t $(DOCKER_IMAGE_NAME):$(VERSION)-SNAPSHOT .

docker-run:
	docker run -v $(PWD):/config $(DOCKER_IMAGE_NAME):$(VERSION)-SNAPSHOT /app/.venv/bin/elastic-ingest -c /config/config.yml --log-level=DEBUG

docker-push:
	docker push $(DOCKER_IMAGE_NAME):$(VERSION)-SNAPSHOT

zip: clean
	zip -r \
		$(PACKAGE_NAME_VERSION).zip \
		./* \
		-x  *htmlcov*/* *docs*/* Dockerfile* *ruff_cache*/* *pytest_cache*/* *__pycache__*/* *build*/* *egg-info*/*

## FIPS Zone
# Build FIPS images
fips-build-base:
	docker build -f $(FIPS_DOCKERFILE_PATH) -t connectors-fips-base .

fips-build-test: fips-build-base
	docker build -f $(FIPS_DOCKERFILE_FTEST_PATH) -t connectors-fips-test .

fips-ftest:
	@if ! docker image inspect connectors-fips-test >/dev/null 2>&1; then \
		echo "connectors-fips-test image not found, building..."; \
		$(MAKE) fips-build-test; \
	fi
	cd $(app_dir); make fips-ftest NAME=$(NAME)

fips-test:
	@if ! docker image inspect connectors-fips-base >/dev/null 2>&1; then \
		echo "connectors-fips-base image not found, building..."; \
		$(MAKE) fips-build-base; \
	fi
	@echo "=== Running unit tests in FIPS mode ==="
	docker run --rm --user root \
		-v $(PWD):/workspace \
		-w /workspace \
		connectors-fips-base \
		/bin/sh -c '\
			apk add --no-cache git && \
			pip install --upgrade pip && \
			pip install -e libs/connectors_sdk[tests] && \
			cd app/connectors_service && pip install -e .[tests] && cd /workspace && \
			cd libs/connectors_sdk && python -m pytest tests -sv && \
			cd /workspace/app/connectors_service && python -m pytest --cov-report term-missing --cov-fail-under 90 --cov-report html --cov=connectors -sv tests'

fips-verify: fips-build-base
	@echo "=== Verifying FIPS mode in container ==="
	@docker run --rm connectors-fips-base /bin/sh -c '\
		echo "OpenSSL version:" && openssl version && \
		echo "" && echo "FIPS providers:" && openssl list -providers && \
		echo "" && echo "Python SSL version:" && python3 -c "import ssl; print(ssl.OPENSSL_VERSION)" && \
		echo "" && echo "FIPS cipher test:" && \
		python3 -c "import ssl; ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT); ctx.set_ciphers(\"RC4-SHA\")" 2>&1 | grep -q "No cipher" && \
		echo "FIPS ENABLED: RC4 correctly rejected" || echo "WARNING: RC4 not rejected"'
## End FIPS Zone

## Agent Docker Zone
# Only use it for local testing, that's it
AGENT_ES_HOSTS?='http://host.docker.internal:9200'
AGENT_ES_USERNAME?=elastic
AGENT_ES_PASSWORD?=changeme
AGENT_DOCKERFILE_NAME?=Dockerfile.agent
AGENT_DOCKER_IMAGE_NAME?=connectors-agent-component-local

agent-docker-build:
	docker build -t $(AGENT_DOCKER_IMAGE_NAME) -f $(AGENT_DOCKERFILE_NAME) .

agent-docker-run:
	docker run \
		--env ELASTICSEARCH_HOSTS=$(AGENT_ES_HOSTS) \
		--env ELASTICSEARCH_USERNAME=$(AGENT_ES_USERNAME) \
		--env ELASTICSEARCH_PASSWORD=$(AGENT_ES_PASSWORD) \
		--network host \
		$(AGENT_DOCKER_IMAGE_NAME)

agent-docker-all: agent-docker-build agent-docker-run
## End Agent Docker Zone
