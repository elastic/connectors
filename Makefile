app_dir := app/connectors_service
connectors_sdk_dir := libs/connectors_sdk

VERSION=$(shell cat app/connectors_service/connectors/VERSION)

DOCKER_IMAGE_NAME?=docker.elastic.co/integrations/elastic-connectors
DOCKERFILE_PATH?=Dockerfile
DOCKERFILE_FTEST_PATH?=app/connectors_service/tests/Dockerfile.ftest

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

ftest: install $(DOCKERFILE_FTEST_PATH) build-connectors-base-image
	cd $(app_dir); make ftest

ftrace:
	cd $(app_dir); make ftrace

notice: install
	cd $(connectors_sdk_dir); make notice
	cd $(app_dir); make notice

typecheck: install
	cd $(connectors_sdk_dir); make typecheck
	cd $(app_dir); make typecheck

lint: install
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
