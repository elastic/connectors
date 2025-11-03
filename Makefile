app_dir := app/connectors_service
connectors_sdk_dir := libs/connectors_sdk

VERSION=$(shell cat app/connectors_service/connectors/VERSION)

DOCKER_IMAGE_NAME?=docker.elastic.co/integrations/elastic-connectors
DOCKERFILE_PATH?=Dockerfile
DOCKERFILE_FTEST_PATH?=app/connectors_service/tests/Dockerfile.ftest

install:
	cd $(connectors_sdk_dir); make install
	cd $(app_dir); make install

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

run:
	cd $(app_dir); make run

build-connectors-base-image:
	docker build . -f ${DOCKERFILE_PATH} -t connectors-base

docker-build: $(DOCKERFILE_PATH) build-connectors-base-image
	docker build --no-cache -f $(DOCKERFILE_PATH) -t $(DOCKER_IMAGE_NAME):$(VERSION)-SNAPSHOT .

docker-run:
	docker run -v $(PWD):/config $(DOCKER_IMAGE_NAME):$(VERSION)-SNAPSHOT /app/.venv/bin/elastic-ingest -c /config/config.yml --log-level=DEBUG

docker-push:
	docker push $(DOCKER_IMAGE_NAME):$(VERSION)-SNAPSHOT
