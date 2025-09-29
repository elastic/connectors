app_dir := app/connectors_service
connectors_sdk_dir := libs/connectors_sdk


.venv/bin/python:
	$(PYTHON) -m venv .venv
	.venv/bin/pip install --upgrade pip
	.venv/bin/python -m pip install build

install:
	.venv/bin/pip install -e \
		libs/connectors_sdk \
		app/connectors_service
test:
	cd $(app_dir); make test
	cd $(connectors_sdk_dir); make test

ftest:
	cd $(app_dir); make ftest

ftrace:
	cd $(app_dir); make ftrace
	cd $(connectors_sdk_dir); make ftrace

notice:
	cd $(app_dir); make notice
	cd $(connectors_sdk_dir); make notice

lint:
	cd $(app_dir); make lint
	cd $(connectors_sdk_dir); make lint

autoformat:
	cd $(app_dir); make autoformat
	cd $(connectors_sdk_dir); make autoformat

clean:
	cd $(app_dir); make clean
	cd $(connectors_sdk_dir); make clean

docker-build:
	cd $(app_dir); make docker-build

docker-push:
	cd $(app_dir); make docker-push

run:
	cd $(app_dir); make run
