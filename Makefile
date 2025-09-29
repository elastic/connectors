app_dir := app/connectors_service
connectors_sdk_dir := libs/connectors_sdk

install:
	cd $(connectors_sdk_dir); make install
	cd $(app_dir); make install

test: install
	cd $(connectors_sdk_dir); make test
	cd $(app_dir); make test

ftest: install
	cd $(app_dir); make ftest

ftrace:
	cd $(app_dir); make ftrace

notice:
	cd $(connectors_sdk_dir); make notice
	cd $(app_dir); make notice

lint:
	cd $(connectors_sdk_dir); make lint
	cd $(app_dir); make lint

autoformat:
	cd $(connectors_sdk_dir); make autoformat
	cd $(app_dir); make autoformat

clean:
	cd $(connectors_sdk_dir); make clean
	cd $(app_dir); make clean

docker-build:
	cd $(app_dir); make docker-build

docker-push:
	cd $(app_dir); make docker-push

run:
	cd $(app_dir); make run
