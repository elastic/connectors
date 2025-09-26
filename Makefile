app_dir := app/

install:
	cd $(app_dir); make install

test:
	cd $(app_dir); make test

ftest:
	cd $(app_dir); make ftest

ftrace:
	cd $(app_dir); make ftrace

notice:
	cd $(app_dir); make notice

lint:
	cd $(app_dir); make lint

autoformat:
	cd $(app_dir); make autoformat

clean:
	cd $(app_dir); make clean

docker-build:
	cd $(app_dir); make docker-build

docker-push:
	cd $(app_dir); make docker-push
