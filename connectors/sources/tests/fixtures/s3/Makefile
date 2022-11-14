
.PHONY: run-stack stop-stack

PYTHON=python3
PORT=5000
export VERSION=8.6.0-SNAPSHOT

bin/python:
	$(PYTHON) -m venv .

install: bin/python
	bin/pip install -U pip
	bin/pip install boto3

run-stack: install
	docker-compose up -d

load-data:
	bin/python load_storage.py $(PORT)

remove-data:
	bin/python remove.py $(PORT)

uninstall:
	rm -rf bin
	rm -rf include
	rm -rf lib
	rm -rf lib64
	rm -rf pyvenv.cfg

stop-stack: uninstall
	docker-compose down --volumes
