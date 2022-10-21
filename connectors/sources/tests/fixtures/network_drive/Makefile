.PHONY: run-stack stop-stack load-data uninstall

PYTHON=python3

export VERSION=8.5.0-SNAPSHOT

run-stack:
	docker-compose up -d

stop-stack:
	docker-compose down --volumes

bin/python:
	$(PYTHON) -m venv .

install: bin/python
	bin/pip install -U pip
	bin/pip install smbprotocol

load-data: install
	bin/python ./loadsample.py

remove-data: install
	bin/python ./remove.py

uninstall:
	rm -rf bin
	rm -rf include
	rm -rf lib
	rm -rf lib64
	rm -rf pyvenv.cfg
