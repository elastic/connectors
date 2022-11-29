.PHONY: run-stack stop-stack load-data uninstall

PYTHON=python3.10
CA_FILEPATH=$(shell echo $$($(PYTHON) -c "import ssl; print(ssl.get_default_verify_paths().cafile)"))
OPENSSL_CA_FILEPATH=$(shell echo $$($(PYTHON) -c "import ssl; print(ssl.get_default_verify_paths().openssl_cafile)"))

export VERSION=8.5.0-SNAPSHOT


run-stack:
	echo "127.0.0.1   storage.googleapis.com" >> /etc/hosts
ifeq ($(CA_FILEPATH), None)
	touch $(OPENSSL_CA_FILEPATH)
	chmod u+x $(OPENSSL_CA_FILEPATH)
	cat ./gcs_dummy_cert.pem >> $(OPENSSL_CA_FILEPATH)
else
	cat ./gcs_dummy_cert.pem >> $(CA_FILEPATH)
endif
	docker-compose up -d

bin/python:
	$(PYTHON) -m venv .

install: bin/python
	bin/pip install -U pip

stop-stack: install
	docker-compose down --volumes
	grep -v "storage.googleapis.com" /etc/hosts > tmpfile && mv tmpfile /etc/hosts
	rm -rf tmpfile
ifeq ($(CA_FILEPATH), None)
	echo "CA file for SSL verifcation is not found...."
else
	diff ./gcs_dummy_cert.pem $(CA_FILEPATH) | grep "^>" | cut -c3- > cert.pem
	mv cert.pem $(CA_FILEPATH)
endif
	mv ./stored_mocked_http_responses.json ./mocked_http_responses.json 

load-data:
	echo "Loaded total 15k number of testing documents on Mockserver for GCS...."

remove-data: install
	cp ./mocked_http_responses.json ./stored_mocked_http_responses.json
	bin/python ./remove.py
	docker-compose up -d --force-recreate mockserver

uninstall:
	rm -rf bin
	rm -rf include
	rm -rf lib
	rm -rf lib64
	rm -rf pyvenv.cfg
