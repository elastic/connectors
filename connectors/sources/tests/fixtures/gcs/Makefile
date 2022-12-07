.PHONY: run-stack stop-stack load-data uninstall

PYTHON=python3.10
CA_FILEPATH=$(shell echo $$($(PYTHON) -c "import ssl; print(ssl.get_default_verify_paths().cafile)"))
OPENSSL_CA_FILEPATH=$(shell echo $$($(PYTHON) -c "import ssl; print(ssl.get_default_verify_paths().openssl_cafile)"))

export VERSION=8.5.0-SNAPSHOT


run-stack: install
	bin/python ./prerequisites.py
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
	bin/pip install google-auth
	bin/pip install google-cloud-storage

stop-stack:
	docker-compose down --volumes
	grep -v "storage.googleapis.com" /etc/hosts > tmpfile && mv tmpfile /etc/hosts
	rm -rf tmpfile
ifeq ($(CA_FILEPATH), None)
	echo "CA file for SSL verifcation is not found...."
else
	diff ./gcs_dummy_cert.pem $(CA_FILEPATH) | grep "^>" | cut -c3- > cert.pem
	mv cert.pem $(CA_FILEPATH)
endif

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
