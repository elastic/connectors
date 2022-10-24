CURDIR = $(shell pwd)

build:
	docker build -t docker.elastic.co/enterprise-search/connectors-python .

run:
	docker run --name connectors_python --rm -v $(CURDIR)/stackpack:/app docker.elastic.co/enterprise-search/connectors-python

shell: 
	docker exec -u root -it connectors_python /bin/bash
