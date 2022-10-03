
.PHONY: run-stack stop-stack load-data

export VERSION=8.4.0-SNAPSHOT

run-stack:
	docker-compose up -d

stop-stack:
	docker-compose down --volumes

load-data:
	./loadsample.sh

remove-data:
	echo "todo"
