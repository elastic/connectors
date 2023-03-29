FROM python:3.10

ADD ../../../../../../connectors-python /app
WORKDIR /app

RUN make clean install

ENTRYPOINT ["bin/elastic-ingest", "--debug"]