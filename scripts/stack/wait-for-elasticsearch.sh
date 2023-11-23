#!/bin/bash

if [ $# -eq 0 ]; then
  ELASTICSEARCH_URL="http://localhost:9200"
else
  ELASTICSEARCH_URL="$1"
  shift
fi

echo "Connecting to Elasticsearch on $ELASTICSEARCH_URL"
until curl -u elastic:changeme --silent --output /dev/null --max-time 1 "$@" ${ELASTICSEARCH_URL}; do
  echo 'Waiting for Elasticsearch to be running...'
  sleep 2
done
