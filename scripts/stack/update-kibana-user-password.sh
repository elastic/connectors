#!/bin/bash

if [ $# -eq 0 ]; then
  ELASTICSEARCH_URL="http://localhost:9200"
else
  ELASTICSEARCH_URL="$1"
  shift
fi

echo "Updating Kibana password in Elasticsearch running on $ELASTICSEARCH_URL"
curl -u elastic:changeme "$@" -X POST "${ELASTICSEARCH_URL}/_security/user/kibana_system/_password?pretty" -H 'Content-Type: application/json' -d' { "password" : "changeme" } '
