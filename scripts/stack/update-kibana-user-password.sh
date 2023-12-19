#!/bin/bash

if [ $# -eq 0 ]; then
  ELASTICSEARCH_URL="http://localhost:9200"
else
  ELASTICSEARCH_URL="$1"
  shift
fi

if [[ ${CURDIR:-} == "" ]]; then
    export CURDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
fi

if [[ "${ELASTIC_PASSWORD:-}" == "" ]]; then
    source $CURDIR/read-env.sh $CURDIR/.env
fi

echo "Updating Kibana password in Elasticsearch running on $ELASTICSEARCH_URL"
change_data="{ \"password\": \"${ELASTIC_PASSWORD}\" }"
curl -u elastic:$ELASTIC_PASSWORD "$@" -X POST "${ELASTICSEARCH_URL}/_security/user/kibana_system/_password?pretty" -H 'Content-Type: application/json' -d"${change_data}"
