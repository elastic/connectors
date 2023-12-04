#!/bin/bash

# Fallback to .env file # if not already set
if [ -z "$ELASTICSEARCH_VERSION" ]
then
  export $(grep -E 'ELASTICSEARCH_VERSION' $1 | xargs)
fi
echo "ELASTICSEARCH_VERSION=$ELASTICSEARCH_VERSION"

if [ -z "$KIBANA_VERSION" ]
then
  export $(grep -E 'KIBANA_VERSION' $1 | xargs)
fi
echo "KIBANA_VERSION=$KIBANA_VERSION"

if [ -z "$CONNECTORS_VERSION" ]
then
  export $(grep -E 'CONNECTORS_VERSION' $1 | xargs)
fi
echo "CONNECTORS_VERSION=$CONNECTORS_VERSION"

if [ -z "$ELASTIC_PASSWORD"]
then
  export ELASTIC_PASSWORD="changeme"
fi
