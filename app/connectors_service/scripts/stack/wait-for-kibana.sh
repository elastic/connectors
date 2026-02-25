#!/bin/bash

if [ $# -eq 0 ]; then
  KIBANA_URL="http://localhost:5601"
else
  KIBANA_URL="$1"
  shift
fi

echo "Connecting to Kibana on $KIBANA_URL"
until curl -XGET --silent --output /dev/null --max-time 1 "$@" ${KIBANA_URL}/status -I; do
  echo 'Waiting for Kibana to be running...'
  sleep 2
done
